//! The message runtime — a thin composer over the three schema-emitted planes.
//!
//! Message owns no durable state: it is a stamp-and-forward ingress. The
//! Signal plane (`schema/signal.schema`) is its wire surface; the Nexus plane
//! (`schema/nexus.schema`) is its internal-feature catalog — the
//! forward-to-router decision and the `ForwardToRouter` effect; the SEMA plane
//! (`schema/sema.schema`) is honestly empty (`Stateless`).
//!
//! `MessageEngine::handle` runs the record-970 flow for the forward-only case:
//! a decoded Signal `Input` becomes `NexusWork::SignalArrived`, the Nexus
//! `decide` either replies directly (already-stamped -> Unimplemented) or emits
//! the `ForwardToRouter` effect. The generated `NexusEngine::execute` runner
//! owns the recursion: it runs the effect, feeds the `EffectCompleted` work
//! back into `decide`, and stops on the Signal `Output`.

use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    error::Error,
    router::{RouterForwardOutcome, RouterForwarder},
    schema::{
        nexus::{
            self as nexus_schema, ForwardRequest, NexusAction, NexusEffectCommand,
            NexusEffectResult, NexusEngine, NexusWork, OriginRoute,
        },
        sema::{
            self as sema_schema, ReadInput as SemaReadInput, ReadOutput as SemaReadOutput,
            Stateless, WriteInput as SemaWriteInput, WriteOutput as SemaWriteOutput,
        },
        signal::{
            Error as SignalError, ErrorMessage, ErrorReport, Input, OperationKind, Output,
            RequestUnimplemented, Unimplemented, UnimplementedReason,
        },
    },
};

/// The daemon runtime: the router forwarder the Nexus effect drives.
///
/// `MessageEngine` owns the component state: the `RouterForwarder` (the
/// outbound `signal-message` client + the owner identity it stamps) and no
/// durable store — message has none.
#[derive(Debug)]
pub struct MessageEngine {
    forwarder: RouterForwarder,
}

/// One request's Nexus runner surface.
///
/// The generated runner needs a `NexusEngine` value whose effect hook can see
/// the accepted connection's peer credentials. Those credentials are request
/// context, not global engine state, so this wrapper owns the request-local
/// borrow while delegating durable behavior to `MessageEngine`.
#[derive(Debug)]
struct RequestEngine<'request> {
    engine: &'request MessageEngine,
}

impl MessageEngine {
    pub fn new(forwarder: RouterForwarder) -> Self {
        Self { forwarder }
    }

    pub fn from_configuration(configuration: &Configuration) -> Self {
        Self::new(RouterForwarder::from_configuration(configuration))
    }

    /// Run one decoded Signal `Input` end to end and return the Signal `Output`.
    ///
    /// This is the forward-only realization of the Signal -> Nexus -> effect ->
    /// Nexus -> Signal pipeline. The generated `NexusEngine::execute` method
    /// owns the typed decision entry; message sequences its one router effect
    /// explicitly and feeds the typed result back through that generated entry.
    ///
    /// `connection` carries the accepted stream's peer credentials; the router
    /// forward stamps the provenance origin minted from them.
    pub async fn handle(
        &self,
        input: Input,
        connection: &ConnectionContext,
    ) -> Result<Output, Error> {
        let mut request_engine = RequestEngine::new(self);
        let signal_action = request_engine
            .execute(
                NexusWork::signal_arrived(input).with_origin_route(Self::forward_origin_route()),
            )
            .await
            .into_root();
        let action = match signal_action {
            NexusAction::CommandEffect(command) => {
                let effect_result = self.run_forward_effect(command.into_payload(), connection);
                request_engine
                    .execute(
                        NexusWork::effect_completed(effect_result)
                            .with_origin_route(Self::forward_origin_route()),
                    )
                    .await
                    .into_root()
            }
            other => other,
        };
        match action {
            NexusAction::ReplyToSignal(output) => Ok(output.into_payload()),
            other => Ok(Self::error_output(format!(
                "nexus runner returned non-reply action: {other:?}"
            ))),
        }
    }

    /// Run the one effect message declares: forward to the router. The effect
    /// translates the schema submission into the `signal-message` wire, calls
    /// the router, and lifts the outcome back into a `NexusEffectResult`. The
    /// connection's peer credentials mint the stamped origin.
    fn run_forward_effect(
        &self,
        effect: NexusEffectCommand,
        connection: &ConnectionContext,
    ) -> NexusEffectResult {
        let NexusEffectCommand::ForwardToRouter(request) = effect;
        let request = request.into_payload();
        match self.forwarder.forward(request, connection) {
            Ok(RouterForwardOutcome::Replied(reply)) => NexusEffectResult::forwarded(reply),
            Ok(RouterForwardOutcome::Unreachable) => {
                NexusEffectResult::router_unavailable(UnimplementedReason::RouterUnreachable)
            }
            Err(error) => NexusEffectResult::forward_failed(ErrorReport::new(ErrorMessage::new(
                format!("router forward failed: {error}"),
            ))),
        }
    }

    /// The forward decision for an arrived Signal `Input`.
    ///
    /// - `Submit(MessageSubmission)` → stamp-and-forward effect.
    /// - `QueryInbox(InboxQuery)` → forward-inbox-query effect.
    /// - `SubmitStamped(_)` → typed `Unimplemented`: re-stamping an
    ///   already-stamped submission is out of the prototype's scope (the
    ///   daemon mints provenance; it does not accept it from a peer).
    fn decide_signal(&self, input: Input) -> NexusAction {
        match input {
            Input::Submit(submission) => {
                NexusAction::command_effect(NexusEffectCommand::forward_to_router(
                    ForwardRequest::stamp_and_forward(submission.into_payload()),
                ))
            }
            Input::QueryInbox(query) => {
                NexusAction::command_effect(NexusEffectCommand::forward_to_router(
                    ForwardRequest::forward_inbox_query(query.into_payload()),
                ))
            }
            Input::SubmitStamped(_) => NexusAction::reply_to_signal(Output::Unimplemented(
                Unimplemented::new(RequestUnimplemented {
                    unimplemented_operation_kind: OperationKind::SubmitStamped.into(),
                    reason: UnimplementedReason::NotInPrototypeScope.into(),
                }),
            )),
        }
    }

    /// Turn a completed router forward into the Signal `Output` to reply with.
    fn decide_effect_completed(&self, result: NexusEffectResult) -> NexusAction {
        match result {
            NexusEffectResult::Forwarded(reply) => {
                NexusAction::reply_to_signal(reply.into_payload())
            }
            NexusEffectResult::RouterUnavailable(reason) => NexusAction::reply_to_signal(
                Self::error_output(Self::router_unavailable_text(reason.into_payload())),
            ),
            NexusEffectResult::ForwardFailed(report) => {
                NexusAction::reply_to_signal(Output::Error(SignalError::new(report.into_payload())))
            }
        }
    }

    fn router_unavailable_text(reason: UnimplementedReason) -> String {
        match reason {
            UnimplementedReason::RouterUnreachable => {
                "router socket unreachable; message not forwarded".to_owned()
            }
            UnimplementedReason::NotInPrototypeScope => {
                "operation not in prototype scope".to_owned()
            }
        }
    }

    /// The single origin route message stamps onto every in-flight mail.
    /// Message serves one request per connection on its own call stack, so
    /// there is no concurrent in-flight mail to disambiguate.
    fn forward_origin_route() -> OriginRoute {
        OriginRoute::new(1)
    }

    fn error_output(message: impl Into<String>) -> Output {
        Output::Error(SignalError::new(ErrorReport::new(ErrorMessage::new(
            message,
        ))))
    }
}

impl<'request> RequestEngine<'request> {
    fn new(engine: &'request MessageEngine) -> Self {
        Self { engine }
    }
}

impl NexusEngine for RequestEngine<'_> {
    fn decide(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            NexusWork::SignalArrived(signal_input) => {
                self.engine.decide_signal(signal_input.into_payload())
            }
            NexusWork::EffectCompleted(result) => {
                self.engine.decide_effect_completed(result.into_payload())
            }
        };
        action.with_origin_route(origin_route)
    }
}

/// Message owns no durable state, so its SEMA engine is the honest no-op: every
/// write and read returns `Stateless`. The plane exists to satisfy the uniform
/// three-plane shape; it never touches a database because there is none.
impl crate::schema::sema::SemaEngine for MessageEngine {
    fn apply_inner(
        &mut self,
        input: sema_schema::sema::Sema<SemaWriteInput>,
    ) -> sema_schema::sema::Sema<SemaWriteOutput> {
        let origin_route = input.origin_route();
        sema_schema::sema::Sema::new(origin_route, SemaWriteOutput::Stateless(Stateless {}))
    }

    fn observe_inner(
        &self,
        input: sema_schema::sema::Sema<SemaReadInput>,
    ) -> sema_schema::sema::Sema<SemaReadOutput> {
        let origin_route = input.origin_route();
        sema_schema::sema::Sema::new(origin_route, SemaReadOutput::Stateless(Stateless {}))
    }
}
