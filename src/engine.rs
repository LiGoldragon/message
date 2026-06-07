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

use triad_runtime::{ConnectionContext, ContinuationExhausted};

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
            ErrorReport, Input, OperationKind, Output, RequestUnimplemented, UnimplementedReason,
        },
    },
};

/// The single origin route message stamps onto every in-flight mail. Message
/// serves one request per connection on its own call stack, so there is no
/// concurrent in-flight mail to disambiguate; the route is a constant.
const FORWARD_ORIGIN_ROUTE: OriginRoute = OriginRoute(1);

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
struct MessageRequestEngine<'request> {
    engine: &'request MessageEngine,
    connection: &'request ConnectionContext,
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
    /// owns the recursive runner loop and continuation budget; message supplies
    /// only decision and effect hooks.
    ///
    /// `connection` carries the accepted stream's peer credentials; the router
    /// forward stamps the provenance origin minted from them.
    pub fn handle(&self, input: Input, connection: &ConnectionContext) -> Result<Output, Error> {
        let mut request_engine = MessageRequestEngine::new(self, connection);
        let action = request_engine
            .execute(NexusWork::signal_arrived(input).with_origin_route(FORWARD_ORIGIN_ROUTE))
            .into_root();
        match action {
            NexusAction::ReplyToSignal(output) => Ok(output),
            other => Ok(Output::Error(ErrorReport(format!(
                "nexus runner returned non-reply action: {other:?}"
            )))),
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
        match self.forwarder.forward(request, connection) {
            Ok(RouterForwardOutcome::Replied(reply)) => NexusEffectResult::Forwarded(reply),
            Ok(RouterForwardOutcome::Unreachable) => {
                NexusEffectResult::RouterUnavailable(UnimplementedReason::RouterUnreachable)
            }
            Err(error) => NexusEffectResult::ForwardFailed(ErrorReport(format!(
                "router forward failed: {error}"
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
            Input::Submit(submission) => NexusAction::CommandEffect(
                NexusEffectCommand::ForwardToRouter(ForwardRequest::StampAndForward(submission)),
            ),
            Input::QueryInbox(query) => NexusAction::CommandEffect(
                NexusEffectCommand::ForwardToRouter(ForwardRequest::ForwardInboxQuery(query)),
            ),
            Input::SubmitStamped(_) => {
                NexusAction::ReplyToSignal(Output::Unimplemented(RequestUnimplemented {
                    operation_kind: OperationKind::SubmitStamped,
                    unimplemented_reason: UnimplementedReason::NotInPrototypeScope,
                }))
            }
        }
    }

    /// Turn a completed router forward into the Signal `Output` to reply with.
    fn decide_effect_completed(&self, result: NexusEffectResult) -> NexusAction {
        match result {
            NexusEffectResult::Forwarded(reply) => NexusAction::ReplyToSignal(reply),
            NexusEffectResult::RouterUnavailable(reason) => NexusAction::ReplyToSignal(
                Output::Error(ErrorReport(Self::router_unavailable_text(reason))),
            ),
            NexusEffectResult::ForwardFailed(report) => {
                NexusAction::ReplyToSignal(Output::Error(report))
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
}

impl<'request> MessageRequestEngine<'request> {
    fn new(engine: &'request MessageEngine, connection: &'request ConnectionContext) -> Self {
        Self { engine, connection }
    }
}

impl NexusEngine for MessageRequestEngine<'_> {
    fn decide(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            NexusWork::SignalArrived(signal_input) => self.engine.decide_signal(signal_input),
            NexusWork::EffectCompleted(result) => self.engine.decide_effect_completed(result),
        };
        action.with_origin_route(origin_route)
    }

    fn run_effect(&mut self, input: NexusEffectCommand) -> NexusEffectResult {
        self.engine.run_forward_effect(input, self.connection)
    }

    fn budget_exhausted_reply(&self, exhausted: ContinuationExhausted) -> Output {
        Output::Error(ErrorReport(format!(
            "nexus continuation budget exhausted after {} steps (limit {})",
            exhausted.completed_step_count(),
            exhausted.limit().count()
        )))
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
