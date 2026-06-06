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
//! `decide` either replies directly (already-stamped → Unimplemented) or emits
//! the `ForwardToRouter` effect; the effect calls the router over the
//! `signal-message` wire and feeds the reply back as `NexusWork::ForwardCompleted`,
//! which `decide` turns into the Signal `Output`.

use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    error::Error,
    router::{RouterForwarder, RouterForwardOutcome},
    schema::{
        nexus::{
            self as nexus_schema, ForwardRequest, ForwardResult, NexusAction, NexusEffectCommand,
            NexusEngine, NexusWork, OriginRoute,
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
/// `MessageEngine` is the data-bearing noun the engine-trait impls hang on. It
/// owns the `RouterForwarder` (the outbound `signal-message` client + the owner
/// identity it stamps) and no durable store — message has none.
#[derive(Debug)]
pub struct MessageEngine {
    forwarder: RouterForwarder,
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
    /// This is the forward-only realization of the Signal → Nexus → effect →
    /// Nexus → Signal pipeline. The loop drives the Nexus `decide` until it
    /// produces a `ReplyToSignal`, running the `ForwardToRouter` effect in
    /// between. `Continue` re-enters `decide` in-process on the same stack.
    ///
    /// `connection` carries the accepted stream's peer credentials; the router
    /// forward stamps the provenance origin minted from them.
    pub fn handle(
        &mut self,
        input: Input,
        connection: &ConnectionContext,
    ) -> Result<Output, Error> {
        let mut work = NexusWork::SignalArrived(input);
        loop {
            let action = self
                .decide(work.with_origin_route(FORWARD_ORIGIN_ROUTE))
                .into_root();
            match action {
                NexusAction::ReplyToSignal(output) => return Ok(output),
                NexusAction::Continue(next) => work = next,
                NexusAction::CommandEffect(effect) => {
                    work = NexusWork::ForwardCompleted(self.run_effect(effect, connection)?);
                }
            }
        }
    }

    /// Run the one effect message declares: forward to the router. The effect
    /// translates the schema submission into the `signal-message` wire, calls
    /// the router, and lifts the outcome back into a `ForwardResult`. The
    /// connection's peer credentials mint the stamped origin.
    fn run_effect(
        &self,
        effect: NexusEffectCommand,
        connection: &ConnectionContext,
    ) -> Result<ForwardResult, Error> {
        let NexusEffectCommand::ForwardToRouter(request) = effect;
        Ok(match self.forwarder.forward(request, connection)? {
            RouterForwardOutcome::Replied(reply) => ForwardResult::Forwarded(reply),
            RouterForwardOutcome::Unreachable => {
                ForwardResult::RouterUnavailable(UnimplementedReason::RouterUnreachable)
            }
        })
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
            Input::SubmitStamped(_) => NexusAction::ReplyToSignal(Output::Unimplemented(
                RequestUnimplemented {
                    operation_kind: OperationKind::SubmitStamped,
                    unimplemented_reason: UnimplementedReason::NotInPrototypeScope,
                },
            )),
        }
    }

    /// Turn a completed router forward into the Signal `Output` to reply with.
    fn decide_forwarded(&self, result: ForwardResult) -> NexusAction {
        match result {
            ForwardResult::Forwarded(reply) => NexusAction::ReplyToSignal(reply),
            ForwardResult::RouterUnavailable(reason) => {
                NexusAction::ReplyToSignal(Output::Error(ErrorReport(Self::router_unavailable_text(
                    reason,
                ))))
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

impl NexusEngine for MessageEngine {
    fn decide(
        &mut self,
        input: nexus_schema::nexus::Nexus<nexus_schema::nexus::Work>,
    ) -> nexus_schema::nexus::Nexus<nexus_schema::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            NexusWork::SignalArrived(signal_input) => self.decide_signal(signal_input),
            NexusWork::ForwardCompleted(result) => self.decide_forwarded(result),
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
