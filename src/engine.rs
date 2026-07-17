//! The message runtime — a thin composer over the three schema-emitted planes.
//!
//! The messenger owns durable state as of train packet 2.1: the agent
//! registry (identity map + local delivery registry) in `messenger.sema`.
//! The Signal plane (`schema/signal.schema`) is its wire surface; the Nexus
//! plane (`schema/nexus.schema`) is its internal-feature catalog — the
//! forward-to-router decision plus the registry apply/read effects; the SEMA
//! plane (`schema/sema.schema`) owns the durable registry commit.
//!
//! `MessageEngine::handle` runs the record-970 flow: a decoded Signal `Input`
//! becomes `NexusWork::SignalArrived`, the Nexus `decide` either replies
//! directly (already-stamped -> Unimplemented) or emits an effect. Router
//! forwards run the outbound `signal-message` client; registry effects run
//! through the `SemaEngine` plane and commit in the store. The generated
//! `NexusEngine::execute` runner owns the recursion: it runs the effect,
//! feeds the `EffectCompleted` work back into `decide`, and stops on the
//! Signal `Output`.

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
            WriteInput as SemaWriteInput, WriteOutput as SemaWriteOutput,
        },
        signal::{
            AgentRegistryCommand, AgentRegistryEntries, AgentRegistryRejection,
            AgentRegistryRejectionReason,
            Error as SignalError, ErrorMessage, ErrorReport, Input, OperationKind, Output,
            RequestUnimplemented, Unimplemented, UnimplementedReason,
        },
    },
    tables::MessengerTables,
};

/// The daemon runtime: the router forwarder the Nexus forward effect drives,
/// and the durable messenger store the SEMA plane commits into.
#[derive(Debug)]
pub struct MessageEngine {
    forwarder: RouterForwarder,
    tables: MessengerTables,
}

/// One request's Nexus runner surface.
///
/// The generated runner needs a `NexusEngine` value for the typed decision
/// entry. Decisions are pure (`decide_signal` / `decide_effect_completed`);
/// effects run between the two `execute` calls in `handle`, where the engine
/// is mutably available for the SEMA plane.
#[derive(Debug)]
struct RequestEngine<'request> {
    engine: &'request MessageEngine,
}

impl MessageEngine {
    pub fn new(forwarder: RouterForwarder, tables: MessengerTables) -> Self {
        Self { forwarder, tables }
    }

    /// Build the runtime from daemon configuration: the outbound router
    /// client plus the `messenger.sema` store opened at the configured
    /// database path.
    pub fn from_configuration(configuration: &Configuration) -> Result<Self, Error> {
        Ok(Self::new(
            RouterForwarder::from_configuration(configuration),
            MessengerTables::open(configuration.database_path())?,
        ))
    }

    /// Run one decoded Signal `Input` end to end and return the Signal `Output`.
    ///
    /// The generated `NexusEngine::execute` method owns the typed decision
    /// entry; message sequences its one effect explicitly and feeds the typed
    /// result back through that generated entry.
    ///
    /// `connection` carries the accepted stream's peer credentials; the router
    /// forward stamps the provenance origin minted from them.
    pub async fn handle(
        &mut self,
        input: Input,
        connection: &ConnectionContext,
    ) -> Result<Output, Error> {
        let signal_action = RequestEngine::new(self)
            .execute(
                NexusWork::signal_arrived(input).with_origin_route(Self::forward_origin_route()),
            )
            .await
            .into_root();
        let action = match signal_action {
            NexusAction::CommandEffect(command) => {
                let effect_result = self.run_effect(command.into_payload(), connection);
                RequestEngine::new(self)
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

    /// Run one Nexus effect command and lift its outcome into a typed
    /// `NexusEffectResult`.
    ///
    /// - `ForwardToRouter` translates the schema submission into the
    ///   `signal-message` wire, calls the router, and stamps the origin minted
    ///   from the connection's peer credentials.
    /// - `ApplyRegistry` / `ReadRegistry` run through the SEMA plane and
    ///   commit in (or read from) `messenger.sema`.
    fn run_effect(
        &mut self,
        effect: NexusEffectCommand,
        connection: &ConnectionContext,
    ) -> NexusEffectResult {
        match effect {
            NexusEffectCommand::ForwardToRouter(request) => {
                self.run_forward_effect(request.into_payload(), connection)
            }
            NexusEffectCommand::ApplyRegistry(command) => {
                let sema_output = <Self as sema_schema::SemaEngine>::apply_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaWriteInput::ApplyRegistry(command.into_payload()),
                    ),
                );
                let SemaWriteOutput::RegistryApplied(reply) = sema_output.into_root();
                NexusEffectResult::registry_completed(reply)
            }
            NexusEffectCommand::ReadRegistry(query) => {
                let sema_output = <Self as sema_schema::SemaEngine>::observe_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaReadInput::ReadRegistry(query.into_payload()),
                    ),
                );
                let SemaReadOutput::RegistryRead(reply) = sema_output.into_root();
                NexusEffectResult::registry_completed(reply)
            }
        }
    }

    fn run_forward_effect(
        &self,
        request: ForwardRequest,
        connection: &ConnectionContext,
    ) -> NexusEffectResult {
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
    /// - `AssignAgentIdentity` / `BindAgentEndpoint` → registry apply effect.
    /// - `QueryAgentRegistry` → registry read effect.
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
            Input::AssignAgentIdentity(assignment) => {
                NexusAction::command_effect(NexusEffectCommand::apply_registry(
                    AgentRegistryCommand::AssignIdentity(assignment.into_payload()),
                ))
            }
            Input::BindAgentEndpoint(binding) => {
                NexusAction::command_effect(NexusEffectCommand::apply_registry(
                    AgentRegistryCommand::BindEndpoint(binding.into_payload()),
                ))
            }
            Input::QueryAgentRegistry(query) => NexusAction::command_effect(
                NexusEffectCommand::read_registry(query.into_payload()),
            ),
            Input::SubmitStamped(_) => NexusAction::reply_to_signal(Output::Unimplemented(
                Unimplemented::new(RequestUnimplemented {
                    unimplemented_operation_kind: OperationKind::SubmitStamped.into(),
                    reason: UnimplementedReason::NotInPrototypeScope.into(),
                }),
            )),
        }
    }

    /// Turn a completed effect into the Signal `Output` to reply with.
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
            NexusEffectResult::RegistryCompleted(reply) => {
                NexusAction::reply_to_signal(reply.into_payload())
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

    /// The SEMA-plane counterpart of `forward_origin_route`: same single
    /// in-flight route, in the sema module's route type.
    fn sema_origin_route() -> sema_schema::OriginRoute {
        sema_schema::OriginRoute::new(1)
    }

    fn error_output(message: impl Into<String>) -> Output {
        Output::Error(SignalError::new(ErrorReport::new(ErrorMessage::new(
            message,
        ))))
    }

    /// Project a registry write onto its typed Signal reply.
    ///
    /// Store and mint failures become typed `AgentRegistryRejected` replies,
    /// not engine errors: the daemon spine closes the connection without a
    /// reply frame on an engine `Err`, and a registry caller must be able to
    /// distinguish a rejected operation from a dead daemon.
    fn apply_registry_command(&self, command: AgentRegistryCommand) -> Output {
        match command {
            AgentRegistryCommand::AssignIdentity(assignment) => {
                match self.tables.assign_identity(&assignment) {
                    Ok(assigned) => Output::agent_identity_assigned(assigned),
                    Err(Error::AgentIdentifierSpanExhausted { .. }) => Self::registry_rejection(
                        AgentRegistryRejectionReason::IdentifierSpanExhausted,
                    ),
                    Err(_) => {
                        Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected)
                    }
                }
            }
            AgentRegistryCommand::BindEndpoint(binding) => {
                match self.tables.bind_endpoint(&binding) {
                    Ok(Some(bound)) => Output::agent_endpoint_bound(bound),
                    Ok(None) => Self::registry_rejection(
                        AgentRegistryRejectionReason::UnknownAgentIdentifier,
                    ),
                    Err(_) => {
                        Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected)
                    }
                }
            }
        }
    }

    /// Project a registry read onto its typed Signal reply.
    fn read_registry_query(
        &self,
        query: crate::schema::signal::AgentRegistryQuery,
    ) -> Output {
        match self.tables.query_entries(&query) {
            Ok(entries) => Output::agent_registry_listing(AgentRegistryEntries::new(entries)),
            Err(_) => Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected),
        }
    }

    fn registry_rejection(reason: AgentRegistryRejectionReason) -> Output {
        Output::agent_registry_rejected(AgentRegistryRejection::new(reason))
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

/// The SEMA plane commits the legal registry transition in `messenger.sema`
/// and returns the typed Signal reply projection.
impl sema_schema::SemaEngine for MessageEngine {
    fn apply_inner(
        &mut self,
        input: sema_schema::sema::Sema<SemaWriteInput>,
    ) -> sema_schema::sema::Sema<SemaWriteOutput> {
        let origin_route = input.origin_route();
        let SemaWriteInput::ApplyRegistry(command) = input.into_root();
        let reply = self.apply_registry_command(command);
        sema_schema::sema::Sema::new(
            origin_route,
            SemaWriteOutput::registry_applied(reply),
        )
    }

    fn observe_inner(
        &self,
        input: sema_schema::sema::Sema<SemaReadInput>,
    ) -> sema_schema::sema::Sema<SemaReadOutput> {
        let origin_route = input.origin_route();
        let SemaReadInput::ReadRegistry(query) = input.into_root();
        let reply = self.read_registry_query(query);
        sema_schema::sema::Sema::new(origin_route, SemaReadOutput::registry_read(reply))
    }
}
