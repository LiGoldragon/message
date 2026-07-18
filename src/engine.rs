//! The message runtime — a thin composer over the three schema-emitted planes.
//!
//! The messenger owns durable state: the agent registry (identity map +
//! local delivery registry) since train packet 2.1, and — since the
//! messenger promotion (packet 3.1) — the message ledger, per-recipient
//! inbox, and thread index, all in `messenger.sema`. The Signal plane
//! (`schema/signal.schema`) is its wire surface; the Nexus plane
//! (`schema/nexus.schema`) is its internal-feature catalog — registry
//! apply/read plus message-store apply/read effects; the SEMA plane
//! (`schema/sema.schema`) owns the durable commits. The router is no longer
//! in the local loop: a submission persists locally and is answered locally.
//!
//! `MessageEngine::handle` runs the record-970 flow: a decoded Signal `Input`
//! becomes `NexusWork::SignalArrived`, the Nexus `decide` either replies
//! directly (already-stamped -> Unimplemented) or emits an effect. Effects
//! run through the `SemaEngine` plane and commit in the store; provenance
//! (origin, resolved sender, ingress stamp) is minted in the effect runner
//! from the accepted connection's kernel-vouched peer credentials. The
//! generated `NexusEngine::execute` runner owns the recursion: it runs the
//! effect, feeds the `EffectCompleted` work back into `decide`, and stops on
//! the Signal `Output`.

use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    provenance::{OriginPolicy, SenderResolver},
    schema::{
        nexus::{
            self as nexus_schema, NexusAction, NexusEffectCommand, NexusEffectResult, NexusEngine,
            NexusWork,
        },
        sema::{
            self as sema_schema, ReadInput as SemaReadInput, ReadOutput as SemaReadOutput,
            WriteInput as SemaWriteInput, WriteOutput as SemaWriteOutput,
        },
        signal::{
            AgentRegistryCommand, AgentRegistryEntries, AgentRegistryRejection,
            AgentRegistryRejectionReason, Error as SignalError, ErrorMessage, ErrorReport,
            InboxContents, InboxEntries, Input, LedgerDraft, OperationKind, Output,
            RequestUnimplemented, StoreCommand, StoreQuery, StoreWrite, SubmissionRejection,
            SubmissionRejectionReason, ThreadIndexEntries, ThreadRejection, ThreadRejectionReason,
            Threads, Unimplemented, UnimplementedReason,
        },
    },
    tables::MessengerTables,
};

/// The daemon runtime: the durable messenger store the SEMA plane commits
/// into, and the origin policy provenance is minted from.
#[derive(Debug)]
pub struct MessageEngine {
    tables: MessengerTables,
    origin_policy: OriginPolicy,
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
    pub fn new(tables: MessengerTables, origin_policy: OriginPolicy) -> Self {
        Self {
            tables,
            origin_policy,
        }
    }

    /// Build the runtime from daemon configuration: the `messenger.sema`
    /// store opened at the configured database path, plus the origin policy
    /// carrying the configured owner identity.
    pub fn from_configuration(configuration: &Configuration) -> Result<Self, Error> {
        Ok(Self::new(
            MessengerTables::open(configuration.database_path())?,
            OriginPolicy::for_owner_user_id(
                configuration.owner_user_id(),
                configuration.owner_name(),
            ),
        ))
    }

    /// Run one decoded Signal `Input` end to end and return the Signal `Output`.
    ///
    /// `connection` carries the accepted stream's peer credentials; stored
    /// provenance (origin, resolved sender, ingress stamp) is minted from
    /// them.
    pub async fn handle(
        &mut self,
        input: Input,
        connection: &ConnectionContext,
    ) -> Result<Output, Error> {
        let signal_action = RequestEngine::new(self)
            .execute(NexusWork::signal_arrived(input).with_origin_route(Self::origin_route()))
            .await
            .into_root();
        let action = match signal_action {
            NexusAction::CommandEffect(command) => {
                let effect_result = self.run_effect(command.into_payload(), connection);
                RequestEngine::new(self)
                    .execute(
                        NexusWork::effect_completed(effect_result)
                            .with_origin_route(Self::origin_route()),
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
    /// `NexusEffectResult`. Registry and message-store effects both run
    /// through the SEMA plane and commit in (or read from) `messenger.sema`;
    /// a `RecordSubmission` command is provenance-stamped here first, where
    /// the connection is available.
    fn run_effect(
        &mut self,
        effect: NexusEffectCommand,
        connection: &ConnectionContext,
    ) -> NexusEffectResult {
        match effect {
            NexusEffectCommand::ApplyRegistry(command) => {
                let sema_output = <Self as sema_schema::SemaEngine>::apply_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaWriteInput::ApplyRegistry(command.into_payload()),
                    ),
                );
                match sema_output.into_root() {
                    SemaWriteOutput::RegistryApplied(reply) => {
                        NexusEffectResult::registry_completed(reply)
                    }
                    SemaWriteOutput::StoreApplied(reply) => {
                        NexusEffectResult::registry_completed(reply)
                    }
                }
            }
            NexusEffectCommand::ReadRegistry(query) => {
                let sema_output = <Self as sema_schema::SemaEngine>::observe_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaReadInput::ReadRegistry(query.into_payload()),
                    ),
                );
                match sema_output.into_root() {
                    SemaReadOutput::RegistryRead(reply) => {
                        NexusEffectResult::registry_completed(reply)
                    }
                    SemaReadOutput::StoreRead(reply) => {
                        NexusEffectResult::registry_completed(reply)
                    }
                }
            }
            NexusEffectCommand::ApplyMessageStore(command) => {
                let write = self.stamped_store_write(command.into_payload(), connection);
                let sema_output = <Self as sema_schema::SemaEngine>::apply_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaWriteInput::ApplyMessageStore(write),
                    ),
                );
                match sema_output.into_root() {
                    SemaWriteOutput::RegistryApplied(reply) => {
                        NexusEffectResult::store_completed(reply)
                    }
                    SemaWriteOutput::StoreApplied(reply) => {
                        NexusEffectResult::store_completed(reply)
                    }
                }
            }
            NexusEffectCommand::ReadMessageStore(query) => {
                let sema_output = <Self as sema_schema::SemaEngine>::observe_inner(
                    self,
                    sema_schema::sema::Sema::new(
                        Self::sema_origin_route(),
                        SemaReadInput::ReadMessageStore(query.into_payload()),
                    ),
                );
                match sema_output.into_root() {
                    SemaReadOutput::RegistryRead(reply) => {
                        NexusEffectResult::store_completed(reply)
                    }
                    SemaReadOutput::StoreRead(reply) => {
                        NexusEffectResult::store_completed(reply)
                    }
                }
            }
        }
    }

    /// Mint provenance onto a store command: a raw submission gains its
    /// origin, resolved sender, and ingress stamp; a subscription passes
    /// through untouched. Provenance is never accepted from the caller
    /// payload.
    fn stamped_store_write(
        &self,
        command: StoreCommand,
        connection: &ConnectionContext,
    ) -> StoreWrite {
        match command {
            StoreCommand::RecordSubmission(submission) => {
                let sender =
                    SenderResolver::new(&self.tables, &self.origin_policy).resolve(connection);
                StoreWrite::RecordSubmission(LedgerDraft {
                    message_submission: submission,
                    message_origin: self.origin_policy.origin_for_connection(connection),
                    sender_name: sender,
                    stamped_at: self.origin_policy.ingress_stamp(),
                })
            }
            StoreCommand::Subscribe(subscription) => StoreWrite::Subscribe(subscription),
        }
    }

    /// The decision for an arrived Signal `Input`.
    ///
    /// - `Submit` → message-store apply effect (provenance minted at run).
    /// - `QueryInbox` / `QueryThread` / `QueryThreads` → message-store read.
    /// - `SubscribeThread` → message-store apply.
    /// - `AssignAgentIdentity` / `BindAgentEndpoint` → registry apply effect.
    /// - `QueryAgentRegistry` → registry read effect.
    /// - `SubmitStamped` → typed `Unimplemented`: re-stamping an
    ///   already-stamped submission is out of scope (the daemon mints
    ///   provenance; it does not accept it from a peer).
    fn decide_signal(&self, input: Input) -> NexusAction {
        match input {
            Input::Submit(submission) => {
                NexusAction::command_effect(NexusEffectCommand::apply_message_store(
                    StoreCommand::RecordSubmission(submission.into_payload()),
                ))
            }
            Input::QueryInbox(query) => NexusAction::command_effect(
                NexusEffectCommand::read_message_store(StoreQuery::Inbox(query.into_payload())),
            ),
            Input::QueryThread(query) => NexusAction::command_effect(
                NexusEffectCommand::read_message_store(StoreQuery::Thread(query.into_payload())),
            ),
            Input::QueryThreads(query) => NexusAction::command_effect(
                NexusEffectCommand::read_message_store(StoreQuery::Threads(query.into_payload())),
            ),
            Input::SubscribeThread(subscription) => {
                NexusAction::command_effect(NexusEffectCommand::apply_message_store(
                    StoreCommand::Subscribe(subscription.into_payload()),
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
            NexusEffectResult::RegistryCompleted(reply) => {
                NexusAction::reply_to_signal(reply.into_payload())
            }
            NexusEffectResult::StoreCompleted(reply) => {
                NexusAction::reply_to_signal(reply.into_payload())
            }
        }
    }

    /// The single origin route message stamps onto every in-flight mail.
    /// Message serves one request per connection on its own call stack, so
    /// there is no concurrent in-flight mail to disambiguate.
    fn origin_route() -> nexus_schema::OriginRoute {
        nexus_schema::OriginRoute::new(1)
    }

    /// The SEMA-plane counterpart of `origin_route`: same single in-flight
    /// route, in the sema module's route type.
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
    /// Store failures become typed rejection replies, not engine errors: the
    /// daemon spine closes the connection without a reply frame on an engine
    /// `Err`, and a caller must be able to distinguish a rejected operation
    /// from a dead daemon.
    fn apply_registry_command(&self, command: AgentRegistryCommand) -> Output {
        match command {
            AgentRegistryCommand::AssignIdentity(assignment) => {
                match self.tables.seat_identity(&assignment) {
                    Ok(assigned) => Output::agent_identity_assigned(assigned),
                    Err(_) => {
                        Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected)
                    }
                }
            }
            AgentRegistryCommand::BindEndpoint(binding) => {
                match self.tables.bind_endpoint(&binding) {
                    Ok(Some(bound)) => {
                        DeliveryRunner::new(&self.tables)
                            .drain_outbox(bound.payload().payload().as_str());
                        Output::agent_endpoint_bound(bound)
                    }
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

    /// Project a provenance-stamped message-store write onto its typed
    /// Signal reply. Same discipline as the registry: store failures are
    /// typed rejections, never reply-less closes.
    fn apply_store_write(&self, write: StoreWrite) -> Output {
        match write {
            StoreWrite::RecordSubmission(draft) => match self.tables.store_submission(&draft) {
                Ok(acceptance) => {
                    if let Ok(Some(record)) = self
                        .tables
                        .ledger_record_public(*acceptance.payload().payload())
                    {
                        DeliveryRunner::new(&self.tables).deliver_committed(&record);
                    }
                    Output::submission_accepted(acceptance)
                }
                Err(_) => Output::submission_rejected(SubmissionRejection::new(
                    SubmissionRejectionReason::StoreRejected,
                )),
            },
            StoreWrite::Subscribe(subscription) => {
                match self.tables.subscribe_thread(&subscription) {
                    Ok(acknowledgment) => Output::thread_subscribed(acknowledgment),
                    Err(_) => Output::thread_rejected(ThreadRejection::new(
                        ThreadRejectionReason::StoreRejected,
                    )),
                }
            }
        }
    }

    /// Project a message-store read onto its typed Signal reply.
    fn read_store_query(&self, query: StoreQuery) -> Output {
        match query {
            StoreQuery::Inbox(inbox_query) => {
                match self.tables.inbox_entries(&inbox_query) {
                    Ok(entries) => Output::inbox_listing(InboxContents::new(InboxEntries::new(
                        entries,
                    ))),
                    Err(_) => Output::submission_rejected(SubmissionRejection::new(
                        SubmissionRejectionReason::StoreRejected,
                    )),
                }
            }
            StoreQuery::Thread(thread_query) => {
                let thread_name = thread_query.into_payload();
                match self.tables.thread_contents(&thread_name) {
                    Ok(Some(contents)) => Output::thread_listing(contents),
                    Ok(None) => Output::thread_rejected(ThreadRejection::new(
                        ThreadRejectionReason::UnknownThread,
                    )),
                    Err(_) => Output::thread_rejected(ThreadRejection::new(
                        ThreadRejectionReason::StoreRejected,
                    )),
                }
            }
            StoreQuery::Threads(_) => match self.tables.thread_summaries() {
                Ok(summaries) => {
                    Output::thread_index_listing(ThreadIndexEntries::new(Threads::new(summaries)))
                }
                Err(_) => Output::thread_rejected(ThreadRejection::new(
                    ThreadRejectionReason::StoreRejected,
                )),
            },
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

/// The SEMA plane commits the legal transition in `messenger.sema` and
/// returns the typed Signal reply projection.
impl sema_schema::SemaEngine for MessageEngine {
    fn apply_inner(
        &mut self,
        input: sema_schema::sema::Sema<SemaWriteInput>,
    ) -> sema_schema::sema::Sema<SemaWriteOutput> {
        let origin_route = input.origin_route();
        let output = match input.into_root() {
            SemaWriteInput::ApplyRegistry(command) => {
                SemaWriteOutput::registry_applied(self.apply_registry_command(command))
            }
            SemaWriteInput::ApplyMessageStore(write) => {
                SemaWriteOutput::store_applied(self.apply_store_write(write))
            }
        };
        sema_schema::sema::Sema::new(origin_route, output)
    }

    fn observe_inner(
        &self,
        input: sema_schema::sema::Sema<SemaReadInput>,
    ) -> sema_schema::sema::Sema<SemaReadOutput> {
        let origin_route = input.origin_route();
        let output = match input.into_root() {
            SemaReadInput::ReadRegistry(query) => {
                SemaReadOutput::registry_read(self.read_registry_query(query))
            }
            SemaReadInput::ReadMessageStore(query) => {
                SemaReadOutput::store_read(self.read_store_query(query))
            }
        };
        sema_schema::sema::Sema::new(origin_route, output)
    }
}
