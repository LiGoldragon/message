//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Query` is decided directly into one durable
//! messenger action and one strict `signal-message::Response`.

use std::{io::Write, os::unix::net::UnixStream, sync::Arc};

use signal_message::{
    AgentRegistryListingReply, AgentRegistryQuery, AgentRegistryRejectionReason, InboxListingReply,
    AgentDeathMark, EndpointSelection, MessageOperationKind, MessageRequestUnimplementedReply, MessageUnimplementedReason, PromptRelayAcceptance, PromptRelayDeliveryDisposition, PromptRelayRejection, PromptRelayRejectionReason, PromptRelaySubmission, Query,
    Response, SubmissionRejectionReason, ThreadIndexEntries, ThreadRejectionReason,
};
use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    provenance::{OriginPolicy, SenderResolver},
    runtime_model::{AgentRegistryCommand, LedgerDraft, StoreQuery, StoreWrite},
    tables::MessengerTables,
    relay::{DeliveryPort, Relay, RelayDisposition, RelayInput, TargetReadiness},
};

#[derive(Debug)]
pub struct MessageEngine {
    tables: Arc<MessengerTables>,
    origin_policy: OriginPolicy,
    prompt_relay_permissions: Vec<signal_message::PromptRelayPermission>,
}

impl MessageEngine {
    pub fn new(tables: MessengerTables, origin_policy: OriginPolicy) -> Self {
        Self {
            tables: Arc::new(tables),
            origin_policy,
            prompt_relay_permissions: vec![],
        }
    }

    pub fn from_configuration(configuration: &Configuration) -> Result<Self, Error> {
        let mut engine = Self::new(MessengerTables::open(configuration.database_path())?, OriginPolicy::for_owner_user_id(configuration.owner_user_id(), configuration.owner_label()));
        engine.prompt_relay_permissions = configuration.prompt_relay_permissions().to_vec();
        Ok(engine)
    }

    pub async fn handle(
        &mut self,
        input: Query,
        connection: &ConnectionContext,
    ) -> Result<Response, Error> {
        Ok(match input {
            Query::Submit(submission) => {
                let sender =
                    SenderResolver::new(&self.tables, &self.origin_policy).resolve(connection);
                self.apply_store_write(StoreWrite::RecordSubmission(LedgerDraft {
                    message_submission: submission,
                    message_origin: self.origin_policy.origin_for_connection(connection),
                    sender_name: sender,
                    stamped_at: self.origin_policy.ingress_stamp(),
                }))
            }
            Query::SubmitPrompt(submission) => self.submit_prompt(submission, connection),
            Query::SubmitStamped(_) => {
                Response::MessageRequestUnimplemented(MessageRequestUnimplementedReply {
                    message_operation_kind: MessageOperationKind::SubmitStamped,
                    message_unimplemented_reason: MessageUnimplementedReason::NotInPrototypeScope,
                })
            }
            Query::QueryInbox(query) => self.read_store_query(StoreQuery::Inbox(query)),
            Query::AssignAgentIdentity(assignment) => {
                self.apply_registry_command(AgentRegistryCommand::AssignIdentity(assignment))
            }
            Query::BindAgentEndpoint(binding) => {
                self.apply_registry_command(AgentRegistryCommand::BindEndpoint(binding))
            }
            Query::QueryAgentRegistry(query) => self.read_registry_query(query),
            Query::QueryThread(query) => self.read_store_query(StoreQuery::Thread(query)),
            Query::SubscribeThread(subscription) => {
                self.apply_store_write(StoreWrite::Subscribe(subscription))
            }
            Query::QueryThreads(query) => self.read_store_query(StoreQuery::Threads(query)),
        })
    }

    fn submit_prompt(&self, submission: PromptRelaySubmission, connection: &ConnectionContext) -> Response {
        let resolver = SenderResolver::new(&self.tables, &self.origin_policy);
        let Some(source) = resolver.registered_identifier(connection) else { return Response::PromptRelayRejected(PromptRelayRejection { prompt_relay_rejection_reason: PromptRelayRejectionReason::UnregisteredSource }); };
        let allowed = self.prompt_relay_permissions.iter().any(|permission| permission.source_agent_identifier == source && permission.destination_agent_identifier == submission.destination_agent_identifier);
        if !allowed { return Response::PromptRelayRejected(PromptRelayRejection { prompt_relay_rejection_reason: PromptRelayRejectionReason::DestinationNotPermitted }); }
        let source_event_identifier = submission.typed_prompt_envelope.source_event_identifier.clone();
        let port = RegistryPromptPort { tables: self.tables.clone() };
        let relay = Relay::from_tables(self.tables.clone());
        let disposition = relay.submit(RelayInput { destination: submission.destination_agent_identifier, origin: self.origin_policy.origin_for_connection(connection), envelope: submission.typed_prompt_envelope }, &port);
        match disposition {
            Ok(disposition) => Response::PromptRelayAccepted(PromptRelayAcceptance { source_event_identifier, prompt_relay_delivery_disposition: relay_disposition(disposition) }),
            Err(_) => Response::PromptRelayRejected(PromptRelayRejection { prompt_relay_rejection_reason: PromptRelayRejectionReason::StoreRejected }),
        }
    }

    fn apply_registry_command(&self, command: AgentRegistryCommand) -> Response {
        match command {
            AgentRegistryCommand::AssignIdentity(assignment) => {
                match self.tables.seat_identity(&assignment) {
                    Ok(assigned) => Response::AgentIdentityAssigned(assigned),
                    Err(_) => Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected),
                }
            }
            AgentRegistryCommand::BindEndpoint(binding) => {
                match self.tables.bind_endpoint(&binding) {
                    Ok(Some(bound)) => {
                        DeliveryRunner::new(&self.tables).drain_outbox(bound.as_str());
                        Response::AgentEndpointBound(bound)
                    }
                    Ok(None) => Self::registry_rejection(
                        AgentRegistryRejectionReason::UnknownAgentIdentifier,
                    ),
                    Err(_) => Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected),
                }
            }
        }
    }

    fn read_registry_query(&self, query: AgentRegistryQuery) -> Response {
        match self.tables.query_entries(&query) {
            Ok(entries) => Response::AgentRegistryListing(AgentRegistryListingReply { entries }),
            Err(_) => Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected),
        }
    }

    fn apply_store_write(&self, write: StoreWrite) -> Response {
        match write {
            StoreWrite::RecordSubmission(draft) => match self.tables.store_submission(&draft) {
                Ok(acceptance) => {
                    if let Ok(Some(record)) = self.tables.ledger_record_public(acceptance) {
                        DeliveryRunner::new(&self.tables).deliver_committed(&record);
                    }
                    Response::SubmissionAccepted(acceptance)
                }
                Err(_) => Response::SubmissionRejected(SubmissionRejectionReason::StoreRejected),
            },
            StoreWrite::Subscribe(subscription) => {
                match self.tables.subscribe_thread(&subscription) {
                    Ok(acknowledgment) => Response::ThreadSubscribed(acknowledgment),
                    Err(_) => Response::ThreadRejected(ThreadRejectionReason::StoreRejected),
                }
            }
        }
    }

    fn read_store_query(&self, query: StoreQuery) -> Response {
        match query {
            StoreQuery::Inbox(inbox_query) => match self.tables.inbox_entries(&inbox_query) {
                Ok(entries) => Response::InboxListing(InboxListingReply { messages: entries }),
                Err(_) => Response::SubmissionRejected(SubmissionRejectionReason::StoreRejected),
            },
            StoreQuery::Thread(thread_query) => {
                let thread_name = thread_query;
                match self.tables.thread_contents(&thread_name) {
                    Ok(Some(contents)) => Response::ThreadListing(contents),
                    Ok(None) => Response::ThreadRejected(ThreadRejectionReason::UnknownThread),
                    Err(_) => Response::ThreadRejected(ThreadRejectionReason::StoreRejected),
                }
            }
            StoreQuery::Threads(_) => match self.tables.thread_summaries() {
                Ok(summaries) => {
                    Response::ThreadIndexListing(ThreadIndexEntries { threads: summaries })
                }
                Err(_) => Response::ThreadRejected(ThreadRejectionReason::StoreRejected),
            },
        }
    }

    fn registry_rejection(reason: AgentRegistryRejectionReason) -> Response {
        Response::AgentRegistryRejected(reason)
    }

    #[allow(dead_code)]
    fn error_output(message: impl Into<String>) -> Response {
        Response::Error(message.into())
    }
}

struct RegistryPromptPort { tables: Arc<MessengerTables> }
impl DeliveryPort for RegistryPromptPort {
 fn readiness(&self, destination: &str) -> TargetReadiness { match self.tables.registry_entry(destination).ok().flatten() { Some(entry) if entry.agent_death_mark != AgentDeathMark::Killed && matches!(entry.endpoint_selection, EndpointSelection::Bound(_)) => TargetReadiness::Ready, _ => TargetReadiness::Dirty } }
 fn deliver(&self, destination: &str, bytes: &[u8]) -> std::io::Result<()> { let entry=self.tables.registry_entry(destination).map_err(std::io::Error::other)?.ok_or_else(|| std::io::Error::other("unregistered destination"))?; let EndpointSelection::Bound(endpoint)=entry.endpoint_selection else { return Err(std::io::Error::other("unbound destination")); }; let mut stream=UnixStream::connect(endpoint.endpoint_path.as_str())?; stream.write_all(bytes)?; stream.flush() }
}
fn relay_disposition(value: RelayDisposition) -> PromptRelayDeliveryDisposition { match value { RelayDisposition::Pending(TargetReadiness::Busy)=>PromptRelayDeliveryDisposition::Busy, RelayDisposition::Pending(TargetReadiness::Dirty)=>PromptRelayDeliveryDisposition::Dirty, RelayDisposition::Pending(TargetReadiness::Ready)=>PromptRelayDeliveryDisposition::Pending, RelayDisposition::RecordedOnly=>PromptRelayDeliveryDisposition::RecordedOnly, RelayDisposition::DuplicatePending(_)=>PromptRelayDeliveryDisposition::DuplicatePending, RelayDisposition::InFlight=>PromptRelayDeliveryDisposition::InFlight, RelayDisposition::ByteAccepted=>PromptRelayDeliveryDisposition::ByteAccepted, RelayDisposition::RecipientObserved=>PromptRelayDeliveryDisposition::RecipientObserved } }
