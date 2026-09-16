//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Query` is decided directly into one durable
//! messenger action and one strict `signal-message::Response`.

use signal_message::{
    AgentRegistryListingReply, AgentRegistryQuery, AgentRegistryRejectionReason,
    FlowDeliveryRejectionReason, FlowDeliveryRequest, InboxListingReply, MessageOperationKind,
    MessageRequestUnimplementedReply, MessageUnimplementedReason, Query, Response,
    SubmissionRejectionReason, TargetFlowName, ThreadIndexEntries, ThreadRejectionReason,
};
use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    flow_delivery::{FlowDeliveryOutbox, ParkAttempt},
    flow_registry::FlowMarkerIndex,
    provenance::{OriginPolicy, SenderResolver},
    runtime_model::{AgentRegistryCommand, LedgerDraft, StoreQuery, StoreWrite},
    tables::MessengerTables,
};

#[derive(Debug)]
pub struct MessageEngine {
    tables: MessengerTables,
    origin_policy: OriginPolicy,
    /// SEAM: the Flow component (item 31) will own flow-name resolution;
    /// interim reads `.flow-id` markers.
    flow_registry: FlowMarkerIndex,
}

impl MessageEngine {
    pub fn new(tables: MessengerTables, origin_policy: OriginPolicy) -> Self {
        Self {
            tables,
            origin_policy,
            flow_registry: FlowMarkerIndex::conventional(),
        }
    }

    /// Replace the interim flow registry — the seam's one injection point,
    /// used by tests today and by the Flow component's client tomorrow.
    pub fn with_flow_registry(mut self, flow_registry: FlowMarkerIndex) -> Self {
        self.flow_registry = flow_registry;
        self
    }

    pub fn from_configuration(configuration: &Configuration) -> Result<Self, Error> {
        Ok(Self::new(
            MessengerTables::open(configuration.database_path())?,
            OriginPolicy::for_owner_user_id(
                configuration.owner_user_id(),
                configuration.owner_label(),
            ),
        ))
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
            Query::FlowDeliver(request) => {
                let origin = self.origin_policy.origin_for_connection(connection);
                self.park_flow_delivery(request, origin)
            }
        })
    }

    /// Park one flow delivery, or refuse it typed.
    fn park_flow_delivery(
        &self,
        request: FlowDeliveryRequest,
        origin: signal_message::MessageOrigin,
    ) -> Response {
        if self
            .flow_registry
            .resolve(&request.target_flow_name)
            .is_none()
        {
            return Response::FlowDeliveryRejected(FlowDeliveryRejectionReason::UnknownFlow);
        }
        match FlowDeliveryOutbox::new(&self.tables).park(&request, origin) {
            Ok(
                ParkAttempt::Parked(acknowledgment) | ParkAttempt::AlreadyParked(acknowledgment),
            ) => Response::DeliveryQueued(acknowledgment),
            Ok(ParkAttempt::Conflicting) => {
                Response::FlowDeliveryRejected(FlowDeliveryRejectionReason::ConflictingEnvelope)
            }
            Err(_) => Response::FlowDeliveryRejected(FlowDeliveryRejectionReason::StoreRejected),
        }
    }

    /// Every envelope currently parked for one flow — the park's observation
    /// surface, which the drain and its proof both read.
    pub fn parked_flow_deliveries(
        &self,
        target_flow_name: &TargetFlowName,
    ) -> Result<Vec<signal_message::TypedPromptEnvelope>, Error> {
        FlowDeliveryOutbox::new(&self.tables).parked(target_flow_name)
    }

    /// A flow announces that its turn went idle: every delivery parked for it
    /// lands, one `DeliveryLanded` receipt each.
    ///
    /// Every receipt is built before any row is retracted, and the whole
    /// landing is retracted in one commit: a flow's idle announce either
    /// lands everything parked for it or lands nothing at all. Two announces
    /// racing for one flow cannot double-deliver, and neither can lose a row.
    ///
    /// SEAM: Flow will publish turn-idleness by subscription; interim: an
    /// explicit idle-announce op / manual prime. The PTY leg that would type
    /// the text at a live harness session is deliberately absent — landing
    /// here means the parked delivery left the store and its receipt exists.
    /// `DeliveryLanded` still reaches no client: the ordinary wire carries
    /// one reply per connection, so the landing half needs a subscription
    /// design that is primary's to make.
    pub fn announce_flow_idle(
        &mut self,
        target_flow_name: &TargetFlowName,
    ) -> Result<Vec<Response>, Error> {
        Ok(FlowDeliveryOutbox::new(&self.tables)
            .drain(target_flow_name)?
            .into_iter()
            .map(Response::DeliveryLanded)
            .collect())
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
