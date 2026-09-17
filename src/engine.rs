//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Query` is decided directly into one durable
//! messenger action and one strict `signal-message::Response`.

use sha2::{Digest, Sha256};
use signal_message::{
    AgentRegistryListingReply, AgentRegistryQuery, AgentRegistryRejectionReason, DeliveryReport,
    DeliveryRequest, FlowDeliveryRejectionReason, FlowDeliveryRequest, FlowIdleAcknowledgment,
    FlowIdleAnnouncement, InboxListingReply, MessageOperationKind,
    MessageRequestUnimplementedReply, MessageUnimplementedReason, Query, ReceiptKind,
    RecipientReceipt, Response, SubmissionRejectionReason, TargetFlowName, ThreadIndexEntries,
    ThreadRejectionReason,
};
use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    flow_delivery::FlowDeliveryOutbox,
    flow_registry::FlowMarkerIndex,
    provenance::{OriginPolicy, SenderResolver},
    runtime_model::{
        AgentRegistryCommand, LedgerDraft, NexusDeliveryRecord, StoreQuery, StoreWrite,
    },
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
            Query::FlowAnnounceIdle(announcement) => self.announce_idle(announcement),
            Query::Deliver(request) => self.deliver(request),
        })
    }

    fn deliver(&self, request: DeliveryRequest) -> Response {
        if let Err(detail) = validate_delivery_request(&request) {
            return Response::Error(detail);
        }
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(crate::text::write(&request.cluster_message).as_bytes())
        );
        let event_key = format!("event\0{}", request.source_event_identifier);
        match self.tables.nexus_delivery(&event_key) {
            Ok(Some(record)) if record.request_fingerprint == fingerprint => {}
            Ok(Some(_)) => {
                return Response::Error(
                    "source event identifier conflicts with an existing payload".into(),
                );
            }
            Ok(None) => {
                if self
                    .tables
                    .admit_nexus_delivery(
                        &event_key,
                        NexusDeliveryRecord {
                            request_fingerprint: fingerprint.clone(),
                            cluster_message: request.cluster_message.clone(),
                            receipt_kind: ReceiptKind::FileOnly,
                            retryable: false,
                        },
                    )
                    .is_err()
                {
                    return Response::Error("delivery event store rejected identity".into());
                }
            }
            Err(_) => return Response::Error("delivery event store rejected lookup".into()),
        }
        let mut recipient_receipts = Vec::with_capacity(request.target_flows.len());
        for flow_identifier in &request.target_flows {
            let key = format!("{}\0{}", request.source_event_identifier, flow_identifier);
            let was_parked = match self.tables.nexus_delivery(&key) {
                Ok(Some(record))
                    if record.request_fingerprint == fingerprint
                        && (record.receipt_kind != ReceiptKind::Parked || !record.retryable) =>
                {
                    recipient_receipts.push(RecipientReceipt {
                        flow_identifier: flow_identifier.clone(),
                        receipt_kind: record.receipt_kind,
                    });
                    continue;
                }
                Ok(Some(record)) if record.request_fingerprint == fingerprint => true,
                Ok(Some(_)) => {
                    return Response::Error(
                        "source event identifier conflicts with an existing delivery".into(),
                    );
                }
                Err(_) => return Response::Error("delivery receipt store rejected lookup".into()),
                Ok(None) => false,
            };
            let node = match crate::nexus_delivery::FlowResolver::conventional()
                .resolve(flow_identifier)
            {
                Ok(Some(node)) => node,
                Ok(None) | Err(_) => {
                    let receipt_kind = ReceiptKind::Parked;
                    let record = NexusDeliveryRecord {
                        request_fingerprint: fingerprint.clone(),
                        cluster_message: request.cluster_message.clone(),
                        receipt_kind: receipt_kind.clone(),
                        retryable: true,
                    };
                    let stored = if was_parked {
                        self.tables.replace_nexus_delivery(&key, record)
                    } else {
                        self.tables.admit_nexus_delivery(&key, record)
                    };
                    if stored.is_err() {
                        return Response::Error("delivery receipt store rejected park".into());
                    }
                    recipient_receipts.push(RecipientReceipt {
                        flow_identifier: flow_identifier.clone(),
                        receipt_kind,
                    });
                    continue;
                }
            };
            // Persist a non-retryable in-flight park before crossing the harness
            // boundary. Only the adapter's positive acknowledgement promotes it
            // to Accepted. An ambiguous timeout remains Parked and never types
            // the same source event a second time.
            let in_flight = NexusDeliveryRecord {
                request_fingerprint: fingerprint.clone(),
                cluster_message: request.cluster_message.clone(),
                receipt_kind: ReceiptKind::Parked,
                retryable: false,
            };
            let stored = if was_parked {
                self.tables.replace_nexus_delivery(&key, in_flight)
            } else {
                self.tables.admit_nexus_delivery(&key, in_flight)
            };
            if stored.is_err() {
                return Response::Error("delivery receipt store rejected acceptance".into());
            }
            let receipt_kind = crate::nexus_delivery::deliver(&node, &request.cluster_message)
                .unwrap_or(ReceiptKind::Parked);
            if self
                .tables
                .replace_nexus_delivery(
                    &key,
                    NexusDeliveryRecord {
                        request_fingerprint: fingerprint.clone(),
                        cluster_message: request.cluster_message.clone(),
                        receipt_kind: receipt_kind.clone(),
                        retryable: false,
                    },
                )
                .is_err()
            {
                return Response::Error("delivery acknowledgment could not be persisted".into());
            }
            recipient_receipts.push(RecipientReceipt {
                flow_identifier: flow_identifier.clone(),
                receipt_kind,
            });
        }
        Response::DeliveryRecorded(DeliveryReport {
            source_event_identifier: request.source_event_identifier,
            recipient_receipts,
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
            Ok((acknowledgment, _)) => Response::DeliveryQueued(acknowledgment),
            Err(_) => Response::FlowDeliveryRejected(FlowDeliveryRejectionReason::StoreRejected),
        }
    }

    /// A Flow-owned adapter has witnessed the named flow becoming idle.  This
    /// Nexus neither infers idleness nor updates its registry; it only drains
    /// the durable park addressed by the typed announcement.
    fn announce_idle(&self, announcement: FlowIdleAnnouncement) -> Response {
        match FlowDeliveryOutbox::new(&self.tables).drain(&announcement.target_flow_name) {
            Ok(landed_receipts) => Response::FlowIdleAcknowledged(FlowIdleAcknowledgment {
                target_flow_name: announcement.target_flow_name,
                landed_receipts,
            }),
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
    /// SEAM: Flow will publish turn-idleness by subscription; interim: an
    /// explicit idle-announce op / manual prime. The PTY leg that would type
    /// the text at a live harness session is deliberately absent — landing
    /// here means the parked delivery left the store and its receipt exists.
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

fn validate_delivery_request(request: &DeliveryRequest) -> Result<(), String> {
    if request.target_flows.is_empty() {
        return Err("delivery requires at least one target flow".into());
    }
    match &request.cluster_message {
        signal_message::ClusterMessage::Peer(peer) => {
            if peer.source_event_identifier != request.source_event_identifier {
                return Err("delivery event does not match Peer source event".into());
            }
            let actual = format!("{:x}", Sha256::digest(peer.peer_body.as_bytes()));
            if peer.peer_body_sha256 != actual {
                return Err("Peer body sha256 does not match its exact body".into());
            }
        }
        signal_message::ClusterMessage::Relay(relay) => {
            // Relay identifies the exact prompt by transcript path, word
            // boundaries, and hash; the prompt bytes intentionally are not
            // duplicated in this derived-context carrier. Enforce the internal
            // source binding that can be decided from the carried value.
            if relay.prompt_sha256 != relay.context.prompt_sha256
                || relay.transcript_path != relay.context.transcript_path
                || relay.flow_identifier != relay.context.flow_identifier
                || relay.prompt_sha256.len() != 64
                || !relay
                    .prompt_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("Relay source provenance fields do not agree".into());
            }
        }
    }
    Ok(())
}
