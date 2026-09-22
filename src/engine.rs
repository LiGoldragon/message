//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Query` is decided directly into one durable
//! messenger action and one strict `signal-message::Response`.

use sha2::{Digest, Sha256};
use signal_message::{
    AgentRegistryListingReply, AgentRegistryQuery, AgentRegistryRejectionReason,
    AttemptDeliveryReceipt, CancelPending, CancelPendingResult, DeliveryAttemptState,
    DeliveryReceiptQuery, DeliveryReceiptQueryRejection, DeliveryReport, DeliveryRequest,
    FlowDeliveryRejectionReason, FlowDeliveryRequest, FlowIdleAcknowledgment, FlowIdleAnnouncement,
    DeliveryLockState, DeliveryModeSelection, DeliveryVisibility, InboxListingReply,
    MessageOperationKind, MessageRequestUnimplementedReply,
    MessageUnimplementedReason, Query, QueryDeliveryReceipt, ReceiptKind, RecipientReceipt,
    Response, SubmissionRejectionReason, SubmitDelivery, SubmitDeliveryRejection,
    RawDeliveryVisibility, SenderAttribution, SubmitDeliveryResult, TargetFlowName,
    ThreadIndexEntries, ThreadRejectionReason, UidAuthorization, WaitOutcome,
};
use std::sync::Mutex;
use triad_runtime::{ConnectionContext, PeerIdentity};

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    delivery_address::DeliveryAddressSelection,
    delivery_gate::{DeliveryGate, EndpointBinding},
    delivery_receipts::{DeliveryReceiptLookup, StoredDeliveryReceipts},
    error::Error,
    flow_delivery::FlowDeliveryOutbox,
    flow_registry::FlowMarkerIndex,
    nexus_delivery::{LiveNexusDelivery, NexusDelivery},
    provenance::{OriginPolicy, SenderResolver},
    runtime_model::{
        AgentRegistryCommand, DeliveryAttemptRecord, LedgerDraft, NexusDeliveryRecord, StoreQuery,
        StoreWrite,
    },
    tables::MessengerTables,
};

pub struct MessageEngine {
    tables: MessengerTables,
    origin_policy: OriginPolicy,
    /// SEAM: the Flow component (item 31) will own flow-name resolution;
    /// interim reads `.flow-id` markers.
    flow_registry: FlowMarkerIndex,
    nexus_delivery: Box<dyn NexusDelivery>,
    delivery_gate: Mutex<DeliveryGate>,
}

impl std::fmt::Debug for MessageEngine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessageEngine")
            .finish_non_exhaustive()
    }
}

impl MessageEngine {
    pub fn new(tables: MessengerTables, origin_policy: OriginPolicy) -> Self {
        Self {
            tables,
            origin_policy,
            flow_registry: FlowMarkerIndex::conventional(),
            nexus_delivery: Box::new(LiveNexusDelivery::conventional()),
            delivery_gate: Mutex::new(DeliveryGate::open()),
        }
    }

    pub fn with_nexus_delivery(mut self, nexus_delivery: impl NexusDelivery + 'static) -> Self {
        self.nexus_delivery = Box::new(nexus_delivery);
        self
    }

    /// Replace the interim flow registry — the seam's one injection point,
    /// used by tests today and by the Flow component's client tomorrow.
    pub fn with_flow_registry(mut self, flow_registry: FlowMarkerIndex) -> Self {
        self.flow_registry = flow_registry;
        self
    }

    /// Flow calls this before it begins a replacement. This only quiesces
    /// Message-owned attempts; it makes no claim about work already accepted
    /// by a harness or terminal queue.
    pub fn hold_delivery_binding(&self, binding: EndpointBinding) -> Result<(), Error> {
        self.delivery_gate
            .lock()
            .map_err(|_| Error::InvalidValidatorArgument {
                detail: "delivery gate lock poisoned".into(),
            })?
            .quiesce(binding)
            .map_err(|reason| Error::InvalidValidatorArgument {
                detail: format!("delivery hold refused: {reason:?}"),
            })
    }

    pub fn acknowledge_delivery_binding_ready(
        &self,
        binding: &EndpointBinding,
    ) -> Result<(), Error> {
        self.delivery_gate
            .lock()
            .map_err(|_| Error::InvalidValidatorArgument {
                detail: "delivery gate lock poisoned".into(),
            })?
            .acknowledge_ready(binding)
            .map_err(|reason| Error::InvalidValidatorArgument {
                detail: format!("delivery release refused: {reason:?}"),
            })
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
            Query::QueryDeliveryReceipts(query) => self.query_delivery_receipts(query),
            Query::SubmitDelivery(request) => self.submit_delivery(request, connection),
            Query::CancelPending(request) => self.cancel_pending(request),
            Query::QueryDeliveryReceipt(query) => self.query_delivery_receipt(query),
        })
    }

    fn submit_delivery(&self, request: SubmitDelivery, connection: &ConnectionContext) -> Response {
        if request.single_flow_recipient.is_empty() {
            return Response::DeliverySubmissionRejected(SubmitDeliveryRejection::InvalidDeadline);
        }
        let delivery_visibility = match &request.delivery_mode_selection {
            DeliveryModeSelection::Raw => match connection.peer() {
                PeerIdentity::Unix(credentials)
                    if self.origin_policy.is_owner_uid(credentials.user_id()) => {
                    DeliveryVisibility::RawUnlocked(RawDeliveryVisibility {
                        delivery_lock_state: DeliveryLockState::Unlocked,
                        uid_authorization: UidAuthorization::UidAuthorized,
                        sender_attribution: SenderAttribution::Unattributed,
                    })
                }
                _ => {
                    return Response::DeliverySubmissionRejected(
                        SubmitDeliveryRejection::RawUnauthorized,
                    );
                }
            },
            // Flow locking needs a live Flow permit plus a validated delegation.
            // Neither is inferred from this requester-selected mode.
            DeliveryModeSelection::FlowLocked => {
                return Response::DeliverySubmissionRejected(
                    SubmitDeliveryRejection::FlowLockUnavailable,
                );
            }
        };
        let request_id = format!(
            "request:{:x}",
            Sha256::digest(
                format!(
                    "{}\0{}",
                    request.source_event_identifier, request.single_flow_recipient
                )
                .as_bytes()
            )
        );
        let attempt_id = format!(
            "attempt:{:x}",
            Sha256::digest(format!("{}\0{}", request_id, request.message_body).as_bytes())
        );
        let key = format!("{request_id}\0{attempt_id}");
        let record = DeliveryAttemptRecord {
            request_id: request_id.clone(),
            attempt_id: attempt_id.clone(),
            source_event_identifier: request.source_event_identifier,
            recipient: request.single_flow_recipient,
            message_body: request.message_body,
            state: crate::runtime_model::DeliveryAttemptState::Queued,
        };
        if self.tables.delivery_attempt(&key).ok().flatten().is_none()
            && self.tables.admit_delivery_attempt(&key, record).is_err()
        {
            return Response::DeliverySubmissionRejected(SubmitDeliveryRejection::StoreRejected);
        }
        Response::DeliverySubmitted(SubmitDeliveryResult {
            delivery_request_id: request_id.clone(),
            delivery_attempt_id: attempt_id.clone(),
            durable_submission_receipt: format!("durable:{request_id}:{attempt_id}"),
            wait_outcome: WaitOutcome::NotWaited,
            delivery_visibility,
        })
    }

    fn cancel_pending(&self, request: CancelPending) -> Response {
        let key = format!(
            "{}\0{}",
            request.delivery_request_id, request.delivery_attempt_id
        );
        let Ok(Some(mut record)) = self.tables.delivery_attempt(&key) else {
            return Response::PendingCancelled(CancelPendingResult::UnknownRequest);
        };
        match record.state {
            crate::runtime_model::DeliveryAttemptState::Queued => {
                record.state = crate::runtime_model::DeliveryAttemptState::WaitCancelled;
                let _ = self.tables.replace_delivery_attempt(&key, record);
                Response::PendingCancelled(CancelPendingResult::CancelledQueued)
            }
            crate::runtime_model::DeliveryAttemptState::PermitHeld
            | crate::runtime_model::DeliveryAttemptState::Ambiguous => {
                Response::PendingCancelled(CancelPendingResult::WaitCancelledDeliveryContinues)
            }
            _ => Response::PendingCancelled(CancelPendingResult::AlreadyTerminal),
        }
    }

    fn query_delivery_receipt(&self, query: QueryDeliveryReceipt) -> Response {
        let key = format!(
            "{}\0{}",
            query.delivery_request_id, query.delivery_attempt_id
        );
        let state = self
            .tables
            .delivery_attempt(&key)
            .ok()
            .flatten()
            .map(|record| match record.state {
                crate::runtime_model::DeliveryAttemptState::Queued
                | crate::runtime_model::DeliveryAttemptState::WaitCancelled => {
                    DeliveryAttemptState::Queued
                }
                crate::runtime_model::DeliveryAttemptState::PermitHeld => {
                    DeliveryAttemptState::PermitHeld
                }
                crate::runtime_model::DeliveryAttemptState::TransportConfirmed => {
                    DeliveryAttemptState::TransportConfirmed
                }
                crate::runtime_model::DeliveryAttemptState::Ambiguous => {
                    DeliveryAttemptState::Ambiguous
                }
                crate::runtime_model::DeliveryAttemptState::Released => {
                    DeliveryAttemptState::Released
                }
            })
            .unwrap_or(DeliveryAttemptState::Missing);
        Response::DeliveryReceiptQueried(AttemptDeliveryReceipt {
            delivery_request_id: query.delivery_request_id,
            delivery_attempt_id: query.delivery_attempt_id,
            delivery_attempt_state: state,
        })
    }

    fn deliver(&self, request: DeliveryRequest) -> Response {
        if let Err(reason) = request.validate_address_selection() {
            return Response::DeliveryRejected(reason);
        }
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
            let node = match self.nexus_delivery.resolve(flow_identifier) {
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
            let binding = EndpointBinding::from_flow_node(&node);
            if self
                .delivery_gate
                .lock()
                .map_err(|_| ())
                .and_then(|gate| gate.permit(&binding).map_err(|_| ()))
                .is_err()
            {
                let record = NexusDeliveryRecord {
                    request_fingerprint: fingerprint.clone(),
                    cluster_message: request.cluster_message.clone(),
                    receipt_kind: ReceiptKind::Parked,
                    retryable: false,
                };
                let _ = self.tables.replace_nexus_delivery(&key, record);
                recipient_receipts.push(RecipientReceipt {
                    flow_identifier: flow_identifier.clone(),
                    receipt_kind: ReceiptKind::Parked,
                });
                continue;
            }
            // Persist a non-retryable in-flight park before crossing the harness
            // boundary. Only the adapter's positive transport-submission
            // acknowledgement promotes it to Accepted; Accepted does not assert
            // target-harness consumption. An ambiguous outcome remains Parked
            // and never types the same source event a second time.
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
            let receipt_kind = self
                .nexus_delivery
                .deliver(&node, &request.cluster_message)
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

    fn query_delivery_receipts(&self, query: DeliveryReceiptQuery) -> Response {
        if let Err(reason) = query.validate_address_selection() {
            return Response::DeliveryReceiptQueryRejected(
                DeliveryReceiptQueryRejection::InvalidAddressSelection(reason),
            );
        }
        Self::receipt_query_response(
            StoredDeliveryReceipts::new(&self.tables).lookup_delivery_receipts(&query),
        )
    }

    fn receipt_query_response(
        result: crate::Result<signal_message::DeliveryReceiptListing>,
    ) -> Response {
        match result {
            Ok(listing) => Response::DeliveryReceiptListing(listing),
            Err(_) => {
                Response::DeliveryReceiptQueryRejected(DeliveryReceiptQueryRejection::StoreRejected)
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::FlowNode;
    use signal_message::{
        ClusterMember, ClusterMessage, ClusterRelay, ClusterTarget, Context,
        DeliveryAddressSelectionRejection, DeliveryModeSelection, DeliveryRequest, Query,
        SubmitDelivery, SubmitDeliveryRejection, WaitDeadline,
    };
    use triad_runtime::UnixCredentials;

    #[derive(Debug)]
    struct PanicNexusDelivery;

    impl NexusDelivery for PanicNexusDelivery {
        fn resolve(&self, _flow: &str) -> std::result::Result<Option<FlowNode>, String> {
            panic!("reserved source reached resolver")
        }

        fn deliver(
            &self,
            _node: &FlowNode,
            _message: &signal_message::ClusterMessage,
        ) -> std::result::Result<ReceiptKind, String> {
            panic!("reserved source reached delivery")
        }
    }

    fn delivery_submission(mode: DeliveryModeSelection) -> SubmitDelivery {
        SubmitDelivery {
            source_event_identifier: "event-raw".into(),
            single_flow_recipient: "disposable-flow".into(),
            message_body: "disposable body".into(),
            wait_deadline: WaitDeadline::Default,
            delivery_mode_selection: mode,
        }
    }

    #[test]
    fn raw_requires_owner_uid_and_records_only_unattributed_visibility() {
        let directory = tempfile::tempdir().unwrap();
        let engine = MessageEngine::new(
            MessengerTables::open(&directory.path().join("messenger.sema")).unwrap(),
            OriginPolicy::for_owner_user_id(1000, "owner"),
        );
        let owner = ConnectionContext::from(UnixCredentials::new(1000, 1000, 1));
        let other = ConnectionContext::from(UnixCredentials::new(1001, 1001, 2));

        assert!(matches!(
            engine.submit_delivery(delivery_submission(DeliveryModeSelection::Raw), &owner),
            Response::DeliverySubmitted(SubmitDeliveryResult {
                delivery_visibility: DeliveryVisibility::RawUnlocked(_),
                ..
            })
        ));
        assert_eq!(
            engine.submit_delivery(delivery_submission(DeliveryModeSelection::Raw), &other),
            Response::DeliverySubmissionRejected(SubmitDeliveryRejection::RawUnauthorized)
        );
    }

    #[test]
    fn flow_locked_refuses_before_any_queue_record_without_a_permit_and_delegation() {
        let directory = tempfile::tempdir().unwrap();
        let engine = MessageEngine::new(
            MessengerTables::open(&directory.path().join("messenger.sema")).unwrap(),
            OriginPolicy::for_owner_user_id(1000, "owner"),
        );
        let owner = ConnectionContext::from(UnixCredentials::new(1000, 1000, 1));

        let submission = delivery_submission(DeliveryModeSelection::FlowLocked);
        let request_id = format!(
            "request:{:x}",
            Sha256::digest(b"event-raw\0disposable-flow")
        );
        let attempt_id = format!(
            "attempt:{:x}",
            Sha256::digest(format!("{request_id}\0disposable body").as_bytes())
        );
        assert_eq!(
            engine.submit_delivery(submission, &owner),
            Response::DeliverySubmissionRejected(SubmitDeliveryRejection::FlowLockUnavailable)
        );
        assert!(engine
            .tables
            .delivery_attempt(&format!("{request_id}\0{attempt_id}"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn receipt_store_failures_are_typed_without_exposing_the_store_error() {
        let response =
            MessageEngine::receipt_query_response(Err(Error::InvalidValidatorArgument {
                detail: "disposable receipt-store fixture fault".into(),
            }));
        assert_eq!(
            response,
            Response::DeliveryReceiptQueryRejected(DeliveryReceiptQueryRejection::StoreRejected)
        );
    }

    #[test]
    fn reserved_event_refuses_the_reachable_relay_receipt_key_alias() {
        let directory = tempfile::tempdir().unwrap();
        let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
        let relay = relay();
        tables
            .admit_nexus_delivery(
                "event\0target",
                NexusDeliveryRecord {
                    request_fingerprint: format!(
                        "{:x}",
                        Sha256::digest(crate::text::write(&relay).as_bytes())
                    ),
                    cluster_message: relay.clone(),
                    receipt_kind: ReceiptKind::FileOnly,
                    retryable: false,
                },
            )
            .unwrap();
        let mut engine = MessageEngine::new(tables, OriginPolicy::for_owner_user_id(1000, "owner"))
            .with_nexus_delivery(PanicNexusDelivery);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let connection = ConnectionContext::from(UnixCredentials::new(1000, 1000, 1));
        let response = runtime
            .block_on(engine.handle(
                Query::Deliver(DeliveryRequest {
                    source_event_identifier: "event".into(),
                    cluster_message: relay,
                    target_flows: vec!["target".into()],
                }),
                &connection,
            ))
            .unwrap();
        assert_eq!(
            response,
            Response::DeliveryRejected(
                DeliveryAddressSelectionRejection::ReservedSourceEventIdentifier
            )
        );
        assert!(engine
            .tables
            .nexus_delivery("event\0target")
            .unwrap()
            .is_some());
    }

    fn relay() -> ClusterMessage {
        let prompt_sha256 = "a".repeat(64);
        ClusterMessage::Relay(ClusterRelay {
            flow_identifier: "source-flow".into(),
            session_identifier: "source-session".into(),
            transcript_path: "flows/source-flow/transcript.md".into(),
            prompt_first_six_words: "one two three four five six".into(),
            prompt_last_six_words: "seven eight nine ten eleven twelve".into(),
            prompt_sha256: prompt_sha256.clone(),
            context: Context {
                flow_identifier: "source-flow".into(),
                source_turn_identifier: "turn-1".into(),
                transcript_path: "flows/source-flow/transcript.md".into(),
                prompt_sha256,
                what_living_said: "fixture".into(),
                context_about: "fixture".into(),
                context_answered: "fixture".into(),
                context_corrected: "fixture".into(),
                context_uncertainties: Vec::new(),
            },
            timestamp_nanos: 1,
            cluster_target: ClusterTarget::Primary,
            cluster_members: vec![ClusterMember {
                flow_identifier: "source-flow".into(),
                session_identifier: "source-session".into(),
            }],
        })
    }
}
