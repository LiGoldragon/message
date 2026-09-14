//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Query` is decided directly into one durable
//! messenger action and one strict `signal-message::Response`.

use signal::{ByteViewable, Signalizable};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::{Duration, timeout};

use signal_message::{
    AgentDeathMark, AgentRegistryListingReply, AgentRegistryQuery, AgentRegistryRejectionReason,
    EndpointSelection, InboxListingReply, MessageOperationKind, MessageRequestUnimplementedReply,
    MessageUnimplementedReason, PromptReceiptObservation, PromptRelayAcceptance,
    PromptRelayDeliveryDisposition, PromptRelayRejection, PromptRelayRejectionReason,
    PromptRelaySubmission, Query, Response, SubmissionRejectionReason, ThreadIndexEntries,
    ThreadRejectionReason,
};
use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    provenance::{OriginPolicy, SenderResolver},
    relay::{DeliveryOutcome, DispatchClaim, Relay, RelayDisposition, RelayInput, TargetReadiness},
    runtime_model::{AgentRegistryCommand, LedgerDraft, StoreQuery, StoreWrite},
    tables::MessengerTables,
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
        let mut engine = Self::new(
            MessengerTables::open(configuration.database_path())?,
            OriginPolicy::for_owner_user_id(
                configuration.owner_user_id(),
                configuration.owner_label(),
            ),
        );
        engine.prompt_relay_permissions = configuration.prompt_relay_permissions().to_vec();
        Ok(engine)
    }

    pub async fn handle(
        &self,
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
            Query::DispatchPrompt(request) => self.dispatch_prompt(request, connection).await,
            Query::ObservePromptReceipt(observation) => {
                self.observe_prompt(observation, connection)
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
        })
    }

    fn submit_prompt(
        &self,
        submission: PromptRelaySubmission,
        connection: &ConnectionContext,
    ) -> Response {
        let resolver = SenderResolver::new(&self.tables, &self.origin_policy);
        let Some(source) = resolver.registered_identifier(connection) else {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::UnregisteredSource,
            });
        };
        let allowed = self.prompt_relay_permissions.iter().any(|permission| {
            permission.source_agent_identifier == source
                && permission.destination_agent_identifier
                    == submission.destination_agent_identifier
        });
        if !allowed {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::DestinationNotPermitted,
            });
        }
        let source_event_identifier = submission
            .typed_prompt_envelope
            .source_event_identifier
            .clone();
        let relay = Relay::from_tables(self.tables.clone());
        let disposition = relay.submit(RelayInput {
            source_agent_identifier: source,
            destination: submission.destination_agent_identifier,
            origin: self.origin_policy.origin_for_connection(connection),
            envelope: submission.typed_prompt_envelope,
        });
        match disposition {
            Ok(disposition) => Response::PromptRelayAccepted(PromptRelayAcceptance {
                source_event_identifier,
                prompt_relay_delivery_disposition: relay_disposition(disposition),
            }),
            Err(_) => Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::StoreRejected,
            }),
        }
    }

    async fn dispatch_prompt(
        &self,
        request: signal_message::PromptDispatchRequest,
        connection: &ConnectionContext,
    ) -> Response {
        let resolver = SenderResolver::new(&self.tables, &self.origin_policy);
        if resolver.registered_identifier(connection).as_deref()
            != Some(request.destination_agent_identifier.as_str())
        {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::UnregisteredSource,
            });
        }
        if self.prompt_relay_permissions.is_empty() {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::RelayDisabled,
            });
        }
        if !self.prompt_relay_permissions.iter().any(|permission| {
            permission.source_agent_identifier == request.source_agent_identifier
                && permission.destination_agent_identifier == request.destination_agent_identifier
        }) {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::DestinationNotPermitted,
            });
        }
        let readiness = match request.prompt_target_readiness {
            signal_message::PromptTargetReadiness::Ready => TargetReadiness::Ready,
            signal_message::PromptTargetReadiness::Busy => TargetReadiness::Busy,
            signal_message::PromptTargetReadiness::Dirty => TargetReadiness::Dirty,
        };
        let relay = Relay::from_tables(self.tables.clone());
        match relay.begin_dispatch(
            &request.destination_agent_identifier,
            &request.source_agent_identifier,
            &request.source_event_identifier,
            readiness,
        ) {
            Ok(DispatchClaim::NoAttempt(value)) => {
                Response::PromptRelayAccepted(PromptRelayAcceptance {
                    source_event_identifier: request.source_event_identifier,
                    prompt_relay_delivery_disposition: relay_disposition(value),
                })
            }
            Ok(DispatchClaim::Attempt(record)) => {
                let outcome = deliver_prompt(&self.tables, &record).await;
                match relay.finish_dispatch(
                    &request.destination_agent_identifier,
                    &request.source_agent_identifier,
                    &request.source_event_identifier,
                    outcome,
                ) {
                    Ok(()) => Response::PromptRelayAccepted(PromptRelayAcceptance {
                        source_event_identifier: request.source_event_identifier,
                        prompt_relay_delivery_disposition: match outcome {
                            DeliveryOutcome::ByteAccepted => {
                                PromptRelayDeliveryDisposition::ByteAccepted
                            }
                            DeliveryOutcome::NoBytesWritten => {
                                PromptRelayDeliveryDisposition::Pending
                            }
                            DeliveryOutcome::Ambiguous => PromptRelayDeliveryDisposition::InFlight,
                        },
                    }),
                    Err(_) => Response::PromptRelayRejected(PromptRelayRejection {
                        prompt_relay_rejection_reason: PromptRelayRejectionReason::StoreRejected,
                    }),
                }
            }
            Err(_) => Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::StoreRejected,
            }),
        }
    }

    fn observe_prompt(
        &self,
        observation: PromptReceiptObservation,
        connection: &ConnectionContext,
    ) -> Response {
        let resolver = SenderResolver::new(&self.tables, &self.origin_policy);
        if resolver.registered_identifier(connection).as_deref()
            != Some(observation.destination_agent_identifier.as_str())
        {
            return Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::UnregisteredSource,
            });
        }
        let relay = Relay::from_tables(self.tables.clone());
        match relay.recipient_observed(
            &observation.destination_agent_identifier,
            &observation.source_agent_identifier,
            &observation.source_event_identifier,
        ) {
            Ok(()) => Response::PromptRelayAccepted(PromptRelayAcceptance {
                source_event_identifier: observation.source_event_identifier,
                prompt_relay_delivery_disposition:
                    PromptRelayDeliveryDisposition::RecipientObserved,
            }),
            Err(_) => Response::PromptRelayRejected(PromptRelayRejection {
                prompt_relay_rejection_reason: PromptRelayRejectionReason::StoreRejected,
            }),
        }
    }

    fn apply_registry_command(&self, command: AgentRegistryCommand) -> Response {
        // An enabled prompt-relay allowlist is bootstrapped before this daemon starts.
        // Keeping its registry immutable prevents another same-UID ordinary client from
        // reseating an allowed identifier with its own process pin.
        if !self.prompt_relay_permissions.is_empty() {
            return Self::registry_rejection(AgentRegistryRejectionReason::StoreRejected);
        }
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

async fn deliver_prompt(
    tables: &MessengerTables,
    record: &crate::runtime_model::RelayRecord,
) -> DeliveryOutcome {
    let mut written = 0usize;
    let result = async {
        let entry = tables
            .registry_entry(&record.destination)
            .map_err(std::io::Error::other)?
            .ok_or_else(|| std::io::Error::other("unregistered destination"))?;
        if entry.agent_death_mark == AgentDeathMark::Killed {
            return Err(std::io::Error::other("killed destination"));
        }
        let EndpointSelection::Bound(endpoint) = entry.endpoint_selection else {
            return Err(std::io::Error::other("unbound destination"));
        };
        let mut stream = UnixStream::connect(endpoint.endpoint_path.as_str()).await?;
        let bytes = signal_message::PromptRelayDelivery {
            source_agent_identifier: record.source_agent_identifier.clone(),
            destination_agent_identifier: record.destination.clone(),
            message_origin: record.origin.clone(),
            typed_prompt_envelope: record.envelope.clone(),
        }
        .signalize()
        .map_err(std::io::Error::other)?
        .bytes()
        .to_vec();
        write_counted(&mut stream, &bytes, &mut written).await
    };
    if timeout(Duration::from_secs(1), result)
        .await
        .is_ok_and(|result| result.is_ok())
    {
        DeliveryOutcome::ByteAccepted
    } else if written == 0 {
        DeliveryOutcome::NoBytesWritten
    } else {
        DeliveryOutcome::Ambiguous
    }
}
fn relay_disposition(value: RelayDisposition) -> PromptRelayDeliveryDisposition {
    match value {
        RelayDisposition::Pending(TargetReadiness::Busy) => PromptRelayDeliveryDisposition::Busy,
        RelayDisposition::Pending(TargetReadiness::Dirty) => PromptRelayDeliveryDisposition::Dirty,
        RelayDisposition::Pending(TargetReadiness::Ready) => {
            PromptRelayDeliveryDisposition::Pending
        }
        RelayDisposition::RecordedOnly => PromptRelayDeliveryDisposition::RecordedOnly,
        RelayDisposition::DuplicatePending(_) => PromptRelayDeliveryDisposition::DuplicatePending,
        RelayDisposition::InFlight => PromptRelayDeliveryDisposition::InFlight,
        RelayDisposition::ByteAccepted => PromptRelayDeliveryDisposition::ByteAccepted,
        RelayDisposition::RecipientObserved => PromptRelayDeliveryDisposition::RecipientObserved,
    }
}

// AsyncWriteExt::write is cancellation-safe; write_all is not suitable when
// retry policy depends on the number of bytes accepted before a deadline.
async fn write_counted<W: tokio::io::AsyncWrite + Unpin>(
    stream: &mut W,
    bytes: &[u8],
    written: &mut usize,
) -> std::io::Result<()> {
    while *written < bytes.len() {
        let count = stream.write(&bytes[*written..]).await?;
        if count == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        *written += count;
    }
    stream.shutdown().await
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    struct PartialThenZero(bool);
    impl tokio::io::AsyncWrite for PartialThenZero {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.0 {
                Poll::Ready(Ok(0))
            } else {
                self.0 = true;
                Poll::Ready(Ok(2))
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn zero_after_partial_write_preserves_earlier_count() {
        let mut written = 0;
        assert!(
            write_counted(&mut PartialThenZero(false), b"frame", &mut written)
                .await
                .is_err()
        );
        assert_eq!(written, 2);
    }
    #[tokio::test]
    async fn cancellation_preserves_bytes_already_accepted_by_transport() {
        let (mut writer, _unread) = tokio::io::duplex(2);
        let mut written = 0;
        assert!(
            timeout(
                Duration::from_millis(20),
                write_counted(&mut writer, b"frame", &mut written)
            )
            .await
            .is_err()
        );
        assert_eq!(written, 2);
    }
}
