//! Executes the two Message requests over its typed memory and Flow edge.

use crate::{
    flow_edge::{CallsFlow, DeliveryReply, EdgeFailure, FlowSubscription},
    nexus::{HoldsNexusState, MessageNexus},
    store::{ChangesConfiguration, KeepsStandard, StoreError},
};
use message_library::Configuration;
use signal_flow::{
    DeliveryCursor, Process, Refusal, SendAcceptance, SendFailure, SendReceipt, SendRequest,
    Unknown_Data as FlowUnknown,
};
use signal_message::{Query, Response, SendRefusal, Unknown_Data as MessageUnknown};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum ConfigureFailure {
    #[error("ordinary Configure is closed after meta Configure")]
    AlreadyConfigured,
    #[error("message store: {0}")]
    Store(#[from] StoreError),
}

pub trait ConfiguresMessage {
    fn configure(&self, configuration: Configuration, meta: bool)
    -> Result<bool, ConfigureFailure>;
}

impl ConfiguresMessage for MessageNexus {
    fn configure(
        &self,
        configuration: Configuration,
        meta: bool,
    ) -> Result<bool, ConfigureFailure> {
        let store = self.store();
        let current = store.standard()?;
        if !meta && current.meta_configured {
            return Err(ConfigureFailure::AlreadyConfigured);
        }
        let restart_required = current.configuration != configuration;
        let updated = current.changed_configuration(&configuration, meta);
        store.replace_standard(updated)?;
        Ok(restart_required)
    }
}

pub trait ServesMessages {
    fn serve_query(&self, process: Process, query: Query) -> Response;
    fn accept_send(
        &self,
        process: Process,
        request: SendRequest,
        thread: Option<String>,
    ) -> Result<SendAnswer, Box<Response>>;
    fn observe_sends(
        &self,
        process: Process,
        thread: Option<String>,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, Box<Response>>;
}

pub enum SendAnswer {
    Accepted {
        acceptance: SendAcceptance,
        subscription: Result<FlowSubscription, Box<Response>>,
    },
    Sent(SendReceipt),
    Failed(SendFailure),
}

impl ServesMessages for MessageNexus {
    fn serve_query(&self, _process: Process, query: Query) -> Response {
        match query {
            Query::Configure(configuration) => match self.configure(configuration, false) {
                Ok(_) => Response::Configured,
                Err(ConfigureFailure::AlreadyConfigured) => {
                    Response::Refused(SendRefusal::AlreadyConfigured)
                }
                Err(ConfigureFailure::Store(error)) => {
                    Response::Refused(SendRefusal::Store(error.to_string()))
                }
            },
            _ => Response::Refused(SendRefusal::FlowUnreachable),
        }
    }

    fn accept_send(
        &self,
        process: Process,
        request: SendRequest,
        thread: Option<String>,
    ) -> Result<SendAnswer, Box<Response>> {
        let edge = match self.flow_edge() {
            Ok(edge) => edge,
            Err(error) => {
                return Err(Box::new(Response::Refused(SendRefusal::Store(
                    error.to_string(),
                ))));
            }
        };
        let identified = match &thread {
            Some(thread) => edge.identify_thread(process.clone(), thread.clone()),
            None => edge.identify(process.clone()),
        };
        let sender = match identified {
            Ok(sender) => sender,
            Err(failure) => return Err(Box::new(Response::Refused(failure.for_message()))),
        };
        let lock = match edge.lock(sender, request.recipient.clone()) {
            Ok(lock) => lock,
            Err(failure) => return Err(Box::new(Response::Refused(failure.for_message()))),
        };
        if SystemTime::now().unix_seconds() >= lock.until {
            return Err(Box::new(Response::Refused(SendRefusal::Lapsed)));
        }
        let delivery = match edge.deliver(lock.clone(), request) {
            Ok(delivery) => delivery,
            Err(
                failure
                @ (EdgeFailure::Unreachable | EdgeFailure::Broken | EdgeFailure::Unexpected),
            ) => {
                let _ = edge.release(lock);
                return Err(Box::new(Response::Refused(failure.for_message())));
            }
            Err(failure @ EdgeFailure::Refused(_)) => {
                return Err(Box::new(Response::Refused(failure.for_message())));
            }
        };
        match delivery {
            DeliveryReply::Sent(receipt) => Ok(SendAnswer::Sent(receipt)),
            DeliveryReply::Failed(failure) => Ok(SendAnswer::Failed(failure)),
            DeliveryReply::Accepted(acceptance) => {
                let subscription = edge
                    .observe_sender(process, thread, Some(acceptance.delivery_cursor))
                    .map_err(|failure| {
                        Box::new(Response::ObservationInterrupted(
                            signal_message::ObservationInterrupted_Data {
                                delivery_cursor_option: Some(acceptance.delivery_cursor),
                                string: format!("Flow subscription could not open: {failure:?}"),
                            },
                        ))
                    });
                Ok(SendAnswer::Accepted {
                    acceptance,
                    subscription,
                })
            }
        }
    }

    fn observe_sends(
        &self,
        process: Process,
        thread: Option<String>,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, Box<Response>> {
        let edge = self
            .flow_edge()
            .map_err(|error| Box::new(Response::Refused(SendRefusal::Store(error.to_string()))))?;
        edge.observe_sender(process, thread, cursor)
            .map_err(|failure| match failure {
                EdgeFailure::Refused(refusal) => Box::new(Response::Refused(
                    EdgeFailure::Refused(refusal).for_message(),
                )),
                _ => Box::new(Response::ObservationInterrupted(
                    signal_message::ObservationInterrupted_Data {
                        delivery_cursor_option: cursor,
                        string: format!("Flow subscription could not open: {failure:?}"),
                    },
                )),
            })
    }
}

trait ReportsMessageRefusal {
    fn for_message(self) -> SendRefusal;
}

impl ReportsMessageRefusal for EdgeFailure {
    fn for_message(self) -> SendRefusal {
        match self {
            Self::Refused(refusal) => {
                let diagnostic = format!("{refusal:?}");
                refusal.for_message().unwrap_or_else(|| {
                    eprintln!(
                        "message-nexus: Flow refused outside the Send contract: {diagnostic}"
                    );
                    SendRefusal::FlowUnreachable
                })
            }
            Self::Unreachable | Self::Broken | Self::Unexpected => SendRefusal::FlowUnreachable,
        }
    }
}

trait MapsFlowRefusal {
    fn for_message(self) -> Option<SendRefusal>;
}

impl MapsFlowRefusal for Refusal {
    fn for_message(self) -> Option<SendRefusal> {
        Some(match self {
            Self::Locked => SendRefusal::Locked,
            Self::Held(lock) => SendRefusal::Held(lock),
            Self::Asleep => SendRefusal::Asleep,
            Self::Lapsed => SendRefusal::Lapsed,
            Self::Unknown(FlowUnknown::Address(address)) => {
                SendRefusal::Unknown(MessageUnknown::Address(address))
            }
            Self::Unknown(FlowUnknown::Lock(lock)) => {
                SendRefusal::Unknown(MessageUnknown::Lock(lock))
            }
            Self::NoLayer => SendRefusal::NoLayer,
            Self::NoneAbove => SendRefusal::NoneAbove,
            Self::NotMessage => SendRefusal::NotMessage,
            Self::NotConfigured => SendRefusal::NotConfigured,
            Self::NotSendServer(process) => SendRefusal::NotSendServer(process),
            Self::Unidentified(process) => SendRefusal::Unidentified(process),
            Self::Store(error) => SendRefusal::Store(error),
            Self::Oversized(size) => SendRefusal::Oversized(size),
            Self::Unknown(_) | Self::Awake(_) | Self::Taken(_) | Self::OffRoute => {
                return None;
            }
        })
    }
}

trait ReadsUnixTime {
    fn unix_seconds(&self) -> i64;
}

impl ReadsUnixTime for SystemTime {
    fn unix_seconds(&self) -> i64 {
        self.duration_since(UNIX_EPOCH)
            .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::ReportsMessageRefusal;
    use crate::flow_edge::EdgeFailure;
    use signal_flow::SendPayloadSize;
    use signal_message::SendRefusal;

    #[test]
    fn oversized_flow_refusal_keeps_its_typed_byte_counts() {
        let refusal = EdgeFailure::Refused(signal_flow::Refusal::Oversized(SendPayloadSize {
            actual_bytes: 65_537,
            maximum_bytes: 65_536,
        }))
        .for_message();
        assert_eq!(
            refusal,
            SendRefusal::Oversized(SendPayloadSize {
                actual_bytes: 65_537,
                maximum_bytes: 65_536,
            })
        );
    }
}
