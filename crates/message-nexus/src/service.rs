//! Executes the two Message requests over its typed memory and Flow edge.

use crate::{
    flow_edge::{CallsFlow, DeliveryOutcome, EdgeFailure},
    nexus::{HoldsNexusState, MessageNexus},
    store::{ChangesConfiguration, KeepsStandard, StoreError},
};
use message_library::Configuration;
use signal_flow::{Process, Refusal, Unknown_Data as FlowUnknown};
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
}

impl ServesMessages for MessageNexus {
    fn serve_query(&self, process: Process, query: Query) -> Response {
        match query {
            Query::Send(request) => self.send(process, request, None),
            Query::SendThread(request) => self.send(
                process,
                signal_message::Send_Data {
                    recipient: request.recipient,
                    request: request.request,
                },
                Some(request.native_session_id),
            ),
            Query::Configure(configuration) => match self.configure(configuration, false) {
                Ok(_) => Response::Configured,
                Err(ConfigureFailure::AlreadyConfigured) => {
                    Response::Refused(SendRefusal::AlreadyConfigured)
                }
                Err(ConfigureFailure::Store(error)) => {
                    Response::Refused(SendRefusal::Store(error.to_string()))
                }
            },
        }
    }
}

trait SendsMessage {
    fn send(
        &self,
        process: Process,
        request: signal_message::Send_Data,
        thread: Option<String>,
    ) -> Response;
}

impl SendsMessage for MessageNexus {
    fn send(
        &self,
        process: Process,
        request: signal_message::Send_Data,
        thread: Option<String>,
    ) -> Response {
        let edge = match self.flow_edge() {
            Ok(edge) => edge,
            Err(error) => {
                return Response::Refused(SendRefusal::Store(error.to_string()));
            }
        };
        let identified = match thread {
            Some(thread) => edge.identify_thread(process, thread),
            None => edge.identify(process),
        };
        let sender = match identified {
            Ok(sender) => sender,
            Err(failure) => return Response::Refused(failure.for_message()),
        };
        let lock = match edge.lock(sender, request.recipient) {
            Ok(lock) => lock,
            Err(failure) => return Response::Refused(failure.for_message()),
        };
        if SystemTime::now().unix_seconds() >= lock.until {
            return Response::Refused(SendRefusal::Lapsed);
        }
        match edge.deliver(lock.clone(), request.request) {
            Ok(DeliveryOutcome::Delivered) => Response::Delivered,
            Ok(DeliveryOutcome::Queued) => Response::Queued,
            Ok(DeliveryOutcome::Woken) => Response::Woken,
            Err(
                failure
                @ (EdgeFailure::Unreachable | EdgeFailure::Broken | EdgeFailure::Unexpected),
            ) => {
                let _ = edge.release(lock);
                Response::Refused(failure.for_message())
            }
            Err(failure @ EdgeFailure::Refused(_)) => Response::Refused(failure.for_message()),
        }
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
            Self::Unknown(_) | Self::Awake(_) | Self::Taken(_) => {
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
