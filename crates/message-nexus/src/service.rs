//! What each request does.

use crate::{
    delivery::{
        AddressesAttempts, DeliversThroughFlow, NamesAttempt, RequestsFlowDelivery,
        WatchesParkedRecipient,
    },
    flow_edge::{CallsFlowMeta, EdgeFailure, PeerName},
    ledger::{Grading, RecordsReceipts},
    nexus::{Addressee, HoldsNexusState, MessageNexus, StampsLedger},
    store::{KeepsLedger, MessageRecord, ReceiptRecord},
};
use meta_signal_flow::{DeliveryRejection, InterruptWitness, ProcessIdentity, Sender};
use meta_signal_message::{Activation, ConfigureRejection, Configured, MessageConfiguration};
use signal_flow::FlowId;
use signal_message::{
    BodyRefused_Data, Grade, MessageId, MessageRejection, RecipientRefused_Data, SendRejection,
    SendRequest, Submission,
};
use std::{sync::Arc, time::SystemTime};

/// Whom a connection's peer is, as a message's sender or recipient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerFlow {
    Flow(FlowId),
    NotAFlow,
    FlowUnreachable,
}

pub trait NamesPeerFlow {
    fn peer_flow(&self, identity: Option<ProcessIdentity>) -> PeerFlow;
}

impl NamesPeerFlow for MessageNexus {
    fn peer_flow(&self, identity: Option<ProcessIdentity>) -> PeerFlow {
        let Some(identity) = identity else {
            return PeerFlow::NotAFlow;
        };
        let Ok(edge) = self.flow_edge() else {
            return PeerFlow::FlowUnreachable;
        };
        match edge.resolve_peer(identity) {
            Ok(PeerName::Flow(caller)) => PeerFlow::Flow(caller.flow_id),
            Ok(PeerName::NotAFlow | PeerName::Mismatched) => PeerFlow::NotAFlow,
            Err(_) => PeerFlow::FlowUnreachable,
        }
    }
}

/// The operations of both sockets, once the caller is known.
pub trait ServesMessages {
    fn send(
        self: &Arc<Self>,
        sender: Sender,
        request: SendRequest,
    ) -> Result<Submission, SendRejection>;
    fn withdraw(
        &self,
        peer: PeerFlow,
        message_id: MessageId,
    ) -> Result<MessageId, MessageRejection>;
    fn acknowledge(
        &self,
        peer: PeerFlow,
        message_id: MessageId,
    ) -> Result<MessageId, MessageRejection>;
    fn query_receipts(&self, message_id: &str) -> Result<Submission, MessageRejection>;
    fn redeliver(
        self: &Arc<Self>,
        message_id: MessageId,
        flow_id: FlowId,
    ) -> Result<signal_message::Receipt, MessageRejection>;
    fn configure(
        &self,
        configuration: MessageConfiguration,
    ) -> Result<Configured, ConfigureRejection>;
}

/// Reads a message for a request that names it.
trait FindsMessage {
    fn message_record(&self, message_id: &str) -> Result<MessageRecord, MessageRejection>;
}

impl FindsMessage for MessageNexus {
    fn message_record(&self, message_id: &str) -> Result<MessageRecord, MessageRejection> {
        match self.store().message(message_id) {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(MessageRejection::UnknownMessage),
            Err(_) => Err(MessageRejection::StoreRefused),
        }
    }
}

impl ServesMessages for MessageNexus {
    fn send(
        self: &Arc<Self>,
        sender: Sender,
        request: SendRequest,
    ) -> Result<Submission, SendRejection> {
        let mut recipients: Vec<FlowId> = Vec::new();
        for flow_id in request.flow_id_vector {
            if !recipients.contains(&flow_id) {
                recipients.push(flow_id);
            }
        }
        if recipients.is_empty() {
            return Err(SendRejection::EmptyRecipients);
        }
        let message = MessageRecord {
            message_id: self.new_message_id(),
            sender,
            flow_id_vector: recipients.clone(),
            priority: request.priority,
            content: request.content,
            stamped_at: SystemTime::now().ledger_stamp(),
        };
        let edge = self.flow_edge().map_err(|_| SendRejection::StoreRefused)?;
        let addressees: Vec<Addressee> = recipients
            .iter()
            .map(|flow_id| Addressee {
                message_id: message.message_id.clone(),
                flow_id: flow_id.clone(),
            })
            .collect();
        // Every recipient is vetted before anything is recorded: the first
        // refusal fails the whole Send.
        for addressee in &addressees {
            let request = message.delivery_request(addressee.delivery_id(0), &addressee.flow_id);
            match edge.vet(request) {
                Ok(Ok(_)) => {}
                Ok(Err(DeliveryRejection::UnknownFlow)) => {
                    return Err(SendRejection::UnknownRecipient(addressee.flow_id.clone()));
                }
                Ok(Err(DeliveryRejection::BodyRefused(body_refusal))) => {
                    return Err(SendRejection::BodyRefused(BodyRefused_Data {
                        flow_id: addressee.flow_id.clone(),
                        body_refusal,
                    }));
                }
                Ok(Err(delivery_rejection)) => {
                    return Err(SendRejection::RecipientRefused(RecipientRefused_Data {
                        flow_id: addressee.flow_id.clone(),
                        delivery_rejection,
                    }));
                }
                Err(failure) => {
                    if let EdgeFailure::Refused(refusal) = &failure {
                        eprintln!("message-nexus: Flow's meta socket refuses Message: {refusal:?}");
                    }
                    return Err(SendRejection::FlowUnreachable);
                }
            }
        }
        let submitted: Vec<ReceiptRecord> = addressees
            .iter()
            .enumerate()
            .map(|(sequence, addressee)| ReceiptRecord {
                message_id: message.message_id.clone(),
                sequence: i64::try_from(sequence).unwrap_or(i64::MAX),
                flow_id: addressee.flow_id.clone(),
                delivery_id: addressee.delivery_id(0),
                interrupt_witness: InterruptWitness::NotRequested,
                grade: Grade::Submitted,
                stamped_at: message.stamped_at,
            })
            .collect();
        let message_id = message.message_id.clone();
        self.store()
            .record_message(message, submitted)
            .map_err(|_| SendRejection::StoreRefused)?;
        for addressee in &addressees {
            if self.settle(addressee, addressee.delivery_id(0)).is_err() {
                return Err(SendRejection::StoreRefused);
            }
        }
        self.submission(&message_id)
            .map_err(|_| SendRejection::StoreRefused)
    }

    fn withdraw(
        &self,
        peer: PeerFlow,
        message_id: MessageId,
    ) -> Result<MessageId, MessageRejection> {
        let message = self.message_record(&message_id)?;
        let sender = match peer {
            PeerFlow::Flow(flow_id) => Sender::Flow(flow_id),
            PeerFlow::NotAFlow => return Err(MessageRejection::NotSender),
            PeerFlow::FlowUnreachable => return Err(MessageRejection::FlowUnreachable),
        };
        if message.sender != sender {
            return Err(MessageRejection::NotSender);
        }
        let _settling = self.settling();
        let parked: Vec<ReceiptRecord> = self
            .latest_receipts(&message_id)
            .map_err(|_| MessageRejection::StoreRefused)?
            .into_iter()
            .filter(|latest| latest.grade == Grade::Parked)
            .collect();
        if parked.is_empty() {
            return Err(MessageRejection::NotParked);
        }
        for latest in parked {
            let addressee = Addressee {
                message_id: message_id.clone(),
                flow_id: latest.flow_id.clone(),
            };
            self.record_grade(
                &addressee,
                Grading {
                    delivery_id: latest.delivery_id,
                    interrupt_witness: latest.interrupt_witness,
                    grade: Grade::Withdrawn,
                },
            )
            .map_err(|_| MessageRejection::StoreRefused)?;
            self.close_watch(&addressee);
        }
        Ok(message_id)
    }

    fn acknowledge(
        &self,
        peer: PeerFlow,
        message_id: MessageId,
    ) -> Result<MessageId, MessageRejection> {
        let message = self.message_record(&message_id)?;
        let flow_id = match peer {
            PeerFlow::Flow(flow_id) => flow_id,
            PeerFlow::NotAFlow => return Err(MessageRejection::NotRecipient),
            PeerFlow::FlowUnreachable => return Err(MessageRejection::FlowUnreachable),
        };
        if !message.flow_id_vector.contains(&flow_id) {
            return Err(MessageRejection::NotRecipient);
        }
        let addressee = Addressee {
            message_id: message_id.clone(),
            flow_id,
        };
        let _settling = self.settling();
        let latest = self
            .latest_receipt(&addressee)
            .map_err(|_| MessageRejection::StoreRefused)?
            .ok_or(MessageRejection::StoreRefused)?;
        if latest.grade != Grade::Read {
            self.record_grade(
                &addressee,
                Grading {
                    delivery_id: latest.delivery_id,
                    interrupt_witness: latest.interrupt_witness,
                    grade: Grade::Read,
                },
            )
            .map_err(|_| MessageRejection::StoreRefused)?;
        }
        Ok(message_id)
    }

    fn query_receipts(&self, message_id: &str) -> Result<Submission, MessageRejection> {
        self.message_record(message_id)?;
        self.submission(message_id)
            .map_err(|_| MessageRejection::StoreRefused)
    }

    fn redeliver(
        self: &Arc<Self>,
        message_id: MessageId,
        flow_id: FlowId,
    ) -> Result<signal_message::Receipt, MessageRejection> {
        let message = self.message_record(&message_id)?;
        if !message.flow_id_vector.contains(&flow_id) {
            return Err(MessageRejection::NotRecipient);
        }
        let addressee = Addressee {
            message_id,
            flow_id,
        };
        let latest = self
            .latest_receipt(&addressee)
            .map_err(|_| MessageRejection::StoreRefused)?
            .ok_or(MessageRejection::StoreRefused)?;
        if latest.grade != Grade::Uncertain {
            return Err(MessageRejection::NotUncertain);
        }
        let attempt = latest.delivery_id.attempt() + 1;
        self.settle(&addressee, addressee.delivery_id(attempt))
            .map_err(|_| MessageRejection::StoreRefused)
    }

    fn configure(
        &self,
        configuration: MessageConfiguration,
    ) -> Result<Configured, ConfigureRejection> {
        self.store()
            .configure(configuration.clone())
            .map_err(|_| ConfigureRejection::StoreRefused)?;
        let restart = configuration.ordinary_socket_path != self.bound.ordinary_socket_path
            || configuration.meta_socket_path != self.bound.meta_socket_path;
        Ok(Configured {
            message_configuration: configuration,
            activation: if restart {
                Activation::NexusRestartRequired
            } else {
                Activation::Applied
            },
        })
    }
}
