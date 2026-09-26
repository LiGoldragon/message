//! Delivery through Flow, and parking.
//!
//! Each recipient's delivery carries a DeliveryId unique to its attempt:
//! `<MessageId>:<FlowId>:<attempt>`. Flow keeps nothing of a refused
//! Deliver, so a parked recipient is tried again under the same id; a
//! Deliver that may have typed is kept by Flow, so a repeat after a crash
//! types nothing and answers the stored outcome. Only Redeliver, out of
//! Uncertain, opens a new attempt.
//!
//! A recipient that is working (Soft) or whose composer holds text is
//! Parked. Message then holds one Observe.Agent subscription for it and
//! tries again on each frame showing it at rest (Idle, Done) or Gone. No
//! timer: a recipient that never changes state is never tried again.

use crate::{
    flow_edge::{CallsFlowMeta, EdgeFailure, ObservesFlowAgent},
    ledger::{Grading, RecordsReceipts},
    nexus::{Addressee, HoldsNexusState, MessageNexus},
    store::{KeepsLedger, MessageRecord, StoreError},
};
use meta_signal_flow::{
    DeliveryGrade, DeliveryId, DeliveryRejection, DeliveryRequest, InterruptWitness, Letter,
    Message,
};
use signal_flow::AgentState;
use signal_message::{Grade, Priority, Receipt};
use std::sync::Arc;

impl MessageRecord {
    /// The typed Message Flow renders: the Priority is its head, then the
    /// MessageId. The id is what the recipient reads in its own pane and
    /// answers with `Acknowledge`, which is the only source of Read; without
    /// it a recipient had no way to name what it had just been handed.
    pub fn flow_message(&self) -> Message {
        let letter = Letter {
            message_id: self.message_id.clone(),
            sender: self.sender.clone(),
            content: self.content.clone(),
        };
        match self.priority {
            Priority::HardAbrupt => Message::HardAbrupt(letter),
            Priority::MiddleAbrupt => Message::MiddleAbrupt(letter),
            Priority::Soft => Message::Soft(letter),
        }
    }

    pub fn delivery_request(&self, delivery_id: DeliveryId, flow_id: &str) -> DeliveryRequest {
        DeliveryRequest {
            delivery_id,
            flow_id: flow_id.to_owned(),
            message: self.flow_message(),
        }
    }
}

impl Addressee {
    pub fn delivery_id(&self, attempt: u64) -> DeliveryId {
        format!("{}:{}:{attempt}", self.message_id, self.flow_id)
    }

    /// The attempt a DeliveryId names.
    pub fn attempt_of(delivery_id: &str) -> u64 {
        delivery_id
            .rsplit(':')
            .next()
            .and_then(|attempt| attempt.parse().ok())
            .unwrap_or(0)
    }
}

/// What one Deliver came to, as a grade.
pub trait GradesDelivery {
    fn grading(self, delivery_id: DeliveryId) -> Grading;
}

impl GradesDelivery for Result<Result<meta_signal_flow::Delivery, DeliveryRejection>, EdgeFailure> {
    fn grading(self, delivery_id: DeliveryId) -> Grading {
        let (interrupt_witness, grade) = match self {
            Ok(Ok(delivery)) => (
                delivery.interrupt_witness,
                match delivery.delivery_grade {
                    DeliveryGrade::Transported => Grade::Transported,
                    DeliveryGrade::Presented => Grade::Presented,
                    DeliveryGrade::Uncertain => Grade::Uncertain,
                },
            ),
            Ok(Err(DeliveryRejection::RecipientWorking | DeliveryRejection::ComposerOccupied)) => {
                (InterruptWitness::NotRequested, Grade::Parked)
            }
            Ok(Err(rejection)) => (InterruptWitness::NotRequested, Grade::Refused(rejection)),
            // Nothing reached Flow, or Flow refused Message itself: nothing
            // was typed.
            Err(EdgeFailure::Unreachable | EdgeFailure::Refused(_)) => (
                InterruptWitness::NotRequested,
                Grade::Refused(DeliveryRejection::NotDelivered),
            ),
            // The Deliver was sent and no answer came: it may have typed.
            Err(EdgeFailure::Broken | EdgeFailure::Unexpected) => {
                (InterruptWitness::NotRequested, Grade::Uncertain)
            }
        };
        Grading {
            delivery_id,
            interrupt_witness,
            grade,
        }
    }
}

pub trait DeliversThroughFlow {
    /// Delivers once to a recipient under the given DeliveryId and records
    /// the grade when it moved. A newly parked recipient is watched.
    fn settle(
        self: &Arc<Self>,
        addressee: &Addressee,
        delivery_id: DeliveryId,
    ) -> Result<Receipt, StoreError>;
    /// Holds a parked recipient's Observe.Agent and lands it at rest.
    fn watch_parked(self: &Arc<Self>, addressee: Addressee);
    /// On start: tries each submitted recipient and watches each parked one.
    fn resume(self: &Arc<Self>) -> Result<(), StoreError>;
}

impl DeliversThroughFlow for MessageNexus {
    fn settle(
        self: &Arc<Self>,
        addressee: &Addressee,
        delivery_id: DeliveryId,
    ) -> Result<Receipt, StoreError> {
        let message = self
            .store()
            .message(&addressee.message_id)?
            .ok_or(StoreError::StateInvariant)?;
        let request = message.delivery_request(delivery_id.clone(), &addressee.flow_id);
        let grading = match self.flow_edge() {
            Ok(edge) => edge.deliver(request),
            Err(_) => Err(EdgeFailure::Unreachable),
        }
        .grading(delivery_id);
        let latest = self.latest_receipt(addressee)?;
        let parked_again = grading.grade == Grade::Parked
            && latest
                .as_ref()
                .is_some_and(|latest| latest.grade == Grade::Parked);
        if parked_again {
            return Ok(latest
                .map(|latest| latest.receipt())
                .unwrap_or_else(|| Receipt {
                    flow_id: addressee.flow_id.clone(),
                    interrupt_witness: InterruptWitness::NotRequested,
                    grade: Grade::Parked,
                }));
        }
        let parked = grading.grade == Grade::Parked;
        let receipt = self.record_grade(addressee, grading)?;
        if parked {
            self.watch_parked(addressee.clone());
        }
        Ok(receipt)
    }

    fn watch_parked(self: &Arc<Self>, addressee: Addressee) {
        let nexus = Arc::clone(self);
        std::thread::spawn(move || nexus.run_watch(addressee));
    }

    fn resume(self: &Arc<Self>) -> Result<(), StoreError> {
        let parks = self.store().parks()?;
        for park in parks {
            let addressee = Addressee {
                message_id: park.message_id,
                flow_id: park.flow_id,
            };
            let Some(latest) = self.latest_receipt(&addressee)? else {
                continue;
            };
            match latest.grade {
                Grade::Parked => self.watch_parked(addressee),
                Grade::Submitted => {
                    let nexus = Arc::clone(self);
                    std::thread::spawn(move || {
                        let _ = nexus.settle(&addressee, latest.delivery_id);
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// The Observe.Agent watch of each parked recipient.
pub trait WatchesParkedRecipient {
    /// The watch of one parked recipient, on its own thread.
    fn run_watch(self: Arc<Self>, addressee: Addressee);
    fn land_parked(self: &Arc<Self>, addressee: &Addressee) -> bool;
    fn forget_watch(&self, addressee: &Addressee);
    /// Ends a parked recipient's subscription, for a Withdraw.
    fn close_watch(&self, addressee: &Addressee);
}

impl WatchesParkedRecipient for MessageNexus {
    fn run_watch(self: Arc<Self>, addressee: Addressee) {
        // A subscription that Flow ends while the recipient is still parked
        // is opened again, but only after one that carried a frame: a Flow
        // that answers nothing leaves the recipient parked until restart.
        loop {
            let Ok(edge) = self.flow_edge() else { return };
            let Ok(mut watch) = edge.observe_agent(&addressee.flow_id) else {
                eprintln!(
                    "message-nexus: cannot observe {}; {} stays parked",
                    addressee.flow_id, addressee.message_id
                );
                return;
            };
            if let (Some(closer), Ok(mut watches)) = (watch.closer(), self.agent_watches.lock()) {
                watches.insert(addressee.clone(), closer);
            }
            let mut heard = false;
            while let Some(agent_state) = watch.next_state() {
                heard = true;
                if !matches!(
                    agent_state,
                    AgentState::Idle | AgentState::Done | AgentState::Gone
                ) {
                    continue;
                }
                if !self.land_parked(&addressee) {
                    self.forget_watch(&addressee);
                    return;
                }
            }
            self.forget_watch(&addressee);
            let still_parked = self
                .latest_receipt(&addressee)
                .ok()
                .flatten()
                .is_some_and(|latest| latest.grade == Grade::Parked);
            if !heard || !still_parked {
                return;
            }
        }
    }

    fn land_parked(self: &Arc<Self>, addressee: &Addressee) -> bool {
        let _settling = self.settling();
        let Ok(Some(latest)) = self.latest_receipt(addressee) else {
            return false;
        };
        if latest.grade != Grade::Parked {
            return false;
        }
        let message = match self.store().message(&addressee.message_id) {
            Ok(Some(message)) => message,
            _ => return false,
        };
        let grading = match self.flow_edge() {
            Ok(edge) => edge
                .deliver(message.delivery_request(latest.delivery_id.clone(), &addressee.flow_id)),
            Err(_) => Err(EdgeFailure::Unreachable),
        }
        .grading(latest.delivery_id);
        if grading.grade == Grade::Parked {
            return true;
        }
        let _ = self.record_grade(addressee, grading);
        false
    }

    fn forget_watch(&self, addressee: &Addressee) {
        if let Ok(mut watches) = self.agent_watches.lock() {
            watches.remove(addressee);
        }
    }

    fn close_watch(&self, addressee: &Addressee) {
        if let Ok(mut watches) = self.agent_watches.lock()
            && let Some(stream) = watches.remove(addressee)
        {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }
}
