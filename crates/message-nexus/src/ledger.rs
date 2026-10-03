//! Receipts: each grade a recipient reaches is appended, never rewritten,
//! and every Observe subscriber of the message hears it.

use crate::{
    nexus::{Addressee, HoldsNexusState, MessageNexus, StampsLedger},
    store::{KeepsLedger, ReceiptRecord, StoreError},
};
use meta_signal_flow::{DeliveryId, InterruptWitness};
use signal_message::{Grade, MessageId, Receipt, Submission};
use std::{
    sync::{atomic::Ordering, mpsc},
    time::SystemTime,
};

/// A ledger row as the wire reports it.
pub trait ReportsReceipt {
    fn receipt(&self) -> Receipt;
}

impl ReportsReceipt for ReceiptRecord {
    fn receipt(&self) -> Receipt {
        Receipt {
            flow_id: self.flow_id.clone(),
            interrupt_witness: self.interrupt_witness.clone(),
            grade: self.grade.clone(),
        }
    }
}

/// A grade about to be recorded for one recipient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grading {
    pub delivery_id: DeliveryId,
    pub interrupt_witness: InterruptWitness,
    pub grade: Grade,
}

pub trait RecordsReceipts {
    /// A new, unique MessageId.
    fn new_message_id(&self) -> MessageId;
    /// Each recipient's latest receipt, in the message's recipient order.
    fn latest_receipts(&self, message_id: &str) -> Result<Vec<ReceiptRecord>, StoreError>;
    fn latest_receipt(&self, addressee: &Addressee) -> Result<Option<ReceiptRecord>, StoreError>;
    /// Appends a receipt and tells the message's observers.
    fn record_grade(&self, addressee: &Addressee, grading: Grading) -> Result<Receipt, StoreError>;
    fn submission(&self, message_id: &str) -> Result<Submission, StoreError>;
    /// Registers an observer of a message's later receipts.
    fn subscribe(&self, message_id: &str) -> mpsc::Receiver<Receipt>;
}

impl RecordsReceipts for MessageNexus {
    fn new_message_id(&self) -> MessageId {
        let count = self.message_count.fetch_add(1, Ordering::Relaxed);
        format!(
            "m-{:x}{:03x}",
            SystemTime::now().ledger_stamp(),
            count % 0x1000
        )
    }

    fn latest_receipts(&self, message_id: &str) -> Result<Vec<ReceiptRecord>, StoreError> {
        let store = self.store();
        let Some(message) = store.message(message_id)? else {
            return Ok(Vec::new());
        };
        let receipts = store.receipts(message_id)?;
        Ok(message
            .flow_id_vector
            .iter()
            .filter_map(|flow_id| {
                receipts
                    .iter()
                    .rev()
                    .find(|receipt| &receipt.flow_id == flow_id)
                    .cloned()
            })
            .collect())
    }

    fn latest_receipt(&self, addressee: &Addressee) -> Result<Option<ReceiptRecord>, StoreError> {
        Ok(self
            .store()
            .receipts(&addressee.message_id)?
            .into_iter()
            .rev()
            .find(|receipt| receipt.flow_id == addressee.flow_id))
    }

    fn record_grade(&self, addressee: &Addressee, grading: Grading) -> Result<Receipt, StoreError> {
        let record = {
            let store = self.store();
            let sequence =
                i64::try_from(store.receipts(&addressee.message_id)?.len()).unwrap_or(i64::MAX);
            let record = ReceiptRecord {
                message_id: addressee.message_id.clone(),
                sequence,
                flow_id: addressee.flow_id.clone(),
                delivery_id: grading.delivery_id,
                interrupt_witness: grading.interrupt_witness,
                grade: grading.grade,
                stamped_at: SystemTime::now().ledger_stamp(),
            };
            store.append_receipt(record.clone())?;
            record
        };
        let receipt = record.receipt();
        if let Ok(mut observers) = self.observers.lock()
            && let Some(listening) = observers.get_mut(&addressee.message_id)
        {
            listening.retain(|observer| observer.send(receipt.clone()).is_ok());
        }
        Ok(receipt)
    }

    fn submission(&self, message_id: &str) -> Result<Submission, StoreError> {
        Ok(Submission {
            message_id: message_id.to_owned(),
            receipt_vector: self
                .latest_receipts(message_id)?
                .iter()
                .map(ReportsReceipt::receipt)
                .collect(),
        })
    }

    fn subscribe(&self, message_id: &str) -> mpsc::Receiver<Receipt> {
        let (sender, receiver) = mpsc::channel();
        if let Ok(mut observers) = self.observers.lock() {
            observers
                .entry(message_id.to_owned())
                .or_default()
                .push(sender);
        }
        receiver
    }
}
