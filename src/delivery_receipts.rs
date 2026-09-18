//! Observation of receipts already durably addressed by one known delivery.

use signal_message::{
    DeliveryReceiptListing, DeliveryReceiptQuery, DeliveryReceiptRecord, DeliveryReceiptState,
};

use crate::{Result, delivery_address::DeliveryReceiptKey, tables::MessengerTables};

pub(crate) trait NexusDeliveryReceiptStore {
    fn read_nexus_delivery(
        &self,
        key: &str,
    ) -> Result<Option<crate::runtime_model::NexusDeliveryRecord>>;
}

impl NexusDeliveryReceiptStore for MessengerTables {
    fn read_nexus_delivery(
        &self,
        key: &str,
    ) -> Result<Option<crate::runtime_model::NexusDeliveryRecord>> {
        self.nexus_delivery(key)
    }
}

pub(crate) trait DeliveryReceiptLookup {
    fn lookup_delivery_receipts(
        &self,
        query: &DeliveryReceiptQuery,
    ) -> Result<DeliveryReceiptListing>;
}

pub(crate) struct StoredDeliveryReceipts<'store> {
    store: &'store dyn NexusDeliveryReceiptStore,
}

impl<'store> StoredDeliveryReceipts<'store> {
    pub(crate) fn new(store: &'store impl NexusDeliveryReceiptStore) -> Self {
        Self { store }
    }
}

impl DeliveryReceiptLookup for StoredDeliveryReceipts<'_> {
    fn lookup_delivery_receipts(
        &self,
        query: &DeliveryReceiptQuery,
    ) -> Result<DeliveryReceiptListing> {
        let mut delivery_receipt_states = Vec::with_capacity(query.target_flows.len());
        for flow_identifier in &query.target_flows {
            let state = match self
                .store
                .read_nexus_delivery(&query.receipt_key(flow_identifier))?
            {
                Some(record) => DeliveryReceiptState::Recorded(DeliveryReceiptRecord {
                    flow_identifier: flow_identifier.clone(),
                    receipt_kind: record.receipt_kind,
                    retryable: record.retryable,
                }),
                None => DeliveryReceiptState::Missing(flow_identifier.clone()),
            };
            delivery_receipt_states.push(state);
        }
        Ok(DeliveryReceiptListing {
            source_event_identifier: query.source_event_identifier.clone(),
            delivery_receipt_states,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct FaultingStore;

    impl NexusDeliveryReceiptStore for FaultingStore {
        fn read_nexus_delivery(
            &self,
            _key: &str,
        ) -> Result<Option<crate::runtime_model::NexusDeliveryRecord>> {
            Err(crate::Error::InvalidValidatorArgument {
                detail: "disposable receipt-store fixture fault".into(),
            })
        }
    }

    #[test]
    fn a_receipt_store_read_fault_reaches_the_engine_mapping_seam() {
        let lookup = StoredDeliveryReceipts::new(&FaultingStore);
        let result = lookup.lookup_delivery_receipts(&DeliveryReceiptQuery {
            source_event_identifier: "event-42".into(),
            target_flows: vec!["target-a".into()],
        });
        assert!(result.is_err());
    }
}
