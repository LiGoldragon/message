//! The shared key domain for direct delivery and receipt lookup.

use std::collections::HashSet;

use signal_message::{
    DeliveryAddressSelectionRejection, DeliveryReceiptQuery, DeliveryRequest, FlowIdentifier,
    SourceEventIdentifier, TargetFlows,
};

const EVENT_SENTINEL: &str = "event";
const MAXIMUM_TARGET_FLOWS: usize = 64;

pub(crate) trait DeliveryAddressSelection {
    fn source_event_identifier(&self) -> &SourceEventIdentifier;
    fn target_flows(&self) -> &TargetFlows;

    fn validate_address_selection(&self) -> Result<(), DeliveryAddressSelectionRejection> {
        let source_event_identifier = self.source_event_identifier();
        if source_event_identifier.is_empty() {
            return Err(DeliveryAddressSelectionRejection::EmptySourceEventIdentifier);
        }
        if source_event_identifier == EVENT_SENTINEL {
            return Err(DeliveryAddressSelectionRejection::ReservedSourceEventIdentifier);
        }
        if source_event_identifier.contains('\0') {
            return Err(DeliveryAddressSelectionRejection::SourceEventIdentifierContainsNull);
        }
        let target_flows = self.target_flows();
        if target_flows.is_empty() {
            return Err(DeliveryAddressSelectionRejection::EmptyTargetFlows);
        }
        if target_flows.len() > MAXIMUM_TARGET_FLOWS {
            return Err(DeliveryAddressSelectionRejection::TooManyTargetFlows);
        }
        let mut seen = HashSet::with_capacity(target_flows.len());
        for target_flow in target_flows {
            if target_flow.is_empty() {
                return Err(DeliveryAddressSelectionRejection::EmptyTargetFlowIdentifier);
            }
            if target_flow.contains('\0') {
                return Err(DeliveryAddressSelectionRejection::TargetFlowIdentifierContainsNull);
            }
            if !seen.insert(target_flow) {
                return Err(DeliveryAddressSelectionRejection::DuplicateTargetFlows);
            }
        }
        Ok(())
    }
}

impl DeliveryAddressSelection for DeliveryRequest {
    fn source_event_identifier(&self) -> &SourceEventIdentifier {
        &self.source_event_identifier
    }

    fn target_flows(&self) -> &TargetFlows {
        &self.target_flows
    }
}

impl DeliveryAddressSelection for DeliveryReceiptQuery {
    fn source_event_identifier(&self) -> &SourceEventIdentifier {
        &self.source_event_identifier
    }

    fn target_flows(&self) -> &TargetFlows {
        &self.target_flows
    }
}

pub(crate) trait DeliveryReceiptKey {
    fn receipt_key(&self, target_flow: &FlowIdentifier) -> String;
}

impl DeliveryReceiptKey for DeliveryReceiptQuery {
    fn receipt_key(&self, target_flow: &FlowIdentifier) -> String {
        format!("{}\0{target_flow}", self.source_event_identifier)
    }
}
