//! The one Message-owned replay consumer for deliveries admitted by Message.

use crate::{
    flow_edge::{
        CallsFlow, EdgeFailure, FlowEdge, FlowSendReply, FlowSubscription, ReadsFlowSubscription,
    },
    request_tracking::{ClassifiesOrdered, OrderedClassifier, TracksRequests},
    store::{KeepsRequestTracking, MessageStore, StoreError},
};
use signal_flow::{Response, VisibleDeliverySnapshot, VisibleSendObservation};
use std::{os::unix::net::UnixStream, sync::Mutex};

#[derive(Debug, thiserror::Error)]
pub enum DeliveryConsumerError {
    #[error("message store: {0}")]
    Store(#[from] StoreError),
    #[error("Flow delivery observation: {0:?}")]
    Flow(EdgeFailure),
    #[error("Flow did not open the Message delivery observation")]
    Opening,
    #[error("request tracking: {0:?}")]
    Tracking(crate::request_tracking::TrackingError),
}

pub struct DeliveryConsumer {
    subscription: Mutex<FlowSubscription>,
    classifier: Mutex<OrderedClassifier<VisibleDeliverySnapshot>>,
    read_highwater: Mutex<i64>,
}

pub trait OpensDeliveryConsumer: Sized {
    fn open(edge: &FlowEdge, store: &mut MessageStore) -> Result<Self, DeliveryConsumerError>;
}

pub trait ConsumesDeliveries {
    fn interrupt_handle(&self) -> Result<UnixStream, DeliveryConsumerError>;
    fn receive_next(&self) -> Result<VisibleDeliverySnapshot, DeliveryConsumerError>;
    fn accept_snapshot(
        &self,
        store: &MessageStore,
        snapshot: VisibleDeliverySnapshot,
    ) -> Result<(), DeliveryConsumerError>;
    fn reclassify(&self, store: &MessageStore) -> Result<(), DeliveryConsumerError>;
}

trait AppliesDeliveryObservation {
    fn apply_opening(
        &self,
        store: &MessageStore,
        observation: VisibleSendObservation,
    ) -> Result<(), DeliveryConsumerError>;
    fn apply_snapshot(
        &self,
        store: &MessageStore,
        snapshot: VisibleDeliverySnapshot,
    ) -> Result<(), DeliveryConsumerError>;
    fn commit_ready(&self, store: &MessageStore) -> Result<(), DeliveryConsumerError>;
}

impl OpensDeliveryConsumer for DeliveryConsumer {
    fn open(edge: &FlowEdge, store: &mut MessageStore) -> Result<Self, DeliveryConsumerError> {
        let cursor = store
            .request_tracking()?
            .opening_cursor()
            .map_err(DeliveryConsumerError::Tracking)?;
        let mut subscription = edge
            .observe_deliveries(cursor)
            .map_err(DeliveryConsumerError::Flow)?;
        let Response::SendsObserved(opening) =
            subscription.next().map_err(DeliveryConsumerError::Flow)?
        else {
            return Err(DeliveryConsumerError::Opening);
        };
        let identity_changed = store
            .request_tracking()?
            .store_identity
            .as_ref()
            .is_some_and(|identity| identity != &opening.store_identity);
        let (subscription, opening) = if identity_changed {
            let mut tracking = store.request_tracking()?;
            tracking.observe_store(opening.store_identity);
            store.replace_request_tracking(tracking)?;
            let mut replacement = edge
                .observe_deliveries(None)
                .map_err(DeliveryConsumerError::Flow)?;
            let Response::SendsObserved(reopened) =
                replacement.next().map_err(DeliveryConsumerError::Flow)?
            else {
                return Err(DeliveryConsumerError::Opening);
            };
            (replacement, reopened)
        } else {
            (subscription, opening)
        };
        let consumer = Self {
            subscription: Mutex::new(subscription),
            classifier: Mutex::new(OrderedClassifier::default()),
            read_highwater: Mutex::new(opening.delivery_cursor),
        };
        consumer.apply_opening(store, opening)?;

        let pending = store.request_tracking()?.pending_deliver_keys();
        for key in pending {
            let request = store
                .request_tracking()?
                .outbox
                .iter()
                .find(|entry| entry.deliver_key == key)
                .map(|entry| entry.request.clone());
            let Some(request) = request else {
                continue;
            };
            match edge.deliver(
                request.lock,
                request.deliver_key.clone(),
                request.sendable_message,
            ) {
                Ok(FlowSendReply::Queued(acceptance)) => {
                    let mut tracking = store.request_tracking()?;
                    tracking
                        .record_acceptance(&key, acceptance.request_id, acceptance.delivery_cursor)
                        .map_err(DeliveryConsumerError::Tracking)?;
                    store.replace_request_tracking(tracking)?;
                }
                Err(EdgeFailure::Refused(_) | EdgeFailure::DeliverKeyReused) => {
                    let mut tracking = store.request_tracking()?;
                    tracking
                        .record_refusal(&key)
                        .map_err(DeliveryConsumerError::Tracking)?;
                    store.replace_request_tracking(tracking)?;
                }
                Err(_) => {}
            }
        }
        let acknowledgements = store.request_tracking()?.acknowledgement_keys();
        if !acknowledgements.is_empty() {
            let _ = edge.acknowledge(acknowledgements);
        }
        consumer.commit_ready(store)?;
        Ok(consumer)
    }
}

impl ConsumesDeliveries for DeliveryConsumer {
    fn interrupt_handle(&self) -> Result<UnixStream, DeliveryConsumerError> {
        self.subscription
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .interrupt_handle()
            .map_err(DeliveryConsumerError::Flow)
    }

    fn receive_next(&self) -> Result<VisibleDeliverySnapshot, DeliveryConsumerError> {
        let response = self
            .subscription
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next()
            .map_err(DeliveryConsumerError::Flow)?;
        match response {
            Response::SendChanged(snapshot) => Ok(snapshot),
            _ => Err(DeliveryConsumerError::Opening),
        }
    }

    fn accept_snapshot(
        &self,
        store: &MessageStore,
        snapshot: VisibleDeliverySnapshot,
    ) -> Result<(), DeliveryConsumerError> {
        self.apply_snapshot(store, snapshot)
    }

    fn reclassify(&self, store: &MessageStore) -> Result<(), DeliveryConsumerError> {
        self.commit_ready(store)
    }
}

impl AppliesDeliveryObservation for DeliveryConsumer {
    fn apply_opening(
        &self,
        store: &MessageStore,
        observation: VisibleSendObservation,
    ) -> Result<(), DeliveryConsumerError> {
        let mut tracking = store.request_tracking()?;
        tracking.observe_store(observation.store_identity);
        store.replace_request_tracking(tracking)?;
        *self
            .read_highwater
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = observation.delivery_cursor;
        let mut classifier = self
            .classifier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for snapshot in observation.visible_delivery_snapshot_vector {
            classifier.push(snapshot);
        }
        drop(classifier);
        self.commit_ready(store)
    }

    fn apply_snapshot(
        &self,
        store: &MessageStore,
        snapshot: VisibleDeliverySnapshot,
    ) -> Result<(), DeliveryConsumerError> {
        *self
            .read_highwater
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot.delivery_cursor;
        self.classifier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(snapshot);
        self.commit_ready(store)
    }

    fn commit_ready(&self, store: &MessageStore) -> Result<(), DeliveryConsumerError> {
        let highwater = *self
            .read_highwater
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut classifier = self
            .classifier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let tracking = store.request_tracking()?;
        let writes = classifier
            .classify_ready(&tracking, highwater)
            .map_err(DeliveryConsumerError::Tracking)?;
        for write in writes {
            store.replace_request_tracking(write)?;
        }
        if classifier.held_len() == 0 {
            let mut tracking = store.request_tracking()?;
            tracking
                .advance_cursor(highwater)
                .map_err(DeliveryConsumerError::Tracking)?;
            store.replace_request_tracking(tracking)?;
        }
        Ok(())
    }
}
