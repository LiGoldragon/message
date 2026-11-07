//! The one Message-owned replay consumer for deliveries admitted by Message.

use crate::{
    flow_edge::{
        CallsFlow, EdgeFailure, FLOW_RESPONSE_TIMEOUT, FlowEdge, FlowSendReply, FlowSubscription,
        ReadsFlowSubscription,
    },
    request_tracking::{ClassifiesOrdered, OrderedClassifier, TracksRequests},
    store::{KeepsRequestTracking, MessageStore, StoreError},
};
use signal_flow::{Response, VisibleDeliverySnapshot, VisibleSendObservation};
use std::{
    net::Shutdown,
    os::unix::net::UnixStream,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

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
    interrupt: Mutex<UnixStream>,
    classifier: Mutex<OrderedClassifier<VisibleDeliverySnapshot>>,
    read_highwater: Mutex<i64>,
    stopping: AtomicBool,
    reconnect_wait: Mutex<()>,
    reconnect_wake: Condvar,
}

pub trait OpensDeliveryConsumer: Sized {
    fn open(edge: &FlowEdge, store: &mut MessageStore) -> Result<Self, DeliveryConsumerError>;
}

pub trait ConsumesDeliveries {
    fn receive_next(&self) -> Result<VisibleDeliverySnapshot, DeliveryConsumerError>;
    fn reconnect(&self, edge: &FlowEdge, store: &MessageStore)
    -> Result<(), DeliveryConsumerError>;
    fn stop(&self) -> Result<(), DeliveryConsumerError>;
    fn is_stopping(&self) -> bool;
    fn wait_to_reconnect(&self) -> bool;
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
    fn recover_deliveries(
        &self,
        edge: &FlowEdge,
        store: &MessageStore,
    ) -> Result<(), DeliveryConsumerError>;
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
        subscription
            .keep_open()
            .map_err(DeliveryConsumerError::Flow)?;
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
            replacement
                .keep_open()
                .map_err(DeliveryConsumerError::Flow)?;
            (replacement, reopened)
        } else {
            (subscription, opening)
        };
        let interrupt = subscription
            .interrupt_handle()
            .map_err(DeliveryConsumerError::Flow)?;
        let consumer = Self {
            subscription: Mutex::new(subscription),
            interrupt: Mutex::new(interrupt),
            classifier: Mutex::new(OrderedClassifier::default()),
            read_highwater: Mutex::new(opening.delivery_cursor),
            stopping: AtomicBool::new(false),
            reconnect_wait: Mutex::new(()),
            reconnect_wake: Condvar::new(),
        };
        consumer.apply_opening(store, opening)?;
        consumer.recover_deliveries(edge, store)?;
        consumer.commit_ready(store)?;
        Ok(consumer)
    }
}

impl ConsumesDeliveries for DeliveryConsumer {
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

    fn reconnect(
        &self,
        edge: &FlowEdge,
        store: &MessageStore,
    ) -> Result<(), DeliveryConsumerError> {
        let cursor = store
            .request_tracking()?
            .opening_cursor()
            .map_err(DeliveryConsumerError::Tracking)?;
        let mut replacement = edge
            .observe_deliveries(cursor)
            .map_err(DeliveryConsumerError::Flow)?;
        let Response::SendsObserved(mut opening) =
            replacement.next().map_err(DeliveryConsumerError::Flow)?
        else {
            return Err(DeliveryConsumerError::Opening);
        };
        replacement
            .keep_open()
            .map_err(DeliveryConsumerError::Flow)?;
        let identity_changed = store
            .request_tracking()?
            .store_identity
            .as_ref()
            .is_some_and(|identity| identity != &opening.store_identity);
        if identity_changed {
            let mut tracking = store.request_tracking()?;
            tracking.observe_store(opening.store_identity);
            store.replace_request_tracking(tracking)?;
            replacement = edge
                .observe_deliveries(None)
                .map_err(DeliveryConsumerError::Flow)?;
            let Response::SendsObserved(reopened) =
                replacement.next().map_err(DeliveryConsumerError::Flow)?
            else {
                return Err(DeliveryConsumerError::Opening);
            };
            replacement
                .keep_open()
                .map_err(DeliveryConsumerError::Flow)?;
            opening = reopened;
        }
        let interrupt = replacement
            .interrupt_handle()
            .map_err(DeliveryConsumerError::Flow)?;
        if self.is_stopping() {
            return Err(DeliveryConsumerError::Flow(EdgeFailure::Broken));
        }
        *self
            .interrupt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = interrupt;
        *self
            .subscription
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = replacement;
        *self
            .classifier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = OrderedClassifier::default();
        self.apply_opening(store, opening)?;
        self.recover_deliveries(edge, store)?;
        self.commit_ready(store)
    }

    fn stop(&self) -> Result<(), DeliveryConsumerError> {
        self.stopping.store(true, Ordering::Release);
        self.reconnect_wake.notify_all();
        self.interrupt
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .shutdown(Shutdown::Both)
            .map_err(|_| DeliveryConsumerError::Flow(EdgeFailure::Broken))
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    fn wait_to_reconnect(&self) -> bool {
        let wait = self
            .reconnect_wait
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = self
            .reconnect_wake
            .wait_timeout_while(wait, FLOW_RESPONSE_TIMEOUT, |_| !self.is_stopping())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        !self.is_stopping()
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
        loop {
            let tracking = store.request_tracking()?;
            let Some(write) = classifier
                .next_ready(&tracking, highwater)
                .map_err(DeliveryConsumerError::Tracking)?
            else {
                break;
            };
            store.replace_request_tracking(write)?;
            classifier.commit_front();
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

    fn recover_deliveries(
        &self,
        edge: &FlowEdge,
        store: &MessageStore,
    ) -> Result<(), DeliveryConsumerError> {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        flow_edge::FlowSubscription,
        generated::memory::Standard,
        message_library::Configuration,
        store::{InjectsTrackingFailure, OpensStore},
    };
    use signal_flow::{Address, Aspect, Model, VisibleDeliveryState, VisibleSender};

    fn seed() -> Standard {
        Standard {
            configuration: Configuration {
                ordinary: "/run/message.sock".into(),
                meta: "/run/message-meta.sock".into(),
                flow: "/run/flow.sock".into(),
            },
            meta_configured: false,
        }
    }

    fn consumer() -> DeliveryConsumer {
        let (stream, peer) = UnixStream::pair().unwrap();
        let interrupt = stream.try_clone().unwrap();
        drop(peer);
        DeliveryConsumer {
            subscription: Mutex::new(FlowSubscription::from_test_stream(stream)),
            interrupt: Mutex::new(interrupt),
            classifier: Mutex::new(OrderedClassifier::default()),
            read_highwater: Mutex::new(0),
            stopping: AtomicBool::new(false),
            reconnect_wait: Mutex::new(()),
            reconnect_wake: Condvar::new(),
        }
    }

    #[test]
    fn failed_live_effect_write_is_reclassified_before_cursor_and_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.sema");
        let store = MessageStore::open(&path, seed()).unwrap();
        let mut tracking = store.request_tracking().unwrap();
        tracking.observe_store("flow-store".into());
        store.replace_request_tracking(tracking).unwrap();
        let address = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            model: Model::Sol,
        };
        let snapshot = VisibleDeliverySnapshot {
            request_id: "outside".into(),
            delivery_cursor: 1,
            visible_sender: VisibleSender::Sender(address.clone()),
            address,
            visible_delivery_state: VisibleDeliveryState::Delivered,
        };
        let consumer = consumer();

        store.fail_next_tracking_replacement();
        assert!(matches!(
            consumer.apply_snapshot(&store, snapshot),
            Err(DeliveryConsumerError::Store(
                StoreError::InjectedTrackingReplacement
            ))
        ));
        assert_eq!(consumer.classifier.lock().unwrap().held_len(), 1);
        let unchanged = store.request_tracking().unwrap();
        assert!(unchanged.classified_events.is_empty());
        assert_eq!(unchanged.observation_cursor, None);

        consumer.reclassify(&store).unwrap();
        assert_eq!(consumer.classifier.lock().unwrap().held_len(), 0);
        drop(store);
        let reopened = MessageStore::open(&path, seed()).unwrap();
        let committed = reopened.request_tracking().unwrap();
        assert_eq!(committed.observation_cursor, Some(1));
        assert_eq!(committed.classified_events.len(), 1);
        assert_eq!(committed.classified_events[0].delivery_cursor, 1);
    }
}
