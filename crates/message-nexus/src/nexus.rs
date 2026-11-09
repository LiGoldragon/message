//! The running Message Nexus and its configuration store.

use crate::{
    configuration::SeedsConfiguration,
    delivery_consumer::{DeliveryConsumer, DeliveryConsumerError, OpensDeliveryConsumer},
    flow_edge::{CallsFlow, FlowEdge},
    generated::memory::{MetaConfigured, Standard},
    peer::ReadsProcess,
    store::{KeepsStandard, MessageStore, OpensStore, StoreError},
};
use message_defaults::{DefaultConfiguration, LaysOutDefaults};
use message_library::Configuration;
use signal_flow::{Address, Aspect, Model, Topic};
use std::sync::{Mutex, MutexGuard};

pub struct MessageNexus {
    pub(crate) store: Mutex<MessageStore>,
    pub(crate) bound: Configuration,
    pub(crate) delivery_consumer: DeliveryConsumer,
}

#[derive(Debug, thiserror::Error)]
pub enum NexusError {
    #[error("message store: {0}")]
    Store(#[from] StoreError),
    #[error("cannot read this process identity: {0}")]
    Process(#[from] std::io::Error),
    #[error("Flow bind failed: {0:?}")]
    Flow(crate::flow_edge::EdgeFailure),
    #[error("Message delivery consumer: {0}")]
    DeliveryConsumer(#[from] DeliveryConsumerError),
}

pub trait OpensNexus: Sized {
    fn open(defaults: &DefaultConfiguration) -> Result<Self, NexusError>;
}

impl OpensNexus for MessageNexus {
    fn open(defaults: &DefaultConfiguration) -> Result<Self, NexusError> {
        let seed = Standard {
            configuration: defaults.message_configuration(),
            meta_configured: MetaConfigured(false),
        };
        let mut store = MessageStore::open(&defaults.store_path(), seed)?;
        let standard = store.standard()?;
        let edge = FlowEdge {
            socket_path: standard.configuration.flow.0.clone(),
        };
        let process = signal_flow::Process::this_process()?;
        let address = Address {
            aspect: Aspect::Field,
            topic: Topic("message".into()),
            model: Model::Astra,
        };
        edge.bind(address, process).map_err(NexusError::Flow)?;
        let delivery_consumer = DeliveryConsumer::open(&edge, &mut store)?;
        Ok(Self {
            store: Mutex::new(store),
            bound: standard.configuration,
            delivery_consumer,
        })
    }
}

pub trait HoldsNexusState {
    fn store(&self) -> MutexGuard<'_, MessageStore>;
    fn bound_configuration(&self) -> &Configuration;
    fn flow_edge(&self) -> Result<FlowEdge, StoreError>;
}

impl HoldsNexusState for MessageNexus {
    fn store(&self) -> MutexGuard<'_, MessageStore> {
        self.store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn bound_configuration(&self) -> &Configuration {
        &self.bound
    }

    fn flow_edge(&self) -> Result<FlowEdge, StoreError> {
        Ok(FlowEdge {
            socket_path: self.store().standard()?.configuration.flow.0,
        })
    }
}
