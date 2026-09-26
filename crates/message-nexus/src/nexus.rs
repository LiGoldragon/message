//! The running Message Nexus: its store, its edge to Flow, and who is
//! watching what.

use crate::{
    configuration::DefaultConfiguration,
    flow_edge::FlowEdge,
    store::{KeepsLedger, MessageStore, StoreError},
};
use meta_signal_message::MessageConfiguration;
use signal_flow::FlowId;
use signal_message::{MessageId, Receipt};
use std::{
    collections::HashMap,
    os::unix::net::UnixStream,
    sync::{Mutex, MutexGuard, atomic::AtomicU64, mpsc},
    time::{SystemTime, UNIX_EPOCH},
};

/// One recipient of one message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Addressee {
    pub message_id: MessageId,
    pub flow_id: FlowId,
}

pub struct MessageNexus {
    pub(crate) store: Mutex<MessageStore>,
    /// Observe subscribers, by the message they watch.
    pub(crate) observers: Mutex<HashMap<MessageId, Vec<mpsc::Sender<Receipt>>>>,
    /// The open Observe.Agent subscription of each parked recipient, kept
    /// so a Withdraw can end it.
    pub(crate) agent_watches: Mutex<HashMap<Addressee, UnixStream>>,
    /// Held while a recipient's grade is checked and moved on, so a landing
    /// parked delivery and a Withdraw never cross.
    pub(crate) settling: Mutex<()>,
    pub(crate) message_count: AtomicU64,
    /// The socket paths the listeners were bound to at start.
    pub(crate) bound: MessageConfiguration,
}

// Exception, noted here: the constructor and the clock are inherent; every
// behavior of the running Nexus lives in a trait.
impl MessageNexus {
    /// Opens the store at the default location, seeding a new one.
    pub fn open(defaults: &DefaultConfiguration) -> Result<Self, StoreError> {
        let store = MessageStore::open(&defaults.store_path(), defaults.message_configuration())?;
        let bound = store.configuration()?;
        Ok(Self {
            store: Mutex::new(store),
            observers: Mutex::new(HashMap::new()),
            agent_watches: Mutex::new(HashMap::new()),
            settling: Mutex::new(()),
            message_count: AtomicU64::new(0),
            bound,
        })
    }

    pub(crate) fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// The shared state every request reaches through.
pub trait HoldsNexusState {
    fn bound_configuration(&self) -> &MessageConfiguration;
    fn store(&self) -> MutexGuard<'_, MessageStore>;
    fn settling(&self) -> MutexGuard<'_, ()>;
    /// The Flow sockets as configured now: a Configure applies on the next
    /// call.
    fn flow_edge(&self) -> Result<FlowEdge, StoreError>;
}

impl HoldsNexusState for MessageNexus {
    fn bound_configuration(&self) -> &MessageConfiguration {
        &self.bound
    }

    fn store(&self) -> MutexGuard<'_, MessageStore> {
        self.store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn settling(&self) -> MutexGuard<'_, ()> {
        self.settling
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn flow_edge(&self) -> Result<FlowEdge, StoreError> {
        let configuration = self.store().configuration()?;
        Ok(FlowEdge {
            flow_socket_path: configuration.flow_socket_path,
            flow_meta_socket_path: configuration.flow_meta_socket_path,
        })
    }
}
