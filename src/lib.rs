//! Messenger runtime.
//!
//! `signal-message` owns every public Message wire Type and the ordinary
//! frame. `meta-signal-message` owns the privileged frame. This component owns
//! only behavior: durable message state, provenance, delivery, and the two
//! listener runtime that executes those contracts.

pub mod client;
pub mod command;
pub mod config;
pub mod daemon;
pub mod delivery;
pub mod engine;
pub mod error;
pub mod flow_delivery;
pub mod flow_registry;
pub mod meta;
pub mod nexus_delivery;
pub mod output_validator;
pub mod provenance;
pub mod relay;
pub mod runtime_model;
pub mod store_preserve;
pub mod tables;
pub mod text;

/// The producer-owned ordinary Message contract, without local aliases.
pub use signal_message as contract;

pub use client::MessageClient;
pub use config::{
    Configuration, ConfigurationError, ConfigurationWriteRequest, ConfigurationWritten,
};
pub use daemon::{MessageDaemon, MessageDaemonError};
pub use delivery::{DeliveryDisposition, DeliveryRunner, ParkPolicy, ParkReason};
pub use engine::MessageEngine;
pub use error::{Error, Result};
pub use flow_delivery::{FlowDeliveryOutbox, ParkOutcome, ParkedDeliveryKey};
pub use flow_registry::{FlowMarkerIndex, ResolvedFlow};
pub use meta::{MetaMessageClient, MetaMessageEndpoint, MetaMessageFrameCodec};
pub use meta::{MetaMessageCommand, MetaMessageCommandEnvironment};
pub use provenance::{OriginPolicy, SenderResolver};
pub use tables::MessengerTables;
