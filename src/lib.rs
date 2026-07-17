//! `message` runtime.
//!
//! Message is a schema-derived triad component on the emitted daemon runtime.
//! The three plane schemas (`schema/signal.schema`, `schema/nexus.schema`,
//! `schema/sema.schema`) generate the checked-in modules under `src/schema/`
//! through `schema` / `schema-rust`; the hand-written code here is the
//! thin runtime around those generated interfaces. `build.rs` regenerates and
//! verifies the modules are fresh.
//!
//! The messenger owns durable state: the Signal plane is its wire surface
//! (`message.sock`), the Nexus plane is its internal-feature catalog (the
//! forward-to-router decision plus the registry apply/read effects), and the
//! SEMA plane commits the agent registry — the authoritative process↔identity
//! map and local delivery registry — in `messenger.sema`. The only daemon
//! code message hand-writes is `impl ComponentDaemon for MessageDaemon` in
//! `daemon.rs`; the daemon skeleton itself is emitted into `schema/daemon.rs`.

pub mod client;
#[cfg(feature = "nota-text")]
pub mod command;
pub mod config;
pub mod daemon;
pub mod engine;
pub mod error;
pub(crate) mod frame_bytes;
pub mod meta;
#[cfg(feature = "nota-text")]
pub mod output_validator;
pub mod router;
pub mod tables;
#[cfg(feature = "nota-text")]
pub mod surface;

pub mod schema {
    #[rustfmt::skip]
    pub mod signal;
    #[rustfmt::skip]
    pub mod nexus;
    #[rustfmt::skip]
    pub mod sema;
    #[rustfmt::skip]
    pub mod daemon;
}

pub use config::{Configuration, ConfigurationError};
pub use daemon::{MessageDaemon, MessageDaemonError};
pub use engine::MessageEngine;
pub use error::{Error, Result};
pub use meta::{MetaMessageClient, MetaMessageEndpoint, MetaMessageFrameCodec};
#[cfg(feature = "nota-text")]
pub use meta::{MetaMessageCommand, MetaMessageCommandEnvironment};
pub use router::{RouterForwardOutcome, RouterForwarder};
pub use tables::MessengerTables;
pub use schema::daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError};
