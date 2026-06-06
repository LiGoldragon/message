//! `message` runtime.
//!
//! Message is a schema-derived triad component on the emitted daemon runtime.
//! The three plane schemas (`schema/signal.schema`, `schema/nexus.schema`,
//! `schema/sema.schema`) generate the checked-in modules under `src/schema/`
//! through `schema-next` / `schema-rust-next`; the hand-written code here is the
//! thin runtime around those generated interfaces. `build.rs` regenerates and
//! verifies the modules are fresh.
//!
//! Message is a stateless stamp-and-forward ingress: the Signal plane is its
//! wire surface (`message.sock`), the Nexus plane is its internal-feature
//! catalog (the forward-to-router decision + the `ForwardToRouter` effect), and
//! the SEMA plane is honestly empty (`Stateless`). The only daemon code message
//! hand-writes is `impl ComponentDaemon for MessageDaemon` in `daemon.rs`; the
//! daemon skeleton itself is emitted into `schema/daemon.rs`.

extern crate self as message;

pub mod command;
pub mod config;
pub mod daemon;
pub mod engine;
pub mod error;
pub mod output_validator;
pub mod router;
pub mod surface;

pub mod schema {
    #[rustfmt::skip]
    pub mod signal;
    #[rustfmt::skip]
    pub mod nexus;
    #[rustfmt::skip]
    pub mod sema;
    // The daemon emitter emits `WorkingTransport::try_clone_stream` unconditionally,
    // but it is only reached on the streaming path. Message declares no stream, so
    // the method is dead in this generated module. The allow is scoped to the
    // generated module only — it does not mask dead code in message's own sources.
    // Residual: schema-rust-next should gate `try_clone_stream` on `emits_stream`.
    #[rustfmt::skip]
    #[allow(dead_code)]
    pub mod daemon;
}

pub use config::{Configuration, ConfigurationError};
pub use daemon::{MessageDaemon, MessageDaemonError};
pub use engine::MessageEngine;
pub use error::{Error, Result};
pub use router::{RouterForwarder, RouterForwardOutcome};
pub use schema::daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError};
