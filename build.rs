use std::{env, path::PathBuf};

use schema_rust_next::{
    NexusDaemonShape, WorkingListenerTier,
    build::{GenerationDriver, GenerationPlan, ModuleEmission},
};

fn main() {
    SchemaBuild::from_environment().run();
}

struct SchemaBuild {
    crate_root: PathBuf,
}

impl SchemaBuild {
    fn from_environment() -> Self {
        Self {
            crate_root: PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir set")),
        }
    }

    fn run(&self) {
        println!("cargo:rerun-if-changed=schema/signal.schema");
        println!("cargo:rerun-if-changed=src/schema/signal.rs");
        println!("cargo:rerun-if-changed=schema/nexus.schema");
        println!("cargo:rerun-if-changed=src/schema/nexus.rs");
        println!("cargo:rerun-if-changed=schema/sema.schema");
        println!("cargo:rerun-if-changed=src/schema/sema.rs");
        println!("cargo:rerun-if-changed=src/schema/daemon.rs");

        let plan = GenerationPlan::new(&self.crate_root, "message", "0.2.0")
            .with_module(ModuleEmission::signal_runtime_module("signal"))
            .with_module(ModuleEmission::nexus_runtime())
            .with_module(ModuleEmission::sema_runtime())
            .with_module(ModuleEmission::daemon_module("signal", self.daemon_shape()));
        GenerationDriver::new(plan)
            .generate()
            .expect("generate message schema artifacts")
            .write_or_check("MESSAGE_UPDATE_SCHEMA_ARTIFACTS")
            .expect("checked-in message schema artifacts are fresh");
    }

    /// Message's daemon shape: the `message-daemon` process bound to a single
    /// working signal listener (`schema/signal.schema`). Message has no
    /// owner-only meta tier — it is a stateless stamp-and-forward ingress with
    /// one peer-callable socket, so no `with_meta_tier`.
    fn daemon_shape(&self) -> NexusDaemonShape {
        NexusDaemonShape::new("message-daemon", WorkingListenerTier::new("signal"))
    }
}
