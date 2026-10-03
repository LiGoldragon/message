use message_nexus::{
    DefaultConfiguration, LaysOutDefaults, ListensOnSockets, MessageNexus, OpensNexus, ReadsAnchors,
};
use std::{process::ExitCode, sync::Arc};

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if std::env::args().count() > 1 {
        eprintln!("message-nexus: a Nexus starts with no arguments");
        return ExitCode::FAILURE;
    }
    let defaults = DefaultConfiguration::from_environment();
    if let Err(error) = std::fs::create_dir_all(defaults.state_directory()) {
        eprintln!("message-nexus: state directory: {error}");
        return ExitCode::FAILURE;
    }
    let nexus = match MessageNexus::open(&defaults) {
        Ok(nexus) => Arc::new(nexus),
        Err(error) => {
            eprintln!(
                "message-nexus: store {}: {error}",
                defaults.store_path().display()
            );
            return ExitCode::FAILURE;
        }
    };
    match nexus.serve() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("message-nexus: {error}");
            ExitCode::FAILURE
        }
    }
}
