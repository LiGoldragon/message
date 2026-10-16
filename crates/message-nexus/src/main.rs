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
    // Block before opening the Engine: its threads must inherit this mask.
    // The listener consumes these signals through signalfd, outside a handler.
    let signal_error = unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGINT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut())
    };
    if signal_error != 0 {
        eprintln!(
            "message-nexus: block shutdown signals: {}",
            std::io::Error::from_raw_os_error(signal_error)
        );
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
