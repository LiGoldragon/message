//! A new store admits ordinary Configure until meta Configure settles it.

mod support;

use signal_message::{Configuration, Query, Response};
use std::path::Path;
use support::{NexusProcess, fake_flow::FakeFlow};

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn configuration(runtime: &Path) -> Configuration {
    Configuration {
        ordinary: path_text(&runtime.join("moved/message.sock")),
        meta: path_text(&runtime.join("moved/message-owner.sock")),
        flow: path_text(&runtime.join("flow/flow.sock")),
    }
}

#[test]
fn configuration_is_stored_and_meta_closes_the_ordinary_path() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let configuration = configuration(runtime.path());
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);

    assert_eq!(
        nexus.ask(&Query::Configure(configuration.clone())),
        Response::Configured
    );
    assert_eq!(
        nexus.ask_meta(&meta_signal_message::Query::Configure(
            configuration.clone()
        )),
        meta_signal_message::Response::Configured
    );
    assert_eq!(
        nexus.ask(&Query::Configure(configuration.clone())),
        Response::Refused(signal_message::SendRefusal::AlreadyConfigured)
    );

    let (home, runtime) = nexus.stop();
    let moved_ordinary = Path::new(&configuration.ordinary).to_path_buf();
    let moved_meta = Path::new(&configuration.meta).to_path_buf();
    let nexus = NexusProcess::start_listening_at(home, runtime, moved_ordinary, moved_meta);
    assert_eq!(
        nexus.ask_meta(&meta_signal_message::Query::Configure(configuration)),
        meta_signal_message::Response::Configured
    );
    assert_eq!(flow.bound().len(), 2);
}
