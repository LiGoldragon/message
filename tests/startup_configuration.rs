//! `message-write-configuration` is the seam a launching peer writes across.
//! Its one inline Datom value is therefore a published shape: this test reads
//! that text, writes the binary configuration, and reads it back, so a change
//! to the shape cannot land without the launcher's text changing with it.

use message::{Configuration, ConfigurationWriteRequest, text};
use signal_message::{MessageDaemonConfiguration, OwnerIdentity};

fn configuration(directory: &std::path::Path) -> MessageDaemonConfiguration {
    MessageDaemonConfiguration {
        message_socket_path: directory.join("message.sock").display().to_string(),
        message_socket_mode: 0o600,
        supervision_socket_path: directory.join("supervision.sock").display().to_string(),
        supervision_socket_mode: 0o600,
        router_socket_path: directory.join("router.sock").display().to_string(),
        component_ingresses: Vec::new(),
        prompt_relay_permissions: vec![],
        owner_identity: OwnerIdentity::UnixUser(1000),
    }
}

fn request(directory: &std::path::Path) -> ConfigurationWriteRequest {
    ConfigurationWriteRequest {
        contract: configuration(directory),
        database_path: directory.join("messenger.sema").display().to_string(),
        owner_label: "owner".to_owned(),
        output_path: directory.join("message-daemon.rkyv").display().to_string(),
    }
}

#[test]
fn the_startup_request_round_trips_through_its_own_datom_text() {
    let directory = tempfile::tempdir().unwrap();
    let original = request(directory.path());

    let rendered = text::write(&original);
    let restored = text::read::<ConfigurationWriteRequest>(&rendered)
        .expect("the rendered startup request reads back");

    assert_eq!(restored, original);
    // Print the shape a launcher must write, so a reader of this test's
    // output sees the real text rather than a hand-spelled guess.
    println!("STARTUP REQUEST: {rendered}");
}

#[test]
fn writing_the_startup_request_produces_a_configuration_the_daemon_loads() {
    let directory = tempfile::tempdir().unwrap();
    let original = request(directory.path());
    let output_path = original.output_path.clone();
    let expected = original.contract.clone();

    let written = original.write().expect("the request writes its file");
    assert_eq!(written.output_path, output_path);

    let loaded = Configuration::from_binary_path(std::path::Path::new(&output_path))
        .expect("the daemon loads what the launcher wrote");
    assert_eq!(loaded.contract(), &expected);
    assert_eq!(loaded.owner_label(), "owner");
}

#[test]
fn a_malformed_startup_request_is_refused() {
    assert!(text::read::<ConfigurationWriteRequest>("NotARequest").is_err());
    assert!(text::read::<ConfigurationWriteRequest>("{ }").is_err());
}

/// The retired dialect wrote a parenthesised, head-led application. A launcher
/// left on that form must be refused loudly rather than half-read, which is
/// the failure that blocked the persona gate before this port.
#[test]
fn the_retired_parenthesised_form_is_refused() {
    let retired = "(ConfigurationWriteRequest /run/message.sock /run/supervision.sock \
                   /run/router.sock /run/messenger.sema owner 1000 /run/daemon.rkyv)";
    assert!(
        text::read::<ConfigurationWriteRequest>(retired).is_err(),
        "the retired parenthesised startup form must not parse"
    );
}
