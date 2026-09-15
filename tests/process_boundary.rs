use std::time::Duration;

use message::{
    Configuration, MessageDaemon, MetaMessageClient, MetaMessageEndpoint, client::MessageSocket,
};
use meta_signal_message::{Query as MetaQuery, Response as MetaResponse};
use signal_message::{MessageDaemonConfiguration, OwnerIdentity, Query, Response};

fn contract(directory: &std::path::Path) -> MessageDaemonConfiguration {
    MessageDaemonConfiguration {
        message_socket_path: directory
            .join("message.sock")
            .to_string_lossy()
            .into_owned(),
        message_socket_mode: 0o600,
        supervision_socket_path: directory
            .join("meta-message.sock")
            .to_string_lossy()
            .into_owned(),
        supervision_socket_mode: 0o600,
        router_socket_path: directory.join("router.sock").to_string_lossy().into_owned(),
        component_ingresses: Vec::new(),
        owner_identity: OwnerIdentity::UnixUser(i64::from(rustix::process::getuid().as_raw())),
    }
}

/// Wait on the tested event — the listener binding its socket — with a
/// generous upper bound so a stuck daemon fails the run instead of hanging it.
/// The bound is a timeout, not a timing assumption: an unoptimised daemon
/// opening its durable store takes seconds on a loaded builder.
fn wait_for(path: &std::path::Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("socket did not appear within the bound: {}", path.display());
}

#[test]
fn daemon_executes_both_producer_owned_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let contract = contract(directory.path());
    let configuration = Configuration::new(
        contract.clone(),
        directory.path().join("messenger.sema"),
        "owner",
    )
    .unwrap();
    let configuration_path = directory.path().join("message.configuration");
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();

    std::thread::spawn(move || {
        MessageDaemon::from_configuration_path(&configuration_path)
            .unwrap()
            .run()
            .unwrap();
    });
    wait_for(configuration.socket_path());
    wait_for(configuration.meta_socket_path());

    let output = MessageSocket::from_path(configuration.socket_path())
        .client()
        .submit(Query::QueryInbox("empty".to_owned()))
        .unwrap();
    match output {
        Response::InboxListing(listing) => assert!(listing.messages.is_empty()),
        other => panic!("unexpected ordinary reply: {other:?}"),
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let reply = runtime
        .block_on(
            MetaMessageClient::new(MetaMessageEndpoint::new(configuration.meta_socket_path()))
                .submit(MetaQuery::Configure(contract.clone())),
        )
        .unwrap();
    assert!(matches!(reply, MetaResponse::OperationUnimplemented(_)));
}
