use std::time::Duration;

use message::{
    Configuration, MessageDaemon, MetaMessageClient, MetaMessageEndpoint, client::MessageSocket,
};
use meta_signal_message::schema::lib::{z2VYLc, z2Vc2e};
use signal_message::schema::lib::{
    Input, Output, z2VL2C, z2VPa3, z2VPn2, z2VQY5, z2VRJp, z2VRPH, z2VSVi, z2VUUz, z2VUqb, z2VYZK,
    z2VZv9, z2VaVk, z2Vari,
};

fn contract(directory: &std::path::Path) -> z2VL2C {
    z2VL2C {
        field_0: z2VUUz::new(z2VQY5::new(
            directory
                .join("message.sock")
                .to_string_lossy()
                .into_owned(),
        )),
        field_1: z2VPa3::new(z2VYZK::new(0o600)),
        field_2: z2VRJp::new(z2VQY5::new(
            directory
                .join("meta-message.sock")
                .to_string_lossy()
                .into_owned(),
        )),
        field_3: z2VaVk::new(z2VYZK::new(0o600)),
        field_4: z2VZv9::new(z2VQY5::new(
            directory.join("router.sock").to_string_lossy().into_owned(),
        )),
        field_5: z2VRPH::new(Vec::new()),
        field_6: z2VUqb::z2Vd9P(z2VPn2::new(u64::from(rustix::process::getuid().as_raw()))),
    }
}

fn wait_for(path: &std::path::Path) {
    for _ in 0..200 {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("socket did not appear: {}", path.display());
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
        .submit(Input::QueryInbox(z2VSVi::new(z2Vari::new(
            "empty".to_owned(),
        ))))
        .unwrap();
    match output {
        Output::InboxListing(listing) => assert!(listing.field_0.payload().is_empty()),
        other => panic!("unexpected ordinary reply: {other:?}"),
    }

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let reply = runtime
        .block_on(
            MetaMessageClient::new(MetaMessageEndpoint::new(configuration.meta_socket_path()))
                .submit(z2Vc2e::z2VWNS(contract)),
        )
        .unwrap();
    assert!(matches!(reply, z2VYLc::z2Vc4F(_)));
}
