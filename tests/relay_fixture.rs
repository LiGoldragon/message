use message::relay::{DeliveryPort, Relay, RelayDisposition, RelayInput, TargetReadiness};
use signal_message::{
    ConnectionClass, MessageOrigin, PromptInterpretationSelection, PromptVariant,
    TypedPromptEnvelope,
};
use std::cell::Cell;
use std::io::Read;
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread;

struct BusyPort;

impl DeliveryPort for BusyPort {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Busy
    }
    fn deliver(&self, _: &str, _: &[u8]) -> std::io::Result<()> {
        panic!("busy must not write")
    }
}
struct DirtyPort;
impl DeliveryPort for DirtyPort {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Dirty
    }
    fn deliver(&self, _: &str, _: &[u8]) -> std::io::Result<()> {
        panic!("dirty must not write")
    }
}

struct SocketPort {
    path: std::path::PathBuf,
}
impl DeliveryPort for SocketPort {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Ready
    }
    fn deliver(&self, _: &str, bytes: &[u8]) -> std::io::Result<()> {
        UnixStream::connect(&self.path)?.write_all(bytes)
    }
}

#[test]
fn dirty_delivery_is_persisted_without_socket_write() {
    let directory = tempfile::tempdir().unwrap();
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    assert_eq!(
        relay
            .submit(input(PromptVariant::HumanPrompt, "raw"), &DirtyPort)
            .unwrap(),
        RelayDisposition::Pending(TargetReadiness::Dirty)
    );
}
use std::io::Write;

fn input(variant: PromptVariant, raw: &str) -> RelayInput {
    RelayInput {
        destination: "other-agent".into(),
        origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
        envelope: TypedPromptEnvelope {
            prompt_variant: variant,
            source_event_identifier: "source-event-1".into(),
            raw_prompt_text: raw.into(),
            prompt_interpretation_selection: PromptInterpretationSelection::None,
        },
    }
}

#[test]
fn unix_socket_receives_unmodified_raw_prompt_and_observation_is_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recipient.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let reader = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    assert_eq!(
        relay
            .submit(
                input(PromptVariant::HumanPrompt, "raw human words"),
                &SocketPort { path }
            )
            .unwrap(),
        RelayDisposition::ByteAccepted
    );
    assert_eq!(reader.join().unwrap(), b"raw human words");
    relay
        .recipient_observed("other-agent", "source-event-1")
        .unwrap();
    assert_eq!(
        relay
            .submit(
                input(PromptVariant::HumanPrompt, "raw human words"),
                &BusyPort
            )
            .unwrap(),
        RelayDisposition::RecipientObserved
    );
}

#[test]
fn conflict_and_non_forwarding_variants_are_durable() {
    let directory = tempfile::tempdir().unwrap();
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    assert_eq!(
        relay
            .submit(input(PromptVariant::PeerMessage, "peer"), &BusyPort)
            .unwrap(),
        RelayDisposition::RecordedOnly
    );
    let mut conflicting = input(PromptVariant::PeerMessage, "changed");
    assert!(matches!(
        relay.submit(conflicting.clone(), &BusyPort),
        Err(message::relay::RelayError::Conflict)
    ));
    conflicting.envelope.source_event_identifier = "receipt".into();
    conflicting.envelope.prompt_variant = PromptVariant::DeliveryReceipt;
    assert_eq!(
        relay.submit(conflicting, &BusyPort).unwrap(),
        RelayDisposition::RecordedOnly
    );
}

struct FailingPort;
impl DeliveryPort for FailingPort {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Ready
    }
    fn deliver(&self, _: &str, _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("ambiguous write"))
    }
}

#[test]
fn ambiguous_write_is_not_retried_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("messenger.sema");
    let relay = Relay::open(&database).unwrap();
    assert_eq!(
        relay
            .submit(input(PromptVariant::HumanPrompt, "raw"), &FailingPort)
            .unwrap(),
        RelayDisposition::InFlight
    );
    drop(relay);
    assert_eq!(
        Relay::open(&database)
            .unwrap()
            .submit(input(PromptVariant::HumanPrompt, "raw"), &BusyPort)
            .unwrap(),
        RelayDisposition::InFlight
    );
}

#[test]
fn observation_rejects_pending_records() {
    let directory = tempfile::tempdir().unwrap();
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    relay
        .submit(input(PromptVariant::HumanPrompt, "pending"), &BusyPort)
        .unwrap();
    assert!(
        relay
            .recipient_observed("other-agent", "source-event-1")
            .is_err()
    );
}

struct CountingPort(Cell<u8>);
impl DeliveryPort for CountingPort {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Ready
    }
    fn deliver(&self, _: &str, _: &[u8]) -> std::io::Result<()> {
        self.0.set(self.0.get() + 1);
        Ok(())
    }
}
#[test]
fn duplicate_event_does_not_deliver_twice() {
    let directory = tempfile::tempdir().unwrap();
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    let port = CountingPort(Cell::new(0));
    relay
        .submit(input(PromptVariant::HumanPrompt, "raw"), &port)
        .unwrap();
    relay
        .submit(input(PromptVariant::HumanPrompt, "raw"), &port)
        .unwrap();
    assert_eq!(port.0.get(), 1);
}

#[test]
fn busy_delivery_is_persisted_before_any_socket_write() {
    let directory = tempfile::tempdir().unwrap();
    let relay = Relay::open(directory.path().join("messenger.sema")).unwrap();
    let input = RelayInput {
        destination: "other-agent".into(),
        origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
        envelope: TypedPromptEnvelope {
            prompt_variant: PromptVariant::HumanPrompt,
            source_event_identifier: "source-event-1".into(),
            raw_prompt_text: "raw human words".into(),
            prompt_interpretation_selection: PromptInterpretationSelection::None,
        },
    };
    assert_eq!(
        relay.submit(input.clone(), &BusyPort).unwrap(),
        RelayDisposition::Pending(TargetReadiness::Busy)
    );
    assert_eq!(
        relay.submit(input, &BusyPort).unwrap(),
        RelayDisposition::DuplicatePending(TargetReadiness::Busy)
    );
    drop(relay);
    assert_eq!(
        Relay::open(directory.path().join("messenger.sema"))
            .unwrap()
            .pending_count()
            .unwrap(),
        1
    );
}
