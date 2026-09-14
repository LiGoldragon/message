use message::relay::{DeliveryPort, Relay, RelayDisposition, RelayInput, TargetReadiness};
use signal_message::{
    ConnectionClass, MessageOrigin, PromptInterpretationSelection, PromptVariant,
    TypedPromptEnvelope,
};
use std::{
    cell::Cell,
    io::Write,
    os::unix::net::{UnixListener, UnixStream},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU8, Ordering},
    },
    thread,
};

fn input() -> RelayInput {
    RelayInput {
        source_agent_identifier: "source".into(),
        destination: "destination".into(),
        origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
        envelope: TypedPromptEnvelope {
            prompt_variant: PromptVariant::HumanPrompt,
            source_event_identifier: "event".into(),
            raw_prompt_text: "raw".into(),
            prompt_interpretation_selection: PromptInterpretationSelection::None,
        },
    }
}

#[test]
fn simultaneous_ready_dispatches_claim_exactly_one_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let relay = Arc::new(Relay::open(dir.path().join("messenger.sema")).unwrap());
    relay.submit(input()).unwrap();
    struct ConcurrentPort(AtomicU8);
    impl DeliveryPort for ConcurrentPort {
        fn readiness(&self, _: &str) -> TargetReadiness {
            TargetReadiness::Ready
        }
        fn deliver(&self, _: &str, _: &message::runtime_model::RelayRecord) -> std::io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let port = Arc::new(ConcurrentPort(AtomicU8::new(0)));
    let barrier = Arc::new(Barrier::new(2));
    let joins: Vec<_> = (0..2)
        .map(|_| {
            let relay = relay.clone();
            let port = port.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                relay
                    .dispatch(
                        "destination",
                        "source",
                        "event",
                        TargetReadiness::Ready,
                        &*port,
                    )
                    .unwrap()
            })
        })
        .collect();
    let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
    assert_eq!(port.0.load(Ordering::SeqCst), 1);
    assert!(results.iter().all(|result| matches!(
        result,
        RelayDisposition::ByteAccepted | RelayDisposition::InFlight
    )));
    assert_eq!(
        relay
            .attempt_count("destination", "source", "event")
            .unwrap()
            .unwrap()
            .reservations,
        1
    );
}
struct Port(Cell<u8>);
impl DeliveryPort for Port {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Ready
    }
    fn deliver(&self, _: &str, _: &message::runtime_model::RelayRecord) -> std::io::Result<()> {
        self.0.set(self.0.get() + 1);
        Ok(())
    }
}
struct Never;
impl DeliveryPort for Never {
    fn readiness(&self, _: &str) -> TargetReadiness {
        TargetReadiness::Ready
    }
    fn deliver(&self, _: &str, _: &message::runtime_model::RelayRecord) -> std::io::Result<()> {
        panic!("must not write")
    }
}

#[test]
fn busy_and_dirty_leave_the_admitted_record_pending_without_bytes() {
    for readiness in [TargetReadiness::Busy, TargetReadiness::Dirty] {
        let dir = tempfile::tempdir().unwrap();
        let relay = Relay::open(dir.path().join("messenger.sema")).unwrap();
        assert_eq!(
            relay.submit(input()).unwrap(),
            RelayDisposition::Pending(TargetReadiness::Dirty)
        );
        assert_eq!(
            relay
                .dispatch("destination", "source", "event", readiness, &Never)
                .unwrap(),
            RelayDisposition::Pending(readiness)
        );
        assert_eq!(relay.pending_count().unwrap(), 1);
    }
}

#[test]
fn ready_writes_one_frame_and_deduped_dispatch_never_retries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recipient.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let reader = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut stream, &mut bytes).unwrap();
        bytes
    });
    struct SocketPort(std::path::PathBuf);
    impl DeliveryPort for SocketPort {
        fn readiness(&self, _: &str) -> TargetReadiness {
            TargetReadiness::Ready
        }
        fn deliver(&self, _: &str, _: &message::runtime_model::RelayRecord) -> std::io::Result<()> {
            UnixStream::connect(&self.0)?.write_all(b"frame")
        }
    }
    let relay = Relay::open(dir.path().join("messenger.sema")).unwrap();
    assert!(matches!(
        relay.submit(input()).unwrap(),
        RelayDisposition::Pending(_)
    ));
    assert_eq!(
        relay
            .dispatch(
                "destination",
                "source",
                "event",
                TargetReadiness::Ready,
                &SocketPort(path)
            )
            .unwrap(),
        RelayDisposition::ByteAccepted
    );
    assert_eq!(reader.join().unwrap(), b"frame");
    assert_eq!(
        relay
            .dispatch(
                "destination",
                "source",
                "event",
                TargetReadiness::Ready,
                &Never
            )
            .unwrap(),
        RelayDisposition::ByteAccepted
    );
}

#[test]
fn duplicate_admission_is_durable_and_delivery_happens_once() {
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("messenger.sema");
    let relay = Relay::open(&database).unwrap();
    let port = Port(Cell::new(0));
    relay.submit(input()).unwrap();
    assert_eq!(
        relay.submit(input()).unwrap(),
        RelayDisposition::DuplicatePending(TargetReadiness::Dirty)
    );
    assert_eq!(
        relay
            .dispatch(
                "destination",
                "source",
                "event",
                TargetReadiness::Ready,
                &port
            )
            .unwrap(),
        RelayDisposition::ByteAccepted
    );
    assert_eq!(port.0.get(), 1);
    drop(relay);
    let relay = Relay::open(&database).unwrap();
    assert_eq!(
        relay
            .dispatch(
                "destination",
                "source",
                "event",
                TargetReadiness::Ready,
                &Never
            )
            .unwrap(),
        RelayDisposition::ByteAccepted
    );
}

#[test]
fn zero_byte_retry_and_late_observation_preserve_identity_and_attempt_count() {
    use message::relay::{DeliveryOutcome, DispatchClaim};
    let dir = tempfile::tempdir().unwrap();
    let database = dir.path().join("messenger.sema");
    let relay = Relay::open(&database).unwrap();
    relay.submit(input()).unwrap();
    assert!(matches!(
        relay
            .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
            .unwrap(),
        DispatchClaim::Attempt(_)
    ));
    relay
        .finish_dispatch(
            "destination",
            "source",
            "event",
            DeliveryOutcome::NoBytesWritten,
        )
        .unwrap();
    assert_eq!(relay.pending_count().unwrap(), 1);
    drop(relay);
    let relay = Relay::open(&database).unwrap();
    assert_eq!(
        relay
            .attempt_count("destination", "source", "event")
            .unwrap()
            .unwrap()
            .reservations,
        1
    );
    assert!(matches!(
        relay
            .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
            .unwrap(),
        DispatchClaim::Attempt(_)
    ));
    relay
        .finish_dispatch("destination", "source", "event", DeliveryOutcome::Ambiguous)
        .unwrap();
    assert_eq!(relay.pending_count().unwrap(), 0);
    assert!(matches!(
        relay
            .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
            .unwrap(),
        DispatchClaim::NoAttempt(RelayDisposition::InFlight)
    ));
    relay
        .recipient_observed("destination", "source", "event")
        .unwrap();
    assert!(matches!(
        relay
            .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
            .unwrap(),
        DispatchClaim::NoAttempt(RelayDisposition::RecipientObserved)
    ));
    assert_eq!(
        relay
            .attempt_count("destination", "source", "event")
            .unwrap()
            .unwrap()
            .reservations,
        2
    );
}

#[test]
fn receipt_racing_transport_completion_is_never_overwritten() {
    use message::relay::{DeliveryOutcome, DispatchClaim};
    let dir = tempfile::tempdir().unwrap();
    let relay = Relay::open(dir.path().join("messenger.sema")).unwrap();
    relay.submit(input()).unwrap();
    assert!(
        relay
            .recipient_observed("destination", "source", "event")
            .is_err()
    );
    relay
        .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
        .unwrap();
    relay
        .recipient_observed("destination", "source", "event")
        .unwrap();
    relay
        .finish_dispatch("destination", "source", "event", DeliveryOutcome::Ambiguous)
        .unwrap();
    assert!(matches!(
        relay
            .begin_dispatch("destination", "source", "event", TargetReadiness::Ready)
            .unwrap(),
        DispatchClaim::NoAttempt(RelayDisposition::RecipientObserved)
    ));
}
