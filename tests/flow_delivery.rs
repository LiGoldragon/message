//! The flow-delivery leg: park on `FlowDeliver`, land on idle.
//!
//! The PTY leg that would type a prompt at a live harness session is out of
//! this prototype; "landed" here means the parked envelope left the store and
//! its compact receipt exists.

use message::{FlowMarkerIndex, MessageEngine, MessengerTables, OriginPolicy, ParkedDeliveryKey};
use signal_message::{
    DeliveryQueueState, FlowDeliveryRejectionReason, FlowDeliveryRequest,
    PromptInterpretationSelection, PromptVariant, Query, Response, TypedPromptEnvelope,
};
use triad_runtime::{ConnectionContext, UnixCredentials};

const KNOWN_FLOW: &str = "57a7aa";
/// Multibyte on purpose: a byte count is a byte count, never a character
/// count, and a re-encode would show up here first.
const RAW_TEXT: &str = "señal — deliver this exact text ✓";

struct Fixture {
    _directory: tempfile::TempDir,
    engine: MessageEngine,
}

impl Fixture {
    fn open() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let lanes = directory.path().join("flows");
        std::fs::create_dir_all(&lanes).unwrap();
        std::fs::write(
            lanes.join(format!(".{KNOWN_FLOW}.flow-id")),
            format!("version=1\nharness=claude\nidentity=fixture\nalias={KNOWN_FLOW}\n"),
        )
        .unwrap();
        let store = directory.path().join("messenger.sema");
        let engine = MessageEngine::new(
            MessengerTables::open(&store).unwrap(),
            OriginPolicy::for_owner_user_id(1000, "owner"),
        )
        .with_flow_registry(FlowMarkerIndex::new(vec![lanes]));
        Self {
            _directory: directory,
            engine,
        }
    }

    fn deliver(&mut self, target_flow_name: &str, raw: &str) -> Response {
        let query = Query::FlowDeliver(FlowDeliveryRequest {
            typed_prompt_envelope: envelope(raw),
            target_flow_name: target_flow_name.to_owned(),
        });
        runtime()
            .block_on(self.engine.handle(query, &connection()))
            .unwrap()
    }

    fn announce_idle(&mut self, target_flow_name: &str) -> Vec<Response> {
        self.engine
            .announce_flow_idle(&target_flow_name.to_owned())
            .unwrap()
    }

    fn announce_idle_query(&mut self, target_flow_name: &str) -> Response {
        runtime()
            .block_on(self.engine.handle(
                Query::FlowAnnounceIdle(signal_message::FlowIdleAnnouncement {
                    target_flow_name: target_flow_name.to_owned(),
                }),
                &connection(),
            ))
            .unwrap()
    }

    fn parked(&self, target_flow_name: &str) -> Vec<TypedPromptEnvelope> {
        self.engine
            .parked_flow_deliveries(&target_flow_name.to_owned())
            .unwrap()
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(1000, 1000, std::process::id() as i32))
}

fn envelope(raw: &str) -> TypedPromptEnvelope {
    TypedPromptEnvelope {
        prompt_variant: PromptVariant::HumanPrompt,
        source_event_identifier: "source-event-1".to_owned(),
        raw_prompt_text: raw.to_owned(),
        prompt_interpretation_selection: PromptInterpretationSelection::None,
    }
}

#[test]
fn a_delivery_to_a_known_flow_parks_and_is_acknowledged_queued() {
    let mut fixture = Fixture::open();
    match fixture.deliver(KNOWN_FLOW, RAW_TEXT) {
        Response::DeliveryQueued(acknowledgment) => {
            assert_eq!(acknowledgment.source_event_identifier, "source-event-1");
            assert_eq!(acknowledgment.target_flow_name, KNOWN_FLOW);
            assert_eq!(
                acknowledgment.delivery_queue_state,
                DeliveryQueueState::Parked
            );
        }
        other => panic!("unexpected reply: {other:?}"),
    }
    assert_eq!(fixture.parked(KNOWN_FLOW).len(), 1);
}

#[test]
fn a_repeated_source_event_is_one_parked_delivery_not_two() {
    let mut fixture = Fixture::open();
    let first = fixture.deliver(KNOWN_FLOW, RAW_TEXT);
    let second = fixture.deliver(KNOWN_FLOW, RAW_TEXT);
    assert!(matches!(first, Response::DeliveryQueued(_)));
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    assert_eq!(fixture.parked(KNOWN_FLOW).len(), 1);
}

#[test]
fn an_unknown_flow_is_refused_and_parks_nothing() {
    let mut fixture = Fixture::open();
    assert_eq!(
        fixture.deliver("no-such-flow", RAW_TEXT),
        Response::FlowDeliveryRejected(FlowDeliveryRejectionReason::UnknownFlow)
    );
    assert!(fixture.parked("no-such-flow").is_empty());
}

#[test]
fn an_idle_announce_lands_the_parked_delivery_with_a_compact_receipt() {
    let mut fixture = Fixture::open();
    fixture.deliver(KNOWN_FLOW, RAW_TEXT);
    let landed = fixture.announce_idle(KNOWN_FLOW);
    assert_eq!(landed.len(), 1);
    match &landed[0] {
        Response::DeliveryLanded(receipt) => {
            assert_eq!(receipt.source_event_identifier, "source-event-1");
            assert_eq!(receipt.byte_count, RAW_TEXT.len() as i64);
            assert!(receipt.landed_at > 0);
        }
        other => panic!("unexpected reply: {other:?}"),
    }
    assert!(fixture.parked(KNOWN_FLOW).is_empty());
    assert!(fixture.announce_idle(KNOWN_FLOW).is_empty());
}

#[test]
fn repeated_idle_queries_are_acknowledged_without_relanding() {
    let mut fixture = Fixture::open();
    fixture.deliver(KNOWN_FLOW, RAW_TEXT);

    let first = fixture.announce_idle_query(KNOWN_FLOW);
    let second = fixture.announce_idle_query(KNOWN_FLOW);

    match first {
        Response::FlowIdleAcknowledged(acknowledgment) => {
            assert_eq!(acknowledgment.landed_receipts.len(), 1);
        }
        other => panic!("unexpected first idle reply: {other:?}"),
    }
    match second {
        Response::FlowIdleAcknowledged(acknowledgment) => {
            assert!(acknowledgment.landed_receipts.is_empty());
        }
        other => panic!("unexpected repeated idle reply: {other:?}"),
    }
    assert!(fixture.parked(KNOWN_FLOW).is_empty());
}

#[test]
fn an_idle_announce_for_another_flow_leaves_the_park_untouched() {
    let mut fixture = Fixture::open();
    fixture.deliver(KNOWN_FLOW, RAW_TEXT);
    assert!(fixture.announce_idle("some-other-flow").is_empty());
    assert_eq!(fixture.parked(KNOWN_FLOW).len(), 1);
}

#[test]
fn the_parked_text_is_the_submitted_bytes_never_a_re_encoding() {
    let mut fixture = Fixture::open();
    fixture.deliver(KNOWN_FLOW, RAW_TEXT);
    let parked = fixture.parked(KNOWN_FLOW);
    assert_eq!(parked[0].raw_prompt_text.as_bytes(), RAW_TEXT.as_bytes());
    assert_eq!(parked[0].prompt_variant, PromptVariant::HumanPrompt);
}

#[test]
fn the_park_key_separates_flows_and_source_events() {
    let one = ParkedDeliveryKey::new("57a7aa".to_owned(), "a:b".to_owned());
    let other = ParkedDeliveryKey::new("57a7aa:a".to_owned(), "b".to_owned());
    assert_ne!(one.record_key(), other.record_key());
}
