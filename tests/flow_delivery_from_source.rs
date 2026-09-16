//! Proof of no-retyping: a `FlowDeliver` built from a source reference — a
//! fixture transcript plus a source event id, never inline text — round-trips
//! through the engine to `DeliveryQueued` with the fixture's exact bytes.

use message::{
    FlowMarkerIndex, JsonlTranscript, MessageEngine, MessengerTables, OriginPolicy,
    PromptExtraction, PromptExtractor, SourceReference,
};
use signal_message::{DeliveryQueueState, Query, Response};
use triad_runtime::{ConnectionContext, UnixCredentials};

const KNOWN_FLOW: &str = "57a7aa";
const SOURCE_EVENT_IDENTIFIER: &str = "0a3c0db4-7d96-401d-a679-a693de78815a";
/// Multibyte on purpose, exactly as the fixture in `tests/flow_delivery.rs`
/// does: a byte count is a byte count, never a character count.
const FIXTURE_RAW_TEXT: &str = "señal — deliver this exact text from the transcript ✓";

struct Fixture {
    _directory: tempfile::TempDir,
    transcript_path: std::path::PathBuf,
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
        let transcript_path = directory.path().join("session.jsonl");
        std::fs::write(
            &transcript_path,
            format!(
                r#"{{"type":"user","message":{{"role":"user","content":"{FIXTURE_RAW_TEXT}"}},"uuid":"{SOURCE_EVENT_IDENTIFIER}","origin":{{"kind":"human"}}}}
{{"type":"user","message":{{"role":"user","content":"an unrelated agent turn"}},"uuid":"other-event","origin":{{"kind":"agent"}}}}
"#
            ),
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
            transcript_path,
            engine,
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(1000, 1000, std::process::id() as i32))
}

#[test]
fn a_source_referenced_delivery_lands_the_transcripts_exact_bytes_never_retyped() {
    let fixture = Fixture::open();

    // No inline text anywhere in this request: only a path and an id.
    let request = PromptExtraction {
        source_transcript_path: fixture.transcript_path.to_string_lossy().into_owned(),
        source_event_identifier: SOURCE_EVENT_IDENTIFIER.to_owned(),
        target_flow_name: KNOWN_FLOW.to_owned(),
    };
    let query = request.resolve().unwrap();
    let Query::FlowDeliver(built_request) = &query else {
        panic!("resolve() must produce a FlowDeliver query");
    };
    assert_eq!(
        built_request.typed_prompt_envelope.raw_prompt_text,
        FIXTURE_RAW_TEXT
    );

    let mut engine = fixture.engine;
    let reply = runtime()
        .block_on(engine.handle(query, &connection()))
        .unwrap();
    match reply {
        Response::DeliveryQueued(acknowledgment) => {
            assert_eq!(
                acknowledgment.source_event_identifier,
                SOURCE_EVENT_IDENTIFIER
            );
            assert_eq!(acknowledgment.target_flow_name, KNOWN_FLOW);
            assert_eq!(
                acknowledgment.delivery_queue_state,
                DeliveryQueueState::Parked
            );
        }
        other => panic!("unexpected reply: {other:?}"),
    }

    let parked = engine
        .parked_flow_deliveries(&KNOWN_FLOW.to_owned())
        .unwrap();
    assert_eq!(parked.len(), 1);
    assert_eq!(
        parked[0].raw_prompt_text.as_bytes(),
        FIXTURE_RAW_TEXT.as_bytes()
    );
}

#[test]
fn the_datom_text_a_caller_gets_is_the_exact_flow_deliver_query() {
    let fixture = Fixture::open();
    let text = format!(
        "{{ «{}» {} {} }}",
        fixture.transcript_path.to_string_lossy(),
        SOURCE_EVENT_IDENTIFIER,
        KNOWN_FLOW
    );
    let request = message::text::read::<PromptExtraction>(&text).unwrap();
    let query = request.resolve().unwrap();
    let printed = message::text::write(&query);
    let round_tripped = message::text::read::<Query>(&printed).unwrap();
    assert_eq!(round_tripped, query);

    let source = JsonlTranscript::open(&fixture.transcript_path);
    let reference = SourceReference::new(&fixture.transcript_path, SOURCE_EVENT_IDENTIFIER);
    let extracted = PromptExtractor::extract(&reference, &source).unwrap();
    assert_eq!(
        extracted.typed_prompt_envelope.raw_prompt_text,
        FIXTURE_RAW_TEXT
    );
}
