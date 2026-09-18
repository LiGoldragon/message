use message::{MessageEngine, MessengerTables, OriginPolicy, nexus_delivery::NexusDelivery};
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRouteSelection, OriginClue,
};
use signal_message::{
    ClusterMember, ClusterMessage, ClusterRelay, ClusterTarget, Context,
    DeliveryAddressSelectionRejection, DeliveryReceiptQuery, DeliveryReceiptQueryRejection,
    DeliveryReceiptState, DeliveryRequest, Query, ReceiptKind, Response,
};
use triad_runtime::{ConnectionContext, UnixCredentials};

struct Fixture {
    directory: tempfile::TempDir,
    store: std::path::PathBuf,
}

struct AddressCase {
    source: &'static str,
    targets: Vec<&'static str>,
    rejection: DeliveryAddressSelectionRejection,
}

impl Fixture {
    fn open() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("messenger.sema");
        Self { directory, store }
    }

    fn engine(&self, delivery: impl NexusDelivery + 'static) -> MessageEngine {
        MessageEngine::new(
            MessengerTables::open(&self.store).unwrap(),
            OriginPolicy::for_owner_user_id(1000, "owner"),
        )
        .with_nexus_delivery(delivery)
    }
}

#[derive(Debug)]
struct ReceiptAdapter;

impl NexusDelivery for ReceiptAdapter {
    fn resolve(&self, flow: &str) -> Result<Option<FlowNode>, String> {
        match flow {
            "retryable" => Ok(None),
            "accepted" | "ambiguous" => Ok(Some(flow_node(flow))),
            other => Err(format!("unexpected flow: {other}")),
        }
    }

    fn deliver(&self, node: &FlowNode, _message: &ClusterMessage) -> Result<ReceiptKind, String> {
        if node.flow_id == "ambiguous" {
            return Err("fixture transport interrupted after durable park".into());
        }
        Ok(ReceiptKind::Accepted)
    }
}

#[derive(Debug)]
struct PanicAdapter;

impl NexusDelivery for PanicAdapter {
    fn resolve(&self, _flow: &str) -> Result<Option<FlowNode>, String> {
        panic!("invalid delivery reached resolver")
    }

    fn deliver(&self, _node: &FlowNode, _message: &ClusterMessage) -> Result<ReceiptKind, String> {
        panic!("invalid delivery reached transport")
    }
}

fn flow_node(flow_id: &str) -> FlowNode {
    FlowNode {
        flow_id: flow_id.into(),
        session_id: "fixture-session".into(),
        harness_kind: HarnessKind::Claude,
        endpoint_selection: EndpointSelection::Unavailable,
        herdr_route_selection: HerdrRouteSelection::Unavailable,
        origin_clue: OriginClue {
            flow_id: "origin".into(),
            session_id: "origin-session".into(),
            turn_id: "origin-turn".into(),
        },
        flow_lifecycle: FlowLifecycle::Active,
    }
}

fn relay() -> ClusterMessage {
    let prompt_sha256 = "a".repeat(64);
    ClusterMessage::Relay(ClusterRelay {
        flow_identifier: "source-flow".into(),
        session_identifier: "source-session".into(),
        transcript_path: "flows/source-flow/transcript.md".into(),
        prompt_first_six_words: "one two three four five six".into(),
        prompt_last_six_words: "seven eight nine ten eleven twelve".into(),
        prompt_sha256: prompt_sha256.clone(),
        context: Context {
            flow_identifier: "source-flow".into(),
            source_turn_identifier: "turn-1".into(),
            transcript_path: "flows/source-flow/transcript.md".into(),
            prompt_sha256,
            what_living_said: "fixture".into(),
            context_about: "fixture".into(),
            context_answered: "fixture".into(),
            context_corrected: "fixture".into(),
            context_uncertainties: Vec::new(),
        },
        timestamp_nanos: 1,
        cluster_target: ClusterTarget::Primary,
        cluster_members: vec![ClusterMember {
            flow_identifier: "source-flow".into(),
            session_identifier: "source-session".into(),
        }],
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(1000, 1000, std::process::id() as i32))
}

fn delivery(source: &str, targets: Vec<&str>) -> DeliveryRequest {
    DeliveryRequest {
        source_event_identifier: source.into(),
        cluster_message: relay(),
        target_flows: targets.into_iter().map(str::to_owned).collect(),
    }
}

fn query(source: &str, targets: Vec<&str>) -> DeliveryReceiptQuery {
    DeliveryReceiptQuery {
        source_event_identifier: source.into(),
        target_flows: targets.into_iter().map(str::to_owned).collect(),
    }
}

fn handle(engine: &mut MessageEngine, query: Query) -> Response {
    runtime()
        .block_on(engine.handle(query, &connection()))
        .unwrap()
}

#[test]
fn receipt_queries_preserve_recorded_states_after_reopen_and_order_missing_targets() {
    let fixture = Fixture::open();
    let mut engine = fixture.engine(ReceiptAdapter);
    assert!(matches!(
        handle(
            &mut engine,
            Query::Deliver(delivery(
                "known-event",
                vec!["accepted", "ambiguous", "retryable"]
            )),
        ),
        Response::DeliveryRecorded(_)
    ));
    drop(engine);

    let mut reopened = fixture.engine(PanicAdapter);
    let bytes_before_query = std::fs::read(&fixture.store).unwrap();
    let response = handle(
        &mut reopened,
        Query::QueryDeliveryReceipts(query(
            "known-event",
            vec!["missing", "ambiguous", "retryable", "accepted"],
        )),
    );
    let Response::DeliveryReceiptListing(listing) = response else {
        panic!("receipt query did not return its listing")
    };
    assert_eq!(listing.source_event_identifier, "known-event");
    assert_eq!(
        listing.delivery_receipt_states,
        vec![
            DeliveryReceiptState::Missing("missing".into()),
            DeliveryReceiptState::Recorded(signal_message::DeliveryReceiptRecord {
                flow_identifier: "ambiguous".into(),
                receipt_kind: ReceiptKind::Parked,
                retryable: false,
            }),
            DeliveryReceiptState::Recorded(signal_message::DeliveryReceiptRecord {
                flow_identifier: "retryable".into(),
                receipt_kind: ReceiptKind::Parked,
                retryable: true,
            }),
            DeliveryReceiptState::Recorded(signal_message::DeliveryReceiptRecord {
                flow_identifier: "accepted".into(),
                receipt_kind: ReceiptKind::Accepted,
                retryable: false,
            }),
        ]
    );
    assert_eq!(bytes_before_query, std::fs::read(&fixture.store).unwrap());
    assert!(fixture.directory.path().exists());
}

#[test]
fn invalid_address_selections_are_typed_and_cannot_mutate_or_resolve() {
    let fixture = Fixture::open();
    let mut engine = fixture.engine(PanicAdapter);
    let before = std::fs::read(&fixture.store).unwrap();
    let cases = [
        AddressCase {
            source: "",
            targets: vec!["target"],
            rejection: DeliveryAddressSelectionRejection::EmptySourceEventIdentifier,
        },
        AddressCase {
            source: "event",
            targets: vec!["target"],
            rejection: DeliveryAddressSelectionRejection::ReservedSourceEventIdentifier,
        },
        AddressCase {
            source: "a\0b",
            targets: vec!["target"],
            rejection: DeliveryAddressSelectionRejection::SourceEventIdentifierContainsNull,
        },
        AddressCase {
            source: "source",
            targets: Vec::new(),
            rejection: DeliveryAddressSelectionRejection::EmptyTargetFlows,
        },
        AddressCase {
            source: "source",
            targets: vec![""],
            rejection: DeliveryAddressSelectionRejection::EmptyTargetFlowIdentifier,
        },
        AddressCase {
            source: "source",
            targets: vec!["same", "same"],
            rejection: DeliveryAddressSelectionRejection::DuplicateTargetFlows,
        },
        AddressCase {
            source: "source",
            targets: vec!["a\0b"],
            rejection: DeliveryAddressSelectionRejection::TargetFlowIdentifierContainsNull,
        },
    ];
    for case in cases {
        assert_eq!(
            handle(
                &mut engine,
                Query::Deliver(delivery(case.source, case.targets.clone()))
            ),
            Response::DeliveryRejected(case.rejection.clone())
        );
        assert_eq!(
            handle(
                &mut engine,
                Query::QueryDeliveryReceipts(query(case.source, case.targets))
            ),
            Response::DeliveryReceiptQueryRejected(
                DeliveryReceiptQueryRejection::InvalidAddressSelection(case.rejection)
            )
        );
    }
    let too_many = (0..65)
        .map(|index| format!("flow-{index}"))
        .collect::<Vec<_>>();
    let exact_limit = (0..64)
        .map(|index| format!("flow-{index}"))
        .collect::<Vec<_>>();
    assert!(matches!(
        handle(
            &mut engine,
            Query::Deliver(delivery(
                "source",
                too_many.iter().map(String::as_str).collect()
            ))
        ),
        Response::DeliveryRejected(DeliveryAddressSelectionRejection::TooManyTargetFlows)
    ));
    assert!(matches!(
        handle(
            &mut engine,
            Query::QueryDeliveryReceipts(query(
                "source",
                too_many.iter().map(String::as_str).collect()
            ))
        ),
        Response::DeliveryReceiptQueryRejected(
            DeliveryReceiptQueryRejection::InvalidAddressSelection(
                DeliveryAddressSelectionRejection::TooManyTargetFlows
            )
        )
    ));
    assert!(matches!(
        handle(
            &mut engine,
            Query::QueryDeliveryReceipts(query(
                "source",
                exact_limit.iter().map(String::as_str).collect()
            ))
        ),
        Response::DeliveryReceiptListing(_)
    ));
    assert_eq!(before, std::fs::read(&fixture.store).unwrap());
}

#[test]
fn exact_limit_delivery_succeeds_and_delimiter_aliases_are_rejected_before_storage() {
    let fixture = Fixture::open();
    let exact_limit = (0..64)
        .map(|index| format!("flow-{index}"))
        .collect::<Vec<_>>();
    let mut engine = fixture.engine(ReceiptAdapter);
    assert!(matches!(
        handle(
            &mut engine,
            Query::Deliver(delivery(
                "limit-event",
                exact_limit.iter().map(String::as_str).collect()
            ))
        ),
        Response::DeliveryRecorded(_)
    ));

    let fixture = Fixture::open();
    let mut engine = fixture.engine(PanicAdapter);
    for case in [
        AddressCase {
            source: "a\0b",
            targets: vec!["c"],
            rejection: DeliveryAddressSelectionRejection::SourceEventIdentifierContainsNull,
        },
        AddressCase {
            source: "a",
            targets: vec!["b\0c"],
            rejection: DeliveryAddressSelectionRejection::TargetFlowIdentifierContainsNull,
        },
    ] {
        assert_eq!(
            handle(
                &mut engine,
                Query::Deliver(delivery(case.source, case.targets))
            ),
            Response::DeliveryRejected(case.rejection)
        );
    }
}
