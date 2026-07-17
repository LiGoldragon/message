//! Witnesses for the messenger's agent registry — the durable consumer view
//! of agent identity and the local delivery registry in `messenger.sema`.
//!
//! The ORCHESTRATOR is the mint (psyche-ruled 2026-07-17): identities arrive
//! already allocated, and the registry seats them. These pin down the
//! contract: seating an orchestrator-supplied identifier (fresh and
//! reseated), the optional pre-launch process pin, endpoint binding, and —
//! the durability the router's in-memory actor registry never had —
//! registry persistence across a store reopen. The engine-level tests drive
//! `MessageEngine::handle` so the full Signal → Nexus → SEMA path is the
//! thing proven, not the store alone.

use std::path::PathBuf;

use message::router::{OriginPolicy, SignalRouterSocket};
use message::schema::signal::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentifier,
    AgentIdentityAssignment, AgentRegistryQuery, AgentRegistryRejectionReason, EndpointPath,
    EndpointSelection, HarnessPid, HarnessProcessPin, HarnessStartTime, IdentityProvenance, Input,
    Output, ProcessPinSelection, ResumeIdentity, ResumeSelection,
};
use message::{MessageEngine, MessengerTables, RouterForwarder};
use tempfile::TempDir;
use triad_runtime::{ConnectionContext, UnixCredentials};

const OWNER_USER_ID: u32 = 1000;
const OWNER_INSTANCE: &str = "operator";

fn owner_connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(OWNER_USER_ID, OWNER_USER_ID, 101))
}

fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("messenger.sema")
}

fn open_tables(root: &TempDir) -> MessengerTables {
    MessengerTables::open(&store_path(root)).expect("open messenger store")
}

/// Registry operations never touch the router, so the forwarder may point at
/// a dead socket path.
fn engine_for(root: &TempDir) -> MessageEngine {
    MessageEngine::new(
        RouterForwarder::new(
            SignalRouterSocket::from_path(root.path().join("no-router.sock")),
            OriginPolicy::for_owner_user_id(OWNER_USER_ID, OWNER_INSTANCE),
        ),
        open_tables(root),
    )
}

fn identifier(code: &str) -> AgentIdentifier {
    AgentIdentifier::new(code.to_owned())
}

fn pinned(pid: u64, start_time: u64) -> ProcessPinSelection {
    ProcessPinSelection::Pinned(HarnessProcessPin {
        harness_pid: HarnessPid::new(pid),
        harness_start_time: HarnessStartTime::new(start_time),
    })
}

fn seat(code: &str, pin: ProcessPinSelection, resume: ResumeSelection) -> AgentIdentityAssignment {
    AgentIdentityAssignment {
        agent_identifier: identifier(code),
        process_pin_selection: pin,
        resume_selection: resume,
    }
}

fn resumed(resume: &str) -> ResumeSelection {
    ResumeSelection::Resumed(ResumeIdentity::new(resume.to_owned()))
}

fn pty_endpoint(path: &str) -> AgentEndpoint {
    AgentEndpoint {
        agent_endpoint_kind: AgentEndpointKind::PtySocket,
        endpoint_path: EndpointPath::new(path.to_owned()),
    }
}

#[test]
fn seating_a_fresh_identifier_reports_seated_and_stores_the_row() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let assigned = tables
        .seat_identity(&seat("x7f2", pinned(4242, 100), ResumeSelection::None))
        .expect("seat");

    assert_eq!(assigned.identity_provenance, IdentityProvenance::Seated);
    assert_eq!(assigned.agent_identifier, identifier("x7f2"));

    let entries = tables
        .query_entries(&AgentRegistryQuery::ByAgent(identifier("x7f2")))
        .expect("query");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].process_pin_selection, pinned(4242, 100));
    assert_eq!(entries[0].endpoint_selection, EndpointSelection::None);
}

#[test]
fn seating_before_launch_carries_no_process_pin() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let assigned = tables
        .seat_identity(&seat("9k4w", ProcessPinSelection::None, ResumeSelection::None))
        .expect("seat pre-launch");

    assert_eq!(assigned.identity_provenance, IdentityProvenance::Seated);
    let entries = tables
        .query_entries(&AgentRegistryQuery::ByAgent(identifier("9k4w")))
        .expect("query");
    assert_eq!(entries[0].process_pin_selection, ProcessPinSelection::None);
}

#[test]
fn reseating_refreshes_the_pin_and_clears_the_stale_endpoint() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    tables
        .seat_identity(&seat("x7f2", pinned(1000, 11), resumed("session-abc")))
        .expect("first seat");
    tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: identifier("x7f2"),
            agent_endpoint: pty_endpoint("/run/terminal-cell/session-a/data.sock"),
            harness_pid: HarnessPid::new(1000),
            harness_start_time: HarnessStartTime::new(11),
        })
        .expect("bind")
        .expect("known identifier");

    let reseated = tables
        .seat_identity(&seat("x7f2", pinned(2000, 22), resumed("session-abc")))
        .expect("reseat");
    assert_eq!(reseated.identity_provenance, IdentityProvenance::Reseated);

    let entries = tables
        .query_entries(&AgentRegistryQuery::ByAgent(identifier("x7f2")))
        .expect("query");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].process_pin_selection, pinned(2000, 22));
    assert_eq!(entries[0].endpoint_selection, EndpointSelection::None);
    assert_eq!(entries[0].resume_selection, resumed("session-abc"));
}

#[test]
fn registry_persists_across_store_reopen() {
    let root = TempDir::new().expect("store dir");

    {
        let tables = open_tables(&root);
        tables
            .seat_identity(&seat("d4rb", pinned(4242, 77), resumed("session-durable")))
            .expect("seat");
        tables
            .bind_endpoint(&AgentEndpointBinding {
                agent_identifier: identifier("d4rb"),
                agent_endpoint: pty_endpoint("/run/terminal-cell/session-a/data.sock"),
                harness_pid: HarnessPid::new(4242),
                harness_start_time: HarnessStartTime::new(77),
            })
            .expect("bind")
            .expect("known identifier");
    }

    let reopened = open_tables(&root);
    let entries = reopened
        .query_entries(&AgentRegistryQuery::All)
        .expect("query after reopen");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].agent_identifier, identifier("d4rb"));
    assert_eq!(
        entries[0].endpoint_selection,
        EndpointSelection::Bound(pty_endpoint("/run/terminal-cell/session-a/data.sock"))
    );
    assert_eq!(entries[0].resume_selection, resumed("session-durable"));

    let reseated = reopened
        .seat_identity(&seat("d4rb", pinned(5000, 88), resumed("session-durable")))
        .expect("seat after reopen");
    assert_eq!(reseated.identity_provenance, IdentityProvenance::Reseated);
}

#[test]
fn binding_an_unknown_identifier_is_reported_not_committed() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let bound = tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: identifier("zzzz"),
            agent_endpoint: pty_endpoint("/run/terminal-cell/ghost/data.sock"),
            harness_pid: HarnessPid::new(1),
            harness_start_time: HarnessStartTime::new(1),
        })
        .expect("bind call");
    assert!(bound.is_none());
    assert!(
        tables
            .query_entries(&AgentRegistryQuery::All)
            .expect("query")
            .is_empty()
    );
}

#[test]
fn engine_seat_bind_query_run_the_full_signal_nexus_sema_path() {
    let root = TempDir::new().expect("store dir");
    let mut engine = engine_for(&root);
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let assigned = match runtime
        .block_on(engine.handle(
            Input::assign_agent_identity(seat(
                "e2e7",
                pinned(4242, 99),
                resumed("session-e2e"),
            )),
            &owner_connection(),
        ))
        .expect("seat reply")
    {
        Output::AgentIdentityAssigned(assigned) => assigned.into_payload(),
        other => panic!("expected AgentIdentityAssigned, got {other:?}"),
    };
    assert_eq!(assigned.identity_provenance, IdentityProvenance::Seated);
    assert_eq!(assigned.agent_identifier, identifier("e2e7"));

    let bound = runtime
        .block_on(engine.handle(
            Input::bind_agent_endpoint(AgentEndpointBinding {
                agent_identifier: identifier("e2e7"),
                agent_endpoint: pty_endpoint("/run/terminal-cell/session-e2e/data.sock"),
                harness_pid: HarnessPid::new(4242),
                harness_start_time: HarnessStartTime::new(99),
            }),
            &owner_connection(),
        ))
        .expect("bind reply");
    match bound {
        Output::AgentEndpointBound(bound) => {
            assert_eq!(bound.into_payload().into_payload(), identifier("e2e7"));
        }
        other => panic!("expected AgentEndpointBound, got {other:?}"),
    }

    let listing = runtime
        .block_on(engine.handle(
            Input::query_agent_registry(AgentRegistryQuery::All),
            &owner_connection(),
        ))
        .expect("query reply");
    match listing {
        Output::AgentRegistryListing(listing) => {
            let entries = listing.into_payload().into_payload();
            assert_eq!(entries.len(), 1);
            assert_eq!(
                entries[0].endpoint_selection,
                EndpointSelection::Bound(pty_endpoint(
                    "/run/terminal-cell/session-e2e/data.sock"
                ))
            );
        }
        other => panic!("expected AgentRegistryListing, got {other:?}"),
    }
}

#[test]
fn engine_rejects_binding_for_an_unknown_identifier_with_a_typed_reply() {
    let root = TempDir::new().expect("store dir");
    let mut engine = engine_for(&root);
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let reply = runtime
        .block_on(engine.handle(
            Input::bind_agent_endpoint(AgentEndpointBinding {
                agent_identifier: identifier("zzzz"),
                agent_endpoint: pty_endpoint("/run/terminal-cell/ghost/data.sock"),
                harness_pid: HarnessPid::new(1),
                harness_start_time: HarnessStartTime::new(1),
            }),
            &owner_connection(),
        ))
        .expect("bind reply");
    match reply {
        Output::AgentRegistryRejected(rejection) => {
            assert_eq!(
                rejection.into_payload().into_payload(),
                AgentRegistryRejectionReason::UnknownAgentIdentifier
            );
        }
        other => panic!("expected AgentRegistryRejected, got {other:?}"),
    }
}
