//! Witnesses for the messenger's agent registry — the durable identity map
//! and local delivery registry born in `messenger.sema` (train packet 2.1).
//!
//! These pin down the packet's verification contract: the launch-time mint
//! round trip, identifier reuse on resume, conflict-driven code-length
//! growth, endpoint binding, and — the durability the router's in-memory
//! actor registry never had — registry persistence across a store reopen.
//! The engine-level tests drive `MessageEngine::handle` so the full
//! Signal → Nexus → SEMA path is the thing proven, not the store alone.

use std::path::PathBuf;

use message::agent_identifier_mint::AgentIdentifierMint;
use message::router::{OriginPolicy, SignalRouterSocket};
use message::schema::signal::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentifier,
    AgentIdentityAssignment, AgentRegistryQuery, AgentRegistryRejectionReason, EndpointPath,
    EndpointSelection, HarnessPid, HarnessStartTime, IdentityProvenance, Input, Output,
    ResumeIdentity, ResumeSelection,
};
use message::{Error, MessageEngine, MessengerTables, RouterForwarder};
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

fn fresh_assignment(pid: u64, start_time: u64) -> AgentIdentityAssignment {
    AgentIdentityAssignment {
        harness_pid: HarnessPid::new(pid),
        harness_start_time: HarnessStartTime::new(start_time),
        resume_selection: ResumeSelection::None,
    }
}

fn resumed_assignment(pid: u64, start_time: u64, resume: &str) -> AgentIdentityAssignment {
    AgentIdentityAssignment {
        harness_pid: HarnessPid::new(pid),
        harness_start_time: HarnessStartTime::new(start_time),
        resume_selection: ResumeSelection::Resumed(ResumeIdentity::new(resume.to_owned())),
    }
}

fn pty_endpoint(path: &str) -> AgentEndpoint {
    AgentEndpoint {
        agent_endpoint_kind: AgentEndpointKind::PtySocket,
        endpoint_path: EndpointPath::new(path.to_owned()),
    }
}

#[test]
fn mint_assigns_four_character_base36_identifier() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let assigned = tables
        .assign_identity(&fresh_assignment(4242, 100))
        .expect("assign");

    assert_eq!(assigned.identity_provenance, IdentityProvenance::Minted);
    let code = assigned.agent_identifier.payload();
    assert_eq!(code.len(), 4);
    assert!(
        code.chars()
            .all(|character| character.is_ascii_digit()
                || character.is_ascii_lowercase())
    );
}

#[test]
fn resumed_session_reuses_its_identifier_and_refreshes_the_process_pin() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let first = tables
        .assign_identity(&resumed_assignment(1000, 11, "session-abc"))
        .expect("first assign");
    assert_eq!(first.identity_provenance, IdentityProvenance::Minted);

    let second = tables
        .assign_identity(&resumed_assignment(2000, 22, "session-abc"))
        .expect("resumed assign");
    assert_eq!(second.identity_provenance, IdentityProvenance::Reused);
    assert_eq!(second.agent_identifier, first.agent_identifier);

    let entries = tables
        .query_entries(&AgentRegistryQuery::ByAgent(first.agent_identifier.clone()))
        .expect("query");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].harness_pid, HarnessPid::new(2000));
    assert_eq!(entries[0].harness_start_time, HarnessStartTime::new(22));
    assert_eq!(entries[0].endpoint_selection, EndpointSelection::None);
}

#[test]
fn distinct_resume_identities_mint_distinct_identifiers() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let first = tables
        .assign_identity(&resumed_assignment(1, 1, "session-one"))
        .expect("assign one");
    let second = tables
        .assign_identity(&resumed_assignment(2, 2, "session-two"))
        .expect("assign two");

    assert_ne!(first.agent_identifier, second.agent_identifier);
}

#[test]
fn mint_grows_code_length_when_a_length_saturates_and_errors_when_the_span_exhausts() {
    let saturated: Vec<String> = "0123456789abcdefghijklmnopqrstuvwxyz"
        .chars()
        .map(|character| character.to_string())
        .collect();

    // A saturated one-character keyspace cannot mint: typed span exhaustion.
    let exhausted = AgentIdentifierMint::with_code_length_bounds(saturated.clone(), 1, 1);
    assert!(matches!(
        exhausted.next_identifier(),
        Err(Error::AgentIdentifierSpanExhausted {
            minimum: 1,
            maximum: 1
        })
    ));

    // With one more length available the mint grows instead of failing.
    let growing = AgentIdentifierMint::with_code_length_bounds(saturated, 1, 2);
    let grown = growing.next_identifier().expect("grown identifier");
    assert_eq!(grown.payload().len(), 2);
}

#[test]
fn registry_persists_across_store_reopen() {
    let root = TempDir::new().expect("store dir");

    let assigned = {
        let tables = open_tables(&root);
        let assigned = tables
            .assign_identity(&resumed_assignment(4242, 77, "session-durable"))
            .expect("assign");
        tables
            .bind_endpoint(&AgentEndpointBinding {
                agent_identifier: assigned.agent_identifier.clone(),
                agent_endpoint: pty_endpoint("/run/terminal-cell/session-a/data.sock"),
                harness_pid: HarnessPid::new(4242),
                harness_start_time: HarnessStartTime::new(77),
            })
            .expect("bind")
            .expect("known identifier");
        assigned
    };

    let reopened = open_tables(&root);
    let entries = reopened
        .query_entries(&AgentRegistryQuery::All)
        .expect("query after reopen");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].agent_identifier, assigned.agent_identifier);
    assert_eq!(
        entries[0].endpoint_selection,
        EndpointSelection::Bound(pty_endpoint("/run/terminal-cell/session-a/data.sock"))
    );
    assert_eq!(
        entries[0].resume_selection,
        ResumeSelection::Resumed(ResumeIdentity::new("session-durable".to_owned()))
    );

    let reused = reopened
        .assign_identity(&resumed_assignment(5000, 88, "session-durable"))
        .expect("assign after reopen");
    assert_eq!(reused.identity_provenance, IdentityProvenance::Reused);
    assert_eq!(reused.agent_identifier, assigned.agent_identifier);
}

#[test]
fn binding_an_unknown_identifier_is_reported_not_committed() {
    let root = TempDir::new().expect("store dir");
    let tables = open_tables(&root);

    let bound = tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: AgentIdentifier::new("zzzz".to_owned()),
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
fn engine_assign_bind_query_run_the_full_signal_nexus_sema_path() {
    let root = TempDir::new().expect("store dir");
    let mut engine = engine_for(&root);
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let assigned = match runtime
        .block_on(engine.handle(
            Input::assign_agent_identity(resumed_assignment(4242, 99, "session-e2e")),
            &owner_connection(),
        ))
        .expect("assign reply")
    {
        Output::AgentIdentityAssigned(assigned) => assigned.into_payload(),
        other => panic!("expected AgentIdentityAssigned, got {other:?}"),
    };
    assert_eq!(assigned.identity_provenance, IdentityProvenance::Minted);

    let bound = runtime
        .block_on(engine.handle(
            Input::bind_agent_endpoint(AgentEndpointBinding {
                agent_identifier: assigned.agent_identifier.clone(),
                agent_endpoint: pty_endpoint("/run/terminal-cell/session-e2e/data.sock"),
                harness_pid: HarnessPid::new(4242),
                harness_start_time: HarnessStartTime::new(99),
            }),
            &owner_connection(),
        ))
        .expect("bind reply");
    match bound {
        Output::AgentEndpointBound(bound) => {
            assert_eq!(
                bound.into_payload().into_payload(),
                assigned.agent_identifier
            );
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
                agent_identifier: AgentIdentifier::new("zzzz".to_owned()),
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
