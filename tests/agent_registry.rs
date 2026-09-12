use message::MessengerTables;
use signal_message::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentityAssignment,
    AgentRegistryQuery, IdentityProvenance, ProcessPinSelection, ResumeSelection,
};

#[test]
fn orchestrator_identity_is_seated_then_endpoint_is_bound() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let agent = "li7f".to_owned();
    let seated = tables
        .seat_identity(&AgentIdentityAssignment {
            agent_identifier: agent.clone(),
            process_pin_selection: ProcessPinSelection::None,
            resume_selection: ResumeSelection::None,
        })
        .unwrap();
    assert_eq!(seated.identity_provenance, IdentityProvenance::Seated);

    let bound = tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: agent.clone(),
            agent_endpoint: AgentEndpoint {
                agent_endpoint_kind: AgentEndpointKind::HarnessSocket,
                endpoint_path: "/tmp/li7f.sock".to_owned(),
            },
            harness_pid: 77,
            harness_start_time: 88,
        })
        .unwrap()
        .unwrap();
    assert_eq!(bound, "li7f");

    let entries = tables
        .query_entries(&AgentRegistryQuery::ByAgent(agent))
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert!(matches!(
        entries[0].endpoint_selection,
        signal_message::EndpointSelection::Bound(_)
    ));
}

#[test]
fn unknown_endpoint_binding_is_rejected_without_minting_identity() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let result = tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: "unknown".to_owned(),
            agent_endpoint: AgentEndpoint {
                agent_endpoint_kind: AgentEndpointKind::HarnessSocket,
                endpoint_path: "/tmp/unknown.sock".to_owned(),
            },
            harness_pid: 1,
            harness_start_time: 2,
        })
        .unwrap();
    assert!(result.is_none());
}
