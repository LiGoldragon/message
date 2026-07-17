//! The messenger's durable store: `messenger.sema`.
//!
//! Born in train packet 2.1 with its first family, the agent registry — the
//! durable consumer view of agent identity plus the local delivery registry.
//! The ORCHESTRATOR is the mint (psyche-ruled 2026-07-17): identities arrive
//! already allocated, and the registry seats them. The stored record IS the
//! emitted wire noun (`AgentRegistryEntry`): agent identifier, endpoint
//! selection, resume identity, death mark, and an optional pid + start-time
//! process pin (`None` until the allocated process launches; the start time
//! disambiguates a recycled pid). This is the durability the router's
//! in-memory actor registry never had: a daemon restart no longer forgets
//! route-back.
//!
//! The message ledger, per-recipient inbox, and thread index join this store
//! as sibling families in packet 3.1. The messenger participates in no
//! version-handover snapshot (that Mirror mechanism is orchestrate's own);
//! store continuity across daemon versions is carried by the store file and
//! its per-family migrations alone.

use sema_engine::{
    Engine, EngineOpen, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference, VersionedStoreName,
    VersioningPolicy,
};

use crate::schema::signal::{
    AgentDeathMark, AgentEndpointBinding, AgentIdentityAssignment, AgentRegistryEntry,
    AgentRegistryQuery, AssignedAgentIdentity, BoundAgentEndpoint, EndpointSelection,
    HarnessProcessPin, IdentityProvenance, ProcessPinSelection,
};
use crate::Result;

/// Bumped when any messenger family's stored layout changes; each family pins
/// the version at which its own layout was last set (the orchestrate
/// convention), so unchanged families keep their catalog identity across
/// store-version bumps.
///
/// Bumped 1 -> 2 for the mint relocation: the registry entry's mandatory pid
/// pin became an optional `ProcessPinSelection` (an orchestrator-allocated
/// identity exists before its process does). No v1 store was ever deployed,
/// so a v1 file fails closed rather than migrating.
const MESSENGER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(2);

const AGENT_REGISTRY: TableName = TableName::new("agent_registry");

/// The messenger's registered families over one `messenger.sema` engine.
///
/// Debug is hand-written: the sema engine handle is not a value worth
/// printing, and the table set is static.
pub struct MessengerTables {
    engine: Engine,
    agent_registry: TableReference<AgentRegistryEntry>,
}

impl std::fmt::Debug for MessengerTables {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessengerTables")
            .field("agent_registry", &AGENT_REGISTRY)
            .finish_non_exhaustive()
    }
}

impl MessengerTables {
    /// Open (or create) the store at the configured database path and
    /// register the current family set.
    pub fn open(database_path: &std::path::Path) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(database_path, MESSENGER_SCHEMA_VERSION)
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
        )?;
        let agent_registry = engine.register_table(Self::family_descriptor(
            AGENT_REGISTRY,
            "agent-registry",
            MESSENGER_SCHEMA_VERSION,
        ))?;
        Ok(Self {
            engine,
            agent_registry,
        })
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
        version: SchemaVersion,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!("messenger-{family}-v{}", version.value())),
        )
    }

    /// Seat an orchestrator-supplied identity. The orchestrator is the mint,
    /// so the identifier arrives with the assignment: a fresh identifier
    /// seats a new row (`Seated`); a known identifier is reseated
    /// (`Reseated`) — process pin and resume identity refreshed, stale
    /// endpoint cleared until the new process re-binds, death mark reset,
    /// since a reseat declares fresh launch intent (e.g. a cold respawn).
    pub fn seat_identity(
        &self,
        assignment: &AgentIdentityAssignment,
    ) -> Result<AssignedAgentIdentity> {
        let identity_provenance = if self
            .entry(assignment.agent_identifier.payload())?
            .is_some()
        {
            IdentityProvenance::Reseated
        } else {
            IdentityProvenance::Seated
        };
        let entry = AgentRegistryEntry {
            agent_identifier: assignment.agent_identifier.clone(),
            endpoint_selection: EndpointSelection::None,
            resume_selection: assignment.resume_selection.clone(),
            agent_death_mark: AgentDeathMark::NotDead,
            process_pin_selection: assignment.process_pin_selection.clone(),
        };
        self.upsert_entry(&entry)?;
        Ok(AssignedAgentIdentity {
            agent_identifier: assignment.agent_identifier.clone(),
            identity_provenance,
        })
    }

    /// Bind (or refresh) a registered agent's live delivery endpoint and
    /// process pin. `None` means the identifier is unknown — the caller owes
    /// the typed rejection.
    pub fn bind_endpoint(
        &self,
        binding: &AgentEndpointBinding,
    ) -> Result<Option<BoundAgentEndpoint>> {
        let Some(existing) = self.entry(binding.agent_identifier.payload())? else {
            return Ok(None);
        };
        let bound = AgentRegistryEntry {
            agent_identifier: existing.agent_identifier.clone(),
            endpoint_selection: EndpointSelection::Bound(binding.agent_endpoint.clone()),
            resume_selection: existing.resume_selection,
            agent_death_mark: existing.agent_death_mark,
            process_pin_selection: ProcessPinSelection::Pinned(HarnessProcessPin {
                harness_pid: binding.harness_pid.clone(),
                harness_start_time: binding.harness_start_time.clone(),
            }),
        };
        self.upsert_entry(&bound)?;
        Ok(Some(BoundAgentEndpoint::new(existing.agent_identifier)))
    }

    /// Read the registry: everything, or one agent's row.
    pub fn query_entries(&self, query: &AgentRegistryQuery) -> Result<Vec<AgentRegistryEntry>> {
        match query {
            AgentRegistryQuery::All => self.registry_entries(),
            AgentRegistryQuery::ByAgent(agent_identifier) => Ok(self
                .entry(agent_identifier.payload())?
                .into_iter()
                .collect()),
        }
    }

    fn registry_entries(&self) -> Result<Vec<AgentRegistryEntry>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.agent_registry))?
            .records()
            .to_vec())
    }

    fn entry(&self, agent_identifier: &str) -> Result<Option<AgentRegistryEntry>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.agent_registry,
                RecordKey::new(agent_identifier),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn upsert_entry(&self, entry: &AgentRegistryEntry) -> Result<()> {
        let key = entry.agent_identifier.payload().as_str();
        let record_key = RecordKey::new(key);
        if self.entry(key)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.agent_registry,
                record_key,
                entry.clone(),
            ))?;
        } else {
            self.engine.assert_keyed(KeyedAssertion::new(
                self.agent_registry,
                record_key,
                entry.clone(),
            ))?;
        }
        Ok(())
    }
}
