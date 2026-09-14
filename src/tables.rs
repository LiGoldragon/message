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
//! The message ledger, per-recipient inbox, and thread index joined this
//! store as sibling families in packet 3.1: the messenger is now the single
//! durable owner of local message state. The ledger is a BOUNDED window
//! (`LEDGER_RETENTION_LIMIT`), never an unbounded archive — entries reflect
//! reality, and every stored message carries its ingress timestamp so age is
//! always documented. The messenger participates in no version-handover
//! snapshot (that Mirror mechanism is orchestrate's own); store continuity
//! across daemon versions is carried by the store file and its per-family
//! migrations alone.

use sema_engine::{
    Engine, EngineOpen, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference, VersionedStoreName,
    VersioningPolicy,
};

use crate::Result;
use crate::runtime_model::{
    InboxRecord, LedgerDraft, LedgerHead, LedgerRecord, NextMessageSlot, OldestMessageSlot,
    RelayAttemptCount, RelayRecord, Slots, ThreadRecord,
};
use crate::store_preserve::PreMigrationPreserve;
use signal_message::{
    AgentDeathMark, AgentEndpointBinding, AgentIdentityAssignment, AgentRegistryEntry,
    AgentRegistryQuery, AssignedAgentIdentity, BoundAgentEndpoint, EndpointSelection, HarnessPid,
    HarnessProcessPin, HarnessStartTime, IdentityProvenance, InboxEntry, InboxQuery,
    MessageRecipient, MessageSlot, ParticipantName, ProcessPinSelection, SubmissionAcceptance,
    ThreadContents, ThreadEntry, ThreadName, ThreadRelationSelection, ThreadSelection,
    ThreadSubscription, ThreadSubscriptionAcknowledgment, ThreadSummary,
};

/// The storage kernel's own meta table and version key — the store-level
/// schema stamp the additive re-stamp rewrites (the orchestrate convention;
/// the kernel offers no stamp-rewrite API yet, a recorded engine debt).
const SEMA_META: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("__sema_meta");
const SEMA_SCHEMA_VERSION_KEY: &str = "schema_version";

/// Bumped when any messenger family's stored layout changes; each family pins
/// the version at which its own layout was last set (the orchestrate
/// convention), so unchanged families keep their catalog identity across
/// store-version bumps.
///
/// Bumped 1 -> 2 for the mint relocation: the registry entry's mandatory pid
/// pin became an optional `ProcessPinSelection` (an orchestrator-allocated
/// identity exists before its process does). No v1 store was ever deployed,
/// so a v1 file fails closed rather than migrating.
///
/// Bumped 2 -> 3 for the messenger promotion (packet 3.1): the message
/// ledger, ledger head, per-recipient inbox, and thread index families are
/// born. The agent registry's layout is unchanged and keeps its v2 catalog
/// identity, so v2 -> v3 is purely additive: a v2 store (production's was
/// born at v2 on 2026-07-18) is preserved aside, re-stamped, and re-opened
/// with the new families empty. A v1 file still fails closed: no v1 store
/// was ever deployed.
///
/// v4 is the Datom-stack move and is **not** additive. Every durable record
/// embeds producer-owned contract types, and those types changed projection:
/// the retired generator wrapped each one in a newtype over `u64`, while the
/// current contract carries the plain signed `Integer` and named struct
/// fields. The archived bytes of `LedgerRecord`, `InboxRecord`, `ThreadRecord`
/// and the registry row therefore differ from v3's, so a v3 store must not be
/// re-stamped forward and read as if it were v4 — that would be silent
/// corruption. v3 is deliberately absent from the additive list below and
/// fails closed, preserving the file aside for an operator to decide about.
const MESSENGER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(6);

/// The prior store versions whose every intervening family layout is additive
/// up to the current version — a store stamped at one of these re-stamps
/// forward after a pre-migration preserve, carrying its rows unchanged.
///
/// v4 -> v5 adds the independent prompt-relay family; existing families keep
/// their layout and are preserved before the store is re-stamped.
// v6 adds only the attempt-count sidecar; existing relay bytes stay at v5.
const ADDITIVE_PRIOR_VERSIONS: [SchemaVersion; 2] = [SchemaVersion::new(4), SchemaVersion::new(5)];

/// The store version at which the agent registry's layout was last set.
const AGENT_REGISTRY_LAYOUT_VERSION: SchemaVersion = SchemaVersion::new(4);
const MESSAGE_LAYOUT_VERSION: SchemaVersion = SchemaVersion::new(4);

/// The bounded ledger window: the store keeps at most this many messages;
/// older messages are reaped oldest-first together with their inbox and
/// thread references. Unchecked data expansion is a defect class, not a
/// feature.
const LEDGER_RETENTION_LIMIT: MessageSlot = 1024;

const AGENT_REGISTRY: TableName = TableName::new("agent_registry");
const MESSAGE_LEDGER: TableName = TableName::new("message_ledger");
const LEDGER_HEAD: TableName = TableName::new("ledger_head");
const RECIPIENT_INBOX: TableName = TableName::new("recipient_inbox");
const THREAD_INDEX: TableName = TableName::new("thread_index");
const DELIVERY_OUTBOX: TableName = TableName::new("delivery_outbox");
const PROMPT_RELAY: TableName = TableName::new("prompt_relay_v2");

const LEDGER_HEAD_KEY: &str = "head";

/// The messenger's registered families over one `messenger.sema` engine.
///
/// Debug is hand-written: the sema engine handle is not a value worth
/// printing, and the table set is static.
pub struct MessengerTables {
    engine: Engine,
    pub(crate) prompt_dispatch_claim: std::sync::Mutex<()>,
    agent_registry: TableReference<AgentRegistryEntry>,
    message_ledger: TableReference<LedgerRecord>,
    ledger_head: TableReference<LedgerHead>,
    recipient_inbox: TableReference<InboxRecord>,
    thread_index: TableReference<ThreadRecord>,
    delivery_outbox: TableReference<InboxRecord>,
    prompt_relay: TableReference<RelayRecord>,
    prompt_attempts: TableReference<RelayAttemptCount>,
}

impl std::fmt::Debug for MessengerTables {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MessengerTables")
            .field("agent_registry", &AGENT_REGISTRY)
            .field("message_ledger", &MESSAGE_LEDGER)
            .field("recipient_inbox", &RECIPIENT_INBOX)
            .field("thread_index", &THREAD_INDEX)
            .finish_non_exhaustive()
    }
}

impl MessengerTables {
    /// Open (or create) the store at the configured database path and
    /// register the current family set. A store stamped at a known additive
    /// prior version is preserved aside, re-stamped, and re-opened; any other
    /// mismatch fails closed unchanged.
    pub fn open(database_path: &std::path::Path) -> Result<Self> {
        match Self::open_current(database_path) {
            Ok(tables) => Ok(tables),
            Err(error) => MessengerStoreMigration::new(database_path).open_after_migration(error),
        }
    }

    /// Open at the current schema version with no repair attempted.
    fn open_current(database_path: &std::path::Path) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(database_path, MESSENGER_SCHEMA_VERSION)
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
        )?;
        let agent_registry = engine.register_table(Self::family_descriptor(
            AGENT_REGISTRY,
            "agent-registry",
            AGENT_REGISTRY_LAYOUT_VERSION,
        ))?;
        let message_ledger = engine.register_table(Self::family_descriptor(
            MESSAGE_LEDGER,
            "message-ledger",
            MESSAGE_LAYOUT_VERSION,
        ))?;
        let ledger_head = engine.register_table(Self::family_descriptor(
            LEDGER_HEAD,
            "message-ledger-head",
            MESSAGE_LAYOUT_VERSION,
        ))?;
        let recipient_inbox = engine.register_table(Self::family_descriptor(
            RECIPIENT_INBOX,
            "recipient-inbox",
            MESSAGE_LAYOUT_VERSION,
        ))?;
        let thread_index = engine.register_table(Self::family_descriptor(
            THREAD_INDEX,
            "thread-index",
            MESSAGE_LAYOUT_VERSION,
        ))?;
        let delivery_outbox = engine.register_table(Self::family_descriptor(
            DELIVERY_OUTBOX,
            "delivery-outbox",
            MESSAGE_LAYOUT_VERSION,
        ))?;
        let prompt_relay = engine.register_table(Self::family_descriptor(
            PROMPT_RELAY,
            "prompt-relay",
            SchemaVersion::new(5),
        ))?;
        let prompt_attempts = engine.register_table(Self::family_descriptor(
            TableName::new("prompt_attempts"),
            "prompt-attempts",
            SchemaVersion::new(6),
        ))?;
        Ok(Self {
            engine,
            prompt_dispatch_claim: std::sync::Mutex::new(()),
            agent_registry,
            message_ledger,
            ledger_head,
            recipient_inbox,
            thread_index,
            delivery_outbox,
            prompt_relay,
            prompt_attempts,
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

    pub(crate) fn relay_attempts(&self, key: &str) -> Result<Option<RelayAttemptCount>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.prompt_attempts, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    pub(crate) fn set_relay_attempts(&self, key: &str, value: RelayAttemptCount) -> Result<()> {
        if self.relay_attempts(key)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.prompt_attempts,
                RecordKey::new(key),
                value,
            ))?;
        } else {
            self.engine.assert_keyed(KeyedAssertion::new(
                self.prompt_attempts,
                RecordKey::new(key),
                value,
            ))?;
        }
        Ok(())
    }

    pub(crate) fn relay_record(&self, key: &str) -> Result<Option<RelayRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.prompt_relay, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    pub(crate) fn admit_relay_record(&self, key: &str, record: RelayRecord) -> Result<()> {
        self.engine.assert_keyed(KeyedAssertion::new(
            self.prompt_relay,
            RecordKey::new(key),
            record,
        ))?;
        Ok(())
    }

    pub(crate) fn replace_relay_record(&self, key: &str, record: RelayRecord) -> Result<()> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.prompt_relay,
            RecordKey::new(key),
            record,
        ))?;
        Ok(())
    }

    pub(crate) fn relay_records(&self) -> Result<Vec<RelayRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.prompt_relay))?
            .records()
            .to_vec())
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
        let identity_provenance = if self.entry(&assignment.agent_identifier)?.is_some() {
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
        let Some(existing) = self.entry(&binding.agent_identifier)? else {
            return Ok(None);
        };
        let bound = AgentRegistryEntry {
            agent_identifier: existing.agent_identifier.clone(),
            endpoint_selection: EndpointSelection::Bound(binding.agent_endpoint.clone()),
            resume_selection: existing.resume_selection,
            agent_death_mark: existing.agent_death_mark,
            process_pin_selection: ProcessPinSelection::Pinned(HarnessProcessPin {
                harness_pid: binding.harness_pid,
                harness_start_time: binding.harness_start_time,
            }),
        };
        self.upsert_entry(&bound)?;
        Ok(Some(existing.agent_identifier))
    }

    /// Read the registry: everything, or one agent's row.
    pub fn query_entries(&self, query: &AgentRegistryQuery) -> Result<Vec<AgentRegistryEntry>> {
        match query {
            AgentRegistryQuery::All => self.registry_entries(),
            AgentRegistryQuery::ByAgent(agent_identifier) => {
                Ok(self.entry(agent_identifier)?.into_iter().collect())
            }
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
        let key = entry.agent_identifier.as_str();
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

    /// One registry row by agent identifier — the delivery runner's
    /// resolution read.
    pub fn registry_entry(&self, agent_identifier: &str) -> Result<Option<AgentRegistryEntry>> {
        self.entry(agent_identifier)
    }

    /// One ledger row by slot — the delivery runner's drain read.
    pub fn ledger_record_public(&self, slot: MessageSlot) -> Result<Option<LedgerRecord>> {
        self.ledger_record(slot)
    }

    /// A thread's participant names, or `None` when no such thread exists —
    /// the delivery runner's fan-out read.
    pub fn thread_participants(&self, thread_name: &str) -> Result<Option<Vec<String>>> {
        Ok(self
            .thread_record(&thread_name.to_owned())?
            .map(|record| record.participants.to_vec()))
    }

    /// The parked delivery slots for one agent.
    pub fn outbox_slots(&self, agent_identifier: &str) -> Result<Vec<MessageSlot>> {
        Ok(self
            .outbox_record(agent_identifier)?
            .map(|record| record.slots.payload().to_vec())
            .unwrap_or_default())
    }

    /// Park one slot for an agent whose endpoint is absent or unreachable.
    pub fn append_outbox_slot(&self, agent_identifier: &str, slot: MessageSlot) -> Result<()> {
        match self.outbox_record(agent_identifier)? {
            Some(record) => {
                let mut slots = record.slots;
                if slots.payload().contains(&slot) {
                    return Ok(());
                }
                slots.payload_mut().push(slot);
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.delivery_outbox,
                    RecordKey::new(agent_identifier),
                    InboxRecord {
                        recipient: agent_identifier.to_owned(),
                        slots,
                    },
                ))?;
            }
            None => {
                self.engine.assert_keyed(KeyedAssertion::new(
                    self.delivery_outbox,
                    RecordKey::new(agent_identifier),
                    InboxRecord {
                        recipient: agent_identifier.to_owned(),
                        slots: Slots::new(vec![slot]),
                    },
                ))?;
            }
        }
        Ok(())
    }

    /// Unpark one delivered (or reaped) slot.
    pub fn remove_outbox_slot(&self, agent_identifier: &str, slot: MessageSlot) -> Result<()> {
        if let Some(record) = self.outbox_record(agent_identifier)? {
            let mut slots = record.slots;
            slots.payload_mut().retain(|kept| *kept != slot);
            self.engine.mutate_keyed(KeyedMutation::new(
                self.delivery_outbox,
                RecordKey::new(agent_identifier),
                InboxRecord {
                    recipient: agent_identifier.to_owned(),
                    slots,
                },
            ))?;
        }
        Ok(())
    }

    fn outbox_record(&self, agent_identifier: &str) -> Result<Option<InboxRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_outbox,
                RecordKey::new(agent_identifier),
            ))?
            .records()
            .first()
            .cloned())
    }

    /// Every registry row currently carrying a live process pin, projected
    /// for sender resolution: identifier plus the pid + start-time pin.
    pub fn pinned_agent_identities(&self) -> Result<Vec<PinnedAgentIdentity>> {
        Ok(self
            .registry_entries()?
            .into_iter()
            .filter_map(PinnedAgentIdentity::from_entry)
            .collect())
    }

    /// Record one provenance-stamped submission: a ledger row under a fresh
    /// slot, an inbox reference for the recipient, and — when the submission
    /// names a thread — a thread-index reference with the sender and
    /// recipient auto-joined as participants. The ledger window is bounded:
    /// past `LEDGER_RETENTION_LIMIT`, the oldest messages are reaped together
    /// with their references before the new row commits its acceptance.
    pub fn store_submission(&self, draft: &LedgerDraft) -> Result<SubmissionAcceptance> {
        let head = self.ledger_head()?;
        let slot = *head.next_message_slot.payload();
        let record = LedgerRecord {
            message_slot: slot,
            message_submission: draft.message_submission.clone(),
            message_origin: draft.message_origin.clone(),
            sender_name: draft.sender_name.clone(),
            stamped_at: draft.stamped_at,
        };
        self.insert_ledger_record(&record)?;
        self.append_inbox_slot(&draft.message_submission.message_recipient, slot)?;
        if let ThreadSelection::Named(thread_name) = &draft.message_submission.thread_selection {
            self.append_thread_slot(
                thread_name,
                slot,
                &[
                    draft.sender_name.payload().clone(),
                    draft.message_submission.message_recipient.clone(),
                ],
            )?;
        }
        let advanced = LedgerHead {
            next_message_slot: NextMessageSlot::new(slot + 1),
            oldest_message_slot: head.oldest_message_slot,
        };
        self.write_ledger_head(&advanced)?;
        self.reap_beyond_retention(&advanced)?;
        Ok(slot)
    }

    /// Subscribe a participant to a thread, creating the thread when absent
    /// (threads are plain sender-chosen names — no minting ceremony). A
    /// `Related` selection sets or replaces the thread's relation; `None`
    /// leaves any existing relation untouched.
    pub fn subscribe_thread(
        &self,
        subscription: &ThreadSubscription,
    ) -> Result<ThreadSubscriptionAcknowledgment> {
        let existing = self.thread_record(&subscription.thread_name)?;
        let mut record = existing.unwrap_or_else(|| ThreadRecord {
            thread_name: subscription.thread_name.clone(),
            thread_relation_selection: ThreadRelationSelection::None,
            participants: Vec::new(),
            slots: Slots::new(Vec::new()),
        });
        if let ThreadRelationSelection::Related(relation) = &subscription.thread_relation_selection
        {
            record.thread_relation_selection = ThreadRelationSelection::Related(relation.clone());
        }
        record.join_participant(&subscription.participant_name);
        self.upsert_thread_record(&record)?;
        Ok(ThreadSubscriptionAcknowledgment {
            thread_name: subscription.thread_name.clone(),
            participant_name: subscription.participant_name.clone(),
        })
    }

    /// The recipient's inbox, resolved through the ledger into typed entries.
    /// Reaped slots drop out naturally: only slots whose ledger row still
    /// exists are listed.
    pub fn inbox_entries(&self, query: &InboxQuery) -> Result<Vec<InboxEntry>> {
        let Some(record) = self.inbox_record(query.as_str())? else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for slot in record.slots.payload() {
            if let Some(row) = self.ledger_record(*slot)? {
                entries.push(InboxEntry {
                    message_slot: row.message_slot,
                    message_sender: row.sender_name.payload().clone(),
                    message_body: row.message_submission.message_body,
                    thread_selection: row.message_submission.thread_selection,
                    stamped_at: row.stamped_at,
                });
            }
        }
        Ok(entries)
    }

    /// One thread's contents: relation, participants, and its surviving
    /// ledger entries. `None` means the thread does not exist.
    pub fn thread_contents(&self, thread_name: &ThreadName) -> Result<Option<ThreadContents>> {
        let Some(record) = self.thread_record(thread_name)? else {
            return Ok(None);
        };
        let mut entries = Vec::new();
        for slot in record.slots.payload() {
            if let Some(row) = self.ledger_record(*slot)? {
                entries.push(ThreadEntry {
                    message_slot: row.message_slot,
                    message_sender: row.sender_name.payload().clone(),
                    message_body: row.message_submission.message_body,
                    stamped_at: row.stamped_at,
                });
            }
        }
        Ok(Some(ThreadContents {
            thread_name: record.thread_name,
            thread_relation_selection: record.thread_relation_selection,
            participants: record.participants,
            thread_entries: entries,
        }))
    }

    /// Every thread, summarized: relation, participants, surviving message
    /// count.
    pub fn thread_summaries(&self) -> Result<Vec<ThreadSummary>> {
        let records = self
            .engine
            .match_records(QueryPlan::all(self.thread_index))?
            .records()
            .to_vec();
        let mut summaries = Vec::new();
        for record in records {
            let mut count: MessageSlot = 0;
            for slot in record.slots.payload() {
                if self.ledger_record(*slot)?.is_some() {
                    count += 1;
                }
            }
            summaries.push(ThreadSummary {
                thread_name: record.thread_name,
                thread_relation_selection: record.thread_relation_selection,
                participants: record.participants,
                message_count: count,
            });
        }
        Ok(summaries)
    }

    fn ledger_head(&self) -> Result<LedgerHead> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.ledger_head,
                RecordKey::new(LEDGER_HEAD_KEY),
            ))?
            .records()
            .first()
            .cloned()
            .unwrap_or_else(|| LedgerHead {
                next_message_slot: NextMessageSlot::new(0),
                oldest_message_slot: OldestMessageSlot::new(0),
            }))
    }

    fn write_ledger_head(&self, head: &LedgerHead) -> Result<()> {
        let key = RecordKey::new(LEDGER_HEAD_KEY);
        let exists = !self
            .engine
            .match_records(QueryPlan::key(
                self.ledger_head,
                RecordKey::new(LEDGER_HEAD_KEY),
            ))?
            .records()
            .is_empty();
        if exists {
            self.engine
                .mutate_keyed(KeyedMutation::new(self.ledger_head, key, head.clone()))?;
        } else {
            self.engine
                .assert_keyed(KeyedAssertion::new(self.ledger_head, key, head.clone()))?;
        }
        Ok(())
    }

    fn ledger_record(&self, slot: MessageSlot) -> Result<Option<LedgerRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.message_ledger,
                RecordKey::new(Self::slot_key(slot).as_str()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn insert_ledger_record(&self, record: &LedgerRecord) -> Result<()> {
        self.engine.assert_keyed(KeyedAssertion::new(
            self.message_ledger,
            RecordKey::new(Self::slot_key(record.message_slot).as_str()),
            record.clone(),
        ))?;
        Ok(())
    }

    fn remove_ledger_record(&self, slot: MessageSlot) -> Result<()> {
        self.engine.retract(sema_engine::Retraction::new(
            self.message_ledger,
            RecordKey::new(Self::slot_key(slot).as_str()),
        ))?;
        Ok(())
    }

    fn slot_key(slot: MessageSlot) -> String {
        format!("{slot:020}")
    }

    fn inbox_record(&self, recipient: &str) -> Result<Option<InboxRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.recipient_inbox,
                RecordKey::new(recipient),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn append_inbox_slot(&self, recipient: &MessageRecipient, slot: MessageSlot) -> Result<()> {
        let key = recipient.as_str();
        match self.inbox_record(key)? {
            Some(record) => {
                let mut slots = record.slots;
                slots.payload_mut().push(slot);
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.recipient_inbox,
                    RecordKey::new(key),
                    InboxRecord {
                        recipient: record.recipient,
                        slots,
                    },
                ))?;
            }
            None => {
                self.engine.assert_keyed(KeyedAssertion::new(
                    self.recipient_inbox,
                    RecordKey::new(key),
                    InboxRecord {
                        recipient: recipient.clone(),
                        slots: Slots::new(vec![slot]),
                    },
                ))?;
            }
        }
        Ok(())
    }

    fn thread_record(&self, thread_name: &ThreadName) -> Result<Option<ThreadRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.thread_index,
                RecordKey::new(thread_name.as_str()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn upsert_thread_record(&self, record: &ThreadRecord) -> Result<()> {
        let key = record.thread_name.as_str();
        if self.thread_record(&record.thread_name)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.thread_index,
                RecordKey::new(key),
                record.clone(),
            ))?;
        } else {
            self.engine.assert_keyed(KeyedAssertion::new(
                self.thread_index,
                RecordKey::new(key),
                record.clone(),
            ))?;
        }
        Ok(())
    }

    fn append_thread_slot(
        &self,
        thread_name: &ThreadName,
        slot: MessageSlot,
        joining: &[ParticipantName],
    ) -> Result<()> {
        let mut record = self
            .thread_record(thread_name)?
            .unwrap_or_else(|| ThreadRecord {
                thread_name: thread_name.clone(),
                thread_relation_selection: ThreadRelationSelection::None,
                participants: Vec::new(),
                slots: Slots::new(Vec::new()),
            });
        let mut slots = record.slots;
        slots.payload_mut().push(slot);
        record.slots = slots;
        for participant in joining {
            record.join_participant(participant);
        }
        self.upsert_thread_record(&record)?;
        Ok(())
    }

    /// Reap the oldest ledger rows past the retention window, dropping their
    /// inbox and thread references with them. Thread and inbox records keep
    /// their identity (participants and relations survive); only the message
    /// references age out.
    fn reap_beyond_retention(&self, head: &LedgerHead) -> Result<()> {
        let next = *head.next_message_slot.payload();
        let mut oldest = *head.oldest_message_slot.payload();
        if next - oldest <= LEDGER_RETENTION_LIMIT {
            return Ok(());
        }
        while next - oldest > LEDGER_RETENTION_LIMIT {
            if let Some(record) = self.ledger_record(oldest)? {
                self.remove_inbox_slot(&record.message_submission.message_recipient, oldest)?;
                if let ThreadSelection::Named(thread_name) =
                    &record.message_submission.thread_selection
                {
                    self.remove_thread_slot(thread_name, oldest)?;
                }
                self.remove_ledger_record(oldest)?;
            }
            oldest += 1;
        }
        self.write_ledger_head(&LedgerHead {
            next_message_slot: head.next_message_slot.clone(),
            oldest_message_slot: OldestMessageSlot::new(oldest),
        })
    }

    fn remove_inbox_slot(&self, recipient: &MessageRecipient, slot: MessageSlot) -> Result<()> {
        let key = recipient.as_str();
        if let Some(record) = self.inbox_record(key)? {
            let mut slots = record.slots;
            slots.payload_mut().retain(|kept| *kept != slot);
            self.engine.mutate_keyed(KeyedMutation::new(
                self.recipient_inbox,
                RecordKey::new(key),
                InboxRecord {
                    recipient: record.recipient,
                    slots,
                },
            ))?;
        }
        Ok(())
    }

    fn remove_thread_slot(&self, thread_name: &ThreadName, slot: MessageSlot) -> Result<()> {
        if let Some(mut record) = self.thread_record(thread_name)? {
            let mut slots = record.slots;
            slots.payload_mut().retain(|kept| *kept != slot);
            record.slots = slots;
            self.upsert_thread_record(&record)?;
        }
        Ok(())
    }
}

impl ThreadRecord {
    /// Add a participant if absent — participants accumulate, never
    /// duplicate.
    fn join_participant(&mut self, participant: &ParticipantName) {
        let present = self
            .participants
            .iter()
            .any(|existing| existing == participant);
        if !present {
            let mut names = self.participants.clone();
            names.push(participant.clone());
            self.participants = names;
        }
    }
}

/// One registry row's process pin, projected for sender resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct PinnedAgentIdentity {
    identifier: String,
    harness_pid: HarnessPid,
    harness_start_time: HarnessStartTime,
}

impl PinnedAgentIdentity {
    fn from_entry(entry: AgentRegistryEntry) -> Option<Self> {
        match entry.process_pin_selection {
            ProcessPinSelection::Pinned(pin) => Some(Self {
                identifier: entry.agent_identifier,
                harness_pid: pin.harness_pid,
                harness_start_time: pin.harness_start_time,
            }),
            ProcessPinSelection::None => None,
        }
    }

    pub fn identifier(&self) -> &str {
        self.identifier.as_str()
    }

    /// Whether a live process (pid + start time) is this pin's process
    /// generation.
    pub fn matches(&self, pid: i32, start_time: HarnessStartTime) -> bool {
        HarnessPid::from(pid) == self.harness_pid && start_time == self.harness_start_time
    }
}

/// Clears the one recognised on-disk store defect — a schema stamp at a known
/// additive prior version — and re-opens. Any error that maps to no repair
/// surfaces unchanged, so a genuinely incompatible store fails closed rather
/// than being silently mutated. Before the repair mutates the file, the store
/// is copied aside as a [`PreMigrationPreserve`]; a preserve failure aborts
/// the migration.
struct MessengerStoreMigration<'store> {
    store: &'store std::path::Path,
}

impl<'store> MessengerStoreMigration<'store> {
    fn new(store: &'store std::path::Path) -> Self {
        Self { store }
    }

    fn open_after_migration(&self, error: crate::Error) -> Result<MessengerTables> {
        let Some(found) = self.additive_prior_stamp(&error) else {
            return Err(error);
        };
        PreMigrationPreserve::create(self.store, MESSENGER_SCHEMA_VERSION)?;
        self.stamp_current_schema_version(found)?;
        MessengerTables::open_current(self.store)
    }

    /// The prior version stamped on the store, when — and only when — the
    /// open failed on a version mismatch from the current expectation to a
    /// declared additive prior.
    fn additive_prior_stamp(&self, error: &crate::Error) -> Option<SchemaVersion> {
        match error {
            crate::Error::SemaEngine(sema_engine::Error::Sema(
                sema_engine::StorageKernelError::SchemaVersionMismatch { expected, found },
            )) if *expected == MESSENGER_SCHEMA_VERSION
                && ADDITIVE_PRIOR_VERSIONS.contains(found) =>
            {
                Some(*found)
            }
            _ => None,
        }
    }

    /// Verify the file genuinely opens at the found prior version, then
    /// rewrite the store-level stamp to the current version. Unchanged
    /// families keep their rows; families introduced since the prior version
    /// open empty on the next registration.
    fn stamp_current_schema_version(&self, found: SchemaVersion) -> Result<()> {
        let storage = sema::Sema::open_with_schema(self.store, &sema::Schema { version: found })
            .map_err(sema_engine::Error::from)?;
        drop(storage);
        let database = redb::Database::create(self.store)
            .map_err(|source| self.failure(source.to_string()))?;
        let transaction = database
            .begin_write()
            .map_err(|source| self.failure(source.to_string()))?;
        {
            let mut table = transaction
                .open_table(SEMA_META)
                .map_err(|source| self.failure(source.to_string()))?;
            table
                .insert(
                    SEMA_SCHEMA_VERSION_KEY,
                    MESSENGER_SCHEMA_VERSION.value() as u64,
                )
                .map_err(|source| self.failure(source.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|source| self.failure(source.to_string()))?;
        Ok(())
    }

    fn failure(&self, message: String) -> crate::Error {
        crate::Error::StoreMigration {
            store: self.store.display().to_string(),
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_model::{LedgerHead, NextMessageSlot, OldestMessageSlot};

    #[test]
    fn v4_ledger_catalog_and_row_survive_v5_relay_addition() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("messenger.sema");
        {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(4))
                    .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
            )
            .unwrap();
            let ledger_head = engine
                .register_table(TableDescriptor::new(
                    LEDGER_HEAD,
                    FamilyName::new("message-ledger-head"),
                    SchemaHash::for_label("messenger-message-ledger-head-v4"),
                ))
                .unwrap();
            engine
                .assert_keyed(KeyedAssertion::new(
                    ledger_head,
                    RecordKey::new(LEDGER_HEAD_KEY),
                    LedgerHead {
                        next_message_slot: NextMessageSlot::new(7),
                        oldest_message_slot: OldestMessageSlot::new(1),
                    },
                ))
                .unwrap();
        }
        let tables = MessengerTables::open(&path).unwrap();
        let head = tables.ledger_head().unwrap();
        assert_eq!(*head.next_message_slot.payload(), 7);
        assert_eq!(*head.oldest_message_slot.payload(), 1);
        assert!(tables.relay_records().unwrap().is_empty());
    }
    #[test]
    fn v5_relay_rows_survive_attempt_sidecar_addition_without_invented_history() {
        use signal_message::{
            ConnectionClass, MessageOrigin, PromptInterpretationSelection, PromptVariant,
            TypedPromptEnvelope,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("messenger.sema");
        let record = RelayRecord {
            source_agent_identifier: "source".into(),
            destination: "destination".into(),
            origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
            envelope: TypedPromptEnvelope {
                prompt_variant: PromptVariant::HumanPrompt,
                source_event_identifier: "event".into(),
                raw_prompt_text: "preserved".into(),
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
            state: crate::relay::DeliveryState::Unknown,
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(5))).unwrap();
            let family = engine
                .register_table(MessengerTables::family_descriptor(
                    PROMPT_RELAY,
                    "prompt-relay",
                    SchemaVersion::new(5),
                ))
                .unwrap();
            engine
                .assert_keyed(KeyedAssertion::new(
                    family,
                    RecordKey::new("old"),
                    record.clone(),
                ))
                .unwrap();
        }
        let tables = MessengerTables::open(&path).unwrap();
        assert_eq!(tables.relay_record("old").unwrap(), Some(record));
        assert!(tables.relay_attempts("old").unwrap().is_none());
    }
}
