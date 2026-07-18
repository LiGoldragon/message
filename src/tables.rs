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

use crate::schema::signal::{
    AgentDeathMark, AgentEndpointBinding, AgentIdentityAssignment, AgentRegistryEntry,
    AgentRegistryQuery, AssignedAgentIdentity, BoundAgentEndpoint, EndpointSelection,
    HarnessProcessPin, IdentityProvenance, InboxEntry, InboxQuery, InboxRecord, LedgerDraft,
    LedgerHead, LedgerRecord, MessageCount, MessageSlot, NextMessageSlot, OldestMessageSlot,
    ParticipantName, Participants, ProcessPinSelection, Recipient, Slots, SubmissionAcceptance,
    ThreadContents, ThreadEntries, ThreadEntry, ThreadName, ThreadRecord,
    ThreadRelationSelection, ThreadSubscription, ThreadSubscriptionAcknowledgment, ThreadSummary,
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
///
/// Bumped 2 -> 3 for the messenger promotion (packet 3.1): the message
/// ledger, ledger head, per-recipient inbox, and thread index families are
/// born. The agent registry's layout is unchanged and keeps its v2 catalog
/// identity. No v2 store was ever deployed, so a v2 file fails closed rather
/// than migrating.
const MESSENGER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);

/// The store version at which the agent registry's layout was last set.
const AGENT_REGISTRY_LAYOUT_VERSION: SchemaVersion = SchemaVersion::new(2);

/// The bounded ledger window: the store keeps at most this many messages;
/// older messages are reaped oldest-first together with their inbox and
/// thread references. Unchecked data expansion is a defect class, not a
/// feature.
const LEDGER_RETENTION_LIMIT: u64 = 1024;

const AGENT_REGISTRY: TableName = TableName::new("agent_registry");
const MESSAGE_LEDGER: TableName = TableName::new("message_ledger");
const LEDGER_HEAD: TableName = TableName::new("ledger_head");
const RECIPIENT_INBOX: TableName = TableName::new("recipient_inbox");
const THREAD_INDEX: TableName = TableName::new("thread_index");

const LEDGER_HEAD_KEY: &str = "head";

/// The messenger's registered families over one `messenger.sema` engine.
///
/// Debug is hand-written: the sema engine handle is not a value worth
/// printing, and the table set is static.
pub struct MessengerTables {
    engine: Engine,
    agent_registry: TableReference<AgentRegistryEntry>,
    message_ledger: TableReference<LedgerRecord>,
    ledger_head: TableReference<LedgerHead>,
    recipient_inbox: TableReference<InboxRecord>,
    thread_index: TableReference<ThreadRecord>,
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
    /// register the current family set.
    pub fn open(database_path: &std::path::Path) -> Result<Self> {
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
            MESSENGER_SCHEMA_VERSION,
        ))?;
        let ledger_head = engine.register_table(Self::family_descriptor(
            LEDGER_HEAD,
            "message-ledger-head",
            MESSENGER_SCHEMA_VERSION,
        ))?;
        let recipient_inbox = engine.register_table(Self::family_descriptor(
            RECIPIENT_INBOX,
            "recipient-inbox",
            MESSENGER_SCHEMA_VERSION,
        ))?;
        let thread_index = engine.register_table(Self::family_descriptor(
            THREAD_INDEX,
            "thread-index",
            MESSENGER_SCHEMA_VERSION,
        ))?;
        Ok(Self {
            engine,
            agent_registry,
            message_ledger,
            ledger_head,
            recipient_inbox,
            thread_index,
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
        let slot = *head.next_message_slot.payload().payload();
        let record = LedgerRecord {
            message_slot: MessageSlot::new(slot),
            message_submission: draft.message_submission.clone(),
            message_origin: draft.message_origin.clone(),
            sender_name: draft.sender_name.clone(),
            stamped_at: draft.stamped_at.clone(),
        };
        self.insert_ledger_record(&record)?;
        self.append_inbox_slot(&draft.message_submission.recipient, slot)?;
        if let crate::schema::signal::ThreadSelection::Named(thread_name) =
            &draft.message_submission.thread_selection
        {
            self.append_thread_slot(
                thread_name,
                slot,
                &[
                    ParticipantName::new(draft.sender_name.payload().clone()),
                    ParticipantName::new(draft.message_submission.recipient.payload().clone()),
                ],
            )?;
        }
        let advanced = LedgerHead {
            next_message_slot: NextMessageSlot::new(MessageSlot::new(slot + 1)),
            oldest_message_slot: head.oldest_message_slot,
        };
        self.write_ledger_head(&advanced)?;
        self.reap_beyond_retention(&advanced)?;
        Ok(SubmissionAcceptance::new(MessageSlot::new(slot)))
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
            participants: Participants::new(Vec::new()),
            slots: Slots::new(Vec::new()),
        });
        if let ThreadRelationSelection::Related(relation) =
            &subscription.thread_relation_selection
        {
            record.thread_relation_selection =
                ThreadRelationSelection::Related(relation.clone());
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
        let Some(record) = self.inbox_record(query.payload().payload().as_str())? else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for slot in record.slots.payload() {
            if let Some(row) = self.ledger_record(*slot.payload())? {
                entries.push(InboxEntry {
                    message_slot: row.message_slot,
                    sender: crate::schema::signal::Sender::new(
                        row.sender_name.into_payload(),
                    ),
                    body: row.message_submission.body,
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
            if let Some(row) = self.ledger_record(*slot.payload())? {
                entries.push(ThreadEntry {
                    message_slot: row.message_slot,
                    sender: crate::schema::signal::Sender::new(
                        row.sender_name.into_payload(),
                    ),
                    body: row.message_submission.body,
                    stamped_at: row.stamped_at,
                });
            }
        }
        Ok(Some(ThreadContents {
            thread_name: record.thread_name,
            thread_relation_selection: record.thread_relation_selection,
            participants: record.participants,
            thread_entries: ThreadEntries::new(entries),
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
            let mut count = 0u64;
            for slot in record.slots.payload() {
                if self.ledger_record(*slot.payload())?.is_some() {
                    count += 1;
                }
            }
            summaries.push(ThreadSummary {
                thread_name: record.thread_name,
                thread_relation_selection: record.thread_relation_selection,
                participants: record.participants,
                message_count: MessageCount::new(count),
            });
        }
        Ok(summaries)
    }

    fn ledger_head(&self) -> Result<LedgerHead> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.ledger_head, RecordKey::new(LEDGER_HEAD_KEY)))?
            .records()
            .first()
            .cloned()
            .unwrap_or_else(|| LedgerHead {
                next_message_slot: NextMessageSlot::new(MessageSlot::new(0)),
                oldest_message_slot: OldestMessageSlot::new(MessageSlot::new(0)),
            }))
    }

    fn write_ledger_head(&self, head: &LedgerHead) -> Result<()> {
        let key = RecordKey::new(LEDGER_HEAD_KEY);
        let exists = !self
            .engine
            .match_records(QueryPlan::key(self.ledger_head, RecordKey::new(LEDGER_HEAD_KEY)))?
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

    fn ledger_record(&self, slot: u64) -> Result<Option<LedgerRecord>> {
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
            RecordKey::new(Self::slot_key(*record.message_slot.payload()).as_str()),
            record.clone(),
        ))?;
        Ok(())
    }

    fn remove_ledger_record(&self, slot: u64) -> Result<()> {
        self.engine.retract(sema_engine::Retraction::new(
            self.message_ledger,
            RecordKey::new(Self::slot_key(slot).as_str()),
        ))?;
        Ok(())
    }

    fn slot_key(slot: u64) -> String {
        format!("{slot:020}")
    }

    fn inbox_record(&self, recipient: &str) -> Result<Option<InboxRecord>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.recipient_inbox, RecordKey::new(recipient)))?
            .records()
            .first()
            .cloned())
    }

    fn append_inbox_slot(&self, recipient: &Recipient, slot: u64) -> Result<()> {
        let key = recipient.payload().as_str();
        match self.inbox_record(key)? {
            Some(record) => {
                let mut slots = record.slots.into_payload();
                slots.push(MessageSlot::new(slot));
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.recipient_inbox,
                    RecordKey::new(key),
                    InboxRecord {
                        recipient: record.recipient,
                        slots: Slots::new(slots),
                    },
                ))?;
            }
            None => {
                self.engine.assert_keyed(KeyedAssertion::new(
                    self.recipient_inbox,
                    RecordKey::new(key),
                    InboxRecord {
                        recipient: recipient.clone(),
                        slots: Slots::new(vec![MessageSlot::new(slot)]),
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
                RecordKey::new(thread_name.payload().as_str()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn upsert_thread_record(&self, record: &ThreadRecord) -> Result<()> {
        let key = record.thread_name.payload().as_str();
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
        slot: u64,
        joining: &[ParticipantName],
    ) -> Result<()> {
        let mut record = self
            .thread_record(thread_name)?
            .unwrap_or_else(|| ThreadRecord {
                thread_name: thread_name.clone(),
                thread_relation_selection: ThreadRelationSelection::None,
                participants: Participants::new(Vec::new()),
                slots: Slots::new(Vec::new()),
            });
        let mut slots = record.slots.into_payload();
        slots.push(MessageSlot::new(slot));
        record.slots = Slots::new(slots);
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
        let next = *head.next_message_slot.payload().payload();
        let mut oldest = *head.oldest_message_slot.payload().payload();
        if next - oldest <= LEDGER_RETENTION_LIMIT {
            return Ok(());
        }
        while next - oldest > LEDGER_RETENTION_LIMIT {
            if let Some(record) = self.ledger_record(oldest)? {
                self.remove_inbox_slot(&record.message_submission.recipient, oldest)?;
                if let crate::schema::signal::ThreadSelection::Named(thread_name) =
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
            oldest_message_slot: OldestMessageSlot::new(MessageSlot::new(oldest)),
        })
    }

    fn remove_inbox_slot(&self, recipient: &Recipient, slot: u64) -> Result<()> {
        let key = recipient.payload().as_str();
        if let Some(record) = self.inbox_record(key)? {
            let mut slots = record.slots.into_payload();
            slots.retain(|kept| *kept.payload() != slot);
            self.engine.mutate_keyed(KeyedMutation::new(
                self.recipient_inbox,
                RecordKey::new(key),
                InboxRecord {
                    recipient: record.recipient,
                    slots: Slots::new(slots),
                },
            ))?;
        }
        Ok(())
    }

    fn remove_thread_slot(&self, thread_name: &ThreadName, slot: u64) -> Result<()> {
        if let Some(mut record) = self.thread_record(thread_name)? {
            let mut slots = record.slots.into_payload();
            slots.retain(|kept| *kept.payload() != slot);
            record.slots = Slots::new(slots);
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
            .payload()
            .iter()
            .any(|existing| existing.payload() == participant.payload());
        if !present {
            let mut names = self.participants.clone().into_payload();
            names.push(participant.clone());
            self.participants = Participants::new(names);
        }
    }
}

/// One registry row's process pin, projected for sender resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedAgentIdentity {
    identifier: String,
    harness_pid: u64,
    harness_start_time: u64,
}

impl PinnedAgentIdentity {
    fn from_entry(entry: AgentRegistryEntry) -> Option<Self> {
        match entry.process_pin_selection {
            ProcessPinSelection::Pinned(pin) => Some(Self {
                identifier: entry.agent_identifier.into_payload(),
                harness_pid: *pin.harness_pid.payload(),
                harness_start_time: *pin.harness_start_time.payload(),
            }),
            ProcessPinSelection::None => None,
        }
    }

    pub fn identifier(&self) -> &str {
        self.identifier.as_str()
    }

    /// Whether a live process (pid + start time) is this pin's process
    /// generation.
    pub fn matches(&self, pid: i32, start_time: u64) -> bool {
        u64::try_from(pid).is_ok_and(|pid| pid == self.harness_pid)
            && start_time == self.harness_start_time
    }
}
