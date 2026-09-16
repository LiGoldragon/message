//! Messenger-owned durable and decision values.
//!
//! Wire identities come directly from `signal-message`. These records exist
//! only because the messenger owns their durable arrangement; none mirror a
//! producer Type or form a second public contract.

use crate::relay::DeliveryState;
use rkyv::{Archive, Deserialize, Serialize};
use signal_message::{
    AgentEndpointBinding, AgentIdentityAssignment, InboxQuery, MessageOrigin, MessageRecipient,
    MessageSlot, MessageSubmission, Participants, StampedAt, ThreadIndexQuery, ThreadName,
    ThreadRelationSelection, ThreadSubscription,
};

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub(crate) struct RelayRecord {
    pub destination: String,
    pub origin: MessageOrigin,
    pub envelope: signal_message::TypedPromptEnvelope,
    pub state: DeliveryState,
}

impl RelayRecord {
    /// The durable address of a relay row: destination and source event, each
    /// length-prefixed so that no pair of values can spell another pair's
    /// key. Both populations of this family — the prompt relay and the flow
    /// park — address rows by this one spelling, written once.
    pub(crate) fn key_for(destination: &str, source_event_identifier: &str) -> String {
        format!(
            "{}:{destination}{}:{source_event_identifier}",
            destination.len(),
            source_event_identifier.len()
        )
    }
}

/// Identity is trait-borne: a relay row knows its own address, so a commit
/// never depends on a caller re-spelling it.
impl sema_engine::EngineRecord for RelayRecord {
    fn record_key(&self) -> sema_engine::RecordKey {
        sema_engine::RecordKey::new(Self::key_for(
            &self.destination,
            &self.envelope.source_event_identifier,
        ))
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SenderName(String);

impl SenderName {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn payload(&self) -> &String {
        &self.0
    }

    pub fn into_payload(self) -> String {
        self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LedgerDraft {
    pub message_submission: MessageSubmission,
    pub message_origin: MessageOrigin,
    pub sender_name: SenderName,
    pub stamped_at: StampedAt,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LedgerRecord {
    pub message_slot: MessageSlot,
    pub message_submission: MessageSubmission,
    pub message_origin: MessageOrigin,
    pub sender_name: SenderName,
    pub stamped_at: StampedAt,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NextMessageSlot(MessageSlot);

impl NextMessageSlot {
    pub fn new(value: MessageSlot) -> Self {
        Self(value)
    }

    pub fn payload(&self) -> &MessageSlot {
        &self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct OldestMessageSlot(MessageSlot);

impl OldestMessageSlot {
    pub fn new(value: MessageSlot) -> Self {
        Self(value)
    }

    pub fn payload(&self) -> &MessageSlot {
        &self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LedgerHead {
    pub next_message_slot: NextMessageSlot,
    pub oldest_message_slot: OldestMessageSlot,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Slots(Vec<MessageSlot>);

impl Slots {
    pub fn new(values: Vec<MessageSlot>) -> Self {
        Self(values)
    }

    pub fn payload(&self) -> &Vec<MessageSlot> {
        &self.0
    }

    pub fn into_payload(self) -> Vec<MessageSlot> {
        self.0
    }

    pub fn payload_mut(&mut self) -> &mut Vec<MessageSlot> {
        &mut self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct InboxRecord {
    pub recipient: MessageRecipient,
    pub slots: Slots,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ThreadRecord {
    pub thread_name: ThreadName,
    pub thread_relation_selection: ThreadRelationSelection,
    pub participants: Participants,
    pub slots: Slots,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgentRegistryCommand {
    AssignIdentity(AgentIdentityAssignment),
    BindEndpoint(AgentEndpointBinding),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoreCommand {
    RecordSubmission(MessageSubmission),
    Subscribe(ThreadSubscription),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoreQuery {
    Inbox(InboxQuery),
    Thread(signal_message::ThreadQuery),
    Threads(ThreadIndexQuery),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StoreWrite {
    RecordSubmission(LedgerDraft),
    Subscribe(ThreadSubscription),
}
