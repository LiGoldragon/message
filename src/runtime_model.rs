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
