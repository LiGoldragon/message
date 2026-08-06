//! Messenger-owned durable and decision values.
//!
//! Wire identities come directly from `signal-message`. These records exist
//! only because the messenger owns their durable arrangement; none mirror a
//! producer Type or form a second public contract.

use rkyv::{Archive, Deserialize, Serialize};
use signal_message::schema::lib::{
    z2VLC8, z2VLZR, z2VMa5, z2VMd2, z2VSVi, z2VTJ1, z2VUSt, z2VVAD, z2VVDs, z2VY2v, z2VY18, z2Vari,
    z2VevD,
};

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerDraft {
    pub message_submission: z2VY2v,
    pub message_origin: z2VTJ1,
    pub sender_name: SenderName,
    pub stamped_at: z2VY18,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerRecord {
    pub message_slot: z2VLZR,
    pub message_submission: z2VY2v,
    pub message_origin: z2VTJ1,
    pub sender_name: SenderName,
    pub stamped_at: z2VY18,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct NextMessageSlot(z2VLZR);

impl NextMessageSlot {
    pub fn new(value: z2VLZR) -> Self {
        Self(value)
    }

    pub fn payload(&self) -> &z2VLZR {
        &self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OldestMessageSlot(z2VLZR);

impl OldestMessageSlot {
    pub fn new(value: z2VLZR) -> Self {
        Self(value)
    }

    pub fn payload(&self) -> &z2VLZR {
        &self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerHead {
    pub next_message_slot: NextMessageSlot,
    pub oldest_message_slot: OldestMessageSlot,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Slots(Vec<z2VLZR>);

impl Slots {
    pub fn new(values: Vec<z2VLZR>) -> Self {
        Self(values)
    }

    pub fn payload(&self) -> &Vec<z2VLZR> {
        &self.0
    }

    pub fn into_payload(self) -> Vec<z2VLZR> {
        self.0
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InboxRecord {
    pub recipient: z2Vari,
    pub slots: Slots,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ThreadRecord {
    pub thread_name: z2VUSt,
    pub thread_relation_selection: z2VVDs,
    pub participants: z2VMa5,
    pub slots: Slots,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentRegistryCommand {
    AssignIdentity(z2VevD),
    BindEndpoint(z2VVAD),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreCommand {
    RecordSubmission(z2VY2v),
    Subscribe(z2VMd2),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreQuery {
    Inbox(z2VSVi),
    Thread(signal_message::schema::lib::z2VVrY),
    Threads(z2VLC8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreWrite {
    RecordSubmission(LedgerDraft),
    Subscribe(z2VMd2),
}
