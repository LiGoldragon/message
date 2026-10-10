#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Bind_Data {
    pub address: signal_flow::Address,
    pub process: signal_flow::Process,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Lock_Data {
    pub sender: signal_flow::Sender,
    pub recipient: signal_flow::Recipient,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Deliver_Data {
    pub lock: signal_flow::Lock,
    pub request: signal_flow::Request,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    Bind(Bind_Data),
    Identify(signal_flow::Process),
    Lock(Lock_Data),
    Deliver(Deliver_Data),
    Release(signal_flow::Lock),
    Store(message_library::Configuration),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Failed_Data_Unknown_Data {
    Address(signal_flow::Address),
    Lock(signal_flow::Lock),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Failed_Data {
    Unidentified(signal_flow::Process),
    Taken(signal_flow::Address),
    NotMessage,
    NoneAbove,
    Unknown(Failed_Data_Unknown_Data),
    Ended(signal_flow::Address),
    Asleep,
    OffRoute,
    Held(signal_flow::Lock),
    Locked,
    NoLayer,
    Lapsed,
    FlowUnreachable,
    StoreRefused,
    Store(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Bound(signal_flow::FlowId),
    Identified(signal_flow::Address),
    Locked(signal_flow::Lock),
    Delivered,
    Woken(signal_flow::FlowId),
    Queued,
    Released,
    Stored,
    Failed(Failed_Data),
}
