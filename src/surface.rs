use nota::{NotaDecode, NotaEncode};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// Name of the message recipient as written on the CLI surface.
#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
)]
pub struct RecipientName(String);

impl RecipientName {
    /// Creates a recipient name from the CLI text projection.
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// Returns the recipient name text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Explicit thread name as written on the CLI surface: a plain
/// sender-chosen wire token.
#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
)]
pub struct ThreadName(String);

impl ThreadName {
    /// Creates a thread name from the CLI text projection.
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// Returns the thread name text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// The sender's thread choice on the CLI surface. Optionality is a typed
/// positional slot, never an omitted field: `None` folds a message into the
/// default derived direct thread; `Named` names an explicit thread verbatim.
#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum ThreadSelection {
    None,
    Named(ThreadName),
}
