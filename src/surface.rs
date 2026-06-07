use nota_next::{NotaDecode, NotaEncode};
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
