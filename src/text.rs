//! The Datom text surface shared by every command-line entry in this component.
//!
//! Each CLI takes exactly one inline Datom value and renders exactly one Datom
//! value; the contract's own type system is the whole interface. The codec
//! ceremony is written once, here.

use datom_codec::{Actualizing, Budget, Composing, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use thiserror::Error;

/// The reader and composer allowance one inline value may spend.
const ALLOWANCE: i64 = 1 << 20;
/// The deepest nesting an inline value may reach.
const MAXIMUM_DEPTH: i64 = 256;

fn budget() -> Budget {
    Budget {
        remaining: ALLOWANCE,
        reader: ReaderBudget {
            remaining: ALLOWANCE as usize,
        },
        depth: 0,
        maximum_depth: MAXIMUM_DEPTH,
    }
}

/// Read one inline Datom value as the contract type expected in its position.
pub fn read<T: Composing>(text: &str) -> Result<T, TextError> {
    Potential::<T>::from(text.to_owned())
        .actualize(&mut budget())
        .map_err(|error| TextError::Malformed {
            detail: format!("{error:?}"),
        })
}

/// Render a contract value as Datom text.
pub fn write<T>(value: &T) -> String
where
    T: Datomizable,
    T::Output: Protosizable,
    <T::Output as Protosizable>::Output: Textualizable,
{
    value.datomize(Vec::new()).protosize().textualize()
}

/// Take the one inline Datom argument a command-line entry is given.
pub fn sole_argument(arguments: &[String]) -> Result<&String, TextError> {
    match arguments {
        [text] => Ok(text),
        other => Err(TextError::ArgumentCount { count: other.len() }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TextError {
    #[error("expected exactly one inline Datom value, received {count}")]
    ArgumentCount { count: usize },
    #[error("malformed Datom value: {detail}")]
    Malformed { detail: String },
}
