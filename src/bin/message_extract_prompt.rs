//! Resolve a source reference into the `FlowDeliver` Query a caller pastes to
//! `message` — from one inline Datom value, without retyping the living's
//! words. This is not a Nexus CLI: it opens no socket and speaks to no Nexus,
//! only turns a source reference into Datom text for one.

use message::{PromptExtraction, text};
use thiserror::Error;

fn main() {
    if let Err(error) = run() {
        eprintln!("message-extract-prompt: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), PromptExtractionCommandError> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let request = text::read::<PromptExtraction>(text::sole_argument(&arguments)?)?;
    println!("{}", text::write(&request.resolve()?));
    Ok(())
}

#[derive(Debug, Error)]
enum PromptExtractionCommandError {
    #[error("{0}")]
    Text(#[from] message::text::TextError),

    #[error("{0}")]
    Extraction(#[from] message::ExtractionError),
}
