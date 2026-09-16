//! Typed prompt extraction: a source transcript plus a source event id
//! resolved into a `TypedPromptEnvelope`, so a `FlowDeliver` caller never
//! retypes the living's words by hand.
//!
//! Ports `tools/prompt-relay`'s proven design — WITNESSED,
//! `flows/57a7aa/reports/messageRelayNexusPocs.md` §4 — into pure Rust: its
//! `select()` locates the one durable record whose id matches (a repeat or
//! zero matches is a refusal, never a silent guess) and its `payload()`
//! carries a hash of the exact bytes alongside the text, never a
//! re-derivation of it. Ruled fork (a), `flows/57a7aa/log.md`: the *design*
//! folds in, not the retired Node script — this module owns no JavaScript
//! and no Python.
//!
//! Only records the source format marks `origin.kind == "human"` are
//! eligible, mirroring the anti-loop discipline of the retired script: a
//! flow's own prior turns are never candidates for re-delivery.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use signal_message::{
    FlowDeliveryRequest, PromptInterpretationSelection, PromptVariant, Query,
    SourceEventIdentifier, TargetFlowName, TypedPromptEnvelope,
};

/// One eligible record read out of a transcript: its id and its exact text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleRecord {
    pub source_event_identifier: SourceEventIdentifier,
    pub raw_prompt_text: String,
}

/// A transcript format able to enumerate its own eligible (human-authored)
/// records. Concrete formats implement this; extraction itself never reads a
/// file directly.
pub trait TranscriptSource {
    fn eligible_records(&self) -> Result<Vec<EligibleRecord>, ExtractionError>;
}

/// The address of one prompt inside one transcript: what a caller supplies
/// instead of retyping the text itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceReference {
    pub source_transcript_path: PathBuf,
    pub source_event_identifier: SourceEventIdentifier,
}

impl SourceReference {
    pub fn new(
        source_transcript_path: impl Into<PathBuf>,
        source_event_identifier: impl Into<SourceEventIdentifier>,
    ) -> Self {
        Self {
            source_transcript_path: source_transcript_path.into(),
            source_event_identifier: source_event_identifier.into(),
        }
    }
}

/// The result of one extraction: the envelope ready for `FlowDeliveryRequest`,
/// plus the sha256 of its exact bytes — carried, per the ported design, never
/// re-derived downstream.
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractedPrompt {
    pub typed_prompt_envelope: TypedPromptEnvelope,
    pub source_sha256_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExtractionError {
    #[error("source transcript {path:?} could not be read: {detail}")]
    UnreadableTranscript { path: PathBuf, detail: String },
    #[error("source transcript {path:?} line {line} is not a well-formed record: {detail}")]
    MalformedRecord {
        path: PathBuf,
        line: usize,
        detail: String,
    },
    #[error("no eligible human-authored record carries source event identifier {0:?}")]
    Unknown(SourceEventIdentifier),
    #[error(
        "{1} eligible human-authored records carry source event identifier {0:?}; resolution must be unambiguous"
    )]
    Ambiguous(SourceEventIdentifier, usize),
}

/// The one inline Datom value `message-extract-prompt` takes: a source
/// reference plus the flow the extracted words are for. This is the CLI's
/// whole text surface, gated by a test rather than buried in the binary —
/// the same convention `ConfigurationWriteRequest` set in `config.rs`.
#[derive(Debug, Clone, PartialEq, datom_codec::Datomizable, datom_codec::Composing)]
pub struct PromptExtraction {
    pub source_transcript_path: String,
    pub source_event_identifier: SourceEventIdentifier,
    pub target_flow_name: TargetFlowName,
}

impl PromptExtraction {
    /// Resolve this request against its named transcript and produce the
    /// exact `Query` a caller pastes to `message` — the extraction's whole
    /// point: no step between the transcript and the wire retypes the text.
    pub fn resolve(self) -> Result<Query, ExtractionError> {
        let reference =
            SourceReference::new(self.source_transcript_path, self.source_event_identifier);
        let source = JsonlTranscript::open(reference.source_transcript_path.clone());
        let extracted = PromptExtractor::extract(&reference, &source)?;
        Ok(Query::FlowDeliver(FlowDeliveryRequest {
            typed_prompt_envelope: extracted.typed_prompt_envelope,
            target_flow_name: self.target_flow_name,
        }))
    }
}

/// The extraction call itself: one source reference resolved against one
/// transcript source, exactly as `select()` did — a durable record located by
/// id, never by re-derivation of its text.
pub struct PromptExtractor;

impl PromptExtractor {
    pub fn extract(
        reference: &SourceReference,
        source: &impl TranscriptSource,
    ) -> Result<ExtractedPrompt, ExtractionError> {
        let mut matches = source
            .eligible_records()?
            .into_iter()
            .filter(|record| record.source_event_identifier == reference.source_event_identifier);
        let first = matches
            .next()
            .ok_or_else(|| ExtractionError::Unknown(reference.source_event_identifier.clone()))?;
        if let Some(_second) = matches.next() {
            let remaining = matches.count();
            return Err(ExtractionError::Ambiguous(
                reference.source_event_identifier.clone(),
                2 + remaining,
            ));
        }
        let source_sha256_hex = hex_sha256(first.raw_prompt_text.as_bytes());
        Ok(ExtractedPrompt {
            typed_prompt_envelope: TypedPromptEnvelope {
                prompt_variant: PromptVariant::HumanPrompt,
                source_event_identifier: first.source_event_identifier,
                raw_prompt_text: first.raw_prompt_text,
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
            source_sha256_hex,
        })
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// One line of a Claude-style session transcript: JSON Lines, one record per
/// line. The shape `tools/prompt-relay` called the `claude` source format.
#[derive(Debug, Clone, serde::Deserialize)]
struct TranscriptLine {
    #[serde(rename = "type")]
    record_type: Option<String>,
    message: Option<TranscriptMessage>,
    origin: Option<TranscriptOrigin>,
    uuid: Option<String>,
    #[serde(rename = "promptId")]
    prompt_id: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TranscriptMessage {
    role: Option<String>,
    content: TranscriptContent,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
enum TranscriptContent {
    Text(String),
    Blocks(Vec<TranscriptBlock>),
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TranscriptBlock {
    #[serde(rename = "type")]
    block_type: Option<String>,
    text: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct TranscriptOrigin {
    kind: Option<String>,
}

impl TranscriptContent {
    fn flattened(&self) -> String {
        match self {
            TranscriptContent::Text(text) => text.clone(),
            TranscriptContent::Blocks(blocks) => blocks
                .iter()
                .filter(|block| block.block_type.as_deref().unwrap_or("text") == "text")
                .filter_map(|block| block.text.as_deref())
                .collect::<Vec<_>>()
                .join(""),
        }
    }
}

/// A Claude-style JSONL session transcript on disk. The one concrete
/// `TranscriptSource` this stage ships; other formats (`codex-rollout`,
/// generic `codex`) are a later increment on the same trait.
pub struct JsonlTranscript {
    path: PathBuf,
}

impl JsonlTranscript {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl TranscriptSource for JsonlTranscript {
    fn eligible_records(&self) -> Result<Vec<EligibleRecord>, ExtractionError> {
        eligible_records_in(&self.path)
    }
}

fn eligible_records_in(path: &Path) -> Result<Vec<EligibleRecord>, ExtractionError> {
    let text =
        std::fs::read_to_string(path).map_err(|error| ExtractionError::UnreadableTranscript {
            path: path.to_path_buf(),
            detail: error.to_string(),
        })?;
    let mut records = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: TranscriptLine =
            serde_json::from_str(line).map_err(|error| ExtractionError::MalformedRecord {
                path: path.to_path_buf(),
                line: index + 1,
                detail: error.to_string(),
            })?;
        if parsed.record_type.as_deref() != Some("user") {
            continue;
        }
        let Some(message) = &parsed.message else {
            continue;
        };
        if message.role.as_deref() != Some("user") {
            continue;
        }
        let is_human = parsed
            .origin
            .as_ref()
            .and_then(|origin| origin.kind.as_deref())
            == Some("human");
        if !is_human {
            continue;
        }
        let Some(source_event_identifier) =
            parsed.uuid.clone().or_else(|| parsed.prompt_id.clone())
        else {
            continue;
        };
        records.push(EligibleRecord {
            source_event_identifier,
            raw_prompt_text: message.content.flattened(),
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(directory: &Path, lines: &[&str]) -> PathBuf {
        let path = directory.join("transcript.jsonl");
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    #[test]
    fn a_known_human_record_is_extracted_with_its_exact_bytes_and_sha() {
        let directory = tempfile::tempdir().unwrap();
        let path = fixture(
            directory.path(),
            &[
                r#"{"type":"user","message":{"role":"user","content":"deliver this exact text"},"uuid":"src-1","origin":{"kind":"human"}}"#,
            ],
        );
        let source = JsonlTranscript::open(&path);
        let reference = SourceReference::new(&path, "src-1");
        let extracted = PromptExtractor::extract(&reference, &source).unwrap();
        assert_eq!(
            extracted.typed_prompt_envelope.raw_prompt_text,
            "deliver this exact text"
        );
        assert_eq!(
            extracted.typed_prompt_envelope.source_event_identifier,
            "src-1"
        );
        assert_eq!(
            extracted.typed_prompt_envelope.prompt_variant,
            PromptVariant::HumanPrompt
        );
        assert_eq!(
            extracted.source_sha256_hex,
            hex_sha256("deliver this exact text".as_bytes())
        );
    }

    #[test]
    fn a_non_human_origin_is_never_eligible() {
        let directory = tempfile::tempdir().unwrap();
        let path = fixture(
            directory.path(),
            &[
                r#"{"type":"user","message":{"role":"user","content":"assistant self-talk"},"uuid":"src-2","origin":{"kind":"agent"}}"#,
            ],
        );
        let source = JsonlTranscript::open(&path);
        let reference = SourceReference::new(&path, "src-2");
        assert_eq!(
            PromptExtractor::extract(&reference, &source),
            Err(ExtractionError::Unknown("src-2".to_owned()))
        );
    }

    #[test]
    fn an_unmatched_source_event_identifier_is_refused_not_guessed() {
        let directory = tempfile::tempdir().unwrap();
        let path = fixture(
            directory.path(),
            &[
                r#"{"type":"user","message":{"role":"user","content":"text"},"uuid":"src-1","origin":{"kind":"human"}}"#,
            ],
        );
        let source = JsonlTranscript::open(&path);
        let reference = SourceReference::new(&path, "no-such-id");
        assert_eq!(
            PromptExtractor::extract(&reference, &source),
            Err(ExtractionError::Unknown("no-such-id".to_owned()))
        );
    }

    #[test]
    fn a_repeated_source_event_identifier_is_ambiguous_not_a_silent_first_pick() {
        let directory = tempfile::tempdir().unwrap();
        let path = fixture(
            directory.path(),
            &[
                r#"{"type":"user","message":{"role":"user","content":"first"},"uuid":"dup","origin":{"kind":"human"}}"#,
                r#"{"type":"user","message":{"role":"user","content":"second"},"uuid":"dup","origin":{"kind":"human"}}"#,
            ],
        );
        let source = JsonlTranscript::open(&path);
        let reference = SourceReference::new(&path, "dup");
        assert_eq!(
            PromptExtractor::extract(&reference, &source),
            Err(ExtractionError::Ambiguous("dup".to_owned(), 2))
        );
    }

    #[test]
    fn content_blocks_flatten_to_their_text_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let path = fixture(
            directory.path(),
            &[
                r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"part one "},{"type":"image"},{"type":"text","text":"part two"}]},"uuid":"src-3","origin":{"kind":"human"}}"#,
            ],
        );
        let source = JsonlTranscript::open(&path);
        let reference = SourceReference::new(&path, "src-3");
        let extracted = PromptExtractor::extract(&reference, &source).unwrap();
        assert_eq!(
            extracted.typed_prompt_envelope.raw_prompt_text,
            "part one part two"
        );
    }
}
