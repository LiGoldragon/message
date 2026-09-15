//! Source-verifying cluster relay preparation.
//!
//! `relay` has only two public arguments: the first and final six prompt
//! words.  Identity, membership, and the selected cluster come from the
//! calling flow's declared environment; the model never constructs a prompt
//! body or a Message registry.  The command prints a producer-owned
//! `ClusterMessage` Datom header followed by the byte-exact source body.  A
//! transport adapter consumes those two values after it has selected an
//! actually supported delivery leg.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use datom_codec::Datomizable;
use protos::{Protosizable, Textualizable};
use serde_json::Value;
use sha2::{Digest, Sha256};
use signal_message::{ClusterMember, ClusterMessage, ClusterRelay, ClusterTarget};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn main() {
    match run(env::args().skip(1).collect()) {
        Ok(rendered) => print!("{rendered}"),
        Err(error) => {
            eprintln!("relay: {error}");
            std::process::exit(1);
        }
    }
}

fn run(arguments: Vec<String>) -> Result<String, String> {
    let [head, tail] = arguments.as_slice() else {
        return Err("expected exactly two arguments: first-six-words last-six-words".into());
    };
    let flow_identifier = required("FLOW_ID")?;
    let session_identifier = required("RELAY_SESSION_ID")?;
    let members = members(&required("RELAY_CLUSTER_MEMBERS")?)?;
    let declared_target = env::var("RELAY_CLUSTER_TARGET").ok();
    let cluster_target = cluster_target(declared_target.as_deref())?;
    let source = locate(head, tail)?;
    let header = ClusterMessage::Relay(ClusterRelay {
        flow_identifier,
        session_identifier,
        transcript_path: source.path.display().to_string(),
        prompt_first_six_words: head.clone(),
        prompt_last_six_words: tail.clone(),
        prompt_sha256: source.sha256,
        timestamp_nanos: source.timestamp_nanos,
        cluster_target,
        cluster_members: members,
    });
    Ok(format!(
        "{}\n\n{}",
        header.datomize(vec![]).protosize().textualize(),
        source.body
    ))
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing {name}; flow launch must declare it"))
}

fn cluster_target(value: Option<&str>) -> Result<ClusterTarget, String> {
    match value.unwrap_or("Primary") {
        "Primary" => Ok(ClusterTarget::Primary),
        "Secondary" => Ok(ClusterTarget::Secondary),
        "Core" => Ok(ClusterTarget::Core),
        other => Err(format!("unknown RELAY_CLUSTER_TARGET {other:?}")),
    }
}

/// Setup declaration, outside the public call: comma-separated `flow@session`
/// members.  A future Flow query replaces this one adapter without changing
/// the producer-owned `ClusterMessage` shape.
fn members(declaration: &str) -> Result<Vec<ClusterMember>, String> {
    declaration
        .split(',')
        .filter(|member| !member.is_empty())
        .map(|member| match member.split_once('@') {
            Some((flow_identifier, session_identifier))
                if !flow_identifier.is_empty() && !session_identifier.is_empty() =>
            {
                Ok(ClusterMember {
                    flow_identifier: flow_identifier.to_owned(),
                    session_identifier: session_identifier.to_owned(),
                })
            }
            _ => Err(format!(
                "invalid cluster member {member:?}; expected flow@session"
            )),
        })
        .collect()
}

#[derive(Debug)]
struct Source {
    path: PathBuf,
    body: String,
    sha256: String,
    timestamp_nanos: i64,
}

fn locate(head: &str, tail: &str) -> Result<Source, String> {
    let paths = match env::var_os("RELAY_TRANSCRIPT") {
        Some(path) => vec![PathBuf::from(path)],
        None => return Err(
            "missing RELAY_TRANSCRIPT; process-to-transcript discovery adapter is not installed"
                .into(),
        ),
    };
    let mut matches = Vec::new();
    for path in paths {
        matches.extend(records(&path, head, tail)?);
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err("no user record has the supplied first and last six words".into()),
        count => Err(format!(
            "{count} user records match; selectors are ambiguous"
        )),
    }
}

fn records(path: &Path, head: &str, tail: &str) -> Result<Vec<Source>, String> {
    let input =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut found = Vec::new();
    for line in input.lines() {
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(body) = user_body(&value) else {
            continue;
        };
        if first_six(&body) != head || last_six(&body) != tail {
            continue;
        }
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("matched record in {} has no timestamp", path.display()))?;
        let timestamp_nanos = OffsetDateTime::parse(timestamp, &Rfc3339)
            .map_err(|error| format!("parse source timestamp: {error}"))?
            .unix_timestamp_nanos()
            .try_into()
            .map_err(|_| "source timestamp exceeds TimestampNanos".to_owned())?;
        found.push(Source {
            path: path.to_path_buf(),
            sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
            body,
            timestamp_nanos,
        });
    }
    Ok(found)
}

fn user_body(value: &Value) -> Option<String> {
    if value.get("type")?.as_str()? == "queue-operation"
        && value.get("operation")?.as_str()? == "enqueue"
    {
        return value.get("content")?.as_str().map(str::to_owned);
    }
    let payload = value.get("payload")?;
    if value.get("type")?.as_str()? != "response_item"
        || payload.get("type")?.as_str()? != "message"
        || payload.get("role")?.as_str()? != "user"
    {
        return None;
    }
    payload.get("content")?.as_array()?.iter().find_map(|part| {
        matches!(part.get("type")?.as_str()?, "input_text" | "text")
            .then(|| part.get("text")?.as_str().map(str::to_owned))?
    })
}

fn first_six(body: &str) -> String {
    body.split_whitespace()
        .take(6)
        .collect::<Vec<_>>()
        .join(" ")
}
fn last_six(body: &str) -> String {
    let words = body.split_whitespace().collect::<Vec<_>>();
    words[words.len().saturating_sub(6)..].join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_record_requires_exact_word_boundaries_and_preserves_body_hash() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.jsonl");
        let body = "one two three four five six seven eight nine ten eleven twelve";
        fs::write(&path, format!("{{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-09-15T22:28:09.894Z\",\"content\":\"{body}\"}}\n")).unwrap();
        let found = records(
            &path,
            "one two three four five six",
            "seven eight nine ten eleven twelve",
        )
        .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].body, body);
        assert_eq!(
            found[0].sha256,
            format!("{:x}", Sha256::digest(body.as_bytes()))
        );
    }

    #[test]
    fn members_are_setup_data_not_public_prompt_arguments() {
        assert_eq!(members("cf7879@root,57a7aa@secondary").unwrap().len(), 2);
        assert!(members("not-a-member").is_err());
    }
}
