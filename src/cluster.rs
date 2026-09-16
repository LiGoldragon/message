//! Producer-owned ClusterMessage rendering at the transport boundary.

use sha2::{Digest, Sha256};
use signal_message::ClusterMessage;

/// Decode a header and validate the separately framed, exact source body.
/// The boundary deliberately never finds a body separator in Datom text:
/// quoted fields themselves may contain blank lines.
pub fn verify(header: &str, body: &str) -> std::result::Result<String, String> {
    let message = crate::text::read::<ClusterMessage>(header)
        .map_err(|error| format!("decode ClusterMessage Datom: {error}"))?;
    let actual = format!("{:x}", Sha256::digest(body.as_bytes()));
    match &message {
        ClusterMessage::Relay(relay) if relay.prompt_sha256 != actual => {
            return Err(
                "ClusterMessage Relay prompt_sha256 does not match the verbatim body".into(),
            );
        }
        ClusterMessage::Peer(peer) if peer.peer_body != body || peer.peer_body_sha256 != actual => {
            return Err(
                "ClusterMessage Peer body or peer_body_sha256 does not match the verbatim body"
                    .into(),
            );
        }
        ClusterMessage::Relay(_) | ClusterMessage::Peer(_) => {}
    }
    Ok(crate::text::write(&message))
}

/// Decode an incoming ClusterMessage Datom without treating its textual
/// internals as a transport frame. This is the ordinary `message cluster`
/// command surface; it returns canonical producer text.
pub fn canonical(header: &str) -> std::result::Result<String, String> {
    let message = crate::text::read::<ClusterMessage>(header)
        .map_err(|error| format!("decode ClusterMessage Datom: {error}"))?;
    Ok(crate::text::write(&message))
}
