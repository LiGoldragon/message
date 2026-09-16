//! Validate or produce one typed producer-owned `ClusterMessage` Datom.
//!
//! The verifier receives the header separately from the body, so it never
//! guesses a framing boundary within Datom text. The peer producer records
//! explicit supplied provenance and hashes the exact UTF-8 source bytes.
use std::{env, fs, io::Read};

use sha2::{Digest, Sha256};
use signal_message::{
    ClusterMessage, PeerBody, PeerBodySha256, PeerEnvelope, PeerSender, PeerSourcePath,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("message-cluster: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    match arguments.as_slice() {
        [command, flag, path] if command == "verify" && flag == "--datom-file" => verify(path),
        [command, source_flag, source_path, flow_flag, flow, session_flag, session, event_flag, event]
            if command == "peer"
                && source_flag == "--source-file"
                && flow_flag == "--sender-flow"
                && session_flag == "--sender-session"
                && event_flag == "--source-event" => peer(source_path, flow, session, event),
        _ => Err("usage: message-cluster verify --datom-file HEADER (body on stdin)\n       message-cluster peer --source-file PATH --sender-flow FLOW --sender-session SESSION --source-event EVENT".into()),
    }
}

fn verify(path: &str) -> Result<(), String> {
    let header = fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"))?;
    let mut body = String::new();
    std::io::stdin()
        .read_to_string(&mut body)
        .map_err(|error| format!("read body: {error}"))?;
    print!(
        "{}\n\n{body}",
        message::cluster::verify(header.trim(), &body)?
    );
    Ok(())
}

fn peer(source_path: &str, flow: &str, session: &str, event: &str) -> Result<(), String> {
    let body =
        fs::read_to_string(source_path).map_err(|error| format!("read {source_path}: {error}"))?;
    let message = ClusterMessage::Peer(PeerEnvelope {
        peer_sender: PeerSender {
            flow_identifier: flow.into(),
            session_identifier: session.into(),
        },
        source_event_identifier: event.into(),
        peer_source_path: PeerSourcePath::from(source_path),
        peer_body_sha256: PeerBodySha256::from(format!("{:x}", Sha256::digest(body.as_bytes()))),
        peer_body: PeerBody::from(body.clone()),
    });
    print!(
        "{}\n\n{body}",
        message::cluster::canonical(&crate_text(&message))?
    );
    Ok(())
}

fn crate_text(message: &ClusterMessage) -> String {
    message::text::write(message)
}
