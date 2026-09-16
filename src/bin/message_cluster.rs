//! Validate one incoming `ClusterMessage` Datom with the producer codec.
//!
//! This is the CLI boundary used by adapters before a recipient sees a prompt:
//! the exact typed Datom is preserved, and no JSON provenance header exists.
use std::{env, fs, io::Read};

fn main() {
    if let Err(error) = run() {
        eprintln!("message-cluster: {error}");
        std::process::exit(2);
    }
}

/// `verify --datom-file HEADER` reads the verbatim prompt body from stdin.
/// It accepts only a producer-owned ClusterMessage::Relay header, confirms
/// its sha256 against those exact bytes, and writes the canonical header plus
/// the unchanged body.  The JSON used by an adapter to find HEADER never
/// crosses this boundary into the recipient prompt.
fn run() -> Result<(), String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let header = match arguments.as_slice() {
        [command, flag, path] if command == "verify" && flag == "--datom-file" => {
            fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"))?
        }
        _ => return Err("usage: message-cluster verify --datom-file HEADER (body on stdin)".into()),
    };
    let mut body = String::new();
    std::io::stdin()
        .read_to_string(&mut body)
        .map_err(|error| format!("read body: {error}"))?;
    print!("{}\n\n{body}", message::cluster::verify(header.trim(), &body)?);
    Ok(())
}
