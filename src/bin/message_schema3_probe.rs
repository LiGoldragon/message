use datom_codec::Datomizable;
use message::schema3_probe::{Schema3ProbeCounts, Schema3ProbeOutcome, Schema3ProbeRefusal, probe};
use protos::{Protosizable, Textualizable};
use std::{
    ffi::OsString,
    process::{Command, ExitCode},
    thread,
    time::{Duration, Instant},
};

const PRIVATE_DECODER: &str = "--private-decoder";
const DECODER_TIMEOUT: Duration = Duration::from_secs(5);

fn refusal_name(refusal: Schema3ProbeRefusal) -> &'static str {
    match refusal {
        Schema3ProbeRefusal::InputNotRegularFile => "InputNotRegularFile",
        Schema3ProbeRefusal::InputUnreadable => "InputUnreadable",
        Schema3ProbeRefusal::PrivateCopyUnavailable => "PrivateCopyUnavailable",
        Schema3ProbeRefusal::SourceChanged => "SourceChanged",
        Schema3ProbeRefusal::LegacyDecodeOrInvariant => "LegacyDecodeOrInvariant",
        Schema3ProbeRefusal::PrivateCleanup => "PrivateCleanup",
    }
}

fn private_decoder(path: OsString) -> ExitCode {
    match probe(std::path::Path::new(&path)) {
        Schema3ProbeOutcome::Observed(counts) => {
            println!(
                "observed {} {} {} {} {} {}",
                counts.agent_registry_count,
                counts.message_ledger_count,
                counts.ledger_head_count,
                counts.recipient_inbox_count,
                counts.thread_index_count,
                counts.delivery_outbox_count,
            );
        }
        Schema3ProbeOutcome::Refused(refusal) => println!("refused {}", refusal_name(refusal)),
    }
    ExitCode::SUCCESS
}

fn decode_in_private_child(path: OsString) -> Schema3ProbeOutcome {
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(_) => return Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::PrivateCopyUnavailable),
    };
    let mut child = match Command::new(executable)
        .arg(PRIVATE_DECODER)
        .arg(path)
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::PrivateCopyUnavailable),
    };
    let deadline = Instant::now() + DECODER_TIMEOUT;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            let output = match child.wait_with_output() {
                Ok(output) => output,
                Err(_) => {
                    return Schema3ProbeOutcome::Refused(
                        Schema3ProbeRefusal::LegacyDecodeOrInvariant,
                    );
                }
            };
            return decode_private_stdout(&output.stdout).unwrap_or(Schema3ProbeOutcome::Refused(
                Schema3ProbeRefusal::LegacyDecodeOrInvariant,
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::PrivateCopyUnavailable)
}

fn decode_private_stdout(stdout: &[u8]) -> Option<Schema3ProbeOutcome> {
    let text = std::str::from_utf8(stdout).ok()?.trim();
    let mut fields = text.split_ascii_whitespace();
    match fields.next()? {
        "observed" => {
            let mut parse = || fields.next()?.parse().ok();
            let result = Schema3ProbeOutcome::Observed(Schema3ProbeCounts {
                agent_registry_count: parse()?,
                message_ledger_count: parse()?,
                ledger_head_count: parse()?,
                recipient_inbox_count: parse()?,
                thread_index_count: parse()?,
                delivery_outbox_count: parse()?,
            });
            fields.next().is_none().then_some(result)
        }
        "refused" => {
            let refusal = match fields.next()? {
                "InputNotRegularFile" => Schema3ProbeRefusal::InputNotRegularFile,
                "InputUnreadable" => Schema3ProbeRefusal::InputUnreadable,
                "PrivateCopyUnavailable" => Schema3ProbeRefusal::PrivateCopyUnavailable,
                "SourceChanged" => Schema3ProbeRefusal::SourceChanged,
                "LegacyDecodeOrInvariant" => Schema3ProbeRefusal::LegacyDecodeOrInvariant,
                "PrivateCleanup" => Schema3ProbeRefusal::PrivateCleanup,
                _ => return None,
            };
            fields
                .next()
                .is_none()
                .then_some(Schema3ProbeOutcome::Refused(refusal))
        }
        _ => None,
    }
}

fn print_public(outcome: Schema3ProbeOutcome) -> ExitCode {
    match outcome {
        observed @ Schema3ProbeOutcome::Observed(_) => {
            println!("{}", observed.datomize(vec![]).protosize().textualize());
            ExitCode::SUCCESS
        }
        refused @ Schema3ProbeOutcome::Refused(_) => {
            println!("{}", refused.datomize(vec![]).protosize().textualize());
            ExitCode::from(1)
        }
    }
}

fn main() -> ExitCode {
    let mut arguments = std::env::args_os();
    let _ = arguments.next();
    let Some(first) = arguments.next() else {
        eprintln!("usage: message-schema3-probe STORE");
        return ExitCode::from(2);
    };
    if first == PRIVATE_DECODER {
        let Some(path) = arguments.next() else {
            return ExitCode::from(2);
        };
        return arguments
            .next()
            .is_none()
            .then(|| private_decoder(path))
            .unwrap_or(ExitCode::from(2));
    }
    if arguments.next().is_some() {
        return ExitCode::from(2);
    }
    print_public(decode_in_private_child(first))
}
