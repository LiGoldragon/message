//! `message`: one inline datom of the ordinary Message contract in, its
//! Signal to the Message Nexus, each reply out as a datom. Observe goes on
//! printing a line per grade change until the Nexus or the reader leaves.

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use protos::{Compactable, Protosizable, ReaderBudget};
use signal_message::{Query, Response};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

struct MessageClient {
    socket: String,
}

trait CallsMessageNexus {
    fn parse(&self, arguments: &[String]) -> Result<Query, String>;
    fn call(&self, query: &Query, each: &mut dyn FnMut(&Response)) -> Result<(), String>;
}

/// Finds the Nexus socket this client speaks to: `MESSAGE_SOCKET` when the
/// caller chose one (a wrapper selecting a slot), else the socket Message's
/// defaults name under this user's runtime directory.
trait FindsNexusSocket {
    fn from_environment() -> Self;
}

impl FindsNexusSocket for MessageClient {
    fn from_environment() -> Self {
        Self {
            socket: env::var("MESSAGE_SOCKET").unwrap_or_else(|_| {
                DefaultConfiguration::from_environment()
                    .ordinary_socket_path()
                    .to_string_lossy()
                    .into_owned()
            }),
        }
    }
}

impl CallsMessageNexus for MessageClient {
    fn parse(&self, arguments: &[String]) -> Result<Query, String> {
        let [datom] = arguments else {
            return Err("usage: message '<one inline Message query datom>'".into());
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<Query>::from(datom.clone())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Message query: {error:?}"))
    }

    fn call(&self, query: &Query, each: &mut dyn FnMut(&Response)) -> Result<(), String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .and_then(|()| peer.write_all(&bytes))
            .map_err(|e| e.to_string())?;
        let streams = matches!(query, Query::Observe(_));
        loop {
            let mut length = [0; 4];
            match peer.read_exact(&mut length) {
                Ok(()) => {}
                Err(error) if streams && error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(());
                }
                Err(error) => return Err(error.to_string()),
            }
            let length = u32::from_be_bytes(length) as usize;
            if length > 1024 * 1024 {
                return Err("Signal frame exceeds 1 MiB".into());
            }
            let mut reply = vec![0; length];
            peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
            let reply = rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply)
                .map_err(|e| e.to_string())?;
            each(&reply);
            if !streams || matches!(reply, Response::MessageRejected(_)) {
                return Ok(());
            }
        }
    }
}

fn main() -> ExitCode {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments == ["--version"] {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let client = MessageClient::from_environment();
    let outcome = client.parse(&arguments).and_then(|query| {
        client.call(&query, &mut |reply| {
            println!("{}", reply.datomize(vec![]).protosize().compact());
        })
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("message: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CallsMessageNexus, MessageClient};
    use signal_message::{Priority, Query};

    #[test]
    fn a_send_is_one_inline_datom_with_the_priority_second() {
        let client = MessageClient {
            socket: "unused".into(),
        };
        let Query::Send(request) = client
            .parse(&["Send.{ [ 7d41e0 ] Soft Text.«land this» }".into()])
            .expect("send parses")
        else {
            panic!("Send datom must stay a Send query");
        };
        assert_eq!(request.flow_id_vector, ["7d41e0"]);
        assert_eq!(request.priority, Priority::Soft);
    }

    #[test]
    fn flags_and_second_values_are_refused() {
        let client = MessageClient {
            socket: "unused".into(),
        };
        assert!(client.parse(&["--to".into(), "7d41e0".into()]).is_err());
        assert!(client.parse(&["--help".into()]).is_err());
    }
}
