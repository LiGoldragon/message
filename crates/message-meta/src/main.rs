//! `message-meta`: one inline datom of the privileged Message contract in,
//! its Signal to the Message Nexus meta socket, the reply out as a datom.

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use meta_signal_message::{Query, Response};
use protos::{Protosizable, ReaderBudget, Textualizable};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

struct MessageMetaClient {
    socket: String,
}

trait CallsMessageNexus {
    fn parse(&self, arguments: &[String]) -> Result<Query, String>;
    fn call(&self, query: &Query, each: &mut dyn FnMut(&Response)) -> Result<(), String>;
}

/// Finds the Nexus socket this client speaks to: `MESSAGE_META_SOCKET` when the
/// caller chose one (a wrapper selecting a slot), else the socket Message's
/// defaults name under this user's runtime directory.
trait FindsNexusSocket {
    fn from_environment() -> Self;
}

impl FindsNexusSocket for MessageMetaClient {
    fn from_environment() -> Self {
        Self {
            socket: env::var("MESSAGE_META_SOCKET").unwrap_or_else(|_| {
                DefaultConfiguration::from_environment()
                    .meta_socket_path()
                    .to_string_lossy()
                    .into_owned()
            }),
        }
    }
}

impl CallsMessageNexus for MessageMetaClient {
    fn parse(&self, arguments: &[String]) -> Result<Query, String> {
        let [datom] = arguments else {
            return Err("usage: message-meta '<one inline Message meta query datom>'".into());
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<Query>::from(datom.clone())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Message meta query: {error:?}"))
    }

    fn call(&self, query: &Query, each: &mut dyn FnMut(&Response)) -> Result<(), String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .and_then(|()| peer.write_all(&bytes))
            .map_err(|e| e.to_string())?;
        let streams = false;
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
            if !streams {
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
    let client = MessageMetaClient::from_environment();
    let outcome = client.parse(&arguments).and_then(|query| {
        client.call(&query, &mut |reply| {
            println!("{}", reply.datomize(vec![]).protosize().textualize());
        })
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("message-meta: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CallsMessageNexus, MessageMetaClient};
    use meta_signal_message::Query;

    #[test]
    fn configure_and_redeliver_are_one_inline_datom_each() {
        let client = MessageMetaClient {
            socket: "unused".into(),
        };
        let Query::Configure(configuration) = client
            .parse(&["Configure.{ /r/m.sock /r/mo.sock /r/f.sock /r/fm.sock [ Psyche ] }".into()])
            .expect("configure parses")
        else {
            panic!("Configure datom must stay a Configure query");
        };
        assert_eq!(configuration.flow_meta_socket_path, "/r/fm.sock");
        assert!(matches!(
            client.parse(&["Redeliver.{ m-1 7d41e0 }".into()]),
            Ok(Query::Redeliver(_))
        ));
        assert!(client.parse(&["--send".into()]).is_err());
    }
}
