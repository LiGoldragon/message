use signal_message::Query;
use std::io::Write;

use crate::{Error, Result, client::MessageSocket};

/// The ordinary Message CLI is a direct Datom view of the producer contract.
/// It does not own a friendlier request or reply vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    arguments: Vec<String>,
}

impl CommandLine {
    pub fn from_env() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn decode_query(&self) -> Result<Query> {
        let text = crate::text::sole_argument(&self.arguments)?;
        Ok(crate::text::read::<Query>(text)?)
    }

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.decode_query()?)?;
        writeln!(output, "{}", crate::text::write(&reply))?;
        Ok(())
    }
}
