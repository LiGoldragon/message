use std::io::Write;

use dotos::{DotosEncode, DotosSource};
use signal_message::schema::lib::Input;

use crate::{Error, Result, client::MessageSocket};

/// The ordinary Message CLI is a direct Dotos view of the producer contract.
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

    pub fn decode_input(&self) -> Result<Input> {
        let [text] = self.arguments.as_slice() else {
            return Err(Error::InvalidCommandArgument {
                detail: format!(
                    "expected exactly one inline Dotos value, received {}",
                    self.arguments.len()
                ),
            });
        };
        Ok(DotosSource::new(text).parse::<Input>()?)
    }

    pub fn run(&self, mut output: impl Write) -> Result<()> {
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.decode_input()?)?;
        writeln!(output, "{}", reply.to_dotos())?;
        Ok(())
    }
}
