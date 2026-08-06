use std::path::Path;

use dotos::{DotosDecode, DotosEncode, DotosSource};
use message::Configuration;
use signal_message::schema::lib::z2VL2C;
use thiserror::Error;

fn main() {
    if let Err(error) = ConfigurationWriterCommand::from_environment().run() {
        eprintln!("message-write-configuration: {error}");
        std::process::exit(1);
    }
}

struct ConfigurationWriterCommand {
    arguments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct ConfigurationWriteRequest {
    contract: z2VL2C,
    database_path: String,
    owner_label: String,
    output_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosEncode)]
struct ConfigurationWritten {
    output_path: String,
}

impl ConfigurationWriterCommand {
    fn from_environment() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
        }
    }

    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let [text] = self.arguments.as_slice() else {
            return Err(ConfigurationWriterError::ArgumentCount {
                count: self.arguments.len(),
            });
        };
        let request = DotosSource::new(text).parse::<ConfigurationWriteRequest>()?;
        let output = request.write()?;
        println!("{}", output.to_dotos());
        Ok(())
    }
}

impl ConfigurationWriteRequest {
    fn write(self) -> Result<ConfigurationWritten, ConfigurationWriterError> {
        let configuration = Configuration::new(
            self.contract,
            Path::new(&self.database_path),
            self.owner_label,
        )?;
        configuration.write_binary_file(Path::new(&self.output_path))?;
        Ok(ConfigurationWritten {
            output_path: self.output_path,
        })
    }
}

#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error("expected exactly one inline Dotos value, received {count}")]
    ArgumentCount { count: usize },

    #[error("decode Dotos request: {0}")]
    Decode(#[from] dotos::DotosDecodeError),

    #[error("daemon configuration archive error: {0}")]
    Configuration(#[from] message::ConfigurationError),
}
