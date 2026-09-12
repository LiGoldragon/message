//! Write the daemon's binary startup configuration from one inline Datom value.

use message::{ConfigurationWriteRequest, text};
use thiserror::Error;

fn main() {
    if let Err(error) = run() {
        eprintln!("message-write-configuration: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), ConfigurationWriterError> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let request = text::read::<ConfigurationWriteRequest>(text::sole_argument(&arguments)?)?;
    println!("{}", text::write(&request.write()?));
    Ok(())
}

#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error("{0}")]
    Text(#[from] message::text::TextError),

    #[error("daemon configuration archive error: {0}")]
    Configuration(#[from] message::ConfigurationError),
}
