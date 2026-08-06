use std::ffi::OsString;
use std::path::PathBuf;

use dotos::DotosSource;
use signal_message::schema::lib::{Output, z2VRQt};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputValidatorCommandLine {
    arguments: Vec<OsString>,
}

impl OutputValidatorCommandLine {
    pub fn from_environment() -> Self {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments<I, S>(arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let validation = OutputValidation::from_arguments(&self.arguments)?;
        validation.check()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutputValidation {
    output_path: PathBuf,
    expectation: OutputExpectation,
}

impl OutputValidation {
    fn from_arguments(arguments: &[OsString]) -> Result<Self> {
        let mut parser = OutputValidatorArguments::new(arguments);
        let output_path = parser.required_path_option("--file")?;
        let expectation = OutputExpectation::from_parser(&mut parser)?;
        parser.expect_finished()?;
        Ok(Self {
            output_path,
            expectation,
        })
    }

    fn check(&self) -> Result<()> {
        let text = std::fs::read_to_string(&self.output_path)?;
        let output = DotosSource::new(&text).parse::<Output>()?;
        self.expectation.check(&output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputExpectation {
    SubmissionAccepted,
    InboxEntryPresent {
        sender: Option<String>,
        body: String,
    },
    InboxBodyAbsent {
        body: String,
    },
}

impl OutputExpectation {
    fn from_parser(parser: &mut OutputValidatorArguments<'_>) -> Result<Self> {
        match parser.required_word("expectation")?.as_str() {
            "expect-submission-accepted" => Ok(Self::SubmissionAccepted),
            "expect-inbox-entry" => Ok(Self::InboxEntryPresent {
                sender: parser.optional_string_option("--sender")?,
                body: parser.required_string_option("--body")?,
            }),
            "expect-inbox-body-absent" => Ok(Self::InboxBodyAbsent {
                body: parser.required_string_option("--body")?,
            }),
            other => Err(Error::InvalidValidatorArgument {
                detail: format!("unknown expectation {other:?}"),
            }),
        }
    }

    fn check(&self, output: &Output) -> Result<()> {
        match self {
            Self::SubmissionAccepted => match output {
                Output::SubmissionAccepted(_) => Ok(()),
                other => Err(Error::OutputValidation {
                    detail: format!("expected SubmissionAccepted, got {other:?}"),
                }),
            },
            Self::InboxEntryPresent { sender, body } => {
                let entries = Self::inbox_entries(output)?;
                if entries.iter().any(|entry| {
                    entry.field_2.payload() == body
                        && sender
                            .as_ref()
                            .map(|expected| entry.field_1.payload() == expected)
                            .unwrap_or(true)
                }) {
                    Ok(())
                } else {
                    Err(Error::OutputValidation {
                        detail: format!(
                            "missing inbox entry sender={sender:?} body={body:?}; output={output:?}"
                        ),
                    })
                }
            }
            Self::InboxBodyAbsent { body } => {
                let entries = Self::inbox_entries(output)?;
                if entries.iter().any(|entry| entry.field_2.payload() == body) {
                    Err(Error::OutputValidation {
                        detail: format!(
                            "inbox unexpectedly contained body={body:?}; output={output:?}"
                        ),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    fn inbox_entries(output: &Output) -> Result<&Vec<z2VRQt>> {
        match output {
            Output::InboxListing(listing) => Ok(listing.field_0.payload()),
            other => Err(Error::OutputValidation {
                detail: format!("expected InboxListing, got {other:?}"),
            }),
        }
    }
}

struct OutputValidatorArguments<'arguments> {
    arguments: &'arguments [OsString],
    index: usize,
}

impl<'arguments> OutputValidatorArguments<'arguments> {
    fn new(arguments: &'arguments [OsString]) -> Self {
        Self {
            arguments,
            index: 0,
        }
    }

    fn required_word(&mut self, description: &str) -> Result<String> {
        let Some(argument) = self.arguments.get(self.index) else {
            return Err(Error::InvalidValidatorArgument {
                detail: format!("missing {description}"),
            });
        };
        self.index += 1;
        argument
            .clone()
            .into_string()
            .map_err(|_| Error::InvalidValidatorArgument {
                detail: format!("{description} is not UTF-8"),
            })
    }

    fn required_path_option(&mut self, option: &str) -> Result<PathBuf> {
        self.expect_option(option)?;
        Ok(PathBuf::from(self.required_word(option)?))
    }

    fn required_string_option(&mut self, option: &str) -> Result<String> {
        self.expect_option(option)?;
        self.required_word(option)
    }

    fn optional_string_option(&mut self, option: &str) -> Result<Option<String>> {
        if self
            .arguments
            .get(self.index)
            .and_then(|value| value.to_str())
            == Some(option)
        {
            self.index += 1;
            return self.required_word(option).map(Some);
        }
        Ok(None)
    }

    fn expect_option(&mut self, option: &str) -> Result<()> {
        let found = self.required_word(option)?;
        if found == option {
            Ok(())
        } else {
            Err(Error::InvalidValidatorArgument {
                detail: format!("expected {option}, got {found:?}"),
            })
        }
    }

    fn expect_finished(&self) -> Result<()> {
        if self.index == self.arguments.len() {
            Ok(())
        } else {
            Err(Error::InvalidValidatorArgument {
                detail: format!(
                    "unexpected trailing arguments: {:?}",
                    &self.arguments[self.index..]
                ),
            })
        }
    }
}
