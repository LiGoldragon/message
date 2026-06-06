use std::ffi::OsString;
use std::path::PathBuf;

use crate::command::{InboxListing, Output};
use crate::error::{Error, Result};
use crate::surface::RecipientName;

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
        let output = Output::from_nota(&text)?;
        self.expectation.check(&output)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputExpectation {
    SubmissionAccepted,
    InboxEntryPresent {
        sender: Option<RecipientName>,
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
            "expect-inbox-entry" => {
                let sender = parser.optional_string_option("--sender")?;
                let body = parser.required_string_option("--body")?;
                Ok(Self::InboxEntryPresent {
                    sender: sender.map(RecipientName::new),
                    body,
                })
            }
            "expect-inbox-body-absent" => {
                let body = parser.required_string_option("--body")?;
                Ok(Self::InboxBodyAbsent { body })
            }
            other => Err(Error::InvalidValidatorArgument {
                detail: format!("unknown expectation {other:?}"),
            }),
        }
    }

    fn check(&self, output: &Output) -> Result<()> {
        match self {
            Self::SubmissionAccepted => self.check_submission_accepted(output),
            Self::InboxEntryPresent { sender, body } => {
                self.check_inbox_entry_present(output, sender.as_ref(), body)
            }
            Self::InboxBodyAbsent { body } => self.check_inbox_body_absent(output, body),
        }
    }

    fn check_submission_accepted(&self, output: &Output) -> Result<()> {
        match output {
            Output::SubmissionAccepted(_) => Ok(()),
            other => Err(Error::OutputValidation {
                detail: format!("expected SubmissionAccepted, got {other:?}"),
            }),
        }
    }

    fn check_inbox_entry_present(
        &self,
        output: &Output,
        sender: Option<&RecipientName>,
        body: &str,
    ) -> Result<()> {
        let listing = RouterInboxOutput::from_output(output)?;
        if listing.contains_entry(sender, body) {
            return Ok(());
        }
        Err(Error::OutputValidation {
            detail: format!(
                "missing inbox entry sender={sender:?} body={body:?}; output={output:?}"
            ),
        })
    }

    fn check_inbox_body_absent(&self, output: &Output, body: &str) -> Result<()> {
        let listing = RouterInboxOutput::from_output(output)?;
        if listing.contains_body(body) {
            return Err(Error::OutputValidation {
                detail: format!("inbox unexpectedly contained body={body:?}; output={output:?}"),
            });
        }
        Ok(())
    }
}

struct RouterInboxOutput<'output> {
    listing: &'output InboxListing,
}

impl<'output> RouterInboxOutput<'output> {
    fn from_output(output: &'output Output) -> Result<Self> {
        match output {
            Output::InboxListing(listing) => Ok(Self { listing }),
            other => Err(Error::OutputValidation {
                detail: format!("expected InboxListing, got {other:?}"),
            }),
        }
    }

    fn contains_entry(&self, sender: Option<&RecipientName>, body: &str) -> bool {
        self.listing.messages.iter().any(|entry| {
            entry.body == body
                && sender
                    .map(|expected_sender| entry.sender == *expected_sender)
                    .unwrap_or(true)
        })
    }

    fn contains_body(&self, body: &str) -> bool {
        self.listing.messages.iter().any(|entry| entry.body == body)
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

    fn required_path_option(&mut self, name: &str) -> Result<PathBuf> {
        self.expect_option_name(name)?;
        self.required_value(name)
    }

    fn required_string_option(&mut self, name: &str) -> Result<String> {
        self.expect_option_name(name)?;
        self.required_word(name)
    }

    fn optional_string_option(&mut self, name: &str) -> Result<Option<String>> {
        if self.peek_word().as_deref() == Some(name) {
            self.index += 1;
            return self.required_word(name).map(Some);
        }
        Ok(None)
    }

    fn required_word(&mut self, name: &str) -> Result<String> {
        self.required_value(name)?
            .into_os_string()
            .into_string()
            .map_err(|got| Error::InvalidValidatorArgument {
                detail: format!("{name} must be UTF-8, got {got:?}"),
            })
    }

    fn expect_finished(&self) -> Result<()> {
        if let Some(extra) = self.arguments.get(self.index) {
            return Err(Error::UnexpectedArgument {
                got: extra.to_string_lossy().to_string(),
            });
        }
        Ok(())
    }

    fn expect_option_name(&mut self, name: &str) -> Result<()> {
        let got = self.required_word("option")?;
        if got == name {
            return Ok(());
        }
        Err(Error::InvalidValidatorArgument {
            detail: format!("expected option {name}, got {got:?}"),
        })
    }

    fn required_value(&mut self, name: &str) -> Result<PathBuf> {
        let Some(value) = self.arguments.get(self.index) else {
            return Err(Error::InvalidValidatorArgument {
                detail: format!("missing value for {name}"),
            });
        };
        self.index += 1;
        Ok(PathBuf::from(value))
    }

    fn peek_word(&self) -> Option<String> {
        self.arguments
            .get(self.index)
            .map(|value| value.to_string_lossy().to_string())
    }
}
