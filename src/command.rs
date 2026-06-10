use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use nota_next::{Block, Delimiter, NotaBody, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::client::MessageSocket;
use crate::error::{Error, Result as MessageResult};
use crate::schema::signal as signal_schema;
use crate::surface::RecipientName;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Send(RecipientName, String),
    Inbox(RecipientName),
}

impl Input {
    pub fn from_nota(text: &str) -> MessageResult<Self> {
        Ok(NotaSource::new(text).parse::<Self>()?)
    }

    pub fn run(self, mut output: impl Write) -> MessageResult<()> {
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.into_signal_input())?;
        writeln!(output, "{}", Output::from_signal_output(reply).to_nota())?;
        Ok(())
    }

    fn into_signal_input(self) -> signal_schema::Input {
        match self {
            Self::Send(recipient, body) => signal_schema::Input::Submit(
                signal_schema::Submit::new(signal_schema::MessageSubmission {
                    recipient: signal_schema::Recipient::new(recipient.as_str().to_owned()),
                    message_kind: signal_schema::MessageKind::Send,
                    body: signal_schema::Body::new(body),
                }),
            ),
            Self::Inbox(recipient) => signal_schema::Input::QueryInbox(
                signal_schema::QueryInbox::new(signal_schema::InboxQuery::new(
                    signal_schema::Recipient::new(recipient.as_str().to_owned()),
                )),
            ),
        }
    }
}

impl NotaDecode for Input {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        let fields =
            NotaBody::from_delimited(block, Delimiter::Parenthesis, "Input")?.root_objects();
        let Some(head) = fields.first().and_then(Block::demote_to_string) else {
            return Err(NotaDecodeError::ExpectedAtom { type_name: "Input" });
        };
        match head {
            "Send" => {
                Self::expect_fields(fields, "Input::Send", 3)?;
                Ok(Self::Send(
                    RecipientName::from_nota_block(&fields[1])?,
                    String::from_nota_block(&fields[2])?,
                ))
            }
            "Inbox" => {
                Self::expect_fields(fields, "Input::Inbox", 2)?;
                Ok(Self::Inbox(RecipientName::from_nota_block(&fields[1])?))
            }
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "Input",
                variant: other.to_owned(),
            }),
        }
    }
}

impl NotaEncode for Input {
    fn to_nota(&self) -> String {
        match self {
            Self::Send(recipient, body) => Delimiter::Parenthesis.wrap([
                String::from("Send"),
                recipient.to_nota(),
                body.to_nota(),
            ]),
            Self::Inbox(recipient) => {
                Delimiter::Parenthesis.wrap([String::from("Inbox"), recipient.to_nota()])
            }
        }
    }
}

impl Input {
    fn expect_fields(
        fields: &[Block],
        type_name: &'static str,
        expected: usize,
    ) -> std::result::Result<(), NotaDecodeError> {
        let found = fields.len();
        if found != expected {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name,
                expected,
                found,
            });
        }
        Ok(())
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum SubmissionRejectionReason {
    StoreRejected,
    RecipientNotFound,
}

pub type InboxListing = Vec<InboxEntry>;

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub struct InboxEntry {
    pub message_slot: u64,
    pub sender: RecipientName,
    pub body: String,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum OperationKind {
    Submit,
    SubmitStamped,
    QueryInbox,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum UnimplementedReason {
    NotInPrototypeScope,
    RouterUnreachable,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SubmissionAccepted(u64),
    SubmissionRejected(SubmissionRejectionReason),
    InboxListing(InboxListing),
    Unimplemented(OperationKind, UnimplementedReason),
    Error(String),
}

impl Output {
    pub fn from_nota(text: &str) -> MessageResult<Self> {
        Ok(NotaSource::new(text).parse::<Self>()?)
    }

    pub fn to_nota(&self) -> String {
        <Self as NotaEncode>::to_nota(self)
    }

    pub fn from_signal_output(reply: signal_schema::Output) -> Self {
        match reply {
            signal_schema::Output::SubmissionAccepted(acceptance) => {
                Self::SubmissionAccepted(acceptance.into_payload().into_payload().into_payload())
            }
            signal_schema::Output::SubmissionRejected(rejection) => Self::SubmissionRejected(
                SubmissionRejectionReason::from_signal(rejection.into_payload().into_payload()),
            ),
            signal_schema::Output::InboxListing(listing) => Self::InboxListing(
                listing
                    .into_payload()
                    .into_payload()
                    .into_payload()
                    .into_iter()
                    .map(InboxEntry::from_signal)
                    .collect(),
            ),
            signal_schema::Output::Unimplemented(unimplemented) => {
                let unimplemented = unimplemented.into_payload();
                Self::Unimplemented(
                    OperationKind::from_signal(unimplemented.operation_kind),
                    UnimplementedReason::from_signal(unimplemented.unimplemented_reason),
                )
            }
            signal_schema::Output::Error(error) => {
                Self::Error(error.into_payload().into_payload().into_payload())
            }
        }
    }
}

impl NotaDecode for Output {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        let fields =
            NotaBody::from_delimited(block, Delimiter::Parenthesis, "Output")?.root_objects();
        let Some(head) = fields.first().and_then(Block::demote_to_string) else {
            return Err(NotaDecodeError::ExpectedAtom {
                type_name: "Output",
            });
        };
        match head {
            "SubmissionAccepted" => {
                Self::expect_fields(fields, "Output::SubmissionAccepted", 2)?;
                Ok(Self::SubmissionAccepted(u64::from_nota_block(&fields[1])?))
            }
            "SubmissionRejected" => {
                Self::expect_fields(fields, "Output::SubmissionRejected", 2)?;
                Ok(Self::SubmissionRejected(
                    SubmissionRejectionReason::from_nota_block(&fields[1])?,
                ))
            }
            "InboxListing" => {
                Self::expect_fields(fields, "Output::InboxListing", 2)?;
                Ok(Self::InboxListing(Vec::<InboxEntry>::from_nota_block(
                    &fields[1],
                )?))
            }
            "Unimplemented" => {
                Self::expect_fields(fields, "Output::Unimplemented", 3)?;
                Ok(Self::Unimplemented(
                    OperationKind::from_nota_block(&fields[1])?,
                    UnimplementedReason::from_nota_block(&fields[2])?,
                ))
            }
            "Error" => {
                Self::expect_fields(fields, "Output::Error", 2)?;
                Ok(Self::Error(String::from_nota_block(&fields[1])?))
            }
            other => Err(NotaDecodeError::UnknownVariant {
                enum_name: "Output",
                variant: other.to_owned(),
            }),
        }
    }
}

impl NotaEncode for Output {
    fn to_nota(&self) -> String {
        match self {
            Self::SubmissionAccepted(message_slot) => Delimiter::Parenthesis
                .wrap([String::from("SubmissionAccepted"), message_slot.to_nota()]),
            Self::SubmissionRejected(reason) => {
                Delimiter::Parenthesis.wrap([String::from("SubmissionRejected"), reason.to_nota()])
            }
            Self::InboxListing(messages) => {
                Delimiter::Parenthesis.wrap([String::from("InboxListing"), messages.to_nota()])
            }
            Self::Unimplemented(operation, reason) => Delimiter::Parenthesis.wrap([
                String::from("Unimplemented"),
                operation.to_nota(),
                reason.to_nota(),
            ]),
            Self::Error(message) => {
                Delimiter::Parenthesis.wrap([String::from("Error"), message.to_nota()])
            }
        }
    }
}

impl Output {
    fn expect_fields(
        fields: &[Block],
        type_name: &'static str,
        expected: usize,
    ) -> std::result::Result<(), NotaDecodeError> {
        let found = fields.len();
        if found != expected {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name,
                expected,
                found,
            });
        }
        Ok(())
    }
}

impl SubmissionRejectionReason {
    fn from_signal(reason: signal_schema::SubmissionRejectionReason) -> Self {
        match reason {
            signal_schema::SubmissionRejectionReason::StoreRejected => Self::StoreRejected,
            signal_schema::SubmissionRejectionReason::RecipientNotFound => Self::RecipientNotFound,
        }
    }
}

impl InboxEntry {
    fn from_signal(entry: signal_schema::InboxEntry) -> Self {
        Self {
            message_slot: entry.message_slot.into_payload(),
            sender: RecipientName::new(entry.sender.into_payload()),
            body: entry.body.into_payload(),
        }
    }
}

impl OperationKind {
    fn from_signal(kind: signal_schema::OperationKind) -> Self {
        match kind {
            signal_schema::OperationKind::Submit => Self::Submit,
            signal_schema::OperationKind::SubmitStamped => Self::SubmitStamped,
            signal_schema::OperationKind::QueryInbox => Self::QueryInbox,
        }
    }
}

impl UnimplementedReason {
    fn from_signal(reason: signal_schema::UnimplementedReason) -> Self {
        match reason {
            signal_schema::UnimplementedReason::NotInPrototypeScope => Self::NotInPrototypeScope,
            signal_schema::UnimplementedReason::RouterUnreachable => Self::RouterUnreachable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    arguments: Vec<OsString>,
}

impl CommandLine {
    pub fn from_env() -> Self {
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

    pub fn decode_input(&self) -> MessageResult<Input> {
        let Some(first) = self.arguments.first() else {
            return Err(Error::MissingInput);
        };
        self.require_single_argument()?;

        if CommandLineArgument::new(first).starts_inline_record() {
            let Some(text) = first.to_str() else {
                return Err(Error::InvalidInlineNotaArgument {
                    got: format!("{first:?}"),
                });
            };
            Input::from_nota(text)
        } else {
            InputFile::from_path(PathBuf::from(first)).decode()
        }
    }

    pub fn run(&self, output: impl Write) -> MessageResult<()> {
        self.decode_input()?.run(output)
    }

    fn require_single_argument(&self) -> MessageResult<()> {
        if let Some(argument) = self.arguments.get(1) {
            return Err(Error::UnexpectedArgument {
                got: argument.to_string_lossy().to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFile {
    path: PathBuf,
}

impl InputFile {
    pub fn from_path(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn decode(&self) -> MessageResult<Input> {
        let text = std::fs::read_to_string(&self.path)?;
        Input::from_nota(&text)
    }
}

struct CommandLineArgument<'argument> {
    argument: &'argument OsString,
}

impl<'argument> CommandLineArgument<'argument> {
    fn new(argument: &'argument OsString) -> Self {
        Self { argument }
    }

    fn starts_inline_record(&self) -> bool {
        self.argument.to_string_lossy().starts_with('(')
    }
}
