use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::client::MessageSocket;
use crate::error::{Error, Result};
use crate::schema::signal as signal_schema;
use crate::surface::{RecipientName, expect_end};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Send {
    pub recipient: RecipientName,
    pub body: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Inbox {
    pub recipient: RecipientName,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Send(Send),
    Inbox(Inbox),
}

impl Input {
    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let input = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(input)
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let socket = MessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let reply = socket.client().submit(self.into_signal_input())?;
        writeln!(output, "{}", Output::from_signal_output(reply).to_nota()?)?;
        Ok(())
    }

    fn into_signal_input(self) -> signal_schema::Input {
        match self {
            Self::Send(send) => send.into_signal_input(),
            Self::Inbox(inbox) => inbox.into_signal_input(),
        }
    }
}

impl Send {
    pub fn into_signal_input(self) -> signal_schema::Input {
        signal_schema::Input::Submit(signal_schema::MessageSubmission {
            recipient: self.recipient.as_str().to_owned(),
            message_kind: signal_schema::MessageKind::Send,
            body: self.body,
        })
    }
}

impl Inbox {
    pub fn into_signal_input(self) -> signal_schema::Input {
        signal_schema::Input::QueryInbox(signal_schema::InboxQuery::new(
            self.recipient.as_str().to_owned(),
        ))
    }
}

impl NotaEncode for Input {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::Send(input) => {
                encoder.start_record("Send")?;
                input.recipient.encode(encoder)?;
                input.body.encode(encoder)?;
                encoder.end_record()
            }
            Self::Inbox(input) => {
                encoder.start_record("Inbox")?;
                input.recipient.encode(encoder)?;
                encoder.end_record()
            }
        }
    }
}

impl NotaDecode for Input {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        let head = decoder.peek_record_head()?;
        match head.as_str() {
            "Send" => {
                decoder.expect_record_head("Send")?;
                let recipient = RecipientName::decode(decoder)?;
                let body = String::decode(decoder)?;
                decoder.expect_record_end()?;
                let input = Send { recipient, body };
                Ok(Self::Send(input))
            }
            "Inbox" => {
                decoder.expect_record_head("Inbox")?;
                let recipient = RecipientName::decode(decoder)?;
                decoder.expect_record_end()?;
                let input = Inbox { recipient };
                Ok(Self::Inbox(input))
            }
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "Input",
                got: other.to_string(),
            }),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionAccepted {
    pub message_slot: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct SubmissionRejected {
    pub reason: SubmissionRejectionReason,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum SubmissionRejectionReason {
    StoreRejected,
    RecipientNotFound,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct InboxListing {
    pub messages: Vec<InboxEntry>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct InboxEntry {
    pub message_slot: u64,
    pub sender: RecipientName,
    pub body: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct Unimplemented {
    pub operation: OperationKind,
    pub reason: UnimplementedReason,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum OperationKind {
    Submit,
    SubmitStamped,
    QueryInbox,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum UnimplementedReason {
    NotInPrototypeScope,
    RouterUnreachable,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct ErrorReport {
    pub message: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SubmissionAccepted(SubmissionAccepted),
    SubmissionRejected(SubmissionRejected),
    InboxListing(InboxListing),
    Unimplemented(Unimplemented),
    Error(ErrorReport),
}

impl Output {
    pub fn from_nota(text: &str) -> Result<Self> {
        let mut decoder = Decoder::new(text);
        let output = Self::decode(&mut decoder)?;
        expect_end(&mut decoder)?;
        Ok(output)
    }

    pub fn to_nota(&self) -> Result<String> {
        let mut encoder = Encoder::new();
        self.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }

    pub fn from_signal_output(reply: signal_schema::Output) -> Self {
        match reply {
            signal_schema::Output::SubmissionAccepted(acceptance) => {
                Self::SubmissionAccepted(SubmissionAccepted {
                    message_slot: acceptance.into_payload(),
                })
            }
            signal_schema::Output::SubmissionRejected(rejection) => {
                Self::SubmissionRejected(SubmissionRejected {
                    reason: SubmissionRejectionReason::from_signal(rejection.into_payload()),
                })
            }
            signal_schema::Output::InboxListing(listing) => Self::InboxListing(InboxListing {
                messages: listing
                    .into_payload()
                    .into_iter()
                    .map(InboxEntry::from_signal)
                    .collect(),
            }),
            signal_schema::Output::Unimplemented(unimplemented) => {
                Self::Unimplemented(Unimplemented {
                    operation: OperationKind::from_signal(unimplemented.operation_kind),
                    reason: UnimplementedReason::from_signal(unimplemented.unimplemented_reason),
                })
            }
            signal_schema::Output::Error(error) => Self::Error(ErrorReport {
                message: error.into_payload(),
            }),
        }
    }
}

impl NotaEncode for Output {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::SubmissionAccepted(output) => {
                encoder.start_record("SubmissionAccepted")?;
                output.message_slot.encode(encoder)?;
                encoder.end_record()
            }
            Self::SubmissionRejected(output) => {
                encoder.start_record("SubmissionRejected")?;
                output.reason.encode(encoder)?;
                encoder.end_record()
            }
            Self::InboxListing(output) => {
                encoder.start_record("InboxListing")?;
                output.messages.encode(encoder)?;
                encoder.end_record()
            }
            Self::Unimplemented(output) => {
                encoder.start_record("Unimplemented")?;
                output.operation.encode(encoder)?;
                output.reason.encode(encoder)?;
                encoder.end_record()
            }
            Self::Error(output) => {
                encoder.start_record("Error")?;
                output.message.encode(encoder)?;
                encoder.end_record()
            }
        }
    }
}

impl NotaDecode for Output {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        let head = decoder.peek_record_head()?;
        match head.as_str() {
            "SubmissionAccepted" => {
                decoder.expect_record_head("SubmissionAccepted")?;
                let message_slot = u64::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = SubmissionAccepted { message_slot };
                Ok(Self::SubmissionAccepted(output))
            }
            "SubmissionRejected" => {
                decoder.expect_record_head("SubmissionRejected")?;
                let reason = SubmissionRejectionReason::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = SubmissionRejected { reason };
                Ok(Self::SubmissionRejected(output))
            }
            "InboxListing" => {
                decoder.expect_record_head("InboxListing")?;
                let messages = Vec::<InboxEntry>::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = InboxListing { messages };
                Ok(Self::InboxListing(output))
            }
            "Unimplemented" => {
                decoder.expect_record_head("Unimplemented")?;
                let operation = OperationKind::decode(decoder)?;
                let reason = UnimplementedReason::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = Unimplemented { operation, reason };
                Ok(Self::Unimplemented(output))
            }
            "Error" => {
                decoder.expect_record_head("Error")?;
                let message = String::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = ErrorReport { message };
                Ok(Self::Error(output))
            }
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "Output",
                got: other.to_string(),
            }),
        }
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

impl NotaEncode for SubmissionRejectionReason {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::StoreRejected => "StoreRejected".to_string().encode(encoder),
            Self::RecipientNotFound => "RecipientNotFound".to_string().encode(encoder),
        }
    }
}

impl NotaDecode for SubmissionRejectionReason {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match String::decode(decoder)?.as_str() {
            "StoreRejected" => Ok(Self::StoreRejected),
            "RecipientNotFound" => Ok(Self::RecipientNotFound),
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "SubmissionRejectionReason",
                got: other.to_string(),
            }),
        }
    }
}

impl InboxEntry {
    fn from_signal(entry: signal_schema::InboxEntry) -> Self {
        Self {
            message_slot: entry.message_slot,
            sender: RecipientName::new(entry.sender),
            body: entry.body,
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

impl NotaEncode for OperationKind {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::Submit => "Submit".to_string().encode(encoder),
            Self::SubmitStamped => "SubmitStamped".to_string().encode(encoder),
            Self::QueryInbox => "QueryInbox".to_string().encode(encoder),
        }
    }
}

impl NotaDecode for OperationKind {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match String::decode(decoder)?.as_str() {
            "Submit" => Ok(Self::Submit),
            "SubmitStamped" => Ok(Self::SubmitStamped),
            "QueryInbox" => Ok(Self::QueryInbox),
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "OperationKind",
                got: other.to_string(),
            }),
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

impl NotaEncode for UnimplementedReason {
    fn encode(&self, encoder: &mut Encoder) -> nota_codec::Result<()> {
        match self {
            Self::NotInPrototypeScope => "NotInPrototypeScope".to_string().encode(encoder),
            Self::RouterUnreachable => "RouterUnreachable".to_string().encode(encoder),
        }
    }
}

impl NotaDecode for UnimplementedReason {
    fn decode(decoder: &mut Decoder<'_>) -> nota_codec::Result<Self> {
        match String::decode(decoder)?.as_str() {
            "NotInPrototypeScope" => Ok(Self::NotInPrototypeScope),
            "RouterUnreachable" => Ok(Self::RouterUnreachable),
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "UnimplementedReason",
                got: other.to_string(),
            }),
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

    pub fn decode_input(&self) -> Result<Input> {
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

    pub fn run(&self, output: impl Write) -> Result<()> {
        self.decode_input()?.run(output)
    }

    fn require_single_argument(&self) -> Result<()> {
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

    pub fn decode(&self) -> Result<Input> {
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
