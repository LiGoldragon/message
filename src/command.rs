use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode, NotaRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_message::{
    InboxQuery as SignalInboxQuery, MessageBody as SignalMessageBody,
    MessageKind as SignalMessageKind, MessageOperationKind as SignalMessageOperationKind,
    MessageRecipient as SignalMessageRecipient, MessageReply, MessageRequest,
    MessageRequestUnimplemented as SignalMessageRequestUnimplemented, MessageSubmission,
    MessageUnimplementedReason as SignalMessageUnimplementedReason,
    SubmissionRejectionReason as SignalSubmissionRejectionReason,
};

use crate::error::{Error, Result};
use crate::router::SignalMessageSocket;
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
        let socket =
            SignalMessageSocket::from_environment().ok_or(Error::SignalMessageSocketMissing)?;
        let request = self.into_message_request();
        let reply = socket.client().submit(request)?;
        writeln!(output, "{}", Output::from_router_reply(reply)?.to_nota()?)?;
        Ok(())
    }

    fn into_message_request(self) -> MessageRequest {
        match self {
            Self::Send(send) => send.into_message_request(),
            Self::Inbox(inbox) => inbox.into_message_request(),
        }
    }
}

impl Send {
    pub fn into_message_request(self) -> MessageRequest {
        MessageRequest::MessageSubmission(MessageSubmission {
            recipient: SignalMessageRecipient::new(self.recipient.as_str()),
            kind: SignalMessageKind::Send,
            body: SignalMessageBody::new(self.body),
        })
    }
}

impl Inbox {
    pub fn into_message_request(self) -> MessageRequest {
        MessageRequest::InboxQuery(SignalInboxQuery {
            recipient: SignalMessageRecipient::new(self.recipient.as_str()),
        })
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
pub struct RouterInboxListing {
    pub messages: Vec<RouterInboxEntry>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct RouterInboxEntry {
    pub message_slot: u64,
    pub sender: RecipientName,
    pub body: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SubmissionAccepted(SubmissionAccepted),
    SubmissionRejected(SubmissionRejected),
    RouterInboxListing(RouterInboxListing),
    MessageRequestUnimplemented(SignalMessageRequestUnimplemented),
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

    pub fn from_router_reply(reply: MessageReply) -> Result<Self> {
        match reply {
            MessageReply::SubmissionAccepted(acceptance) => {
                Ok(Self::SubmissionAccepted(SubmissionAccepted {
                    message_slot: acceptance.message_slot.into_u64(),
                }))
            }
            MessageReply::SubmissionRejected(rejection) => {
                Ok(Self::SubmissionRejected(SubmissionRejected {
                    reason: SubmissionRejectionReason::from_signal(rejection.reason),
                }))
            }
            MessageReply::InboxListing(listing) => {
                Ok(Self::RouterInboxListing(RouterInboxListing {
                    messages: listing
                        .messages
                        .into_iter()
                        .map(RouterInboxEntry::from_signal)
                        .collect(),
                }))
            }
            MessageReply::MessageRequestUnimplemented(unimplemented) => {
                Ok(Self::MessageRequestUnimplemented(unimplemented))
            }
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
            Self::RouterInboxListing(output) => {
                encoder.start_record("RouterInboxListing")?;
                output.messages.encode(encoder)?;
                encoder.end_record()
            }
            Self::MessageRequestUnimplemented(output) => {
                encoder.start_record("MessageRequestUnimplemented")?;
                output.operation.encode(encoder)?;
                output.reason.encode(encoder)?;
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
            "RouterInboxListing" => {
                decoder.expect_record_head("RouterInboxListing")?;
                let messages = Vec::<RouterInboxEntry>::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = RouterInboxListing { messages };
                Ok(Self::RouterInboxListing(output))
            }
            "MessageRequestUnimplemented" => {
                decoder.expect_record_head("MessageRequestUnimplemented")?;
                let operation = SignalMessageOperationKind::decode(decoder)?;
                let reason = SignalMessageUnimplementedReason::decode(decoder)?;
                decoder.expect_record_end()?;
                let output = SignalMessageRequestUnimplemented { operation, reason };
                Ok(Self::MessageRequestUnimplemented(output))
            }
            other => Err(nota_codec::Error::UnknownVariant {
                enum_name: "Output",
                got: other.to_string(),
            }),
        }
    }
}

impl SubmissionRejectionReason {
    fn from_signal(reason: SignalSubmissionRejectionReason) -> Self {
        match reason {
            SignalSubmissionRejectionReason::StoreRejected => Self::StoreRejected,
            SignalSubmissionRejectionReason::RecipientNotFound => Self::RecipientNotFound,
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

impl RouterInboxEntry {
    fn from_signal(entry: signal_message::InboxEntry) -> Self {
        Self {
            message_slot: entry.message_slot.into_u64(),
            sender: RecipientName::new(entry.sender.as_str()),
            body: entry.body.as_str().to_string(),
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
