use std::io::Write;
use std::path::PathBuf;

use nota::{Block, Delimiter, NotaBody, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::client::MessageSocket;
use crate::error::{Error, Result as MessageResult};
use crate::schema::signal as signal_schema;
use crate::surface::{RecipientName, ThreadName as SurfaceThreadName, ThreadSelection};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Send(RecipientName, String, ThreadSelection),
    Inbox(RecipientName),
    Thread(SurfaceThreadName),
    Threads,
    Subscribe(SurfaceThreadName, RecipientName),
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
            Self::Send(recipient, body, thread) => signal_schema::Input::Submit(
                signal_schema::Submit::new(signal_schema::MessageSubmission {
                    recipient: signal_schema::Recipient::new(recipient.as_str().to_owned()),
                    kind: signal_schema::Kind::new(signal_schema::MessageKind::Send),
                    body: signal_schema::Body::new(body),
                    thread_selection: Self::signal_thread_selection(thread),
                }),
            ),
            Self::Inbox(recipient) => signal_schema::Input::QueryInbox(
                signal_schema::QueryInbox::new(signal_schema::InboxQuery::new(
                    signal_schema::Recipient::new(recipient.as_str().to_owned()),
                )),
            ),
            Self::Thread(thread) => signal_schema::Input::QueryThread(
                signal_schema::QueryThread::new(signal_schema::ThreadQuery::new(
                    signal_schema::ThreadName::new(thread.as_str().to_owned()),
                )),
            ),
            Self::Threads => signal_schema::Input::QueryThreads(
                signal_schema::QueryThreads::new(signal_schema::ThreadIndexQuery::All),
            ),
            Self::Subscribe(thread, participant) => signal_schema::Input::SubscribeThread(
                signal_schema::SubscribeThread::new(signal_schema::ThreadSubscription {
                    thread_name: signal_schema::ThreadName::new(thread.as_str().to_owned()),
                    participant_name: signal_schema::ParticipantName::new(
                        participant.as_str().to_owned(),
                    ),
                    thread_relation_selection: signal_schema::ThreadRelationSelection::None,
                }),
            ),
        }
    }

    fn signal_thread_selection(thread: ThreadSelection) -> signal_schema::ThreadSelection {
        match thread {
            ThreadSelection::None => signal_schema::ThreadSelection::None,
            ThreadSelection::Named(name) => signal_schema::ThreadSelection::Named(
                signal_schema::ThreadName::new(name.as_str().to_owned()),
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
                Self::expect_fields(fields, "Input::Send", 4)?;
                Ok(Self::Send(
                    RecipientName::from_nota_block(&fields[1])?,
                    String::from_nota_block(&fields[2])?,
                    ThreadSelection::from_nota_block(&fields[3])?,
                ))
            }
            "Inbox" => {
                Self::expect_fields(fields, "Input::Inbox", 2)?;
                Ok(Self::Inbox(RecipientName::from_nota_block(&fields[1])?))
            }
            "Thread" => {
                Self::expect_fields(fields, "Input::Thread", 2)?;
                Ok(Self::Thread(SurfaceThreadName::from_nota_block(&fields[1])?))
            }
            "Threads" => {
                Self::expect_fields(fields, "Input::Threads", 1)?;
                Ok(Self::Threads)
            }
            "Subscribe" => {
                Self::expect_fields(fields, "Input::Subscribe", 3)?;
                Ok(Self::Subscribe(
                    SurfaceThreadName::from_nota_block(&fields[1])?,
                    RecipientName::from_nota_block(&fields[2])?,
                ))
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
            Self::Send(recipient, body, thread) => Delimiter::Parenthesis.wrap([
                String::from("Send"),
                recipient.to_nota(),
                body.to_nota(),
                thread.to_nota(),
            ]),
            Self::Inbox(recipient) => {
                Delimiter::Parenthesis.wrap([String::from("Inbox"), recipient.to_nota()])
            }
            Self::Thread(thread) => {
                Delimiter::Parenthesis.wrap([String::from("Thread"), thread.to_nota()])
            }
            Self::Threads => Delimiter::Parenthesis.wrap([String::from("Threads")]),
            Self::Subscribe(thread, participant) => Delimiter::Parenthesis.wrap([
                String::from("Subscribe"),
                thread.to_nota(),
                participant.to_nota(),
            ]),
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
    pub thread: ThreadSelection,
    pub stamped_at: u64,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum OperationKind {
    Submit,
    SubmitStamped,
    QueryInbox,
    AssignAgentIdentity,
    BindAgentEndpoint,
    QueryAgentRegistry,
    QueryThread,
    SubscribeThread,
    QueryThreads,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub enum UnimplementedReason {
    NotInPrototypeScope,
    DependencyMissing(signal_schema::DependencyKind),
    ResourceUnavailable(signal_schema::ResourceKind),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Output {
    SubmissionAccepted(u64),
    SubmissionRejected(SubmissionRejectionReason),
    InboxListing(InboxListing),
    AgentIdentityAssigned(signal_schema::AssignedAgentIdentity),
    AgentEndpointBound(signal_schema::BoundAgentEndpoint),
    AgentRegistryListing(signal_schema::AgentRegistryEntries),
    AgentRegistryRejected(signal_schema::AgentRegistryRejection),
    Unimplemented(OperationKind, UnimplementedReason),
    Error(String),
    ThreadListing(signal_schema::ThreadContents),
    ThreadSubscribed(signal_schema::ThreadSubscriptionAcknowledgment),
    ThreadIndexListing(signal_schema::ThreadIndexEntries),
    ThreadRejected(signal_schema::ThreadRejection),
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
            signal_schema::Output::AgentIdentityAssigned(assigned) => {
                Self::AgentIdentityAssigned(assigned.into_payload())
            }
            signal_schema::Output::AgentEndpointBound(bound) => {
                Self::AgentEndpointBound(bound.into_payload())
            }
            signal_schema::Output::AgentRegistryListing(listing) => {
                Self::AgentRegistryListing(listing.into_payload())
            }
            signal_schema::Output::AgentRegistryRejected(rejection) => {
                Self::AgentRegistryRejected(rejection.into_payload())
            }
            signal_schema::Output::Unimplemented(unimplemented) => {
                let unimplemented = unimplemented.into_payload();
                Self::Unimplemented(
                    OperationKind::from_signal(
                        unimplemented.unimplemented_operation_kind.into_payload(),
                    ),
                    UnimplementedReason::from_signal(unimplemented.reason.into_payload()),
                )
            }
            signal_schema::Output::Error(error) => {
                Self::Error(error.into_payload().into_payload().into_payload())
            }
            signal_schema::Output::ThreadListing(listing) => {
                Self::ThreadListing(listing.into_payload())
            }
            signal_schema::Output::ThreadSubscribed(acknowledgment) => {
                Self::ThreadSubscribed(acknowledgment.into_payload())
            }
            signal_schema::Output::ThreadIndexListing(listing) => {
                Self::ThreadIndexListing(listing.into_payload())
            }
            signal_schema::Output::ThreadRejected(rejection) => {
                Self::ThreadRejected(rejection.into_payload())
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
            "AgentIdentityAssigned" => {
                Self::expect_fields(fields, "Output::AgentIdentityAssigned", 2)?;
                Ok(Self::AgentIdentityAssigned(
                    signal_schema::AssignedAgentIdentity::from_nota_block(&fields[1])?,
                ))
            }
            "AgentEndpointBound" => {
                Self::expect_fields(fields, "Output::AgentEndpointBound", 2)?;
                Ok(Self::AgentEndpointBound(
                    signal_schema::BoundAgentEndpoint::from_nota_block(&fields[1])?,
                ))
            }
            "AgentRegistryListing" => {
                Self::expect_fields(fields, "Output::AgentRegistryListing", 2)?;
                Ok(Self::AgentRegistryListing(
                    signal_schema::AgentRegistryEntries::from_nota_block(&fields[1])?,
                ))
            }
            "AgentRegistryRejected" => {
                Self::expect_fields(fields, "Output::AgentRegistryRejected", 2)?;
                Ok(Self::AgentRegistryRejected(
                    signal_schema::AgentRegistryRejection::from_nota_block(&fields[1])?,
                ))
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
            "ThreadListing" => {
                Self::expect_fields(fields, "Output::ThreadListing", 2)?;
                Ok(Self::ThreadListing(
                    signal_schema::ThreadContents::from_nota_block(&fields[1])?,
                ))
            }
            "ThreadSubscribed" => {
                Self::expect_fields(fields, "Output::ThreadSubscribed", 2)?;
                Ok(Self::ThreadSubscribed(
                    signal_schema::ThreadSubscriptionAcknowledgment::from_nota_block(&fields[1])?,
                ))
            }
            "ThreadIndexListing" => {
                Self::expect_fields(fields, "Output::ThreadIndexListing", 2)?;
                Ok(Self::ThreadIndexListing(
                    signal_schema::ThreadIndexEntries::from_nota_block(&fields[1])?,
                ))
            }
            "ThreadRejected" => {
                Self::expect_fields(fields, "Output::ThreadRejected", 2)?;
                Ok(Self::ThreadRejected(
                    signal_schema::ThreadRejection::from_nota_block(&fields[1])?,
                ))
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
            Self::AgentIdentityAssigned(assigned) => Delimiter::Parenthesis
                .wrap([String::from("AgentIdentityAssigned"), assigned.to_nota()]),
            Self::AgentEndpointBound(bound) => Delimiter::Parenthesis
                .wrap([String::from("AgentEndpointBound"), bound.to_nota()]),
            Self::AgentRegistryListing(listing) => Delimiter::Parenthesis
                .wrap([String::from("AgentRegistryListing"), listing.to_nota()]),
            Self::AgentRegistryRejected(rejection) => Delimiter::Parenthesis
                .wrap([String::from("AgentRegistryRejected"), rejection.to_nota()]),
            Self::Unimplemented(operation, reason) => Delimiter::Parenthesis.wrap([
                String::from("Unimplemented"),
                operation.to_nota(),
                reason.to_nota(),
            ]),
            Self::Error(message) => {
                Delimiter::Parenthesis.wrap([String::from("Error"), message.to_nota()])
            }
            Self::ThreadListing(listing) => {
                Delimiter::Parenthesis.wrap([String::from("ThreadListing"), listing.to_nota()])
            }
            Self::ThreadSubscribed(acknowledgment) => Delimiter::Parenthesis.wrap([
                String::from("ThreadSubscribed"),
                acknowledgment.to_nota(),
            ]),
            Self::ThreadIndexListing(listing) => Delimiter::Parenthesis.wrap([
                String::from("ThreadIndexListing"),
                listing.to_nota(),
            ]),
            Self::ThreadRejected(rejection) => Delimiter::Parenthesis.wrap([
                String::from("ThreadRejected"),
                rejection.to_nota(),
            ]),
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
            thread: ThreadSelection::from_signal(entry.thread_selection),
            stamped_at: entry.stamped_at.into_payload().into_payload(),
        }
    }
}

impl ThreadSelection {
    fn from_signal(selection: signal_schema::ThreadSelection) -> Self {
        match selection {
            signal_schema::ThreadSelection::None => Self::None,
            signal_schema::ThreadSelection::Named(name) => {
                Self::Named(SurfaceThreadName::new(name.into_payload()))
            }
        }
    }
}

impl OperationKind {
    fn from_signal(kind: signal_schema::OperationKind) -> Self {
        match kind {
            signal_schema::OperationKind::Submit => Self::Submit,
            signal_schema::OperationKind::SubmitStamped => Self::SubmitStamped,
            signal_schema::OperationKind::QueryInbox => Self::QueryInbox,
            signal_schema::OperationKind::AssignAgentIdentity => Self::AssignAgentIdentity,
            signal_schema::OperationKind::BindAgentEndpoint => Self::BindAgentEndpoint,
            signal_schema::OperationKind::QueryAgentRegistry => Self::QueryAgentRegistry,
            signal_schema::OperationKind::QueryThread => Self::QueryThread,
            signal_schema::OperationKind::SubscribeThread => Self::SubscribeThread,
            signal_schema::OperationKind::QueryThreads => Self::QueryThreads,
        }
    }
}

impl UnimplementedReason {
    fn from_signal(reason: signal_schema::UnimplementedReason) -> Self {
        match reason {
            signal_schema::UnimplementedReason::NotInPrototypeScope => Self::NotInPrototypeScope,
            signal_schema::UnimplementedReason::DependencyMissing(kind) => {
                Self::DependencyMissing(kind)
            }
            signal_schema::UnimplementedReason::ResourceUnavailable(kind) => {
                Self::ResourceUnavailable(kind)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandLine {
    command: ComponentCommand,
}

impl CommandLine {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
        }
    }

    pub fn decode_input(&self) -> MessageResult<Input> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Input::from_nota(&argument.into_string()),
            ComponentArgument::NotaFile(file) => InputFile::from_path(file.into_path()).decode(),
            ComponentArgument::SignalFile(file) => InputFile::from_path(file.into_path()).decode(),
        }
    }

    pub fn run(&self, output: impl Write) -> MessageResult<()> {
        self.decode_input()?.run(output)
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
        let text = std::fs::read_to_string(&self.path).map_err(|source| Error::ReadNotaFile {
            path: self.path.clone(),
            source,
        })?;
        Input::from_nota(&text)
    }
}
