//! Messenger behavior over the producer-owned Message contract.
//!
//! The component does not own a second Signal, Nexus, or Sema vocabulary.
//! Each strict `signal-message::Input` is decided directly into one durable
//! messenger action and one strict `signal-message::Output`.

use signal_message::schema::lib::{
    Input, Output, z2VLsC, z2VPEF, z2VPW5, z2VR9d, z2VRGD, z2VS1e, z2VUzX, z2VV6N, z2VW5p, z2VYJe,
    z2VYf6, z2VZEr, z2VZuS, z2Vasi, z2Vc6L, z2Ve52,
};
use triad_runtime::ConnectionContext;

use crate::{
    config::Configuration,
    delivery::DeliveryRunner,
    error::Error,
    provenance::{OriginPolicy, SenderResolver},
    runtime_model::{AgentRegistryCommand, LedgerDraft, StoreQuery, StoreWrite},
    tables::MessengerTables,
};

#[derive(Debug)]
pub struct MessageEngine {
    tables: MessengerTables,
    origin_policy: OriginPolicy,
}

impl MessageEngine {
    pub fn new(tables: MessengerTables, origin_policy: OriginPolicy) -> Self {
        Self {
            tables,
            origin_policy,
        }
    }

    pub fn from_configuration(configuration: &Configuration) -> Result<Self, Error> {
        Ok(Self::new(
            MessengerTables::open(configuration.database_path())?,
            OriginPolicy::for_owner_user_id(
                configuration.owner_user_id(),
                configuration.owner_label(),
            ),
        ))
    }

    pub async fn handle(
        &mut self,
        input: Input,
        connection: &ConnectionContext,
    ) -> Result<Output, Error> {
        Ok(match input {
            Input::Submit(submission) => {
                let sender =
                    SenderResolver::new(&self.tables, &self.origin_policy).resolve(connection);
                self.apply_store_write(StoreWrite::RecordSubmission(LedgerDraft {
                    message_submission: submission,
                    message_origin: self.origin_policy.origin_for_connection(connection),
                    sender_name: sender,
                    stamped_at: self.origin_policy.ingress_stamp(),
                }))
            }
            Input::SubmitStamped(_) => Output::MessageRequestUnimplemented(z2VYf6 {
                field_0: z2VLsC::z2VT63,
                field_1: z2Vc6L::z2VXy5,
            }),
            Input::QueryInbox(query) => self.read_store_query(StoreQuery::Inbox(query)),
            Input::AssignAgentIdentity(assignment) => {
                self.apply_registry_command(AgentRegistryCommand::AssignIdentity(assignment))
            }
            Input::BindAgentEndpoint(binding) => {
                self.apply_registry_command(AgentRegistryCommand::BindEndpoint(binding))
            }
            Input::QueryAgentRegistry(query) => self.read_registry_query(query),
            Input::QueryThread(query) => self.read_store_query(StoreQuery::Thread(query)),
            Input::SubscribeThread(subscription) => {
                self.apply_store_write(StoreWrite::Subscribe(subscription))
            }
            Input::QueryThreads(query) => self.read_store_query(StoreQuery::Threads(query)),
        })
    }

    fn apply_registry_command(&self, command: AgentRegistryCommand) -> Output {
        match command {
            AgentRegistryCommand::AssignIdentity(assignment) => {
                match self.tables.seat_identity(&assignment) {
                    Ok(assigned) => Output::AgentIdentityAssigned(assigned),
                    Err(_) => Self::registry_rejection(z2VW5p::z2VYC4),
                }
            }
            AgentRegistryCommand::BindEndpoint(binding) => {
                match self.tables.bind_endpoint(&binding) {
                    Ok(Some(bound)) => {
                        DeliveryRunner::new(&self.tables)
                            .drain_outbox(bound.payload().payload().as_str());
                        Output::AgentEndpointBound(bound)
                    }
                    Ok(None) => Self::registry_rejection(z2VW5p::z2VYA6),
                    Err(_) => Self::registry_rejection(z2VW5p::z2VYC4),
                }
            }
        }
    }

    fn read_registry_query(&self, query: z2VYJe) -> Output {
        match self.tables.query_entries(&query) {
            Ok(entries) => Output::AgentRegistryListing(z2VUzX {
                field_0: z2VS1e::new(entries),
            }),
            Err(_) => Self::registry_rejection(z2VW5p::z2VYC4),
        }
    }

    fn apply_store_write(&self, write: StoreWrite) -> Output {
        match write {
            StoreWrite::RecordSubmission(draft) => match self.tables.store_submission(&draft) {
                Ok(acceptance) => {
                    if let Ok(Some(record)) = self
                        .tables
                        .ledger_record_public(*acceptance.payload().payload())
                    {
                        DeliveryRunner::new(&self.tables).deliver_committed(&record);
                    }
                    Output::SubmissionAccepted(acceptance)
                }
                Err(_) => Output::SubmissionRejected(z2VZEr::new(z2VPW5::z2Veun)),
            },
            StoreWrite::Subscribe(subscription) => {
                match self.tables.subscribe_thread(&subscription) {
                    Ok(acknowledgment) => Output::ThreadSubscribed(acknowledgment),
                    Err(_) => Output::ThreadRejected(z2VPEF::new(z2Ve52::z2Vdxv)),
                }
            }
        }
    }

    fn read_store_query(&self, query: StoreQuery) -> Output {
        match query {
            StoreQuery::Inbox(inbox_query) => match self.tables.inbox_entries(&inbox_query) {
                Ok(entries) => Output::InboxListing(z2VRGD {
                    field_0: signal_message::schema::lib::z2Vb4S::new(entries),
                }),
                Err(_) => Output::SubmissionRejected(z2VZEr::new(z2VPW5::z2Veun)),
            },
            StoreQuery::Thread(thread_query) => {
                let thread_name = thread_query.into_payload();
                match self.tables.thread_contents(&thread_name) {
                    Ok(Some(contents)) => Output::ThreadListing(contents),
                    Ok(None) => Output::ThreadRejected(z2VPEF::new(z2Ve52::z2VQY2)),
                    Err(_) => Output::ThreadRejected(z2VPEF::new(z2Ve52::z2Vdxv)),
                }
            }
            StoreQuery::Threads(_) => match self.tables.thread_summaries() {
                Ok(summaries) => Output::ThreadIndexListing(z2VR9d {
                    field_0: z2VV6N::new(summaries),
                }),
                Err(_) => Output::ThreadRejected(z2VPEF::new(z2Ve52::z2Vdxv)),
            },
        }
    }

    fn registry_rejection(reason: z2VW5p) -> Output {
        Output::AgentRegistryRejected(signal_message::schema::lib::z2VP29::new(reason))
    }

    #[allow(dead_code)]
    fn error_output(message: impl Into<String>) -> Output {
        Output::Error(z2Vasi::new(z2VZuS::new(message.into())))
    }
}
