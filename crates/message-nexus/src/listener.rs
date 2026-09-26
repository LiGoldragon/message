//! The two sockets. Every connection carries one request; Observe goes on
//! answering on its connection until the client leaves.
//!
//! The meta socket answers the owner (a process in no flow's pane) and the
//! flows whose aspect is in MetaAspects, as Flow's meta socket does; every
//! other peer is answered MetaRefused. Both sockets are `0600` under one
//! Unix user, so this gate stops accidents and model mistakes, not an
//! adversary. While Flow cannot be reached, only Configure is answered, so
//! the owner can repair a wrong Flow socket path.

use crate::{
    flow_edge::{CallsFlowMeta, PeerName},
    frame::FramedStream,
    ledger::RecordsReceipts,
    nexus::{HoldsNexusState, MessageNexus},
    peer::IdentifiesPeer,
    service::{NamesPeerFlow, PeerFlow, ServesMessages},
    store::KeepsLedger,
};
use meta_signal_flow::{MetaRefusal, Sender};
use meta_signal_message::{Query as MetaQuery, Response as MetaResponse};
use signal_message::{Query, Response};
use std::{
    fs, io,
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
    sync::Arc,
};

/// Which socket a connection arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Socket {
    Ordinary,
    Meta,
}

pub trait ListensOnSockets {
    /// Binds both sockets, resumes owed deliveries, and serves forever.
    fn serve(self: Arc<Self>) -> io::Result<()>;
}

impl ListensOnSockets for MessageNexus {
    fn serve(self: Arc<Self>) -> io::Result<()> {
        let ordinary = Socket::Ordinary.bind(&self.bound.ordinary_socket_path)?;
        let meta = Socket::Meta.bind(&self.bound.meta_socket_path)?;
        if let Err(error) = crate::delivery::DeliversThroughFlow::resume(&self) {
            eprintln!("message-nexus: cannot resume owed deliveries: {error}");
        }
        let meta_nexus = Arc::clone(&self);
        let meta_thread = std::thread::spawn(move || Socket::Meta.accept(meta, meta_nexus));
        Socket::Ordinary.accept(ordinary, self);
        let _ = meta_thread.join();
        Ok(())
    }
}

impl Socket {
    fn bind(self, path: &str) -> io::Result<UnixListener> {
        let path = Path::new(path);
        if let Some(directory) = path.parent() {
            fs::create_dir_all(directory)?;
        }
        // A socket file left by a Nexus that is gone.
        if path.exists() && UnixStream::connect(path).is_err() {
            fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(listener)
    }

    fn accept(self, listener: UnixListener, nexus: Arc<MessageNexus>) {
        for connection in listener.incoming() {
            let Ok(stream) = connection else { continue };
            let nexus = Arc::clone(&nexus);
            std::thread::spawn(move || {
                let outcome = match self {
                    Socket::Ordinary => nexus.answer_ordinary(stream),
                    Socket::Meta => nexus.answer_meta(stream),
                };
                // A peer that connects and leaves (a readiness probe) is not an
                // error worth a line.
                if let Err(error) = outcome
                    && !matches!(error, crate::frame::FrameError::Closed)
                {
                    eprintln!("message-nexus: {self:?} connection: {error}");
                }
            });
        }
    }
}

trait AnswersConnection {
    fn answer_ordinary(
        self: &Arc<Self>,
        stream: UnixStream,
    ) -> Result<(), crate::frame::FrameError>;
    fn answer_meta(self: &Arc<Self>, stream: UnixStream) -> Result<(), crate::frame::FrameError>;
    fn observe(
        &self,
        stream: UnixStream,
        message_id: String,
    ) -> Result<(), crate::frame::FrameError>;
    fn meta_refusal(&self, stream: &UnixStream, query: &MetaQuery) -> Option<MetaRefusal>;
}

impl AnswersConnection for MessageNexus {
    fn answer_ordinary(
        self: &Arc<Self>,
        mut stream: UnixStream,
    ) -> Result<(), crate::frame::FrameError> {
        let query = stream.read_frame::<Query>()?;
        let peer = || self.peer_flow(stream.peer_identity());
        let response = match query {
            Query::Send(request) => match peer() {
                PeerFlow::Flow(flow_id) => match self.send(Sender::Flow(flow_id), request) {
                    Ok(submission) => Response::Submitted(submission),
                    Err(rejection) => Response::SendRejected(rejection),
                },
                PeerFlow::NotAFlow => {
                    Response::SendRejected(signal_message::SendRejection::SenderUnknown)
                }
                PeerFlow::FlowUnreachable => {
                    Response::SendRejected(signal_message::SendRejection::FlowUnreachable)
                }
            },
            Query::Withdraw(message_id) => match self.withdraw(peer(), message_id) {
                Ok(message_id) => Response::Withdrawn(message_id),
                Err(rejection) => Response::MessageRejected(rejection),
            },
            Query::Acknowledge(message_id) => match self.acknowledge(peer(), message_id) {
                Ok(message_id) => Response::Acknowledged(message_id),
                Err(rejection) => Response::MessageRejected(rejection),
            },
            Query::QueryReceipts(message_id) => match self.query_receipts(&message_id) {
                Ok(submission) => Response::Receipts(submission),
                Err(rejection) => Response::MessageRejected(rejection),
            },
            Query::Observe(message_id) => return self.observe(stream, message_id),
        };
        stream.write_frame(&response)
    }

    fn observe(
        &self,
        mut stream: UnixStream,
        message_id: String,
    ) -> Result<(), crate::frame::FrameError> {
        // Subscribed before the opening read, so no grade falls between.
        let later = self.subscribe(&message_id);
        let opening = match self.query_receipts(&message_id) {
            Ok(submission) => Response::Receipts(submission),
            Err(rejection) => return stream.write_frame(&Response::MessageRejected(rejection)),
        };
        stream.write_frame(&opening)?;
        for receipt in later {
            stream.write_frame(&Response::ReceiptObserved(receipt))?;
        }
        Ok(())
    }

    fn answer_meta(
        self: &Arc<Self>,
        mut stream: UnixStream,
    ) -> Result<(), crate::frame::FrameError> {
        let query = stream.read_frame::<MetaQuery>()?;
        if let Some(refusal) = self.meta_refusal(&stream, &query) {
            return stream.write_frame(&MetaResponse::MetaRefused(refusal));
        }
        let response = match query {
            MetaQuery::Configure(configuration) => match self.configure(configuration) {
                Ok(configured) => MetaResponse::Configured(configured),
                Err(rejection) => MetaResponse::ConfigureRejected(rejection),
            },
            MetaQuery::Send(request) => match self.send(Sender::Owner, request) {
                Ok(submission) => MetaResponse::Submitted(submission),
                Err(rejection) => MetaResponse::SendRejected(rejection),
            },
            MetaQuery::Redeliver(request) => {
                match self.redeliver(request.message_id, request.flow_id) {
                    Ok(receipt) => MetaResponse::Redelivered(receipt),
                    Err(rejection) => MetaResponse::RedeliverRejected(rejection),
                }
            }
        };
        stream.write_frame(&response)
    }

    fn meta_refusal(&self, stream: &UnixStream, query: &MetaQuery) -> Option<MetaRefusal> {
        let Some(identity) = stream.peer_identity() else {
            return Some(MetaRefusal::PeerUnknown);
        };
        let resolution = self
            .flow_edge()
            .ok()
            .map(|edge| edge.resolve_peer(identity));
        match resolution {
            Some(Ok(PeerName::NotAFlow)) => None,
            Some(Ok(PeerName::Flow(caller))) => {
                let admitted = self.store().configuration().is_ok_and(|configuration| {
                    configuration.meta_aspects.contains(&caller.flow_aspect)
                });
                if admitted {
                    None
                } else {
                    Some(MetaRefusal::PeerNotAuthorized(caller))
                }
            }
            Some(Ok(PeerName::Mismatched)) => Some(MetaRefusal::PeerUnknown),
            _ if matches!(query, MetaQuery::Configure(_)) => None,
            _ => Some(MetaRefusal::PeerUnknown),
        }
    }
}
