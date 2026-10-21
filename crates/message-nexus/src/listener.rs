//! The ordinary and owner-only Message sockets.

use crate::{
    flow_edge::ReadsFlowSubscription,
    frame::{FrameError, FramedStream},
    nexus::{HoldsNexusState, MessageNexus},
    peer::IdentifiesPeer,
    service::{AcceptedSend, ConfigureFailure, ConfiguresMessage, ServesMessages},
};
use meta_signal_message::{Query as MetaQuery, Response as MetaResponse};
use signal_flow::{DeliveryState, LogicalDeliveryId, Response as FlowResponse};
use signal_message::{Query, Response};
use std::{
    fs,
    io::{self, Read},
    net::Shutdown,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::{fs::PermissionsExt, net::UnixListener, net::UnixStream},
    path::Path,
    sync::Arc,
};

pub trait ListensOnSockets {
    fn serve(self: Arc<Self>) -> io::Result<()>;
}

impl ListensOnSockets for MessageNexus {
    fn serve(self: Arc<Self>) -> io::Result<()> {
        let ordinary = Socket::Ordinary.bind(&self.bound_configuration().ordinary)?;
        let meta = Socket::Meta.bind(&self.bound_configuration().meta)?;
        // The executable blocks these signals before creating any threads.
        let raw = unsafe {
            let mut mask: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut mask);
            libc::sigaddset(&mut mask, libc::SIGTERM);
            libc::sigaddset(&mut mask, libc::SIGINT);
            libc::signalfd(-1, &mask, libc::SFD_CLOEXEC)
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: signalfd returned a new descriptor owned by this scope.
        let signals = unsafe { OwnedFd::from_raw_fd(raw) };
        ordinary.set_nonblocking(true)?;
        meta.set_nonblocking(true)?;
        let mut workers: Vec<(UnixStream, std::thread::JoinHandle<()>)> = Vec::new();
        let result = loop {
            let mut ready = [
                libc::pollfd {
                    fd: signals.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: ordinary.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
                libc::pollfd {
                    fd: meta.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                },
            ];
            // A blocking readiness wait, not a timer-driven scan.
            if unsafe { libc::poll(ready.as_mut_ptr(), ready.len() as libc::nfds_t, -1) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                break Err(error);
            }
            if ready[0].revents != 0 {
                break Ok(());
            }
            let mut failure = None;
            for (index, socket, listener) in
                [(1, Socket::Ordinary, &ordinary), (2, Socket::Meta, &meta)]
            {
                if ready[index].revents == 0 {
                    continue;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let control = match stream.try_clone() {
                            Ok(control) => control,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                        let nexus = Arc::clone(&self);
                        let completion = match control.try_clone() {
                            Ok(completion) => completion,
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        };
                        let worker = std::thread::spawn(move || {
                            let result = match socket {
                                Socket::Ordinary => nexus.answer_ordinary(stream),
                                Socket::Meta => nexus.answer_meta(stream),
                            };
                            // The listener retains the control handle until its
                            // next event reaps this worker. Close the response
                            // half now so clients observe completion promptly.
                            let _ = completion.shutdown(Shutdown::Write);
                            if let Err(error) = result
                                && !matches!(error, FrameError::Closed)
                            {
                                eprintln!("message-nexus: {socket:?} connection: {error}");
                            }
                        });
                        workers.push((control, worker));
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            let mut index = 0;
            while index < workers.len() {
                if workers[index].1.is_finished() {
                    let (_, worker) = workers.swap_remove(index);
                    let _ = worker.join();
                } else {
                    index += 1;
                }
            }
            if let Some(error) = failure {
                break Err(error);
            }
        };
        drop(ordinary);
        drop(meta);
        for (stream, _) in &workers {
            let _ = stream.shutdown(Shutdown::Both);
        }
        for (_, worker) in workers {
            let _ = worker.join();
        }
        // Every handler has released its Arc before the final Engine is dropped.
        result?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Socket {
    Ordinary,
    Meta,
}

trait ServesSocket {
    fn bind(self, path: &str) -> io::Result<UnixListener>;
}

impl ServesSocket for Socket {
    fn bind(self, path: &str) -> io::Result<UnixListener> {
        let path = Path::new(path);
        if let Some(directory) = path.parent() {
            fs::create_dir_all(directory)?;
        }
        if path.exists() && UnixStream::connect(path).is_err() {
            fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok(listener)
    }
}

trait AnswersConnection {
    fn answer_ordinary(&self, stream: UnixStream) -> Result<(), FrameError>;
    fn answer_meta(&self, stream: UnixStream) -> Result<(), FrameError>;
    fn answer_send(
        &self,
        stream: &mut UnixStream,
        process: signal_flow::Process,
        request: signal_flow::SendRequest,
        thread: Option<String>,
    ) -> Result<(), FrameError>;
    fn answer_observation(
        &self,
        stream: &mut UnixStream,
        process: signal_flow::Process,
        thread: Option<String>,
        cursor: Option<signal_flow::DeliveryCursor>,
    ) -> Result<(), FrameError>;
    fn forward_subscription(
        &self,
        stream: &mut UnixStream,
        subscription: &mut crate::flow_edge::FlowSubscription,
        terminal_id: Option<LogicalDeliveryId>,
    ) -> Result<(), FrameError>;
}

impl AnswersConnection for MessageNexus {
    fn answer_ordinary(&self, mut stream: UnixStream) -> Result<(), FrameError> {
        let query = stream.read_frame::<Query>()?;
        let Some(process) = stream.peer_process() else {
            return Err(FrameError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot read the peer process identity",
            )));
        };
        match query {
            Query::Send(request) => self.answer_send(&mut stream, process, request, None),
            Query::SendThread(request) => self.answer_send(
                &mut stream,
                process,
                request.send_request,
                Some(request.native_session_id),
            ),
            Query::ObserveSends(cursor) => {
                self.answer_observation(&mut stream, process, None, cursor)
            }
            Query::ObserveThread(request) => self.answer_observation(
                &mut stream,
                process,
                Some(request.native_session_id),
                request.delivery_cursor_option,
            ),
            query => stream.write_frame(&self.serve_query(process, query)),
        }
    }

    fn answer_send(
        &self,
        stream: &mut UnixStream,
        process: signal_flow::Process,
        request: signal_flow::SendRequest,
        thread: Option<String>,
    ) -> Result<(), FrameError> {
        match self.accept_send(process, request, thread) {
            Err(response) => stream.write_frame(response.as_ref()),
            Ok(AcceptedSend {
                acceptance: accepted,
                subscription,
            }) => {
                stream.write_frame(&Response::SendAccepted(accepted.clone()))?;
                match subscription {
                    Ok(mut subscription) => self.forward_subscription(
                        stream,
                        &mut subscription,
                        Some(accepted.logical_delivery_id),
                    ),
                    Err(response) => stream.write_frame(response.as_ref()),
                }
            }
        }
    }

    fn answer_observation(
        &self,
        stream: &mut UnixStream,
        process: signal_flow::Process,
        thread: Option<String>,
        cursor: Option<signal_flow::DeliveryCursor>,
    ) -> Result<(), FrameError> {
        match self.observe_sends(process, thread, cursor) {
            Err(response) => stream.write_frame(response.as_ref()),
            Ok(mut subscription) => self.forward_subscription(stream, &mut subscription, None),
        }
    }

    fn forward_subscription(
        &self,
        stream: &mut UnixStream,
        subscription: &mut crate::flow_edge::FlowSubscription,
        terminal_id: Option<LogicalDeliveryId>,
    ) -> Result<(), FrameError> {
        let flow_control = subscription.interrupt_handle().map_err(|_| {
            FrameError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "cannot monitor Flow subscription",
            ))
        })?;
        let mut client_control = stream.try_clone()?;
        let watcher = std::thread::spawn(move || {
            let mut byte = [0; 1];
            loop {
                match client_control.read(&mut byte) {
                    Ok(0) | Err(_) => {
                        let _ = flow_control.shutdown(Shutdown::Both);
                        break;
                    }
                    Ok(_) => {}
                }
            }
        });
        let result = (|| {
            let mut last_cursor = None;
            loop {
                let response = match subscription.next() {
                    Ok(response) => response,
                    Err(error) => {
                        return stream.write_frame(&Response::ObservationInterrupted(
                            signal_message::ObservationInterrupted_Data {
                                delivery_cursor_option: last_cursor,
                                string: format!("Flow status stream ended: {error:?}"),
                            },
                        ));
                    }
                };
                let (message_response, finished) = match response {
                    FlowResponse::SendsObserved(observation) => {
                        last_cursor = Some(observation.delivery_cursor);
                        let finished = terminal_id.as_deref().is_some_and(|id| {
                            observation.delivery_snapshot_vector.iter().any(|item| {
                                item.logical_delivery_id.as_str() == id
                                    && item.delivery_state.is_terminal()
                            })
                        });
                        (Response::SendsObserved(observation), finished)
                    }
                    FlowResponse::SendChanged(snapshot) => {
                        last_cursor = Some(snapshot.delivery_cursor);
                        let finished = terminal_id.as_deref()
                            == Some(snapshot.logical_delivery_id.as_str())
                            && snapshot.delivery_state.is_terminal();
                        (Response::SendChanged(snapshot), finished)
                    }
                    other => {
                        return stream.write_frame(&Response::ObservationInterrupted(
                            signal_message::ObservationInterrupted_Data {
                                delivery_cursor_option: last_cursor,
                                string: format!("Unexpected Flow status response: {other:?}"),
                            },
                        ));
                    }
                };
                stream.write_frame(&message_response)?;
                if finished {
                    return Ok(());
                }
            }
        })();
        // The request has been fully consumed. Shutting down only this
        // connection's receive half releases the EOF watcher while preserving
        // delivery of the final response to the client.
        let _ = stream.shutdown(Shutdown::Read);
        let _ = watcher.join();
        result
    }

    fn answer_meta(&self, mut stream: UnixStream) -> Result<(), FrameError> {
        let query = stream.read_frame::<MetaQuery>()?;
        let MetaQuery::Configure(configuration) = query;
        let response = match self.configure(configuration, true) {
            Ok(true) => MetaResponse::RestartRequired,
            Ok(false) => MetaResponse::Configured,
            Err(ConfigureFailure::AlreadyConfigured | ConfigureFailure::Store(_)) => {
                MetaResponse::Refused(meta_signal_message::ConfigureRefusal::StoreRefused)
            }
        };
        stream.write_frame(&response)
    }
}

trait TerminalDeliveryState {
    fn is_terminal(&self) -> bool;
}

impl TerminalDeliveryState for DeliveryState {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Delivered(_) | Self::Failed(_))
    }
}
