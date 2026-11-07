//! A scripted Flow that records the typed request received by Deliver.

use message_nexus::frame::FramedStream;
use signal_flow::{
    Address, Aspect, Bind_Data, Deliver_Data, FlowId, Lock, Lock_Data, Model, Process, Query,
    Refusal, Response, VisibleDeliveryState, VisibleSendAcceptance, VisibleSendObservation,
};
use std::{
    io::Read,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[derive(Default)]
struct State {
    bound: Vec<(Address, Process)>,
    delivered: Vec<Deliver_Data>,
    acknowledged: Vec<signal_flow::DeliverKey>,
    identified_threads: Vec<signal_flow::IdentifyThread_Data>,
}

#[derive(Clone)]
pub struct FakeFlow {
    state: Arc<Mutex<State>>,
    socket_path: PathBuf,
    quiet_observer: bool,
    observer_closed: Arc<(Mutex<usize>, Condvar)>,
}

impl FakeFlow {
    pub fn start(runtime_directory: &Path) -> Self {
        Self::start_with_observer(runtime_directory, false)
    }

    pub fn start_quiet(runtime_directory: &Path) -> Self {
        Self::start_with_observer(runtime_directory, true)
    }

    fn start_with_observer(runtime_directory: &Path, quiet_observer: bool) -> Self {
        let directory = runtime_directory.join("flow");
        std::fs::create_dir_all(&directory).unwrap();
        let socket_path = directory.join("flow.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let flow = Self {
            state: Arc::new(Mutex::new(State::default())),
            socket_path,
            quiet_observer,
            observer_closed: Arc::new((Mutex::new(0), Condvar::new())),
        };
        let accepting = flow.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let flow = accepting.clone();
                std::thread::spawn(move || flow.answer(stream));
            }
        });
        flow
    }

    pub fn wait_for_observer_close(&self) {
        let (count, changed) = &*self.observer_closed;
        let count = count.lock().unwrap();
        let (count, timeout) = changed
            .wait_timeout_while(count, Duration::from_secs(5), |count| *count == 0)
            .unwrap();
        assert!(*count > 0, "Flow observation socket stayed open");
        assert!(!timeout.timed_out() || *count > 0);
    }

    pub fn socket_path(&self) -> PathBuf {
        self.socket_path.clone()
    }

    pub fn typed(&self) -> Vec<Deliver_Data> {
        self.state.lock().unwrap().delivered.clone()
    }

    pub fn identified_threads(&self) -> Vec<signal_flow::IdentifyThread_Data> {
        self.state.lock().unwrap().identified_threads.clone()
    }

    pub fn bound(&self) -> Vec<(Address, Process)> {
        self.state.lock().unwrap().bound.clone()
    }

    pub fn acknowledged(&self) -> Vec<signal_flow::DeliverKey> {
        self.state.lock().unwrap().acknowledged.clone()
    }

    fn sender(&self) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            model: Model::Sol,
        }
    }

    fn answer(&self, mut stream: UnixStream) {
        let Ok(query) = stream.read_frame::<Query>() else {
            return;
        };
        let observing = matches!(&query, Query::ObserveSender(_));
        let response = match query {
            Query::Bind(Bind_Data { address, process }) => {
                self.state.lock().unwrap().bound.push((address, process));
                Response::Bound(FlowId::from("message-flow"))
            }
            Query::Identify(_) => Response::Identified(self.sender()),
            Query::IdentifyThread(request) => {
                self.state
                    .lock()
                    .unwrap()
                    .identified_threads
                    .push(request.clone());
                match request.native_session_id.as_str() {
                    "not-configured" => Response::Refused(Refusal::NotConfigured),
                    "not-send-server" => Response::Refused(Refusal::NotSendServer(request.process)),
                    "unknown" => {
                        Response::Refused(Refusal::Unidentified(signal_flow::SeatEvidence {
                            seat_pane_option: None,
                            pane_label_option: None,
                            native_session_id_option: Some(request.native_session_id),
                            observed_at: 0,
                            process: request.process,
                        }))
                    }
                    _ => Response::Identified(self.sender()),
                }
            }
            Query::Lock(Lock_Data { sender, recipient }) => Response::Locked(Lock {
                sender,
                address: recipient,
                until: i64::MAX,
            }),
            Query::Deliver(delivery) => {
                self.state.lock().unwrap().delivered.push(delivery);
                Response::SendAccepted(VisibleSendAcceptance {
                    request_id: "fake-request".into(),
                    delivery_cursor: 1,
                    visible_delivery_state: VisibleDeliveryState::Queued,
                })
            }
            Query::Acknowledge(deliver_keys) => {
                self.state.lock().unwrap().acknowledged.extend(deliver_keys);
                Response::Acknowledged
            }
            Query::ObserveSender(_) => Response::SendsObserved(VisibleSendObservation {
                store_identity: "fake-store".into(),
                delivery_cursor: 1,
                visible_delivery_snapshot_vector: vec![],
            }),
            Query::Release(_) => Response::Released,
            _ => Response::Refused(Refusal::NotConfigured),
        };
        let _ = stream.write_frame(&response);
        if observing && self.quiet_observer {
            let mut byte = [0; 1];
            let _ = stream.read(&mut byte);
            let (count, changed) = &*self.observer_closed;
            *count.lock().unwrap() += 1;
            changed.notify_all();
        }
    }
}
