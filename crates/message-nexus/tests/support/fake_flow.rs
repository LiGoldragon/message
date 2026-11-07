//! A scripted Flow that records the typed request received by Deliver.

use message_nexus::frame::FramedStream;
use signal_flow::{
    Address, Aspect, Bind_Data, Deliver_Data, FlowId, Lock, Lock_Data, Model, Process, Query,
    Refusal, Response, VisibleDeliverySnapshot, VisibleDeliveryState, VisibleSendAcceptance,
    VisibleSendObservation, VisibleSender,
};
use std::{
    io::Read,
    net::Shutdown,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultBoundary {
    AnswerBeforeLedger,
    LedgerBeforeAcknowledge,
    HeldBeforeCursor,
}

#[derive(Default)]
struct State {
    bound: Vec<(Address, Process)>,
    delivered: Vec<Deliver_Data>,
    delivery_attempts: Vec<signal_flow::DeliverKey>,
    acknowledged: Vec<signal_flow::DeliverKey>,
    identified_threads: Vec<signal_flow::IdentifyThread_Data>,
    delivery_observer: Option<UnixStream>,
    delivery_observation_cursors: Vec<Option<i64>>,
    delivery_snapshots: Vec<VisibleDeliverySnapshot>,
    delivery_cursor: i64,
    unavailable_delivery_observations: usize,
    fault: Option<FaultBoundary>,
    withheld_delivery_answers: usize,
}

#[derive(Clone)]
pub struct FakeFlow {
    state: Arc<Mutex<State>>,
    socket_path: PathBuf,
    quiet_observer: bool,
    observer_closed: Arc<(Mutex<usize>, Condvar)>,
    delivery_observed: Arc<(Mutex<usize>, Condvar)>,
    fault_reached: Arc<(Mutex<bool>, Condvar)>,
    acknowledgements_changed: Arc<(Mutex<usize>, Condvar)>,
    delivery_answers_withheld: Arc<(Mutex<usize>, Condvar)>,
    delivery_changes_sent: Arc<(Mutex<usize>, Condvar)>,
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
            delivery_observed: Arc::new((Mutex::new(0), Condvar::new())),
            fault_reached: Arc::new((Mutex::new(false), Condvar::new())),
            acknowledgements_changed: Arc::new((Mutex::new(0), Condvar::new())),
            delivery_answers_withheld: Arc::new((Mutex::new(0), Condvar::new())),
            delivery_changes_sent: Arc::new((Mutex::new(0), Condvar::new())),
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

    pub fn delivery_attempts(&self) -> Vec<signal_flow::DeliverKey> {
        self.state.lock().unwrap().delivery_attempts.clone()
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

    pub fn arm_fault(&self, boundary: FaultBoundary) {
        self.state.lock().unwrap().fault = Some(boundary);
        *self.fault_reached.0.lock().unwrap() = false;
    }

    pub fn wait_for_fault(&self) {
        let (reached, changed) = &*self.fault_reached;
        let reached = reached.lock().unwrap();
        let (reached, timeout) = changed
            .wait_timeout_while(reached, Duration::from_secs(5), |reached| !*reached)
            .unwrap();
        assert!(*reached, "Message did not reach the armed durable boundary");
        assert!(!timeout.timed_out() || *reached);
    }

    pub fn wait_for_acknowledgements(&self, expected: usize) {
        let (count, changed) = &*self.acknowledgements_changed;
        let count = count.lock().unwrap();
        let (count, timeout) = changed
            .wait_timeout_while(count, Duration::from_secs(5), |count| *count < expected)
            .unwrap();
        assert!(
            *count >= expected,
            "Message did not acknowledge its durable ledger"
        );
        assert!(!timeout.timed_out() || *count >= expected);
    }

    pub fn withhold_delivery_answers(&self, count: usize) {
        self.state.lock().unwrap().withheld_delivery_answers = count;
    }

    pub fn wait_for_withheld_delivery_answers(&self, expected: usize) {
        let (count, changed) = &*self.delivery_answers_withheld;
        let count = count.lock().unwrap();
        let (count, timeout) = changed
            .wait_timeout_while(count, Duration::from_secs(8), |count| *count < expected)
            .unwrap();
        assert!(
            *count >= expected,
            "Message did not reach the withheld admission answer"
        );
        assert!(!timeout.timed_out() || *count >= expected);
    }

    pub fn wait_for_delivery_changes(&self, expected: usize) {
        let (count, changed) = &*self.delivery_changes_sent;
        let count = count.lock().unwrap();
        let (count, timeout) = changed
            .wait_timeout_while(count, Duration::from_secs(5), |count| *count < expected)
            .unwrap();
        assert!(*count >= expected, "Flow did not write the delivery change");
        assert!(!timeout.timed_out() || *count >= expected);
    }

    pub fn interrupt_delivery_observer(&self) {
        let observer = self
            .state
            .lock()
            .unwrap()
            .delivery_observer
            .take()
            .expect("Message opened its delivery observation");
        observer.shutdown(std::net::Shutdown::Both).unwrap();
    }

    pub fn make_delivery_observations_unavailable(&self, attempts: usize) {
        self.state.lock().unwrap().unavailable_delivery_observations = attempts;
    }

    pub fn archive_and_stop_before_message_retirement(&self) {
        let (snapshot, observer) = {
            let mut state = self.state.lock().unwrap();
            let admitted = state
                .delivered
                .last()
                .expect("a Message delivery exists before archive");
            let snapshot = VisibleDeliverySnapshot {
                request_id: "fake-request".into(),
                delivery_cursor: 3,
                visible_sender: VisibleSender::Sender(admitted.lock.sender.clone()),
                address: admitted.lock.address.clone(),
                visible_delivery_state: VisibleDeliveryState::Archived,
            };
            state.delivery_cursor = snapshot.delivery_cursor;
            state.delivery_snapshots.push(snapshot.clone());
            (snapshot, state.delivery_observer.take())
        };
        self.stop_message_and_signal();
        if let Some(mut observer) = observer {
            let _ = observer.write_frame(&Response::SendChanged(snapshot));
        }
    }

    pub fn wait_for_delivery_observations(&self, expected: usize) -> Vec<Option<i64>> {
        let (count, changed) = &*self.delivery_observed;
        let count = count.lock().unwrap();
        let (count, timeout) = changed
            .wait_timeout_while(count, Duration::from_secs(5), |count| *count < expected)
            .unwrap();
        assert!(
            *count >= expected,
            "Message did not reopen its delivery stream"
        );
        assert!(!timeout.timed_out() || *count >= expected);
        self.state
            .lock()
            .unwrap()
            .delivery_observation_cursors
            .clone()
    }

    fn sender(&self) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            model: Model::Sol,
        }
    }

    fn stop_message_and_signal(&self) {
        let pid = self
            .state
            .lock()
            .unwrap()
            .bound
            .last()
            .expect("Message bound before a delivery fault")
            .1
            .pid;
        // SAFETY: this is the child Message process which bound to this test Flow.
        assert_eq!(unsafe { libc::kill(pid as i32, libc::SIGSTOP) }, 0);
        let (reached, changed) = &*self.fault_reached;
        *reached.lock().unwrap() = true;
        changed.notify_all();
    }

    fn answer(&self, mut stream: UnixStream) {
        let Ok(query) = stream.read_frame::<Query>() else {
            return;
        };
        let observing_sender = matches!(&query, Query::ObserveSender(_));
        let observing_deliveries = matches!(&query, Query::ObserveDeliveries(_));
        let mut delivery_change = None;
        let mut register_delivery_observer = false;
        let mut answer_before_ledger = false;
        let mut ledger_before_acknowledge = false;
        let mut hold_before_cursor = false;
        let mut held_reconnect = false;
        let mut withhold_delivery_answer = false;
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
                let key = delivery.deliver_key.clone();
                let snapshot = VisibleDeliverySnapshot {
                    request_id: "fake-request".into(),
                    delivery_cursor: 2,
                    visible_sender: VisibleSender::Sender(delivery.lock.sender.clone()),
                    address: delivery.lock.address.clone(),
                    visible_delivery_state: VisibleDeliveryState::Delivered,
                };
                let mut state = self.state.lock().unwrap();
                state.delivery_attempts.push(key.clone());
                if state.withheld_delivery_answers > 0 {
                    state.withheld_delivery_answers -= 1;
                    withhold_delivery_answer = true;
                }
                let acceptance = VisibleSendAcceptance {
                    request_id: "fake-request".into(),
                    delivery_cursor: 1,
                    visible_delivery_state: VisibleDeliveryState::Queued,
                };
                if let Some(admitted) = state
                    .delivered
                    .iter()
                    .find(|admitted| admitted.deliver_key == key)
                {
                    if admitted == &delivery {
                        Response::SendAccepted(acceptance)
                    } else {
                        Response::DeliverKeyReused
                    }
                } else {
                    state.delivered.push(delivery);
                    state.delivery_cursor = snapshot.delivery_cursor;
                    state.delivery_snapshots.push(snapshot.clone());
                    delivery_change = Some(snapshot);
                    match state.fault {
                        Some(FaultBoundary::AnswerBeforeLedger) => {
                            state.fault = None;
                            answer_before_ledger = true;
                        }
                        Some(FaultBoundary::HeldBeforeCursor) => {
                            hold_before_cursor = true;
                        }
                        _ => {}
                    }
                    Response::SendAccepted(acceptance)
                }
            }
            Query::Acknowledge(deliver_keys) => {
                let mut state = self.state.lock().unwrap();
                if state.fault == Some(FaultBoundary::LedgerBeforeAcknowledge) {
                    state.fault = None;
                    ledger_before_acknowledge = true;
                } else {
                    state.acknowledged.extend(deliver_keys);
                    let (count, changed) = &*self.acknowledgements_changed;
                    *count.lock().unwrap() += 1;
                    changed.notify_all();
                }
                Response::Acknowledged
            }
            Query::ObserveSender(_) => Response::SendsObserved(VisibleSendObservation {
                store_identity: "fake-store".into(),
                delivery_cursor: 1,
                visible_delivery_snapshot_vector: vec![],
            }),
            Query::ObserveDeliveries(cursor) => {
                let mut state = self.state.lock().unwrap();
                state.delivery_observation_cursors.push(cursor);
                if state.fault == Some(FaultBoundary::HeldBeforeCursor)
                    && cursor == Some(0)
                    && !state.delivered.is_empty()
                {
                    state.fault = None;
                    held_reconnect = true;
                }
                let (count, changed) = &*self.delivery_observed;
                *count.lock().unwrap() += 1;
                changed.notify_all();
                if state.unavailable_delivery_observations > 0 {
                    state.unavailable_delivery_observations -= 1;
                    Response::Refused(Refusal::NotConfigured)
                } else {
                    register_delivery_observer = true;
                    Response::SendsObserved(VisibleSendObservation {
                        store_identity: "fake-store".into(),
                        delivery_cursor: state.delivery_cursor,
                        visible_delivery_snapshot_vector: state
                            .delivery_snapshots
                            .iter()
                            .filter(|snapshot| {
                                cursor.is_none_or(|cursor| snapshot.delivery_cursor > cursor)
                            })
                            .cloned()
                            .collect(),
                    })
                }
            }
            Query::Release(_) => Response::Released,
            _ => Response::Refused(Refusal::NotConfigured),
        };
        if hold_before_cursor && let Some(snapshot) = delivery_change.take() {
            if let Some(observer) = self.state.lock().unwrap().delivery_observer.take() {
                let mut observer = observer;
                let _ = observer.write_frame(&Response::SendChanged(snapshot));
                let _ = observer.shutdown(Shutdown::Both);
            }
            let (reached, changed) = &*self.fault_reached;
            let reached = reached.lock().unwrap();
            let _ = changed
                .wait_timeout_while(reached, Duration::from_secs(5), |reached| !*reached)
                .unwrap();
        }
        if answer_before_ledger || ledger_before_acknowledge || held_reconnect {
            self.stop_message_and_signal();
        }
        if withhold_delivery_answer {
            let (count, changed) = &*self.delivery_answers_withheld;
            *count.lock().unwrap() += 1;
            changed.notify_all();
            let mut byte = [0; 1];
            let _ = stream.read(&mut byte);
            return;
        }
        let _ = stream.write_frame(&response);
        if register_delivery_observer {
            self.state.lock().unwrap().delivery_observer = stream.try_clone().ok();
        }
        if let Some(snapshot) = delivery_change
            && let Some(observer) = self.state.lock().unwrap().delivery_observer.as_mut()
        {
            let _ = observer.write_frame(&Response::SendChanged(snapshot));
            let (count, changed) = &*self.delivery_changes_sent;
            *count.lock().unwrap() += 1;
            changed.notify_all();
        }
        if observing_deliveries || (observing_sender && self.quiet_observer) {
            let mut byte = [0; 1];
            let _ = stream.read(&mut byte);
            if observing_sender {
                let (count, changed) = &*self.observer_closed;
                *count.lock().unwrap() += 1;
                changed.notify_all();
            }
        }
    }
}
