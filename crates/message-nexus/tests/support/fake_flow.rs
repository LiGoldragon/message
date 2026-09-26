//! A scripted Flow Nexus: its meta socket answers ResolvePeer, Vet and
//! Deliver as the test sets it up, its ordinary socket streams
//! Observe.Agent, and it keeps every Deliver it was handed. It is the
//! oracle the Message Nexus is judged against: what reached "the pane" is
//! what this Flow recorded as typed.

use message_nexus::frame::FramedStream;
use meta_signal_flow::{
    BodyRefusal, Content, Delivery, DeliveryGrade, DeliveryRejection, DeliveryRequest,
    InterruptWitness, Message, Query as MetaQuery, Response as MetaResponse,
};
use signal_flow::{
    AgentObservation, AgentState, Caller, CallerResolutionRejection, FlowAspect, ModelName,
    ObserveSelection, PowerLevel, Query, Response,
};
use std::{
    collections::HashMap,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

/// How a recipient's pane stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Idle,
    Working,
    ComposerOccupied,
    /// Deliver types and cannot say whether the text landed.
    Uncertain,
    /// A permission dialog is up.
    Blocked,
}

#[derive(Default)]
struct State {
    peer: Option<Caller>,
    panes: HashMap<String, Pane>,
    typed: Vec<DeliveryRequest>,
    settled: HashMap<String, Delivery>,
    observers: HashMap<String, Vec<UnixStream>>,
    observers_closed: usize,
}

#[derive(Clone)]
pub struct FakeFlow {
    state: Arc<(Mutex<State>, Condvar)>,
    pub runtime_directory: PathBuf,
}

impl FakeFlow {
    /// Binds `flow/flow.sock` and `flow/flow-meta.sock` under the runtime
    /// directory, as Flow's defaults do.
    pub fn start(runtime_directory: &Path) -> Self {
        let directory = runtime_directory.join("flow");
        std::fs::create_dir_all(&directory).unwrap();
        let flow = Self {
            state: Arc::new((Mutex::new(State::default()), Condvar::new())),
            runtime_directory: runtime_directory.to_path_buf(),
        };
        let meta = UnixListener::bind(directory.join("flow-meta.sock")).unwrap();
        let ordinary = UnixListener::bind(directory.join("flow.sock")).unwrap();
        let meta_flow = flow.clone();
        std::thread::spawn(move || {
            for stream in meta.incoming().flatten() {
                let flow = meta_flow.clone();
                std::thread::spawn(move || flow.answer_meta(stream));
            }
        });
        let ordinary_flow = flow.clone();
        std::thread::spawn(move || {
            for stream in ordinary.incoming().flatten() {
                let flow = ordinary_flow.clone();
                std::thread::spawn(move || flow.answer_ordinary(stream));
            }
        });
        flow
    }

    pub fn caller(flow_id: &str, flow_aspect: FlowAspect) -> Caller {
        Caller {
            flow_id: flow_id.into(),
            flow_aspect,
            power_level: PowerLevel::Medium,
            model_name: ModelName::from("fixture"),
        }
    }

    /// Whom every peer process resolves to; None is the owner.
    pub fn set_peer(&self, peer: Option<Caller>) {
        self.state.0.lock().unwrap().peer = peer;
    }

    /// Sets a recipient's pane and tells its observers, as Herdr would.
    pub fn set_pane(&self, flow_id: &str, pane: Pane) {
        let mut state = self.state.0.lock().unwrap();
        state.panes.insert(flow_id.into(), pane);
        let agent_state = Self::agent_state(pane);
        if let Some(observers) = state.observers.get_mut(flow_id) {
            observers.retain_mut(|stream| {
                stream
                    .write_frame(&Response::AgentObserved(AgentObservation {
                        flow_id: flow_id.into(),
                        agent_state: agent_state.clone(),
                    }))
                    .is_ok()
            });
        }
        self.state.1.notify_all();
    }

    pub fn typed(&self) -> Vec<DeliveryRequest> {
        self.state.0.lock().unwrap().typed.clone()
    }

    /// Waits until `count` Delivers have typed.
    pub fn wait_typed(&self, count: usize) -> Vec<DeliveryRequest> {
        let (lock, changed) = &*self.state;
        let state = changed
            .wait_while(lock.lock().unwrap(), |state| state.typed.len() < count)
            .unwrap();
        state.typed.clone()
    }

    /// Waits until `count` Observe.Agent subscriptions have closed.
    pub fn wait_observers_closed(&self, count: usize) {
        let (lock, changed) = &*self.state;
        let _state = changed
            .wait_while(lock.lock().unwrap(), |state| state.observers_closed < count)
            .unwrap();
    }

    /// Waits until a recipient has an open Observe.Agent subscription.
    pub fn wait_observed(&self, flow_id: &str) {
        let (lock, changed) = &*self.state;
        let _state = changed
            .wait_while(lock.lock().unwrap(), |state| {
                state.observers.get(flow_id).is_none_or(Vec::is_empty)
            })
            .unwrap();
    }

    fn agent_state(pane: Pane) -> AgentState {
        match pane {
            Pane::Working => AgentState::Working,
            Pane::Blocked => AgentState::Blocked,
            Pane::Idle | Pane::ComposerOccupied | Pane::Uncertain => AgentState::Idle,
        }
    }

    fn refusal(request: &DeliveryRequest) -> Option<BodyRefusal> {
        let (Message::HardAbrupt(letter) | Message::MiddleAbrupt(letter) | Message::Soft(letter)) =
            &request.message;
        match &letter.content {
            Content::Text(text) if text.is_empty() => Some(BodyRefusal::EmptyBody),
            Content::Text(text) if text.starts_with('/') => {
                Some(BodyRefusal::HarnessCommand(text.clone()))
            }
            _ => None,
        }
    }

    fn vet(&self, request: &DeliveryRequest) -> Result<(), DeliveryRejection> {
        let state = self.state.0.lock().unwrap();
        if !state.panes.contains_key(&request.flow_id) {
            return Err(DeliveryRejection::UnknownFlow);
        }
        match Self::refusal(request) {
            Some(refusal) => Err(DeliveryRejection::BodyRefused(refusal)),
            None => Ok(()),
        }
    }

    fn deliver(&self, request: DeliveryRequest) -> MetaResponse {
        if let Err(rejection) = self.vet(&request) {
            return MetaResponse::DeliveryRejected(rejection);
        }
        let mut state = self.state.0.lock().unwrap();
        if let Some(delivery) = state.settled.get(&request.delivery_id) {
            return MetaResponse::Delivered(delivery.clone());
        }
        let pane = state.panes[&request.flow_id];
        let soft = matches!(request.message, Message::Soft(_));
        let delivery_grade = match pane {
            Pane::Working if soft => {
                return MetaResponse::DeliveryRejected(DeliveryRejection::RecipientWorking);
            }
            Pane::ComposerOccupied => {
                return MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied);
            }
            Pane::Blocked => {
                return MetaResponse::DeliveryRejected(DeliveryRejection::RecipientBlocked);
            }
            Pane::Working => DeliveryGrade::Transported,
            Pane::Idle => DeliveryGrade::Presented,
            Pane::Uncertain => DeliveryGrade::Uncertain,
        };
        let delivery = Delivery {
            delivery_id: request.delivery_id.clone(),
            flow_id: request.flow_id.clone(),
            interrupt_witness: InterruptWitness::NotRequested,
            delivery_grade,
        };
        state
            .settled
            .insert(request.delivery_id.clone(), delivery.clone());
        state.typed.push(request);
        self.state.1.notify_all();
        MetaResponse::Delivered(delivery)
    }

    fn answer_meta(&self, mut stream: UnixStream) {
        let Ok(query) = stream.read_frame::<MetaQuery>() else {
            return;
        };
        let response = match query {
            MetaQuery::ResolvePeer(_) => match self.state.0.lock().unwrap().peer.clone() {
                Some(caller) => MetaResponse::PeerResolved(caller),
                None => {
                    MetaResponse::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown)
                }
            },
            MetaQuery::Vet(request) => match self.vet(&request) {
                Ok(()) => MetaResponse::Vetted(request.flow_id),
                Err(rejection) => MetaResponse::DeliveryRejected(rejection),
            },
            MetaQuery::Deliver(request) => self.deliver(request),
            _ => return,
        };
        let _ = stream.write_frame(&response);
    }

    fn answer_ordinary(&self, mut stream: UnixStream) {
        let Ok(Query::Observe(ObserveSelection::Agent(flow_id))) = stream.read_frame::<Query>()
        else {
            return;
        };
        {
            let mut state = self.state.0.lock().unwrap();
            let pane = state.panes.get(&flow_id).copied().unwrap_or(Pane::Idle);
            let opening = Response::AgentObserved(AgentObservation {
                flow_id: flow_id.clone(),
                agent_state: Self::agent_state(pane),
            });
            if stream.write_frame(&opening).is_err() {
                return;
            }
            let Ok(kept) = stream.try_clone() else { return };
            state.observers.entry(flow_id).or_default().push(kept);
            self.state.1.notify_all();
        }
        // The subscriber closes its end when it stops watching.
        let mut byte = [0; 1];
        let _ = std::io::Read::read(&mut stream, &mut byte);
        let mut state = self.state.0.lock().unwrap();
        state.observers_closed += 1;
        self.state.1.notify_all();
    }
}
