//! A scripted Flow that records the typed request received by Deliver.

use message_nexus::frame::FramedStream;
use signal_flow::{
    Address, Aspect, Bind_Data, Deliver_Data, FlowId, Layer, Lock, Lock_Data, Process, Query,
    Recipient, Refusal, Response,
};
use std::{
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    bound: Vec<(Address, Process)>,
    delivered: Vec<Deliver_Data>,
}

#[derive(Clone)]
pub struct FakeFlow {
    state: Arc<Mutex<State>>,
    socket_path: PathBuf,
}

impl FakeFlow {
    pub fn start(runtime_directory: &Path) -> Self {
        let directory = runtime_directory.join("flow");
        std::fs::create_dir_all(&directory).unwrap();
        let socket_path = directory.join("flow.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let flow = Self {
            state: Arc::new(Mutex::new(State::default())),
            socket_path,
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

    pub fn socket_path(&self) -> PathBuf {
        self.socket_path.clone()
    }

    pub fn typed(&self) -> Vec<Deliver_Data> {
        self.state.lock().unwrap().delivered.clone()
    }

    pub fn bound(&self) -> Vec<(Address, Process)> {
        self.state.lock().unwrap().bound.clone()
    }

    fn sender(&self) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: Layer::Secondary,
        }
    }

    fn resolve(&self, recipient: Recipient, sender: &Address) -> Address {
        match recipient {
            Recipient::Address(address) => address,
            Recipient::Up => sender.clone(),
        }
    }

    fn answer(&self, mut stream: UnixStream) {
        let Ok(query) = stream.read_frame::<Query>() else {
            return;
        };
        let response = match query {
            Query::Bind(Bind_Data { address, process }) => {
                self.state.lock().unwrap().bound.push((address, process));
                Response::Bound(FlowId::from("message-flow"))
            }
            Query::Identify(_) => Response::Identified(self.sender()),
            Query::Lock(Lock_Data { sender, recipient }) => {
                let address = self.resolve(recipient, &sender);
                Response::Locked(Lock {
                    sender,
                    address,
                    until: i64::MAX,
                })
            }
            Query::Deliver(delivery) => {
                self.state.lock().unwrap().delivered.push(delivery);
                Response::Delivered
            }
            Query::Release(_) => Response::Released,
            _ => Response::Refused(Refusal::NotConfigured),
        };
        let _ = stream.write_frame(&response);
    }
}
