//! A real message-nexus process against a scripted Flow, in a private
//! home and runtime directory.

pub mod fake_flow;

use message_nexus::frame::FramedStream;
use std::{
    os::unix::net::UnixStream,
    path::PathBuf,
    process::{Child, Command},
    time::{Duration, Instant},
};

pub struct NexusProcess {
    child: Child,
    home: Option<tempfile::TempDir>,
    runtime: Option<tempfile::TempDir>,
    ordinary_socket: PathBuf,
    meta_socket: PathBuf,
}

impl NexusProcess {
    /// Starts the Nexus with no arguments and waits until its ordinary
    /// socket answers a connection.
    pub fn start(home: tempfile::TempDir, runtime: tempfile::TempDir) -> Self {
        let ordinary = runtime.path().join("message/message.sock");
        let meta = runtime.path().join("message/message-owner.sock");
        Self::start_listening_at(home, runtime, ordinary, meta, &[])
    }

    /// Starts the Nexus with no arguments and the given extra environment,
    /// and waits until it answers on the two sockets named here.
    pub fn start_listening_at(
        home: tempfile::TempDir,
        runtime: tempfile::TempDir,
        ordinary_socket: PathBuf,
        meta_socket: PathBuf,
        environment: &[(&str, &str)],
    ) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_message-nexus"))
            .env_clear()
            .env("HOME", home.path())
            .env("XDG_RUNTIME_DIR", runtime.path())
            .envs(environment.iter().copied())
            .spawn()
            .expect("message-nexus starts");
        let process = Self {
            child,
            ordinary_socket,
            meta_socket,
            home: Some(home),
            runtime: Some(runtime),
        };
        process.wait_for_socket();
        process
    }

    pub fn ordinary_socket(&self) -> PathBuf {
        self.ordinary_socket.clone()
    }

    pub fn meta_socket(&self) -> PathBuf {
        self.meta_socket.clone()
    }

    fn wait_for_socket(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while UnixStream::connect(self.meta_socket()).is_err()
            || UnixStream::connect(self.ordinary_socket()).is_err()
        {
            assert!(Instant::now() < deadline, "message-nexus never listened");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn ask(&self, query: &signal_message::Query) -> signal_message::Response {
        let mut stream = UnixStream::connect(self.ordinary_socket()).unwrap();
        stream.write_frame(query).unwrap();
        stream.read_frame().unwrap()
    }

    pub fn ask_meta(&self, query: &meta_signal_message::Query) -> meta_signal_message::Response {
        let mut stream = UnixStream::connect(self.meta_socket()).unwrap();
        stream.write_frame(query).unwrap();
        stream.read_frame().unwrap()
    }

    /// Opens Observe on a message: the opening frame, then later ones.
    pub fn observe(&self, message_id: &str) -> UnixStream {
        let mut stream = UnixStream::connect(self.ordinary_socket()).unwrap();
        stream
            .write_frame(&signal_message::Query::Observe(message_id.into()))
            .unwrap();
        stream
    }

    /// Stops the Nexus by the PID this test holds, and hands back its
    /// directories so a second Nexus can open the same store.
    pub fn stop(mut self) -> (tempfile::TempDir, tempfile::TempDir) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(self.ordinary_socket());
        let _ = std::fs::remove_file(self.meta_socket());
        (
            self.home.take().expect("home held until stop"),
            self.runtime.take().expect("runtime held until stop"),
        )
    }
}

impl Drop for NexusProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
