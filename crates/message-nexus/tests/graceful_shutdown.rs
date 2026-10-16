//! Exercise actual process termination, not an in-process store drop.
use message_nexus::frame::FramedStream;
use signal_flow::{Address, Aspect, Layer, Recipient, Request};
use signal_message::{Query, Send_Data};
use std::{
    os::unix::net::{UnixListener, UnixStream},
    process::{Child, Command},
    time::{Duration, Instant},
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sigterm_closes_the_store_even_with_an_idle_connected_client() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let socket = runtime.path().join("message/message.sock");
    let store = home.path().join(".local/state/message/message.sema");
    // The second start exercises the same durable store, not a fresh fixture.
    std::fs::create_dir_all(runtime.path().join("flow")).unwrap();
    let flow = UnixListener::bind(runtime.path().join("flow/flow.sock")).unwrap();
    flow.set_nonblocking(true).unwrap();
    for iteration in 0..2 {
        let mut child = ChildGuard(
            Command::new(env!("CARGO_BIN_EXE_message-nexus"))
                .env_clear()
                .env("HOME", home.path())
                .env("XDG_RUNTIME_DIR", runtime.path())
                .spawn()
                .unwrap(),
        );
        // Opening Message's store includes a Flow Bind exchange before either
        // Message listener is created. Complete it on every start, including
        // the reopen, before waiting for socket readiness.
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut startup = loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "Nexus exited before Bind"
            );
            match flow.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("Flow startup accept: {error}"),
            }
            assert!(Instant::now() < deadline, "Message did not bind to Flow");
            std::thread::sleep(Duration::from_millis(10));
        };
        startup
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        startup
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let signal_flow::Query::Bind(binding) = startup.read_frame::<signal_flow::Query>().unwrap()
        else {
            panic!("Message startup must send Bind");
        };
        assert_eq!(
            binding.address,
            Address {
                aspect: Aspect::Field,
                topic: "message".into(),
                layer: Layer::Primary,
            }
        );
        startup
            .write_frame(&signal_flow::Response::Bound(signal_flow::FlowId::from(
                "message-shutdown-fixture",
            )))
            .unwrap();
        drop(startup);
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut connection = loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "Nexus exited before readiness"
            );
            if let Ok(stream) = UnixStream::connect(&socket) {
                break stream;
            }
            assert!(Instant::now() < deadline, "Nexus did not listen");
            std::thread::sleep(Duration::from_millis(10));
        };
        // First stop: a real Flow request is waiting for a reply. Second stop:
        // an idle accepted client supplies no frame at all.
        let _flow_connection = if iteration == 0 {
            connection
                .write_frame(&Query::Send(Send_Data {
                    recipient: Recipient::Address(Address {
                        aspect: Aspect::Mind,
                        topic: "shutdown-test".into(),
                        layer: Layer::Secondary,
                    }),
                    request: Request::Question("fixture".into()),
                }))
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut stream = loop {
                match flow.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) => panic!("Flow fixture accept: {error}"),
                }
                assert!(Instant::now() < deadline, "Message did not contact Flow");
                std::thread::sleep(Duration::from_millis(10));
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert!(matches!(
                stream.read_frame::<signal_flow::Query>().unwrap(),
                signal_flow::Query::Identify(_)
            ));
            Some(stream)
        } else {
            None
        };
        // This is the owned child, still unreaped: its PID cannot have been reused.
        assert_eq!(
            unsafe { libc::kill(child.0.id() as libc::pid_t, libc::SIGTERM) },
            0
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "SIGTERM did not finish shutdown");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success(), "SIGTERM must return normally: {status}");
        drop(connection);
        // This reader cannot repair: success proves the writer closed cleanly.
        let database = redb::ReadOnlyDatabase::open(&store).expect("clean read-only reopen");
        drop(database);
    }
}
