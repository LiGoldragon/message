//! The ordinary and owner-only Message sockets.

use crate::{
    frame::{FrameError, FramedStream},
    nexus::{HoldsNexusState, MessageNexus},
    peer::IdentifiesPeer,
    service::{ConfiguresMessage, ConfigureFailure, ServesMessages},
};
use meta_signal_message::{Query as MetaQuery, Response as MetaResponse};
use signal_message::Query;
use std::{
    fs, io,
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
        let meta_nexus = Arc::clone(&self);
        let meta_thread = std::thread::spawn(move || Socket::Meta.accept(meta, meta_nexus));
        Socket::Ordinary.accept(ordinary, self);
        let _ = meta_thread.join();
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
    fn accept(self, listener: UnixListener, nexus: Arc<MessageNexus>);
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

    fn accept(self, listener: UnixListener, nexus: Arc<MessageNexus>) {
        for connection in listener.incoming() {
            let Ok(stream) = connection else { continue };
            let nexus = Arc::clone(&nexus);
            std::thread::spawn(move || {
                let result = match self {
                    Socket::Ordinary => nexus.answer_ordinary(stream),
                    Socket::Meta => nexus.answer_meta(stream),
                };
                if let Err(error) = result
                    && !matches!(error, FrameError::Closed)
                {
                    eprintln!("message-nexus: {self:?} connection: {error}");
                }
            });
        }
    }
}

trait AnswersConnection {
    fn answer_ordinary(&self, stream: UnixStream) -> Result<(), FrameError>;
    fn answer_meta(&self, stream: UnixStream) -> Result<(), FrameError>;
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
        stream.write_frame(&self.serve_query(process, query))
    }

    fn answer_meta(&self, mut stream: UnixStream) -> Result<(), FrameError> {
        let query = stream.read_frame::<MetaQuery>()?;
        let configuration = match query {
            MetaQuery::Configure(configuration) => configuration,
        };
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
