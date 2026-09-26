//! The process at the other end of a connection, as the kernel names it.
//!
//! Message never takes a sender from a payload. It reads its peer's
//! credentials (`SO_PEERCRED`) and the process's start time, and asks Flow
//! (`ResolvePeer`) which flow that exact process runs in. The start time
//! makes the identity name one process, never a reused process ID.

use meta_signal_flow::ProcessIdentity;
use std::{fs, os::unix::net::UnixStream};

/// Names the peer process of a connection.
pub trait IdentifiesPeer {
    fn peer_identity(&self) -> Option<ProcessIdentity>;
}

impl IdentifiesPeer for UnixStream {
    fn peer_identity(&self) -> Option<ProcessIdentity> {
        let credentials = rustix::net::sockopt::socket_peercred(self).ok()?;
        let process_id = i64::from(credentials.pid.as_raw_nonzero().get());
        let process_user_id = i64::from(credentials.uid.as_raw());
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
        // Field 22 of /proc/<pid>/stat, counted after the command name.
        let (_, fields) = stat.rsplit_once(") ")?;
        let process_start_token = fields.split_whitespace().nth(19)?.to_owned();
        Some(ProcessIdentity {
            process_id,
            process_user_id,
            process_start_token,
        })
    }
}
