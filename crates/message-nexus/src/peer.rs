//! Reads Flow's process identity from the connection peer.

use signal_flow::Process;
use std::{fs, io, os::unix::net::UnixStream};

pub trait ReadsProcess {
    fn for_pid(process_id: i64) -> io::Result<Process>;
    fn this_process() -> io::Result<Process>;
}

impl ReadsProcess for Process {
    fn for_pid(process_id: i64) -> io::Result<Process> {
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat"))?;
        let (_, fields) = stat
            .rsplit_once(") ")
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed process stat"))?;
        let started = fields
            .split_whitespace()
            .nth(19)
            .and_then(|field| field.parse::<i64>().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing process start"))?;
        Ok(Process {
            pid: process_id,
            started,
        })
    }

    fn this_process() -> io::Result<Process> {
        Self::for_pid(i64::from(std::process::id()))
    }
}

pub trait IdentifiesPeer {
    fn peer_process(&self) -> Option<Process>;
}

impl IdentifiesPeer for UnixStream {
    fn peer_process(&self) -> Option<Process> {
        let credentials = rustix::net::sockopt::socket_peercred(self).ok()?;
        let process_id = i64::from(credentials.pid.as_raw_nonzero().get());
        Process::for_pid(process_id).ok()
    }
}
