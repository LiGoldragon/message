//! Provenance minting at the messenger ingress.
//!
//! Every stored message carries its origin (psyche-ruled: "the incoming
//! messages get the origin, like who sent this"). Provenance is never
//! accepted from a caller payload — it is minted here from the
//! operating-system trust boundary: the accepted connection's kernel-vouched
//! `SO_PEERCRED` credentials classify the origin, and the sender's agent
//! identity is resolved by walking the peer's `/proc` ancestry against the
//! process pins the registry already holds (the same pid + start-time pin
//! discipline orchestrate's reachability discovery uses; the start time
//! defeats pid recycling).

use std::time::{SystemTime, UNIX_EPOCH};

use triad_runtime::{ConnectionContext, PeerIdentity, UnixCredentials};

use crate::runtime_model::SenderName;
use crate::tables::{MessengerTables, PinnedAgentIdentity};
use signal_message::schema::lib::{
    z2VLai, z2VPn2, z2VPq6, z2VTJ1, z2VTaw, z2VY3v, z2VY18, z2Vdkj, z2Vf2p,
};

/// Classifies an accepted connection into the daemon-local stored origin and
/// stamps ingress time. The owner identity comes from daemon configuration,
/// never from a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginPolicy {
    owner_user_id: u32,
    owner_name: String,
}

impl OriginPolicy {
    /// The origin policy for a local Unix user owner identified by uid.
    pub fn for_owner_user_id(owner_user_id: u32, owner_name: impl Into<String>) -> Self {
        Self {
            owner_user_id,
            owner_name: owner_name.into(),
        }
    }

    /// The configured owner name label stamped onto every stored origin.
    pub fn owner_name(&self) -> &str {
        self.owner_name.as_str()
    }

    /// Whether a kernel-vouched peer uid is the configured owner.
    pub fn is_owner_uid(&self, user_id: u32) -> bool {
        user_id == self.owner_user_id
    }

    /// Classify the accepted connection's peer identity into the typed
    /// origin (the same vocabulary the published contract speaks). A peer
    /// uid matching the owner's Unix uid is this daemon's local harness
    /// component instance; any other local user is `NonOwnerUser(uid)`; a
    /// TCP peer carries no Unix credentials and classifies as a network
    /// peer by remote address.
    pub fn origin_for_connection(&self, connection: &ConnectionContext) -> z2VTJ1 {
        match connection.peer() {
            PeerIdentity::Unix(credentials) if credentials.user_id() == self.owner_user_id => {
                z2VTJ1::z2VS4W(z2VPq6 {
                    field_0: z2Vdkj::z2VPrB,
                    field_1: z2VTaw::new(self.owner_name.clone()),
                })
            }
            PeerIdentity::Unix(credentials) => z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(
                u64::from(credentials.user_id()),
            ))),
            PeerIdentity::Tcp(address) => {
                z2VTJ1::z2VWSr(z2VY3v::z2VVrk(z2VLai::new(address.to_string())))
            }
        }
    }

    /// Daemon-minted ingress timestamp for a stored message.
    pub fn ingress_stamp(&self) -> z2VY18 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        z2VY18::new(z2Vf2p::new(nanos))
    }
}

/// Resolves the sending agent's identity for one accepted connection.
///
/// The peer's kernel-vouched pid anchors a `/proc` ancestry walk; any
/// ancestor matching a registry process pin (pid AND start time) names the
/// sending agent by its orchestrator-minted identifier. No match falls back
/// to the origin label: the configured owner name for owner connections, a
/// uid label otherwise.
#[derive(Debug)]
pub struct SenderResolver<'runtime> {
    tables: &'runtime MessengerTables,
    origin_policy: &'runtime OriginPolicy,
}

impl<'runtime> SenderResolver<'runtime> {
    pub fn new(tables: &'runtime MessengerTables, origin_policy: &'runtime OriginPolicy) -> Self {
        Self {
            tables,
            origin_policy,
        }
    }

    /// Resolve the sender name for the accepted connection.
    pub fn resolve(&self, connection: &ConnectionContext) -> SenderName {
        match connection.peer() {
            PeerIdentity::Unix(credentials) => self.resolve_unix(credentials),
            PeerIdentity::Tcp(address) => SenderName::new(address.to_string()),
        }
    }

    fn resolve_unix(&self, credentials: &UnixCredentials) -> SenderName {
        if let Some(identifier) = self.pinned_ancestor_identifier(credentials.process_id()) {
            return SenderName::new(identifier);
        }
        if self.origin_policy.is_owner_uid(credentials.user_id()) {
            return SenderName::new(self.origin_policy.owner_name().to_owned());
        }
        SenderName::new(format!("uid-{}", credentials.user_id()))
    }

    /// Walk the peer's `/proc` ancestry and return the first ancestor whose
    /// pid AND start time match a registry pin. Registry read failures and
    /// `/proc` races resolve to no match — sender resolution is best-effort
    /// provenance enrichment, never a submission gate.
    fn pinned_ancestor_identifier(&self, peer_pid: i32) -> Option<String> {
        let pins = self.tables.pinned_agent_identities().ok()?;
        if pins.is_empty() {
            return None;
        }
        ProcessAncestry::from_pid(peer_pid).matching_pin(&pins)
    }
}

/// A `/proc` ancestry walk anchored at one pid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessAncestry {
    pid: i32,
}

impl ProcessAncestry {
    const ANCESTRY_CEILING: usize = 64;

    fn from_pid(pid: i32) -> Self {
        Self { pid }
    }

    /// The first ancestor (including the anchor) whose pid and start time
    /// match one of the supplied registry pins.
    fn matching_pin(self, pins: &[PinnedAgentIdentity]) -> Option<String> {
        let mut current = self.pid;
        for _ in 0..Self::ANCESTRY_CEILING {
            if current <= 1 {
                return None;
            }
            let stat = ProcessStat::read(current)?;
            if let Some(pin) = pins
                .iter()
                .find(|pin| pin.matches(current, stat.start_time))
            {
                return Some(pin.identifier().to_owned());
            }
            current = stat.parent_pid;
        }
        None
    }
}

/// The two `/proc/<pid>/stat` fields the ancestry walk needs: the parent pid
/// (field 4) and the process start time (field 22, clock ticks since boot —
/// the recycled-pid disambiguator).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessStat {
    parent_pid: i32,
    start_time: u64,
}

impl ProcessStat {
    fn read(pid: i32) -> Option<Self> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        Self::parse(&stat)
    }

    /// Parse the stat line. The comm field (2) is parenthesized and may
    /// contain spaces or parentheses, so fields are taken after the LAST
    /// closing parenthesis: state is post-comm field 0, ppid field 1, and
    /// start time overall field 22 = post-comm field 19.
    fn parse(stat: &str) -> Option<Self> {
        let after_comm = stat.rsplit_once(')')?.1;
        let mut fields = after_comm.split_whitespace();
        let parent_pid = fields.nth(1)?.parse().ok()?;
        let start_time = fields.nth(17)?.parse().ok()?;
        Some(Self {
            parent_pid,
            start_time,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ProcessStat;

    #[test]
    fn stat_parse_survives_parenthesized_comm_with_spaces() {
        let stat = "4242 (tmux: server) S 1 4242 4242 0 -1 4194304 1 0 0 0 0 0 0 0 20 0 1 0 \
                    777888 1000000 100 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0";
        let parsed = ProcessStat::parse(stat).expect("parse stat");
        assert_eq!(parsed.parent_pid, 1);
        assert_eq!(parsed.start_time, 777888);
    }

    #[test]
    fn stat_parse_rejects_a_line_with_no_comm() {
        assert_eq!(ProcessStat::parse("not a stat line"), None);
    }
}
