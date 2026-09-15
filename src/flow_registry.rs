//! Flow-name resolution for the flow-delivery leg.
//!
//! SEAM: the Flow component (item 31) will own flow-name resolution; interim
//! reads `.flow-id` markers. A flow's lane carries a hidden marker file named
//! `.<alias>.flow-id` beside its lane directory, and the marker's alias IS the
//! flow identifier. Existence of that marker is, for this prototype, the whole
//! of "this flow exists"; when the Flow component lands, this type is replaced
//! by a query to it and nothing else in the messenger moves.

use std::path::{Path, PathBuf};

use signal_message::TargetFlowName;

/// The flow lane roots searched for `.flow-id` markers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowMarkerIndex {
    roots: Vec<PathBuf>,
}

/// One resolved flow: its name and the marker that witnessed it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedFlow {
    pub target_flow_name: TargetFlowName,
    pub marker_path: PathBuf,
}

impl FlowMarkerIndex {
    pub fn new(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }

    /// The workspace lane roots this host keeps. SEAM: a conventional path
    /// list stands in for the Flow component's registry; it is the one place
    /// the interim resolution knows where to look.
    pub fn conventional() -> Self {
        let Some(home) = std::env::home_dir() else {
            return Self::new(Vec::new());
        };
        Self::new(vec![
            home.join("primary").join("flows"),
            home.join("secondary").join("flows"),
        ])
    }

    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Resolve a flow name to its marker, or `None` when no root carries it.
    pub fn resolve(&self, target_flow_name: &TargetFlowName) -> Option<ResolvedFlow> {
        self.roots
            .iter()
            .map(|root| Self::marker_path(root, target_flow_name))
            .find(|candidate| candidate.is_file())
            .map(|marker_path| ResolvedFlow {
                target_flow_name: target_flow_name.clone(),
                marker_path,
            })
    }

    fn marker_path(root: &Path, target_flow_name: &str) -> PathBuf {
        root.join(format!(".{target_flow_name}.flow-id"))
    }
}
