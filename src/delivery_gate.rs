//! Fail-closed delivery hold around a Flow-owned refresh.
//!
//! Flow supplies one exact binding before it replaces a TUI and acknowledges
//! readiness afterwards. Message keeps the delivery decision closed until the
//! acknowledged binding equals the held binding. A restarted Message process
//! reconstructs the closed state; it must never infer a release from absence.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointBinding {
    pub flow_identifier: String,
    pub session_identifier: String,
    pub endpoint_path: String,
}

impl EndpointBinding {
    pub fn from_flow_node(node: &signal_flow::FlowNode) -> Self {
        let endpoint_path = match &node.herdr_route_selection {
            signal_flow::HerdrRouteSelection::Available(route) => {
                format!("herdr:{}:{}", route.herdr_pane_id, route.herdr_terminal_id)
            }
            signal_flow::HerdrRouteSelection::Unavailable => match &node.endpoint_selection {
                signal_flow::EndpointSelection::Available(endpoint) => endpoint.endpoint_path.clone(),
                signal_flow::EndpointSelection::Unavailable => "unavailable".into(),
            },
        };
        Self {
            flow_identifier: node.flow_id.clone(),
            session_identifier: node.session_id.clone(),
            endpoint_path,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryGate {
    Open,
    Held(EndpointBinding),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GateRefusal {
    Held,
    Rebound,
}

impl DeliveryGate {
    pub fn open() -> Self {
        Self::Open
    }

    /// Crash recovery is closed until Flow explicitly repeats readiness.
    pub fn recovered_hold(binding: EndpointBinding) -> Self {
        Self::Held(binding)
    }

    /// Quiescing is idempotent only for the same exact binding. A replacement
    /// observed while already held stays closed instead of changing the hold.
    pub fn quiesce(&mut self, binding: EndpointBinding) -> Result<(), GateRefusal> {
        match self {
            Self::Open => {
                *self = Self::Held(binding);
                Ok(())
            }
            Self::Held(held) if *held == binding => Ok(()),
            Self::Held(_) => Err(GateRefusal::Rebound),
        }
    }

    pub fn permit(&self, binding: &EndpointBinding) -> Result<(), GateRefusal> {
        match self {
            Self::Open => Ok(()),
            Self::Held(held) if held == binding => Err(GateRefusal::Held),
            Self::Held(_) => Err(GateRefusal::Rebound),
        }
    }

    /// A readiness acknowledgement opens the gate only for the original exact
    /// target. A timeout has no transition and therefore remains held.
    pub fn acknowledge_ready(&mut self, binding: &EndpointBinding) -> Result<(), GateRefusal> {
        match self {
            Self::Held(held) if held == binding => {
                *self = Self::Open;
                Ok(())
            }
            Self::Held(_) => Err(GateRefusal::Rebound),
            Self::Open => Err(GateRefusal::Rebound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(session: &str, endpoint: &str) -> EndpointBinding {
        EndpointBinding {
            flow_identifier: "disposable-flow".into(),
            session_identifier: session.into(),
            endpoint_path: endpoint.into(),
        }
    }

    #[test]
    fn held_gate_denies_in_flight_and_releases_only_the_same_binding() {
        let original = binding("thread-a", "/tmp/disposable-a.sock");
        let replacement = binding("thread-b", "/tmp/disposable-b.sock");
        let mut gate = DeliveryGate::open();
        gate.quiesce(original.clone()).unwrap();
        assert_eq!(gate.permit(&original), Err(GateRefusal::Held));
        assert_eq!(gate.permit(&replacement), Err(GateRefusal::Rebound));
        assert_eq!(gate.acknowledge_ready(&replacement), Err(GateRefusal::Rebound));
        assert_eq!(gate.permit(&original), Err(GateRefusal::Held));
        gate.acknowledge_ready(&original).unwrap();
        assert_eq!(gate.permit(&original), Ok(()));
    }

    #[test]
    fn duplicate_quiesce_is_safe_but_rebound_and_crash_stay_closed() {
        let original = binding("thread-a", "/tmp/disposable-a.sock");
        let replacement = binding("thread-b", "/tmp/disposable-b.sock");
        let mut gate = DeliveryGate::recovered_hold(original.clone());
        gate.quiesce(original.clone()).unwrap();
        assert_eq!(gate.quiesce(replacement), Err(GateRefusal::Rebound));
        assert_eq!(gate.permit(&original), Err(GateRefusal::Held));
    }
}
