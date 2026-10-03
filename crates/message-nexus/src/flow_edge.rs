//! The Message→Flow edge.
//!
//! Message is a client of Flow's meta socket (ResolvePeer, Vet, Deliver) and
//! of its ordinary socket (Observe.Agent). Flow is the only pane writer:
//! Message hands it a typed Deliver addressed by FlowId and never runs
//! Herdr, never resolves a pane, never types.

use crate::frame::{FrameError, FramedStream};
use meta_signal_flow::{
    Delivery, DeliveryRejection, DeliveryRequest, MetaRefusal, ProcessIdentity, Query as MetaQuery,
    Response as MetaResponse,
};
use signal_flow::{
    AgentObservation, AgentState, Caller, CallerResolutionRejection, FlowId, ObserveSelection,
    Query, Response,
};
use std::os::unix::net::UnixStream;

/// Why an exchange with Flow gave no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeFailure {
    /// No connection: nothing reached Flow.
    Unreachable,
    /// The request was written and no reply came: Flow may have acted.
    Broken,
    /// Flow's meta gate refused Message itself.
    Refused(MetaRefusal),
    /// Flow answered with a reply that does not belong to the request.
    Unexpected,
}

/// Whom Flow says a peer process is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerName {
    /// The process runs in this flow's pane.
    Flow(Caller),
    /// The process runs in no flow's pane: the owner.
    NotAFlow,
    /// Flow knows the process's pane but not as the flow it names.
    Mismatched,
}

/// The two Flow sockets Message speaks to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEdge {
    pub flow_socket_path: String,
    pub flow_meta_socket_path: String,
}

/// The meta operations Message calls on Flow.
pub trait CallsFlowMeta {
    fn resolve_peer(&self, identity: ProcessIdentity) -> Result<PeerName, EdgeFailure>;
    fn vet(
        &self,
        request: DeliveryRequest,
    ) -> Result<Result<FlowId, DeliveryRejection>, EdgeFailure>;
    fn deliver(
        &self,
        request: DeliveryRequest,
    ) -> Result<Result<Delivery, DeliveryRejection>, EdgeFailure>;
}

/// The one ordinary operation Message calls on Flow.
pub trait ObservesFlowAgent {
    fn observe_agent(&self, flow_id: &FlowId) -> Result<AgentWatch, EdgeFailure>;
}

/// One request and its reply over Flow's meta socket.
trait ExchangesWithFlowMeta {
    fn meta_exchange(&self, query: &MetaQuery) -> Result<MetaResponse, EdgeFailure>;
}

impl ExchangesWithFlowMeta for FlowEdge {
    fn meta_exchange(&self, query: &MetaQuery) -> Result<MetaResponse, EdgeFailure> {
        let mut stream = UnixStream::connect(&self.flow_meta_socket_path)
            .map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .write_frame(query)
            .map_err(|_| EdgeFailure::Unreachable)?;
        match stream.read_frame::<MetaResponse>() {
            Ok(MetaResponse::MetaRefused(refusal)) => Err(EdgeFailure::Refused(refusal)),
            Ok(response) => Ok(response),
            Err(_) => Err(EdgeFailure::Broken),
        }
    }
}

impl CallsFlowMeta for FlowEdge {
    fn resolve_peer(&self, identity: ProcessIdentity) -> Result<PeerName, EdgeFailure> {
        match self.meta_exchange(&MetaQuery::ResolvePeer(identity))? {
            MetaResponse::PeerResolved(caller) => Ok(PeerName::Flow(caller)),
            MetaResponse::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown) => {
                Ok(PeerName::NotAFlow)
            }
            MetaResponse::PeerResolutionRejected(CallerResolutionRejection::CallerMismatch(_)) => {
                Ok(PeerName::Mismatched)
            }
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn vet(
        &self,
        request: DeliveryRequest,
    ) -> Result<Result<FlowId, DeliveryRejection>, EdgeFailure> {
        match self.meta_exchange(&MetaQuery::Vet(request))? {
            MetaResponse::Vetted(flow_id) => Ok(Ok(flow_id)),
            MetaResponse::DeliveryRejected(rejection) => Ok(Err(rejection)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn deliver(
        &self,
        request: DeliveryRequest,
    ) -> Result<Result<Delivery, DeliveryRejection>, EdgeFailure> {
        match self.meta_exchange(&MetaQuery::Deliver(request))? {
            MetaResponse::Delivered(delivery) => Ok(Ok(delivery)),
            MetaResponse::DeliveryRejected(rejection) => Ok(Err(rejection)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }
}

impl ObservesFlowAgent for FlowEdge {
    fn observe_agent(&self, flow_id: &FlowId) -> Result<AgentWatch, EdgeFailure> {
        let mut stream =
            UnixStream::connect(&self.flow_socket_path).map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .write_frame(&Query::Observe(ObserveSelection::Agent(flow_id.clone())))
            .map_err(|_| EdgeFailure::Unreachable)?;
        Ok(AgentWatch { stream })
    }
}

/// An open Observe.Agent subscription: the state on open, then each change.
#[derive(Debug)]
pub struct AgentWatch {
    stream: UnixStream,
}

/// Reads an open Observe.Agent subscription.
pub trait StreamsAgentStates {
    /// The next state Flow announces; None once the subscription ends.
    fn next_state(&mut self) -> Option<AgentState>;
    /// A handle that ends the subscription from another thread.
    fn closer(&self) -> Option<UnixStream>;
}

impl StreamsAgentStates for AgentWatch {
    fn next_state(&mut self) -> Option<AgentState> {
        match self.stream.read_frame::<Response>() {
            Ok(Response::AgentObserved(AgentObservation { agent_state, .. })) => Some(agent_state),
            Ok(_) | Err(FrameError::Closed) | Err(_) => None,
        }
    }

    fn closer(&self) -> Option<UnixStream> {
        self.stream.try_clone().ok()
    }
}
