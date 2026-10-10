//! The typed ordinary-socket operations Message sends to Flow.

use crate::frame::{FrameError, FramedStream};
use signal_flow::{
    Address, Bind_Data, Deliver_Data, FlowId, Lock, Lock_Data, Process, Query, Recipient,
    Refusal, Response, Request, Sender,
};
use std::os::unix::net::UnixStream;

#[derive(Debug)]
pub enum EdgeFailure {
    Unreachable,
    Broken,
    Refused(Refusal),
    Unexpected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered,
    Queued,
    Woken,
}

pub struct FlowEdge {
    pub socket_path: String,
}

trait ExchangesWithFlow {
    fn exchange(&self, query: &Query) -> Result<Response, EdgeFailure>;
}

impl ExchangesWithFlow for FlowEdge {
    fn exchange(&self, query: &Query) -> Result<Response, EdgeFailure> {
        let mut stream = UnixStream::connect(&self.socket_path)
            .map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .write_frame(query)
            .map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .read_frame::<Response>()
            .map_err(|_| EdgeFailure::Broken)
    }
}

pub trait CallsFlow {
    fn bind(&self, address: Address, process: Process) -> Result<FlowId, EdgeFailure>;
    fn identify(&self, process: Process) -> Result<Address, EdgeFailure>;
    fn lock(&self, sender: Sender, recipient: Recipient) -> Result<Lock, EdgeFailure>;
    fn deliver(&self, lock: Lock, request: Request) -> Result<DeliveryOutcome, EdgeFailure>;
    fn release(&self, lock: Lock) -> Result<(), EdgeFailure>;
}

impl CallsFlow for FlowEdge {
    fn bind(&self, address: Address, process: Process) -> Result<FlowId, EdgeFailure> {
        match self.exchange(&Query::Bind(Bind_Data { address, process }))? {
            Response::Bound(flow_id) => Ok(flow_id),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn identify(&self, process: Process) -> Result<Address, EdgeFailure> {
        match self.exchange(&Query::Identify(process))? {
            Response::Identified(address) => Ok(address),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn lock(&self, sender: Sender, recipient: Recipient) -> Result<Lock, EdgeFailure> {
        match self.exchange(&Query::Lock(Lock_Data { sender, recipient }))? {
            Response::Locked(lock) => Ok(lock),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn deliver(&self, lock: Lock, request: Request) -> Result<DeliveryOutcome, EdgeFailure> {
        match self.exchange(&Query::Deliver(Deliver_Data { lock, request }))? {
            Response::Delivered => Ok(DeliveryOutcome::Delivered),
            Response::Queued => Ok(DeliveryOutcome::Queued),
            Response::Woken(_) => Ok(DeliveryOutcome::Woken),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn release(&self, lock: Lock) -> Result<(), EdgeFailure> {
        match self.exchange(&Query::Release(lock))? {
            Response::Released => Ok(()),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }
}
