//! The typed ordinary-socket operations Message sends to Flow.

use crate::frame::FramedStream;
use signal_flow::{
    Address, Bind_Data, Deliver_Data, DeliveryCursor, FlowId, IdentifyThread_Data, Lock, Lock_Data,
    ObserveSender_Data, Process, Query, Recipient, Refusal, Response, SendAcceptance, SendFailure,
    SendReceipt, SendRequest, Sender,
};
use std::os::unix::net::UnixStream;

#[derive(Debug)]
pub enum EdgeFailure {
    Unreachable,
    Broken,
    Refused(Refusal),
    Unexpected,
}

pub enum FlowSendReply {
    Queued(SendAcceptance),
    Sent(SendReceipt),
    Failed(SendFailure),
}

pub struct FlowEdge {
    pub socket_path: String,
}

pub struct FlowSubscription {
    stream: UnixStream,
}

pub trait ReadsFlowSubscription {
    fn next(&mut self) -> Result<Response, EdgeFailure>;
    fn interrupt_handle(&self) -> Result<UnixStream, EdgeFailure>;
}

impl ReadsFlowSubscription for FlowSubscription {
    fn next(&mut self) -> Result<Response, EdgeFailure> {
        self.stream.read_frame().map_err(|_| EdgeFailure::Broken)
    }

    fn interrupt_handle(&self) -> Result<UnixStream, EdgeFailure> {
        self.stream.try_clone().map_err(|_| EdgeFailure::Broken)
    }
}

trait ExchangesWithFlow {
    fn exchange(&self, query: &Query) -> Result<Response, EdgeFailure>;
}

impl ExchangesWithFlow for FlowEdge {
    fn exchange(&self, query: &Query) -> Result<Response, EdgeFailure> {
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|_| EdgeFailure::Unreachable)?;
        // Bound an unavailable peer so shutdown can join an in-flight handler.
        // A timeout is a failed exchange, never permission to replay a delivery.
        let timeout = Some(std::time::Duration::from_secs(2));
        let read_timeout = if matches!(query, Query::Deliver(delivery)
            if delivery.delivery == signal_flow::Delivery::Wake
                && !matches!(delivery.message, signal_flow::Message::Soft(_)))
        {
            None
        } else {
            timeout
        };
        stream
            .set_read_timeout(read_timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .set_write_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
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
    fn identify_thread(
        &self,
        process: Process,
        native_session_id: String,
    ) -> Result<Address, EdgeFailure>;
    fn lock(&self, sender: Sender, recipient: Recipient) -> Result<Lock, EdgeFailure>;
    fn deliver(&self, lock: Lock, request: SendRequest) -> Result<FlowSendReply, EdgeFailure>;
    fn observe_sender(
        &self,
        process: Process,
        native_session_id: Option<String>,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, EdgeFailure>;
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

    fn identify_thread(
        &self,
        process: Process,
        native_session_id: String,
    ) -> Result<Address, EdgeFailure> {
        match self.exchange(&Query::IdentifyThread(IdentifyThread_Data {
            process,
            native_session_id,
        }))? {
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

    fn deliver(&self, lock: Lock, request: SendRequest) -> Result<FlowSendReply, EdgeFailure> {
        let abrupt = !matches!(&request.message, signal_flow::Message::Soft(_));
        match self.exchange(&Query::Deliver(Deliver_Data {
            lock,
            delivery: request.delivery,
            message: request.message,
        }))? {
            Response::SendAccepted(acceptance) if abrupt => {
                Ok(FlowSendReply::Failed(SendFailure {
                    logical_delivery_id_option: Some(acceptance.logical_delivery_id),
                    send_failure_reason: signal_flow::SendFailureReason::ReceiptUncertain,
                }))
            }
            Response::SendAccepted(acceptance) => Ok(FlowSendReply::Queued(acceptance)),
            Response::Sent(receipt) => Ok(FlowSendReply::Sent(receipt)),
            Response::Failed(failure) => Ok(FlowSendReply::Failed(failure)),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn observe_sender(
        &self,
        process: Process,
        native_session_id: Option<String>,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, EdgeFailure> {
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|_| EdgeFailure::Unreachable)?;
        let timeout = Some(std::time::Duration::from_secs(2));
        stream
            .set_write_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .write_frame(&Query::ObserveSender(ObserveSender_Data {
                process,
                native_session_id_option: native_session_id,
                delivery_cursor_option: cursor,
            }))
            .map_err(|_| EdgeFailure::Unreachable)?;
        Ok(FlowSubscription { stream })
    }

    fn release(&self, lock: Lock) -> Result<(), EdgeFailure> {
        match self.exchange(&Query::Release(lock))? {
            Response::Released => Ok(()),
            Response::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }
}
