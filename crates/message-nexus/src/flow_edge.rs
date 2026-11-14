//! The typed ordinary-socket operations Message sends to Flow.

use crate::frame::FramedStream;
use signal_flow::{
    Address, Bind_Data, Contracted, Deliver_Data, DeliverKey, DeliveryCursor, FlowId,
    HandshakeReceipt, IdentifyThread_Data, Input, Lock, Lock_Data, NativeSessionId,
    ObserveSender_Data, Output, Process, Refusal, SendableMessage, VisibleSendAcceptance,
};
use std::os::unix::net::UnixStream;

pub const FLOW_RESPONSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug)]
pub enum EdgeFailure {
    Unreachable,
    /// Flow refused the greeting: it speaks another signal-flow contract.
    ContractMismatch,
    Broken,
    Refused(Refusal),
    Unidentified(Process),
    DeliverKeyReused,
    Unexpected,
}

pub enum FlowSendReply {
    Queued(VisibleSendAcceptance),
}

pub struct FlowEdge {
    pub socket_path: String,
}

pub struct FlowSubscription {
    stream: UnixStream,
}

#[cfg(test)]
impl FlowSubscription {
    pub(crate) fn from_test_stream(stream: UnixStream) -> Self {
        Self { stream }
    }
}

pub trait ReadsFlowSubscription {
    fn next(&mut self) -> Result<Output, EdgeFailure>;
    fn interrupt_handle(&self) -> Result<UnixStream, EdgeFailure>;
    fn keep_open(&self) -> Result<(), EdgeFailure>;
}

impl ReadsFlowSubscription for FlowSubscription {
    fn next(&mut self) -> Result<Output, EdgeFailure> {
        self.stream.read_frame().map_err(|_| EdgeFailure::Broken)
    }

    fn interrupt_handle(&self) -> Result<UnixStream, EdgeFailure> {
        self.stream.try_clone().map_err(|_| EdgeFailure::Broken)
    }

    fn keep_open(&self) -> Result<(), EdgeFailure> {
        self.stream
            .set_read_timeout(None)
            .map_err(|_| EdgeFailure::Broken)
    }
}

/// Every ordinary connection greets first: signal-flow's greeting is written
/// and the receipt read before any Input.
trait GreetsFlow {
    fn greet_flow(&mut self) -> Result<(), EdgeFailure>;
}

impl GreetsFlow for UnixStream {
    fn greet_flow(&mut self) -> Result<(), EdgeFailure> {
        self.write_frame(&Input::greeting())
            .map_err(|_| EdgeFailure::Unreachable)?;
        match self
            .read_frame::<HandshakeReceipt>()
            .map_err(|_| EdgeFailure::Broken)?
        {
            HandshakeReceipt::Greeted(digest) if digest == Input::contract_digest() => Ok(()),
            _ => Err(EdgeFailure::ContractMismatch),
        }
    }
}

trait ExchangesWithFlow {
    fn exchange(&self, query: &Input) -> Result<Output, EdgeFailure>;
}

impl ExchangesWithFlow for FlowEdge {
    fn exchange(&self, query: &Input) -> Result<Output, EdgeFailure> {
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|_| EdgeFailure::Unreachable)?;
        // Every one-answer exchange is bounded so shutdown can join its worker.
        // Deliver answers admission; later delivery fate belongs to observation.
        let timeout = Some(FLOW_RESPONSE_TIMEOUT);
        stream
            .set_read_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .set_write_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream.greet_flow()?;
        stream
            .write_frame(query)
            .map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .read_frame::<Output>()
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
    fn lock(&self, sender: Address, recipient: Address) -> Result<Lock, EdgeFailure>;
    fn deliver(
        &self,
        lock: Lock,
        deliver_key: DeliverKey,
        sendable_message: SendableMessage,
    ) -> Result<FlowSendReply, EdgeFailure>;
    fn acknowledge(&self, deliver_keys: Vec<DeliverKey>) -> Result<(), EdgeFailure>;
    fn observe_sender(
        &self,
        process: Process,
        native_session_id: Option<String>,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, EdgeFailure>;
    fn observe_deliveries(
        &self,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, EdgeFailure>;
    fn release(&self, lock: Lock) -> Result<(), EdgeFailure>;
}

impl CallsFlow for FlowEdge {
    fn bind(&self, address: Address, process: Process) -> Result<FlowId, EdgeFailure> {
        match self.exchange(&Input::Bind(Bind_Data { address, process }))? {
            Output::Bound(flow_id) => Ok(flow_id),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn identify(&self, process: Process) -> Result<Address, EdgeFailure> {
        match self.exchange(&Input::Identify(process.clone()))? {
            Output::Identified(address) => Ok(address),
            Output::Refused(Refusal::Unidentified(_)) => Err(EdgeFailure::Unidentified(process)),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn identify_thread(
        &self,
        process: Process,
        native_session_id: String,
    ) -> Result<Address, EdgeFailure> {
        match self.exchange(&Input::IdentifyThread(IdentifyThread_Data {
            process: process.clone(),
            native_session_id: NativeSessionId(native_session_id),
        }))? {
            Output::ThreadIdentified(address) => Ok(address),
            Output::Refused(Refusal::Unidentified(_)) => Err(EdgeFailure::Unidentified(process)),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn lock(&self, sender: Address, recipient: Address) -> Result<Lock, EdgeFailure> {
        match self.exchange(&Input::Lock(Lock_Data {
            first_address: sender,
            second_address: recipient,
        }))? {
            Output::Locked(lock) => Ok(lock),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn deliver(
        &self,
        lock: Lock,
        deliver_key: DeliverKey,
        sendable_message: SendableMessage,
    ) -> Result<FlowSendReply, EdgeFailure> {
        match self.exchange(&Input::Deliver(Deliver_Data {
            lock,
            deliver_key,
            sendable_message,
        }))? {
            Output::SendAccepted(acceptance) => Ok(FlowSendReply::Queued(acceptance)),
            Output::DeliverKeyReused => Err(EdgeFailure::DeliverKeyReused),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }

    fn acknowledge(&self, deliver_keys: Vec<DeliverKey>) -> Result<(), EdgeFailure> {
        match self.exchange(&Input::Acknowledge(deliver_keys))? {
            Output::Acknowledged => Ok(()),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
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
        let timeout = Some(FLOW_RESPONSE_TIMEOUT);
        stream
            .set_write_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .set_read_timeout(timeout)
            .map_err(|_| EdgeFailure::Broken)?;
        stream.greet_flow()?;
        stream
            .set_read_timeout(None)
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .write_frame(&Input::ObserveSender(ObserveSender_Data {
                process,
                native_session_id_option: native_session_id.map(NativeSessionId),
                delivery_cursor_option: cursor,
            }))
            .map_err(|_| EdgeFailure::Unreachable)?;
        Ok(FlowSubscription { stream })
    }

    fn observe_deliveries(
        &self,
        cursor: Option<DeliveryCursor>,
    ) -> Result<FlowSubscription, EdgeFailure> {
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|_| EdgeFailure::Unreachable)?;
        stream
            .set_read_timeout(Some(FLOW_RESPONSE_TIMEOUT))
            .map_err(|_| EdgeFailure::Broken)?;
        stream
            .set_write_timeout(Some(FLOW_RESPONSE_TIMEOUT))
            .map_err(|_| EdgeFailure::Broken)?;
        stream.greet_flow()?;
        stream
            .write_frame(&Input::ObserveDeliveries(cursor))
            .map_err(|_| EdgeFailure::Unreachable)?;
        Ok(FlowSubscription { stream })
    }

    fn release(&self, lock: Lock) -> Result<(), EdgeFailure> {
        match self.exchange(&Input::Release(lock))? {
            Output::Released => Ok(()),
            Output::Refused(refusal) => Err(EdgeFailure::Refused(refusal)),
            _ => Err(EdgeFailure::Unexpected),
        }
    }
}
