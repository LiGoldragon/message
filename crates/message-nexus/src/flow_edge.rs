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

pub struct FlowEdge {
    pub socket_path: String,
}

pub struct FlowSubscription {
    stream: UnixStream,
}

pub enum DeliveryReply {
    Accepted(SendAcceptance),
    Sent(SendReceipt),
    Failed(SendFailure),
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
        stream
            .set_read_timeout(timeout)
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
    fn deliver(&self, lock: Lock, request: SendRequest) -> Result<DeliveryReply, EdgeFailure>;
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

    fn deliver(&self, lock: Lock, request: SendRequest) -> Result<DeliveryReply, EdgeFailure> {
        match self.exchange(&Query::Deliver(Deliver_Data {
            lock,
            delivery: request.delivery,
            message: request.message,
        }))? {
            Response::SendAccepted(acceptance) => Ok(DeliveryReply::Accepted(acceptance)),
            Response::Sent(receipt) => Ok(DeliveryReply::Sent(receipt)),
            Response::Failed(failure) => Ok(DeliveryReply::Failed(failure)),
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

#[cfg(test)]
mod tests {
    use super::{CallsFlow, DeliveryReply, FlowEdge};
    use crate::frame::FramedStream;
    use signal_flow::{
        Address, Aspect, Delivery, HarnessKind, Layer, Lock, Message, NativeReceipt, Recipient,
        Request, Response, SendFailure, SendFailureReason, SendReceipt, SendRequest,
    };
    use std::os::unix::net::UnixListener;

    fn delivery(response: Response) -> DeliveryReply {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("flow.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            let _: signal_flow::Query = peer.read_frame().unwrap();
            peer.write_frame(&response).unwrap();
        });
        let address = Address {
            aspect: Aspect::Mind,
            topic: "core".into(),
            layer: Layer::Secondary,
        };
        let reply = FlowEdge {
            socket_path: socket.to_string_lossy().into_owned(),
        }
        .deliver(
            Lock {
                sender: address.clone(),
                address: address.clone(),
                until: i64::MAX,
            },
            SendRequest {
                recipient: Recipient::Address(address),
                delivery: Delivery::Wake,
                message: Message::MiddleAbrupt(Request::Question("whole body".into())),
            },
        )
        .unwrap();
        server.join().unwrap();
        reply
    }

    #[test]
    fn terminal_flow_delivery_replies_remain_terminal() {
        let receipt = SendReceipt {
            logical_delivery_id: "logical-1".into(),
            native_receipt: NativeReceipt {
                harness_kind: HarnessKind::Claude,
                native_session_id: "session-1".into(),
                native_prompt_id: "prompt-1".into(),
            },
        };
        assert!(matches!(
            delivery(Response::Sent(receipt.clone())),
            DeliveryReply::Sent(observed) if observed == receipt
        ));
        let failure = SendFailure {
            logical_delivery_id_option: Some("logical-2".into()),
            send_failure_reason: SendFailureReason::ReceiptUncertain,
        };
        assert!(matches!(
            delivery(Response::Failed(failure.clone())),
            DeliveryReply::Failed(observed) if observed == failure
        ));
    }
}
