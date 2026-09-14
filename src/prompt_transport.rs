//! Isolated two-phase transport. No live harness is installed by this module.
//! A trusted dispatcher authenticates with its kernel PID/start-time pin;
//! logical source/destination permissions are configured separately.
use crate::{
    MessengerTables,
    runtime_model::{ReceivedPrompt, ReceivedPromptState},
};
use signal::{ByteViewable, Restorable, Signal, Signalizable};
use signal_message::{
    PromptDeliveryHeader, PromptDeliveryIdentity, PromptRelayDelivery, Query, Response,
};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

const MAX_PAYLOAD: usize = 4 * 1024 * 1024;
fn error(value: impl std::fmt::Display) -> io::Error {
    io::Error::other(value.to_string())
}
fn key(id: &PromptDeliveryIdentity) -> String {
    format!(
        "{}:{}{}:{}{}:{}",
        id.source_agent_identifier.len(),
        id.source_agent_identifier,
        id.destination_agent_identifier.len(),
        id.destination_agent_identifier,
        id.source_event_identifier.len(),
        id.source_event_identifier
    )
}
pub fn process_start_time(pid: u32) -> io::Result<i64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    stat.rsplit_once(')')
        .and_then(|(_, tail)| tail.split_whitespace().nth(19))
        .ok_or_else(|| error("invalid process stat"))?
        .parse()
        .map_err(error)
}
async fn write_query(stream: &mut UnixStream, query: Query) -> io::Result<()> {
    LengthPrefixedCodec::default()
        .write_body_async(
            stream,
            &FrameBody::new(query.signalize().map_err(error)?.bytes().to_vec()),
        )
        .await
        .map_err(error)
}
async fn read_reply(stream: &mut UnixStream) -> io::Result<Response> {
    let bytes = LengthPrefixedCodec::default()
        .read_body_async(stream)
        .await
        .map_err(error)?;
    Signal::<Response>::from(bytes.bytes().to_vec())
        .restore()
        .map_err(error)
}
async fn write_reply(stream: &mut UnixStream, response: Response) -> io::Result<()> {
    LengthPrefixedCodec::default()
        .write_body_async(
            stream,
            &FrameBody::new(response.signalize().map_err(error)?.bytes().to_vec()),
        )
        .await
        .map_err(error)
}

#[derive(Clone)]
pub struct PromptReceiver {
    tables: Arc<MessengerTables>,
    dispatcher_pid: u32,
    dispatcher_started: i64,
    source: String,
    destination: String,
}
struct ActiveReception {
    tables: Arc<MessengerTables>,
    key: String,
}
impl Drop for ActiveReception {
    fn drop(&mut self) {
        if let Ok(mut active) = self.tables.prompt_receiving.lock() {
            active.remove(&self.key);
        }
    }
}
impl PromptReceiver {
    pub fn new(
        tables: Arc<MessengerTables>,
        dispatcher_pid: u32,
        dispatcher_started: i64,
        source: String,
        destination: String,
    ) -> Self {
        Self {
            tables,
            dispatcher_pid,
            dispatcher_started,
            source,
            destination,
        }
    }
    fn permitted(&self, id: &PromptDeliveryIdentity) -> bool {
        id.source_agent_identifier == self.source
            && id.destination_agent_identifier == self.destination
            && !id.source_event_identifier.is_empty()
            && id.source_event_identifier.len() <= 512
    }
    pub fn status(&self, id: &PromptDeliveryIdentity) -> io::Result<Response> {
        if !self.permitted(id) {
            return Ok(Response::HeaderRejected(id.clone()));
        }
        let key = key(id);
        let active = self.tables.prompt_receiving.lock().map_err(error)?;
        let _state = self.tables.prompt_dispatch_claim.lock().map_err(error)?;
        let record = self.tables.received_prompt(&key).map_err(error)?;
        Ok(match record.map(|record| record.state) {
            Some(ReceivedPromptState::RecipientObserved) => Response::RecipientObserved(id.clone()),
            Some(ReceivedPromptState::PayloadComplete) => Response::PayloadComplete(id.clone()),
            _ if active.contains(&key) => Response::PayloadInProgress(id.clone()),
            _ => Response::PayloadAbsent(id.clone()),
        })
    }
    /// Only records a recipient's observation. Calling this does not inject a
    /// harness turn. Returns false for an already-recorded observation.
    pub fn record_observed(&self, id: &PromptDeliveryIdentity) -> io::Result<bool> {
        if !self.permitted(id) {
            return Err(error("identity not permitted"));
        }
        let _state = self.tables.prompt_dispatch_claim.lock().map_err(error)?;
        let key = key(id);
        let mut record = self
            .tables
            .received_prompt(&key)
            .map_err(error)?
            .ok_or_else(|| error("no payload"))?;
        match record.state {
            ReceivedPromptState::RecipientObserved => Ok(false),
            ReceivedPromptState::PayloadComplete => {
                record.state = ReceivedPromptState::RecipientObserved;
                self.tables
                    .put_received_prompt(&key, record)
                    .map_err(error)?;
                Ok(true)
            }
            _ => Err(error("incomplete payload cannot be observed")),
        }
    }
    pub async fn serve(&self, stream: &mut UnixStream) -> io::Result<()> {
        let pid = stream
            .peer_cred()?
            .pid()
            .ok_or_else(|| error("no kernel PID"))?;
        if pid < 0
            || pid as u32 != self.dispatcher_pid
            || process_start_time(pid as u32)? != self.dispatcher_started
        {
            return Err(error("dispatcher process pin rejected"));
        }
        let body = LengthPrefixedCodec::default()
            .read_body_async(stream)
            .await
            .map_err(error)?;
        if body.bytes().len() > 4096 {
            return Err(error("header too large"));
        }
        let query = Signal::<Query>::from(body.bytes().to_vec())
            .restore()
            .map_err(error)?;
        let header = match query {
            Query::Reconcile(id) => return write_reply(stream, self.status(&id)?).await,
            Query::Header(header) => header,
            _ => return Err(error("not a delivery header or reconciliation query")),
        };
        let id = &header.prompt_delivery_identity;
        if !self.permitted(id)
            || header.prompt_delivery_protocol_version != 1
            || header.dispatcher_process_id != i64::from(self.dispatcher_pid)
            || header.dispatcher_process_start_time != self.dispatcher_started
            || header.prompt_delivery_payload_length <= 0
            || header.prompt_delivery_payload_length as u64 > MAX_PAYLOAD as u64
        {
            return write_reply(stream, Response::HeaderRejected(id.clone())).await;
        }
        let key = key(id);
        let immediate = {
            let mut active = self.tables.prompt_receiving.lock().map_err(error)?;
            let _state = self.tables.prompt_dispatch_claim.lock().map_err(error)?;
            let record = self.tables.received_prompt(&key).map_err(error)?;
            if let Some(record) = record.as_ref() {
                if record.header.prompt_delivery_identity != header.prompt_delivery_identity
                    || record.header.prompt_delivery_payload_length
                        != header.prompt_delivery_payload_length
                    || record.header.prompt_delivery_protocol_version
                        != header.prompt_delivery_protocol_version
                {
                    Some(Response::HeaderRejected(id.clone()))
                } else {
                    match record.state {
                        ReceivedPromptState::PayloadComplete => {
                            Some(Response::PayloadComplete(id.clone()))
                        }
                        ReceivedPromptState::RecipientObserved => {
                            Some(Response::RecipientObserved(id.clone()))
                        }
                        _ if active.contains(&key) => Some(Response::PayloadInProgress(id.clone())),
                        _ => None,
                    }
                }
            } else {
                None
            }
            .map_or_else(
                || {
                    self.tables
                        .put_received_prompt(
                            &key,
                            ReceivedPrompt {
                                header: header.clone(),
                                payload: Vec::new(),
                                state: ReceivedPromptState::IdentitySeen,
                            },
                        )
                        .map_err(error)?;
                    active.insert(key.clone());
                    Ok::<_, io::Error>(None)
                },
                |reply| Ok(Some(reply)),
            )?
        };
        if let Some(reply) = immediate {
            return write_reply(stream, reply).await;
        }
        let _active = ActiveReception {
            tables: self.tables.clone(),
            key: key.clone(),
        };
        write_reply(stream, Response::HeaderAccepted(id.clone())).await?;
        let mut payload = vec![0; header.prompt_delivery_payload_length as usize];
        // EOF/timeout/cancellation leaves IdentitySeen. The active guard drops;
        // only then may reconciliation report PayloadAbsent and allow retry.
        stream.read_exact(&mut payload).await?;
        let value = Signal::<PromptRelayDelivery>::from(payload.clone())
            .restore()
            .map_err(error)?;
        if value.source_agent_identifier != id.source_agent_identifier
            || value.destination_agent_identifier != id.destination_agent_identifier
            || value.typed_prompt_envelope.source_event_identifier != id.source_event_identifier
        {
            return Err(error("payload identity differs from acknowledged header"));
        }
        {
            let _state = self.tables.prompt_dispatch_claim.lock().map_err(error)?;
            self.tables
                .put_received_prompt(
                    &key,
                    ReceivedPrompt {
                        header: header.clone(),
                        payload,
                        state: ReceivedPromptState::PayloadComplete,
                    },
                )
                .map_err(error)?;
        }
        write_reply(stream, Response::PayloadComplete(id.clone())).await
    }
}

/// Sender sends no payload until an identity-matching HeaderAccepted reply.
/// Caller owns the deadline. Any ambiguous failure reconciles by exact event.
pub async fn send(
    stream: &mut UnixStream,
    header: PromptDeliveryHeader,
    payload: &[u8],
) -> io::Result<Response> {
    if header.prompt_delivery_payload_length != payload.len() as i64 {
        return Err(error("payload length mismatch"));
    }
    let id = header.prompt_delivery_identity.clone();
    write_query(stream, Query::Header(header)).await?;
    let reply = read_reply(stream).await?;
    match reply {
        Response::HeaderAccepted(ref accepted) if accepted == &id => {}
        Response::HeaderRejected(ref rejected)
        | Response::PayloadComplete(ref rejected)
        | Response::RecipientObserved(ref rejected)
        | Response::PayloadInProgress(ref rejected)
            if rejected == &id =>
        {
            return Ok(reply);
        }
        _ => return Err(error("invalid header acknowledgment")),
    }
    stream.write_all(payload).await?;
    let reply = read_reply(stream).await?;
    match &reply {
        Response::PayloadComplete(accepted) | Response::RecipientObserved(accepted)
            if accepted == &id =>
        {
            Ok(reply)
        }
        _ => Err(error("invalid payload receipt")),
    }
}
pub async fn reconcile(
    stream: &mut UnixStream,
    id: PromptDeliveryIdentity,
) -> io::Result<Response> {
    write_query(stream, Query::Reconcile(id.clone())).await?;
    let reply = read_reply(stream).await?;
    match &reply {
        Response::PayloadAbsent(seen)
        | Response::PayloadInProgress(seen)
        | Response::PayloadComplete(seen)
        | Response::RecipientObserved(seen)
        | Response::HeaderRejected(seen)
            if seen == &id =>
        {
            Ok(reply)
        }
        _ => Err(error("invalid reconciliation reply")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_message::{
        ConnectionClass, MessageOrigin, PromptInterpretationSelection, PromptVariant,
        TypedPromptEnvelope,
    };
    use tokio::time::{Duration, timeout};
    fn receiver(path: &std::path::Path) -> PromptReceiver {
        PromptReceiver::new(
            Arc::new(MessengerTables::open(path).unwrap()),
            std::process::id(),
            process_start_time(std::process::id()).unwrap(),
            "source".into(),
            "destination".into(),
        )
    }
    fn frame() -> (PromptDeliveryHeader, Vec<u8>) {
        let payload = PromptRelayDelivery {
            source_agent_identifier: "source".into(),
            destination_agent_identifier: "destination".into(),
            message_origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
            typed_prompt_envelope: TypedPromptEnvelope {
                prompt_variant: PromptVariant::HumanPrompt,
                source_event_identifier: "event".into(),
                raw_prompt_text: "synthetic turn".into(),
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
        }
        .signalize()
        .unwrap()
        .bytes()
        .to_vec();
        (
            PromptDeliveryHeader {
                prompt_delivery_protocol_version: 1,
                prompt_delivery_identity: PromptDeliveryIdentity {
                    source_agent_identifier: "source".into(),
                    destination_agent_identifier: "destination".into(),
                    source_event_identifier: "event".into(),
                },
                prompt_delivery_payload_length: payload.len() as i64,
                dispatcher_process_id: i64::from(std::process::id()),
                dispatcher_process_start_time: process_start_time(std::process::id()).unwrap(),
            },
            payload,
        )
    }
    async fn query_status(receiver: PromptReceiver, id: PromptDeliveryIdentity) -> Response {
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(async move {
            receiver.serve(&mut server).await.unwrap();
        });
        let reply = timeout(Duration::from_secs(2), reconcile(&mut client, id))
            .await
            .unwrap()
            .unwrap();
        task.await.unwrap();
        reply
    }
    #[tokio::test]
    async fn rejected_header_sends_zero_payload_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let receiver = receiver(&directory.path().join("store"));
        let (mut header, payload) = frame();
        header.prompt_delivery_protocol_version = 2;
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let task = tokio::spawn(async move {
            receiver.serve(&mut server).await.unwrap();
            let mut remaining = Vec::new();
            server.read_to_end(&mut remaining).await.unwrap();
            remaining
        });
        assert!(matches!(
            timeout(Duration::from_secs(2), send(&mut client, header, &payload))
                .await
                .unwrap()
                .unwrap(),
            Response::HeaderRejected(_)
        ));
        client.shutdown().await.unwrap();
        assert!(
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn partial_payload_reconciles_absent_then_retries_once_after_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let recipient = receiver(&path);
        let (header, payload) = frame();
        let id = header.prompt_delivery_identity.clone();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let copy = recipient.clone();
        let task = tokio::spawn(async move { copy.serve(&mut server).await });
        write_query(&mut client, Query::Header(header.clone()))
            .await
            .unwrap();
        assert!(matches!(
            read_reply(&mut client).await.unwrap(),
            Response::HeaderAccepted(_)
        ));
        client
            .write_all(&payload[..payload.len() / 2])
            .await
            .unwrap();
        assert!(matches!(
            query_status(recipient.clone(), id.clone()).await,
            Response::PayloadInProgress(_)
        ));
        client.shutdown().await.unwrap();
        assert!(
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        drop(recipient);
        let recipient = receiver(&path);
        assert!(matches!(
            query_status(recipient.clone(), id.clone()).await,
            Response::PayloadAbsent(_)
        ));
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let copy = recipient.clone();
        let task = tokio::spawn(async move {
            copy.serve(&mut server).await.unwrap();
        });
        assert!(matches!(
            timeout(Duration::from_secs(2), send(&mut client, header, &payload))
                .await
                .unwrap()
                .unwrap(),
            Response::PayloadComplete(_)
        ));
        task.await.unwrap();
        // This is a durable observation fixture, not a live harness turn.
        assert!(recipient.record_observed(&id).unwrap());
        assert!(!recipient.record_observed(&id).unwrap());
    }
    #[tokio::test]
    async fn complete_payload_lost_receipt_reconciles_without_second_payload() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let recipient = receiver(&path);
        let (header, payload) = frame();
        let id = header.prompt_delivery_identity.clone();
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let copy = recipient.clone();
        let task = tokio::spawn(async move { copy.serve(&mut server).await });
        write_query(&mut client, Query::Header(header.clone()))
            .await
            .unwrap();
        assert!(matches!(
            read_reply(&mut client).await.unwrap(),
            Response::HeaderAccepted(_)
        ));
        client.write_all(&payload).await.unwrap();
        drop(client); // Deliberately never read the payload receipt.
        let _ = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        drop(recipient);
        let recipient = receiver(&path);
        assert!(matches!(
            query_status(recipient.clone(), id.clone()).await,
            Response::PayloadComplete(_)
        ));
        let (mut client, mut server) = UnixStream::pair().unwrap();
        let copy = recipient.clone();
        let task = tokio::spawn(async move {
            copy.serve(&mut server).await.unwrap();
            let mut extra = Vec::new();
            server.read_to_end(&mut extra).await.unwrap();
            extra
        });
        assert!(matches!(
            send(&mut client, header, &payload).await.unwrap(),
            Response::PayloadComplete(_)
        ));
        client.shutdown().await.unwrap();
        assert!(
            timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_empty()
        );
        assert!(recipient.record_observed(&id).unwrap());
        assert!(!recipient.record_observed(&id).unwrap());
        assert!(matches!(
            query_status(recipient, id).await,
            Response::RecipientObserved(_)
        ));
    }
}
