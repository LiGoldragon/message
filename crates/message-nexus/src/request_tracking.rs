//! Durable facts and pure ordering for Message's `Deliver` consumer.
//!
//! A held observation deliberately lives in [`OrderedClassifier`] rather than
//! [`RequestTracking`].  Consequently a durable cursor can never cover a held
//! observation: the only operation which advances it also records that
//! observation's effect.

use signal_flow::{Counter, DeliverKey, Namespace};
use std::collections::VecDeque;

/// A `Deliver` written before Message sends it.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct OutboxEntry<Request> {
    pub deliver_key: DeliverKey,
    pub request: Request,
    pub refused: bool,
}

/// Flow's durable answer to one outbox entry.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LedgerEntry {
    pub deliver_key: DeliverKey,
    pub request_id: String,
    pub acceptance_cursor: i64,
    pub retired: bool,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Classification {
    MessageRelay,
    FlowOwn,
}

/// The durable effect committed with the observation cursor.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClassifiedEvent {
    pub store_identity: String,
    pub delivery_cursor: i64,
    pub request_id: String,
    pub classification: Classification,
    pub archived: bool,
}

/// The one durable Message-owned value used by the consumer.
///
/// Persisting this value as one record makes an event's effect, retirement and
/// cursor advance one write.  Replacing the value is the store boundary; the
/// classifier itself performs no I/O.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RequestTracking<Request> {
    pub namespace: Namespace,
    pub next_counter: Counter,
    pub store_identity: Option<String>,
    pub observation_cursor: Option<i64>,
    pub outbox: Vec<OutboxEntry<Request>>,
    pub ledger: Vec<LedgerEntry>,
    pub pending: Vec<DeliverKey>,
    pub classified_events: Vec<ClassifiedEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrackingError {
    DuplicateDeliverKey,
    UnknownDeliverKey,
    AnswerAlreadyRecorded,
    CursorRequiredWhileTracking,
    StoreIdentityRequired,
    ObservationOutOfOrder,
}

impl<Request> RequestTracking<Request> {
    pub fn new(namespace: Namespace) -> Self {
        Self {
            namespace,
            next_counter: 0,
            store_identity: None,
            observation_cursor: None,
            outbox: Vec::new(),
            ledger: Vec::new(),
            pending: Vec::new(),
            classified_events: Vec::new(),
        }
    }
}

impl<Request: Clone> RequestTracking<Request> {
    /// The store write which precedes sending `Deliver`.
    pub fn queue_deliver(
        &mut self,
        request: impl FnOnce(&DeliverKey) -> Request,
    ) -> Result<DeliverKey, TrackingError> {
        let deliver_key = DeliverKey {
            namespace: self.namespace.clone(),
            counter: self.next_counter,
        };
        self.next_counter = self
            .next_counter
            .checked_add(1)
            .ok_or(TrackingError::DuplicateDeliverKey)?;
        self.outbox.push(OutboxEntry {
            deliver_key: deliver_key.clone(),
            request: request(&deliver_key),
            refused: false,
        });
        self.pending.push(deliver_key.clone());
        Ok(deliver_key)
    }

    /// The store write which must precede `Acknowledge`.
    pub fn record_acceptance(
        &mut self,
        deliver_key: &DeliverKey,
        request_id: String,
        acceptance_cursor: i64,
    ) -> Result<(), TrackingError> {
        if !self
            .outbox
            .iter()
            .any(|entry| entry.deliver_key == deliver_key)
        {
            return Err(TrackingError::UnknownDeliverKey);
        }
        if self
            .ledger
            .iter()
            .any(|entry| entry.deliver_key == deliver_key)
        {
            return Err(TrackingError::AnswerAlreadyRecorded);
        }
        self.ledger.push(LedgerEntry {
            deliver_key: deliver_key.clone(),
            request_id,
            acceptance_cursor,
            retired: false,
        });
        self.pending.retain(|key| key != deliver_key);
        Ok(())
    }

    pub fn record_refusal(&mut self, deliver_key: &DeliverKey) -> Result<(), TrackingError> {
        let entry = self
            .outbox
            .iter_mut()
            .find(|entry| entry.deliver_key == deliver_key)
            .ok_or(TrackingError::UnknownDeliverKey)?;
        entry.refused = true;
        self.pending.retain(|key| key != deliver_key);
        Ok(())
    }

    pub fn pending_deliver_keys(&self) -> Vec<DeliverKey> {
        self.pending.clone()
    }

    pub fn acknowledgement_keys(&self) -> Vec<DeliverKey> {
        self.ledger
            .iter()
            .map(|entry| entry.deliver_key.clone())
            .collect()
    }

    pub fn opening_cursor(&self) -> Result<Option<i64>, TrackingError> {
        if self.observation_cursor.is_none()
            && (!self.pending.is_empty() || self.ledger.iter().any(|entry| !entry.retired))
        {
            return Err(TrackingError::CursorRequiredWhileTracking);
        }
        Ok(self.observation_cursor)
    }

    /// Applies the identity from the opening observation.
    pub fn observe_store(&mut self, identity: String) {
        if self
            .store_identity
            .as_ref()
            .is_some_and(|old| old != &identity)
        {
            self.observation_cursor = None;
            self.ledger.clear();
            self.pending.clear();
        }
        self.store_identity = Some(identity);
    }

    fn highest_acceptance_cursor(&self) -> Option<i64> {
        self.ledger
            .iter()
            .filter(|entry| !entry.retired)
            .map(|entry| entry.acceptance_cursor)
            .max()
    }

    fn candidate_index(&self, request_id: &str) -> Option<usize> {
        self.ledger
            .iter()
            .enumerate()
            .filter(|(_, entry)| !entry.retired && entry.request_id == request_id)
            .min_by_key(|(_, entry)| entry.acceptance_cursor)
            .map(|(index, _)| index)
    }

    fn last_archive_before(&self, request_id: &str, cursor: i64) -> i64 {
        self.classified_events
            .iter()
            .filter(|event| {
                event.request_id == request_id && event.archived && event.delivery_cursor < cursor
            })
            .map(|event| event.delivery_cursor)
            .max()
            .unwrap_or(0)
    }

    fn decision<S: SenderSnapshot>(
        &self,
        snapshot: &S,
        read_highwater: i64,
        known_archive_boundary: Option<i64>,
    ) -> Decision {
        if let Some(index) = self.candidate_index(snapshot.request_id()) {
            let entry = &self.ledger[index];
            if read_highwater < entry.acceptance_cursor {
                return Decision::Hold;
            }
            let boundary = known_archive_boundary.unwrap_or_else(|| {
                self.last_archive_before(snapshot.request_id(), entry.acceptance_cursor)
            });
            return if boundary < snapshot.delivery_cursor() {
                Decision::Classify(Classification::MessageRelay, Some(index))
            } else {
                Decision::Classify(Classification::FlowOwn, None)
            };
        }

        let admission_is_unknown = !self.pending.is_empty()
            || self
                .highest_acceptance_cursor()
                .is_some_and(|cursor| read_highwater < cursor);
        if admission_is_unknown {
            Decision::Hold
        } else {
            Decision::Classify(Classification::FlowOwn, None)
        }
    }

    fn committed<S: SenderSnapshot>(
        &self,
        snapshot: &S,
        classification: Classification,
        retire: Option<usize>,
    ) -> Result<Self, TrackingError> {
        if self
            .observation_cursor
            .is_some_and(|cursor| snapshot.delivery_cursor() <= cursor)
        {
            return Err(TrackingError::ObservationOutOfOrder);
        }
        let mut next = self.clone();
        let store_identity = next
            .store_identity
            .clone()
            .ok_or(TrackingError::StoreIdentityRequired)?;
        next.classified_events.push(ClassifiedEvent {
            store_identity,
            delivery_cursor: snapshot.delivery_cursor(),
            request_id: snapshot.request_id().to_owned(),
            classification,
            archived: snapshot.is_archived(),
        });
        if snapshot.is_archived()
            && let Some(index) = retire
        {
            next.ledger[index].retired = true;
        }
        next.observation_cursor = Some(snapshot.delivery_cursor());
        Ok(next)
    }
}

/// Projection of the exact Flow observation fields used by the classifier.
/// The signal boundary supplies this projection without changing their types.
pub trait SenderSnapshot {
    fn request_id(&self) -> &str;
    fn delivery_cursor(&self) -> i64;
    fn is_archived(&self) -> bool;
}

enum Decision {
    Hold,
    Classify(Classification, Option<usize>),
}

/// Memory-only ordered observations waiting to be committed.
#[derive(Clone, Debug)]
pub struct OrderedClassifier<S> {
    held: VecDeque<S>,
}

impl<S> Default for OrderedClassifier<S> {
    fn default() -> Self {
        Self {
            held: VecDeque::new(),
        }
    }
}

impl<S: SenderSnapshot> OrderedClassifier<S> {
    pub fn push(&mut self, snapshot: S) {
        self.held.push_back(snapshot);
    }

    pub fn held_len(&self) -> usize {
        self.held.len()
    }

    /// Returns successive whole durable values. The caller durably replaces
    /// the preceding value before performing the next transition.
    pub fn classify_ready<Request: Clone>(
        &mut self,
        durable: &RequestTracking<Request>,
        read_highwater: i64,
    ) -> Result<Vec<RequestTracking<Request>>, TrackingError> {
        let mut current = durable.clone();
        let mut writes = Vec::new();
        while let Some(snapshot) = self.held.front() {
            // Reading through an acceptance tells us every earlier archive,
            // even when ordering keeps that archive behind this front event.
            let known_archive_boundary =
                current.candidate_index(snapshot.request_id()).map(|index| {
                    let acceptance = current.ledger[index].acceptance_cursor;
                    self.held
                        .iter()
                        .filter(|event| {
                            event.request_id() == snapshot.request_id()
                                && event.is_archived()
                                && event.delivery_cursor() < acceptance
                        })
                        .map(|event| event.delivery_cursor())
                        .chain(std::iter::once(
                            current.last_archive_before(snapshot.request_id(), acceptance),
                        ))
                        .max()
                        .unwrap_or(0)
                });
            match current.decision(snapshot, read_highwater, known_archive_boundary) {
                Decision::Hold => break,
                Decision::Classify(classification, retirement) => {
                    let next = current.committed(snapshot, classification, retirement)?;
                    self.held.pop_front();
                    writes.push(next.clone());
                    current = next;
                }
            }
        }
        Ok(writes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct Snapshot(&'static str, i64, bool);
    impl SenderSnapshot for Snapshot {
        fn request_id(&self) -> &str {
            self.0
        }
        fn delivery_cursor(&self) -> i64 {
            self.1
        }
        fn is_archived(&self) -> bool {
            self.2
        }
    }

    fn tracking() -> RequestTracking<()> {
        let mut state = RequestTracking::new("message-store".into());
        state.observe_store("flow-store".into());
        state
    }

    fn accepted(_key: &str, id: &str, cursor: i64) -> RequestTracking<()> {
        let mut state = tracking();
        let deliver_key = state.queue_deliver(|_| ()).unwrap();
        assert_eq!(deliver_key.namespace, "message-store");
        state
            .record_acceptance(&deliver_key, id.into(), cursor)
            .unwrap();
        state
    }

    fn classify(
        state: &RequestTracking<()>,
        highwater: i64,
        snapshots: impl IntoIterator<Item = Snapshot>,
    ) -> (OrderedClassifier<Snapshot>, Vec<RequestTracking<()>>) {
        let mut classifier = OrderedClassifier::default();
        for snapshot in snapshots {
            classifier.push(snapshot);
        }
        let writes = classifier.classify_ready(state, highwater).unwrap();
        (classifier, writes)
    }

    #[test]
    fn observe_sender_tells_relays_from_own_sends_by_id() {
        let state = accepted("k", "mine", 2);
        let (_, writes) = classify(
            &state,
            3,
            [Snapshot("mine", 2, false), Snapshot("other", 3, false)],
        );
        assert_eq!(
            writes[0].classified_events[0].classification,
            Classification::MessageRelay
        );
        assert_eq!(
            writes[1].classified_events[1].classification,
            Classification::FlowOwn
        );
    }

    #[test]
    fn message_holds_unknown_snapshots_while_a_deliver_is_pending() {
        let mut state = tracking();
        let _k_key = state.queue_deliver(|_| ()).unwrap();
        let (classifier, writes) = classify(&state, 8, [Snapshot("drawn", 8, false)]);
        assert!(writes.is_empty());
        assert_eq!(classifier.held_len(), 1);
        assert_eq!(state.observation_cursor, None);
    }

    #[test]
    fn message_classifies_held_snapshots_once_its_stream_passes_the_acceptance_cursor() {
        let mut state = accepted("k", "mine", 9);
        let (mut classifier, writes) = classify(&state, 8, [Snapshot("mine", 8, false)]);
        assert!(writes.is_empty());
        state.observation_cursor = Some(7);
        let writes = classifier.classify_ready(&state, 9).unwrap();
        assert_eq!(
            writes[0].classified_events[0].classification,
            Classification::MessageRelay
        );
    }

    #[test]
    fn message_takes_a_snapshot_below_the_last_archived_as_not_its_relay() {
        let state = accepted("new", "i", 4);
        let (_, writes) = classify(
            &state,
            4,
            [
                Snapshot("i", 1, false),
                Snapshot("i", 2, true),
                Snapshot("i", 4, false),
            ],
        );
        assert_eq!(
            writes[0].classified_events.last().unwrap().classification,
            Classification::FlowOwn
        );
    }

    #[test]
    fn message_retires_a_ledger_entry_on_archived() {
        let state = accepted("k", "i", 1);
        let (_, writes) = classify(&state, 2, [Snapshot("i", 2, true)]);
        assert!(writes[0].ledger[0].retired);
        assert_eq!(writes[0].observation_cursor, Some(2));
    }

    #[test]
    fn message_drops_its_ledger_on_a_store_identity_change() {
        let mut state = accepted("k", "i", 1);
        state.observation_cursor = Some(1);
        state.observe_store("replacement".into());
        assert!(state.ledger.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.observation_cursor, None);
    }

    #[test]
    fn message_resends_pending_delivers_by_key_on_restart() {
        let mut state = tracking();
        let stable_key_key = state.queue_deliver(|_| ()).unwrap();
        let restarted = state.clone();
        assert_eq!(restarted.pending_deliver_keys(), [stable_key_key]);
    }

    #[test]
    fn message_allocates_the_counter_with_the_outbox_entry() {
        let before = tracking();
        let mut committed = before.clone();
        let key = committed.queue_deliver(|_| ()).unwrap();
        assert_eq!(before.next_counter, 0);
        assert!(before.outbox.is_empty());
        assert_eq!(key.namespace, committed.namespace);
        assert_eq!(key.counter, 0);
        assert_eq!(committed.next_counter, 1);
        assert_eq!(committed.outbox[0].deliver_key, key);
    }

    #[test]
    fn message_acknowledges_only_after_its_ledger_entry_is_durable() {
        let mut before = tracking();
        let k_key = before.queue_deliver(|_| ()).unwrap();
        assert!(before.acknowledgement_keys().is_empty());
        let mut durable = before.clone();
        durable.record_acceptance(&k_key, "i".into(), 1).unwrap();
        assert_eq!(durable.acknowledgement_keys(), [k_key]);
        assert!(before.acknowledgement_keys().is_empty());
    }

    #[test]
    fn message_never_opens_without_a_cursor_while_tracking() {
        let mut state = tracking();
        let k_key = state.queue_deliver(|_| ()).unwrap();
        assert_eq!(
            state.opening_cursor(),
            Err(TrackingError::CursorRequiredWhileTracking)
        );
    }

    #[test]
    fn message_cursor_never_passes_an_unclassified_event() {
        let mut state = tracking();
        let k_key = state.queue_deliver(|_| ()).unwrap();
        let (classifier, writes) = classify(
            &state,
            10,
            [Snapshot("unknown", 4, false), Snapshot("own", 5, false)],
        );
        assert!(writes.is_empty());
        assert_eq!(classifier.held_len(), 2);
        assert_eq!(state.observation_cursor, None);
    }

    #[test]
    fn message_acts_on_events_in_cursor_order_behind_a_held_one() {
        let mut state = tracking();
        let k_key = state.queue_deliver(|_| ()).unwrap();
        let (mut classifier, writes) = classify(
            &state,
            4,
            [
                Snapshot("unknown", 3, false),
                Snapshot("known-own", 4, false),
            ],
        );
        assert!(writes.is_empty());
        state.record_refusal(&k_key).unwrap();
        let writes = classifier.classify_ready(&state, 4).unwrap();
        assert_eq!(
            writes
                .iter()
                .map(|s| s.observation_cursor.unwrap())
                .collect::<Vec<_>>(),
            [3, 4]
        );
    }

    #[test]
    fn message_writes_an_events_effect_and_its_cursor_together() {
        let state = tracking();
        let (_, writes) = classify(&state, 1, [Snapshot("i", 1, false)]);
        assert_eq!(state.observation_cursor, None);
        assert!(state.classified_events.is_empty());
        assert_eq!(writes[0].observation_cursor, Some(1));
        assert_eq!(writes[0].classified_events.len(), 1);
    }

    #[test]
    fn message_crash_after_holding_before_any_cursor_write_replays_the_held_event() {
        let mut durable = tracking();
        let k_key = durable.queue_deliver(|_| ()).unwrap();
        let (_, writes) = classify(&durable, 1, [Snapshot("i", 1, false)]);
        assert!(writes.is_empty());
        durable.record_acceptance(&k_key, "i".into(), 1).unwrap();
        let (_, replay) = classify(&durable, 1, [Snapshot("i", 1, false)]);
        assert_eq!(replay[0].observation_cursor, Some(1));
    }

    #[test]
    fn message_crash_after_the_cursor_write_below_a_held_event_replays_it() {
        let mut durable = tracking();
        durable.observation_cursor = Some(2);
        let k_key = durable.queue_deliver(|_| ()).unwrap();
        let (_, writes) = classify(&durable, 3, [Snapshot("i", 3, false)]);
        assert!(writes.is_empty());
        assert_eq!(durable.observation_cursor, Some(2));
    }

    #[test]
    fn message_crash_with_the_answer_lost_recovers_by_key_and_classifies_the_replayed_snapshot() {
        let mut durable = tracking();
        let k_key = durable.queue_deliver(|_| ()).unwrap();
        let (_, held) = classify(&durable, 1, [Snapshot("i", 1, false)]);
        assert!(held.is_empty());
        durable.record_acceptance(&k_key, "i".into(), 1).unwrap();
        let (_, replay) = classify(&durable, 1, [Snapshot("i", 1, false)]);
        assert_eq!(
            replay[0].classified_events[0].classification,
            Classification::MessageRelay
        );
    }

    #[test]
    fn message_crash_after_the_answer_before_its_ledger_write_resends_by_key() {
        let mut durable = tracking();
        let k_key = durable.queue_deliver(|_| ()).unwrap();
        assert_eq!(durable.pending_deliver_keys(), [k_key]);
    }

    #[test]
    fn message_crash_after_the_ledger_write_before_acknowledge_classifies_on_replay() {
        let durable = accepted("k", "i", 1);
        assert_eq!(durable.acknowledgement_keys().len(), 1);
        let (_, replay) = classify(&durable, 1, [Snapshot("i", 1, false)]);
        assert_eq!(replay.len(), 1);
    }

    #[test]
    fn message_crash_after_an_events_effect_and_cursor_write_replays_nothing_twice() {
        let state = tracking();
        let (_, writes) = classify(&state, 1, [Snapshot("i", 1, false)]);
        let durable = writes.last().unwrap();
        let (_, replay) = classify(durable, 1, [Snapshot("i", 1, false)]);
        assert_eq!(replay, Err(TrackingError::ObservationOutOfOrder));
    }

    #[test]
    fn message_crash_with_an_archived_held_behind_its_span_retires_the_entry_after_the_span() {
        let state = accepted("mine", "i", 2);
        let (mut classifier, writes) =
            classify(&state, 1, [Snapshot("i", 3, false), Snapshot("i", 4, true)]);
        assert!(writes.is_empty());
        let writes = classifier.classify_ready(&state, 4).unwrap();
        assert!(!writes[0].ledger[0].retired);
        assert!(writes[1].ledger[0].retired);
    }

    #[test]
    fn message_crash_between_an_archived_and_its_retirement_write_retires_once_on_replay() {
        let state = accepted("k", "i", 1);
        let (_, replay) = classify(&state, 2, [Snapshot("i", 2, true)]);
        assert!(replay[0].ledger[0].retired);
        assert_eq!(replay[0].classified_events.len(), 1);
    }

    #[test]
    fn message_crash_with_a_reused_id_held_classifies_it_as_the_flows_own() {
        let state = accepted("k", "i", 1);
        let (_, archived) = classify(&state, 2, [Snapshot("i", 2, true)]);
        let durable = archived.last().unwrap();
        let (_, replay) = classify(durable, 3, [Snapshot("i", 3, false)]);
        assert_eq!(
            replay[0].classified_events.last().unwrap().classification,
            Classification::FlowOwn
        );
    }

    #[test]
    fn message_crash_with_its_own_relay_reusing_an_id_gives_each_span_its_entry() {
        let mut state = accepted("one", "i", 1);
        let two_key = state.queue_deliver(|_| ()).unwrap();
        state.record_acceptance(&two_key, "i".into(), 4).unwrap();
        let (_, writes) = classify(&state, 5, [Snapshot("i", 2, true), Snapshot("i", 5, false)]);
        assert!(writes[0].ledger[0].retired);
        assert_eq!(
            writes[1].classified_events.last().unwrap().classification,
            Classification::MessageRelay
        );
    }

    #[test]
    fn message_selects_the_ledger_entry_whose_span_holds_the_snapshot() {
        let mut state = tracking();
        let (_, own) = classify(&state, 2, [Snapshot("i", 1, false), Snapshot("i", 2, true)]);
        state = own.last().unwrap().clone();
        let k_key = state.queue_deliver(|_| ()).unwrap();
        state.record_acceptance(&k_key, "i".into(), 3).unwrap();
        let (_, relay) = classify(
            &state,
            4,
            [Snapshot("i", 3, false), Snapshot("i", 4, false)],
        );
        assert_eq!(
            relay[0].classified_events.last().unwrap().classification,
            Classification::MessageRelay
        );
    }

    #[test]
    fn message_crash_with_two_relay_generations_of_one_id_pending_classifies_each_by_its_span() {
        let mut state = accepted("one", "i", 1);
        let two_key = state.queue_deliver(|_| ()).unwrap();
        state.record_acceptance(&two_key, "i".into(), 4).unwrap();
        let (_, replay) = classify(
            &state,
            5,
            [
                Snapshot("i", 2, false),
                Snapshot("i", 3, true),
                Snapshot("i", 5, false),
            ],
        );
        assert_eq!(
            replay
                .iter()
                .map(|s| s.classified_events.last().unwrap().classification)
                .collect::<Vec<_>>(),
            [Classification::MessageRelay; 3]
        );
        assert!(replay[1].ledger[0].retired);
    }

    #[test]
    fn message_classifies_on_a_quiet_stream_whose_last_event_is_the_acceptance() {
        let state = accepted("k", "i", 7);
        let (_, writes) = classify(&state, 7, [Snapshot("i", 7, false)]);
        assert_eq!(writes.len(), 1);
    }

    #[test]
    fn message_crash_repeats_an_outside_effect_named_by_store_identity_and_cursor() {
        let mut state = tracking();
        state.observe_store("identity".into());
        let (_, first) = classify(&state, 1, [Snapshot("i", 1, false)]);
        let event = &first[0].classified_events[0];
        assert_eq!(
            (&*event.store_identity, event.delivery_cursor),
            ("identity", 1)
        );
        // A crash before the returned value is persisted replays the same name.
        let (_, replay) = classify(&state, 1, [Snapshot("i", 1, false)]);
        assert_eq!(replay[0].classified_events[0], *event);
    }
}
