//! The ordinary Message wire is a 4-byte big-endian length prefix followed by
//! the bare rkyv archive of the producer-owned contract root. No envelope:
//! one connection carries one request and one reply, so exchange identity,
//! lane and batch would be ceremony over a wire that never used them.
//!
//! Peers outside this repository decode exactly that shape, so these tests
//! assert the framing itself, not only that the value survives a round trip.

use signal::{ByteViewable, Restorable, Signal, Signalizable};
use signal_message::{
    MessageKind, MessageRecipient, MessageSubmission, Query, Response, ThreadSelection,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

fn submission() -> MessageSubmission {
    MessageSubmission {
        message_recipient: MessageRecipient::from("designer"),
        message_kind: MessageKind::Send,
        message_body: "the structure is the interface".to_owned(),
        thread_selection: ThreadSelection::None,
    }
}

#[test]
fn a_request_frame_is_a_length_prefix_over_a_bare_contract_archive() {
    let query = Query::Submit(submission());
    let body = query.signalize().expect("archive").bytes().to_vec();

    let mut framed = Vec::new();
    LengthPrefixedCodec::default()
        .write_body(&mut framed, &FrameBody::new(body.clone()))
        .expect("frame");

    // A peer that knows only the framing rule must be able to read it.
    assert!(framed.len() >= 4, "frame carries its length prefix");
    let declared = u32::from_be_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
    assert_eq!(
        declared,
        framed.len() - 4,
        "the prefix counts exactly the body that follows it"
    );
    assert_eq!(
        &framed[4..],
        body.as_slice(),
        "the body is the bare archive"
    );

    let restored: Query = Signal::<Query>::from(framed[4..].to_vec())
        .restore()
        .expect("restore");
    assert_eq!(restored, query);
}

#[test]
fn a_reply_frame_carries_the_bare_response_archive() {
    let response = Response::SubmissionAccepted(41);
    let body = response.signalize().expect("archive").bytes().to_vec();
    let restored: Response = Signal::<Response>::from(body).restore().expect("restore");
    assert_eq!(restored, response);
}

#[test]
fn a_reply_archive_is_not_readable_as_a_request() {
    let bytes = Response::SubmissionAccepted(41)
        .signalize()
        .expect("archive")
        .bytes()
        .to_vec();
    assert!(
        Signal::<Query>::from(bytes).restore().is_err(),
        "the two roots are distinct on the wire"
    );
}

#[test]
fn malformed_bytes_are_rejected_rather_than_misread() {
    assert!(
        Signal::<Query>::from(vec![0xff, 0x00, 0x01])
            .restore()
            .is_err()
    );
}
