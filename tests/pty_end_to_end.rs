use std::io::{Read, Write};
use std::os::unix::net::UnixListener;

use dotos::DotosSource;
use message::{
    DeliveryDisposition, DeliveryRunner, MessengerTables,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::schema::lib::{
    z2VMBf, z2VNPW, z2VNcG, z2VPEW, z2VPn2, z2VQY5, z2VRQt, z2VRqE, z2VTJ1, z2VTiK, z2VUs6, z2VVAD,
    z2VXMQ, z2VY2v, z2VY3v, z2VY18, z2VYrY, z2Vari, z2Vcfd, z2VdsV, z2VevD, z2Vf2p,
};

#[test]
fn pty_leg_sends_the_producer_inbox_entry_in_dotos() {
    let directory = tempfile::tempdir().unwrap();
    let session = directory.path().join("terminal-session");
    std::fs::create_dir(&session).unwrap();
    let control = session.join("control.sock");
    let data = session.join("data.sock");
    let listener = UnixListener::bind(&control).unwrap();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut kind = [0_u8; 1];
        stream.read_exact(&mut kind).unwrap();
        assert_eq!(&kind, b"P");
        let mut length = [0_u8; 8];
        stream.read_exact(&mut length).unwrap();
        let mut body = vec![0; u64::from_be_bytes(length) as usize];
        stream.read_exact(&mut body).unwrap();
        stream.write_all(b"A").unwrap();
        String::from_utf8(body).unwrap()
    });

    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    tables
        .seat_identity(&z2VevD {
            field_0: z2VNPW::new("recipient".to_owned()),
            field_1: z2Vcfd::z2VRLv,
            field_2: z2VXMQ::z2VNZi,
        })
        .unwrap();
    tables
        .bind_endpoint(&z2VVAD {
            field_0: z2VNPW::new("recipient".to_owned()),
            field_1: z2VMBf {
                field_0: z2VUs6::z2VZk6,
                field_1: z2VRqE::new(z2VQY5::new(data.to_string_lossy().into_owned())),
            },
            field_2: z2VPEW::new(1),
            field_3: z2VYrY::new(1),
        })
        .unwrap();
    let accepted = tables
        .store_submission(&LedgerDraft {
            message_submission: z2VY2v {
                field_0: z2Vari::new("recipient".to_owned()),
                field_1: z2VdsV::z2VXeo,
                field_2: z2VNcG::new("visible Dotos".to_owned()),
                field_3: z2VTiK::z2VR2m,
            },
            message_origin: z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(1000))),
            sender_name: SenderName::new("sender".to_owned()),
            stamped_at: z2VY18::new(z2Vf2p::new(9)),
        })
        .unwrap();
    let record = tables
        .ledger_record_public(*accepted.payload().payload())
        .unwrap()
        .unwrap();
    assert_eq!(
        DeliveryRunner::new(&tables).deliver_committed(&record),
        DeliveryDisposition::Delivered
    );

    let text = receiver.join().unwrap();
    let entry = DotosSource::new(&text).parse::<z2VRQt>().unwrap();
    assert_eq!(entry.field_1.payload(), "sender");
    assert_eq!(entry.field_2.payload(), "visible Dotos");
}
