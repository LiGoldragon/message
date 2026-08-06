use message::MessengerTables;
use signal_message::schema::lib::{z2VNPW, z2VXMQ, z2VYJe, z2Vcfd, z2VevD};

#[test]
fn current_store_reopens_without_repair_or_identity_loss() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("messenger.sema");
    {
        let tables = MessengerTables::open(&path).unwrap();
        tables
            .seat_identity(&z2VevD {
                field_0: z2VNPW::new("persistent".to_owned()),
                field_1: z2Vcfd::z2VRLv,
                field_2: z2VXMQ::z2VNZi,
            })
            .unwrap();
    }
    let reopened = MessengerTables::open(&path).unwrap();
    let entries = reopened.query_entries(&z2VYJe::z2VPkz).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].field_0.payload(), "persistent");
}
