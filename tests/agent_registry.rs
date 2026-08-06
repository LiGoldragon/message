use message::MessengerTables;
use signal_message::schema::lib::{
    z2VMBf, z2VNPW, z2VPEW, z2VQY5, z2VRqE, z2VUs6, z2VVAD, z2VXMQ, z2VYJe, z2VYrY, z2Vcfd, z2Vdpc,
    z2VevD,
};

#[test]
fn orchestrator_identity_is_seated_then_endpoint_is_bound() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let agent = z2VNPW::new("li7f".to_owned());
    let seated = tables
        .seat_identity(&z2VevD {
            field_0: agent.clone(),
            field_1: z2Vcfd::z2VRLv,
            field_2: z2VXMQ::z2VNZi,
        })
        .unwrap();
    assert_eq!(seated.field_1, z2Vdpc::z2VRYp);

    let bound = tables
        .bind_endpoint(&z2VVAD {
            field_0: agent.clone(),
            field_1: z2VMBf {
                field_0: z2VUs6::z2VTin,
                field_1: z2VRqE::new(z2VQY5::new("/tmp/li7f.sock".to_owned())),
            },
            field_2: z2VPEW::new(77),
            field_3: z2VYrY::new(88),
        })
        .unwrap()
        .unwrap();
    assert_eq!(bound.payload().payload(), "li7f");

    let entries = tables.query_entries(&z2VYJe::z2VbtY(agent)).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(matches!(
        entries[0].field_1,
        signal_message::schema::lib::z2VNbH::z2Vb3C(_)
    ));
}

#[test]
fn unknown_endpoint_binding_is_rejected_without_minting_identity() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let result = tables
        .bind_endpoint(&z2VVAD {
            field_0: z2VNPW::new("unknown".to_owned()),
            field_1: z2VMBf {
                field_0: z2VUs6::z2VTin,
                field_1: z2VRqE::new(z2VQY5::new("/tmp/unknown.sock".to_owned())),
            },
            field_2: z2VPEW::new(1),
            field_3: z2VYrY::new(2),
        })
        .unwrap();
    assert!(result.is_none());
}
