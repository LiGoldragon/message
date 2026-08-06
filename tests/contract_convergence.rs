use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, SessionEpoch};
use signal_message::schema::lib::{
    ContractMarker, FrameBody, Input, z2VNcG, z2VTiK, z2VY2v, z2Vari, z2VdsV,
};

fn exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(7),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[test]
fn component_executes_the_producer_contract_by_identity() {
    let input = Input::Submit(z2VY2v {
        field_0: z2Vari::new("designer".to_owned()),
        field_1: z2VdsV::z2VXeo,
        field_2: z2VNcG::new("the structure is the interface".to_owned()),
        field_3: z2VTiK::z2VR2m,
    });
    let bytes = input.clone().encode_request_frame(exchange()).unwrap();
    let (decoded_exchange, decoded) = ContractMarker::decode_single_request(&bytes).unwrap();
    assert_eq!(decoded_exchange, exchange());
    assert_eq!(decoded, input);

    let frame = ContractMarker::decode_frame(&bytes).unwrap();
    assert!(matches!(frame.into_body(), FrameBody::Request { .. }));
}

#[test]
fn component_has_no_structural_ownership_inputs() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    assert!(!root.join("build.rs").exists());
    for directory in [root.join("schema"), root.join("src/schema")] {
        assert!(
            !directory.exists() || directory.read_dir().unwrap().next().is_none(),
            "structural source directory is not empty: {}",
            directory.display()
        );
    }
    assert!(!root.join("src/frame_bytes.rs").exists());
}
