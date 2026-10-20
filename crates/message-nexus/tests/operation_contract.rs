//! Message's typed operation failures retain Flow's exact result.

use message_nexus::generated::{Failed_Data, Outcome, Unknown_Data};
use signal_message::{Address, Aspect, Layer, Lock};

fn archived(outcome: &Outcome) -> Outcome {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(outcome).expect("archives operation outcome");
    rkyv::from_bytes::<Outcome, rkyv::rancor::Error>(&bytes).expect("restores operation outcome")
}

#[test]
fn reachable_flow_failures_keep_their_distinct_payloads() {
    let address = Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer: Layer::Secondary,
    };
    let lock = Lock {
        sender: address.clone(),
        address: address.clone(),
        until: 12_345,
    };

    for outcome in [
        Outcome::Failed(Failed_Data::Unknown(Unknown_Data::Address(address.clone()))),
        Outcome::Failed(Failed_Data::Unknown(Unknown_Data::Lock(lock.clone()))),
        Outcome::Failed(Failed_Data::Ended(address)),
        Outcome::Failed(Failed_Data::Held(lock)),
        Outcome::Failed(Failed_Data::Asleep),
        Outcome::Failed(Failed_Data::FlowUnreachable),
        Outcome::Failed(Failed_Data::NoLayer),
        Outcome::Failed(Failed_Data::StoreRefused),
        Outcome::Failed(Failed_Data::Store("disk is unavailable".into())),
    ] {
        assert_eq!(archived(&outcome), outcome);
    }
}
