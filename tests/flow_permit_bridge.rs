use message::flow_permit::{FlowPermitBridge, FlowPermitError, FlowPermitTransport};
use signal_flow::{
    AcquireDelivery, DeliveryBinding, DeliveryPermit, DeliveryRejection, Query, Response,
};

struct RejectingFlow;

impl FlowPermitTransport for RejectingFlow {
    fn query(&self, request: Query) -> Result<Response, FlowPermitError> {
        assert!(matches!(request, Query::AcquireDelivery(_)));
        Ok(Response::DeliveryAcquireRejected(DeliveryRejection::RefreshHeld(
            signal_flow::RefreshHeld {
                delivery_permit_option: None,
            },
        )))
    }
}

fn binding() -> DeliveryBinding {
    DeliveryBinding {
        native_thread: "native".into(),
        harness_session: "harness".into(),
        route_identity: "route".into(),
        endpoint_identity: "endpoint".into(),
        process_id: 7,
        process_start_time: 8,
    }
}

#[test]
fn rejected_flow_acquire_never_falls_back_to_a_message_permit() {
    let bridge = FlowPermitBridge::new(RejectingFlow);
    let result = bridge.acquire(AcquireDelivery {
        flow_id: "flow".into(),
        delivery_binding: binding(),
        binding_generation: 1,
        attempt_id: "attempt".into(),
        source_event_identifier: "source".into(),
    });
    assert!(matches!(
        result,
        Err(FlowPermitError::Rejected(DeliveryRejection::RefreshHeld(_)))
    ));
}
