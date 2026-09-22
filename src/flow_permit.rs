//! The Message-to-Flow permit seam.
//!
//! This module transports only the producer-owned Flow query and response
//! values. It deliberately has no sender identity input: a Flow permit is
//! delivery admission, not proof of the logical sender.

use signal_flow::{
    AcquireDelivery, DeliveryPermit, DeliveryRejection, Query, ReleaseConfirmed, Response,
};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FlowPermitError {
    #[error("Flow permit service is unavailable")]
    Unavailable,
    #[error("Flow rejected the permit operation: {0:?}")]
    Rejected(DeliveryRejection),
    #[error("Flow returned an unexpected response to {operation}")]
    UnexpectedResponse { operation: &'static str },
}

/// A client implemented only by an actual Flow Nexus transport. Tests may use
/// a fixture, but Message has no fallback that fabricates a permit response.
pub trait FlowPermitTransport {
    fn query(&self, request: Query) -> Result<Response, FlowPermitError>;
}

pub struct FlowPermitBridge<T> {
    transport: T,
}

impl<T> FlowPermitBridge<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T: FlowPermitTransport> FlowPermitBridge<T> {
    pub fn acquire(&self, request: AcquireDelivery) -> Result<DeliveryPermit, FlowPermitError> {
        match self.transport.query(Query::AcquireDelivery(request))? {
            Response::DeliveryGranted(permit) | Response::DeliveryAlreadyGranted(permit) => {
                Ok(permit)
            }
            Response::DeliveryAcquireRejected(rejection) => Err(FlowPermitError::Rejected(rejection)),
            _ => Err(FlowPermitError::UnexpectedResponse {
                operation: "AcquireDelivery",
            }),
        }
    }

    pub fn release_confirmed(
        &self,
        request: ReleaseConfirmed,
    ) -> Result<(), FlowPermitError> {
        match self.transport.query(Query::ReleaseConfirmed(request))? {
            Response::DeliveryReleased(_) | Response::DeliveryAlreadyReleased(_) => Ok(()),
            Response::DeliveryReleaseRejected(rejection) => Err(FlowPermitError::Rejected(rejection)),
            _ => Err(FlowPermitError::UnexpectedResponse {
                operation: "ReleaseConfirmed",
            }),
        }
    }
}
