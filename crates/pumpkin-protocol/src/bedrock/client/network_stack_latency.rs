use crate::serial::PacketWrite;
use pumpkin_macros::packet;

/// Requests an application round-trip measurement from the client.
#[derive(Debug, PacketWrite)]
#[packet(115)]
pub struct CNetworkStackLatency {
    /// Opaque creation timestamp echoed by the recipient.
    pub timestamp: u64,
    /// Whether the recipient must send a response with this timestamp.
    pub needs_response: bool,
}
