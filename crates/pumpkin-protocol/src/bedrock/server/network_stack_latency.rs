use crate::serial::PacketRead;
use pumpkin_macros::packet;

/// Measures round-trip time through the game packet stack in either direction.
#[derive(Debug, PacketRead)]
#[packet(115)]
pub struct SNetworkStackLatency {
    /// Opaque creation timestamp echoed by the recipient.
    pub timestamp: u64,
    /// Whether the recipient must send a response with this timestamp.
    pub needs_response: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bedrock::client::network_stack_latency::CNetworkStackLatency, serial::PacketWrite,
    };

    #[test]
    fn latency_packet_uses_fixed_little_endian_timestamp_and_boolean() {
        let mut bytes = Vec::new();
        CNetworkStackLatency {
            timestamp: 0x0102_0304_0506_0708,
            needs_response: true,
        }
        .write(&mut bytes)
        .unwrap();
        assert_eq!(bytes, [8, 7, 6, 5, 4, 3, 2, 1, 1]);
        let response =
            SNetworkStackLatency::read(&mut [8, 7, 6, 5, 4, 3, 2, 1, 0].as_slice()).unwrap();
        assert_eq!(response.timestamp, 0x0102_0304_0506_0708);
        assert!(!response.needs_response);
        for len in 0..9 {
            assert!(SNetworkStackLatency::read(&mut &bytes[..len]).is_err());
        }
    }
}
