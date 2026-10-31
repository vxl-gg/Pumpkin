use std::{collections::VecDeque, time::Instant};

use pumpkin_protocol::bedrock::{
    client::network_stack_latency::CNetworkStackLatency,
    server::network_stack_latency::SNetworkStackLatency,
};

use crate::{entity::player::Player, net::bedrock::BedrockClient};

#[derive(Default)]
pub(in crate::net::bedrock) struct PingTracker {
    next_id: u64,
    pending: VecDeque<(u64, Instant)>,
}

impl PingTracker {
    pub(in crate::net::bedrock) fn start(&mut self, now: Instant) -> u64 {
        self.next_id = self.next_id.wrapping_add(1);
        // Match Java's bounded in-flight keep-alive queue.
        if self.pending.len() == 16 {
            self.pending.pop_front();
        }
        self.pending.push_back((self.next_id, now));
        self.next_id
    }

    pub(in crate::net::bedrock) fn finish(&mut self, id: u64, now: Instant) -> Option<u32> {
        let index = self
            .pending
            .iter()
            .position(|(pending_id, _)| *pending_id == id)?;
        let (_, sent) = self.pending.remove(index)?;
        Some(
            now.saturating_duration_since(sent)
                .as_millis()
                .min(u128::from(u32::MAX)) as u32,
        )
    }
}

impl BedrockClient {
    pub fn handle_network_stack_latency(&self, player: &Player, packet: &SNetworkStackLatency) {
        if packet.needs_response {
            self.try_enqueue_client_packet(&CNetworkStackLatency {
                timestamp: packet.timestamp,
                needs_response: false,
            });
            return;
        }
        let sample = self
            .ping_tracker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finish(packet.timestamp, Instant::now());
        if let Some(sample) = sample {
            player.update_ping(sample);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_matching_replies_measure_elapsed_time_and_cannot_be_replayed() {
        let mut tracker = PingTracker::default();
        let sent = Instant::now();
        let first = tracker.start(sent);
        let second = tracker.start(sent + Duration::from_millis(10));
        let received = sent + Duration::from_millis(110);
        assert_eq!(tracker.finish(999, received), None);
        assert_eq!(tracker.finish(second, received), Some(100));
        assert_eq!(tracker.finish(second, received), None);
        assert_eq!(tracker.finish(first, received), Some(110));
    }

    #[test]
    fn unanswered_probes_are_bounded_and_evicted_replies_are_ignored() {
        let mut tracker = PingTracker::default();
        let now = Instant::now();
        let first = tracker.start(now);
        for _ in 0..16 {
            tracker.start(now);
        }
        assert_eq!(tracker.pending.len(), 16);
        assert_eq!(tracker.finish(first, now), None);
    }
}
