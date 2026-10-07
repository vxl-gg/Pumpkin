use crate::wit::pumpkin::plugin::event::{Event, EventType, ServerPlatformReadyEventData};

use super::super::FromIntoEvent;

/// An event that fires once per listening platform during startup.
///
/// The data identifies Java or Bedrock. Connections are processed concurrently
/// with handlers. Disabled platforms, failed listeners, and plugins loaded after
/// startup do not receive this event.
pub struct ServerPlatformReadyEvent;
impl FromIntoEvent for ServerPlatformReadyEvent {
    const EVENT_TYPE: EventType = EventType::ServerPlatformReadyEvent;
    type Data = ServerPlatformReadyEventData;

    fn data_from_event(event: Event) -> Self::Data {
        match event {
            Event::ServerPlatformReadyEvent(data) => data,
            _ => panic!("unexpected event"),
        }
    }

    fn data_into_event(data: Self::Data) -> Event {
        Event::ServerPlatformReadyEvent(data)
    }
}
