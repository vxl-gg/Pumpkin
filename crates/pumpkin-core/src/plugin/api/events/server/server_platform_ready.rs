use pumpkin_macros::Event;

/// A platform on which players can connect to the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerPlatform {
    Java,
    Bedrock,
}

/// An event that fires once per listening platform during startup.
///
/// Connection processing runs concurrently with handlers of this event, so handlers
/// can wait for connections. Disabled platforms and failed listeners do not emit it.
/// Plugins loaded after startup do not receive this event.
#[derive(Event, Clone)]
pub struct ServerPlatformReadyEvent {
    pub platform: ServerPlatform,
}

impl ServerPlatformReadyEvent {
    #[must_use]
    pub const fn new(platform: ServerPlatform) -> Self {
        Self { platform }
    }
}
