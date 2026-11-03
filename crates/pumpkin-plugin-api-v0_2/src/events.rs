//! Event handlers for the async v0.2 world
//!
//! # How an event reaches your code
//!
//! The server fires an event, the host calls the `handle-event(id, server, event)` export, and
//! the dispatcher looks `id` up in the table that [`Context::register_event_handler`] filled; your
//! [`EventHandler`] gets the event data by value and hands back the data the host should use
//!
//! Nothing is borrowed from the host across that call, so a handler can `.await` as often as it
//! likes and the event data is still its own afterwards
//!
//! # Where to look
//!
//! * [`EventHandler`] is the trait you implement
//! * [`Context::register_event_handler`] registers it and explains `blocking`
//! * [`define_event!`](crate::define_event) adds an event marker that this module does not ship

use std::{marker::PhantomData, sync::Arc};

pub use crate::host::event::{Event, EventPriority, EventType};
use crate::{
    Context, Server,
    registry::{LocalBoxFuture, Registry},
};

static EVENT_HANDLERS: Registry<dyn ErasedEventHandler> = Registry::new();

/// Ties an event marker type to its WIT data record and [`EventType`] discriminant
///
/// Implement it with [`define_event!`](crate::define_event) instead of by hand; a marker is a unit
/// struct that exists so [`EventHandler`] and [`Context::register_event_handler`] can name an
/// event type
pub trait FromIntoEvent: Sized {
    /// The discriminant the host uses to identify this event when a handler is registered
    const EVENT_TYPE: EventType;

    /// The WIT record this event carries
    type Data;

    /// Extracts the event data from an [`Event`]
    ///
    /// # Errors
    ///
    /// Returns the event back, boxed because [`Event`] is over 900 bytes, when it is a different
    /// variant than this marker
    ///
    /// The dispatcher hands that event to the host untouched, so an id that points at the wrong
    /// handler is a host bug the plugin survives instead of a trap
    fn data_from_event(event: Event) -> Result<Self::Data, Box<Event>>;

    /// Wraps event data back into the matching [`Event`] variant
    fn data_into_event(data: Self::Data) -> Event;
}

/// The data type carried by an event
pub type EventData<E> = <E as FromIntoEvent>::Data;

/// A handler for one event type
///
/// Register an implementation with [`Context::register_event_handler`]
///
/// # Examples
///
/// A handler that awaits a permission check before it decides whether the chat goes through:
///
/// ```rust,ignore
/// struct ChatGuard;
///
/// impl EventHandler<PlayerChatEvent> for ChatGuard {
///     async fn handle(
///         &self,
///         _server: Server,
///         mut event: EventData<PlayerChatEvent>,
///     ) -> EventData<PlayerChatEvent> {
///         if !event.player.has_permission("chat.send".to_string()).await {
///             event.cancelled = true;
///         }
///         event
///     }
/// }
/// ```
#[allow(async_fn_in_trait, reason = "guest futures are intentionally not Send")]
pub trait EventHandler<E: FromIntoEvent>: Send + Sync + 'static {
    /// Processes an event and returns the data the host should use afterwards
    ///
    /// The server handle and the event data are owned, so nothing is borrowed from the host across
    /// an await and the handler can suspend as often as it needs to
    ///
    /// The host treats a returned variant that differs from the dispatched one as a handler error
    /// and keeps the original event; the data type pins the variant, so this signature cannot
    /// produce one
    ///
    /// # Reentry
    ///
    /// The host can start another dispatch while this one is suspended, which is why the receiver
    /// is `&self`; shared mutable state needs interior mutability, and no lock guard may be held
    /// across an await
    async fn handle(&self, server: Server, event: E::Data) -> E::Data;
}

trait ErasedEventHandler: Send + Sync {
    fn handle_erased(&self, server: Server, event: Event) -> LocalBoxFuture<'_, Event>;
}

struct HandlerWrapper<E, H> {
    handler: H,
    _event: PhantomData<fn() -> E>,
}

impl<E: FromIntoEvent, H: EventHandler<E>> ErasedEventHandler for HandlerWrapper<E, H> {
    fn handle_erased(&self, server: Server, event: Event) -> LocalBoxFuture<'_, Event> {
        Box::pin(async move {
            match E::data_from_event(event) {
                Ok(data) => E::data_into_event(self.handler.handle(server, data).await),
                Err(event) => *event,
            }
        })
    }
}

pub(crate) async fn dispatch(event_id: u32, server: Server, event: Event) -> Event {
    // The registry lock is released before the handler runs
    let Some(handler) = EVENT_HANDLERS.get(event_id) else {
        return event;
    };
    handler.handle_erased(server, event).await
}

impl Context {
    /// Registers an event handler and returns its handler id
    ///
    /// The handler is stored in this crate's table under the id, and the host is told the id, the
    /// event type, the priority and `blocking`; registering is a synchronous host call, and only
    /// the handler is async
    ///
    /// `blocking` says whether the dispatcher uses the value the handler returns, and it does not
    /// say the callback blocks an operating system thread: the host applies the returned event for
    /// a blocking handler and ignores it for a notification handler, so a notification handler
    /// must not count on its edits being seen
    ///
    /// `event_priority` decides when this handler runs relative to the others for the same event
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// context.register_event_handler::<PlayerChatEvent, _>(
    ///     ChatGuard,
    ///     EventPriority::Normal,
    ///     true,
    /// );
    /// ```
    pub fn register_event_handler<E, H>(
        &self,
        handler: H,
        event_priority: EventPriority,
        blocking: bool,
    ) -> u32
    where
        E: FromIntoEvent + 'static,
        H: EventHandler<E>,
    {
        let id = EVENT_HANDLERS.register(Arc::new(HandlerWrapper {
            handler,
            _event: PhantomData::<fn() -> E>,
        }));
        self.register_event(id, E::EVENT_TYPE, event_priority, blocking);
        id
    }
}

/// Defines an event marker type that implements [`FromIntoEvent`]
///
/// The marker name has to match both the [`Event`] variant and the [`EventType`] case, because the
/// macro uses it for all three
///
/// # Examples
///
/// ```rust,ignore
/// pumpkin_plugin_api_v0_2::define_event! {
///     /// A player finished joining the server
///     PlayerJoinEvent => pumpkin_plugin_api_v0_2::host::event::PlayerJoinEventData
/// }
/// ```
#[macro_export]
macro_rules! define_event {
    ($(#[$meta:meta])* $name:ident => $data:ty) => {
        $(#[$meta])*
        pub struct $name;

        impl $crate::events::FromIntoEvent for $name {
            const EVENT_TYPE: $crate::events::EventType = $crate::events::EventType::$name;
            type Data = $data;

            fn data_from_event(
                event: $crate::events::Event,
            ) -> ::core::result::Result<Self::Data, ::std::boxed::Box<$crate::events::Event>> {
                match event {
                    $crate::events::Event::$name(data) => Ok(data),
                    other => Err(::std::boxed::Box::new(other)),
                }
            }

            fn data_into_event(data: Self::Data) -> $crate::events::Event {
                $crate::events::Event::$name(data)
            }
        }
    };
}

define_event! {
    /// Player join event.
    PlayerJoinEvent => crate::host::event::PlayerJoinEventData
}

define_event! {
    /// Player leave event.
    PlayerLeaveEvent => crate::host::event::PlayerLeaveEventData
}

define_event! {
    /// Player login event.
    PlayerLoginEvent => crate::host::event::PlayerLoginEventData
}

define_event! {
    /// Player chat event.
    PlayerChatEvent => crate::host::event::PlayerChatEventData
}

define_event! {
    /// Player command send event.
    PlayerCommandSendEvent => crate::host::event::PlayerCommandSendEventData
}

define_event! {
    /// Player permission check event.
    PlayerPermissionCheckEvent => crate::host::event::PlayerPermissionCheckEventData
}

define_event! {
    /// Player move event.
    PlayerMoveEvent => crate::host::event::PlayerMoveEventData
}

define_event! {
    /// Player teleport event.
    PlayerTeleportEvent => crate::host::event::PlayerTeleportEventData
}

define_event! {
    /// Player change world event.
    PlayerChangeWorldEvent => crate::host::event::PlayerChangeWorldEventData
}

define_event! {
    /// Player respawn event.
    PlayerRespawnEvent => crate::host::event::PlayerRespawnEventData
}

define_event! {
    /// Player exp change event.
    PlayerExpChangeEvent => crate::host::event::PlayerExpChangeEventData
}

define_event! {
    /// Player item held event.
    PlayerItemHeldEvent => crate::host::event::PlayerItemHeldEventData
}

define_event! {
    /// Player changed main hand event.
    PlayerChangedMainHandEvent => crate::host::event::PlayerChangedMainHandEventData
}

define_event! {
    /// Player gamemode change event.
    PlayerGamemodeChangeEvent => crate::host::event::PlayerGamemodeChangeEventData
}

define_event! {
    /// Player custom payload event.
    PlayerCustomPayloadEvent => crate::host::event::PlayerCustomPayloadEventData
}

define_event! {
    /// Player fish event.
    PlayerFishEvent => crate::host::event::PlayerFishEventData
}

define_event! {
    /// Player egg throw event.
    PlayerEggThrowEvent => crate::host::event::PlayerEggThrowEventData
}

define_event! {
    /// Player interact unknown entity event.
    PlayerInteractUnknownEntityEvent => crate::host::event::PlayerInteractUnknownEntityEventData
}

define_event! {
    /// Player interact entity event.
    PlayerInteractEntityEvent => crate::host::event::PlayerInteractEntityEventData
}

define_event! {
    /// Player interact event.
    PlayerInteractEvent => crate::host::event::PlayerInteractEventData
}

define_event! {
    /// Player toggle sneak event.
    PlayerToggleSneakEvent => crate::host::event::PlayerToggleSneakEventData
}

define_event! {
    /// Player toggle flight event.
    PlayerToggleFlightEvent => crate::host::event::PlayerToggleFlightEventData
}

define_event! {
    /// Player toggle sprint event.
    PlayerToggleSprintEvent => crate::host::event::PlayerToggleSprintEventData
}

define_event! {
    /// Inventory click event.
    InventoryClickEvent => crate::host::event::InventoryClickEventData
}

define_event! {
    /// Inventory close event.
    InventoryCloseEvent => crate::host::event::InventoryCloseEventData
}

define_event! {
    /// Block redstone event.
    BlockRedstoneEvent => crate::host::event::BlockRedstoneEventData
}

define_event! {
    /// Block break event.
    BlockBreakEvent => crate::host::event::BlockBreakEventData
}

define_event! {
    /// Block burn event.
    BlockBurnEvent => crate::host::event::BlockBurnEventData
}

define_event! {
    /// Block can build event.
    BlockCanBuildEvent => crate::host::event::BlockCanBuildEventData
}

define_event! {
    /// Block grow event.
    BlockGrowEvent => crate::host::event::BlockGrowEventData
}

define_event! {
    /// Block place event.
    BlockPlaceEvent => crate::host::event::BlockPlaceEventData
}

define_event! {
    /// Dialog click action event.
    DialogClickActionEvent => crate::host::event::DialogClickActionEventData
}

define_event! {
    /// Dialog show event.
    DialogShowEvent => crate::host::event::DialogShowEventData
}

define_event! {
    /// Dialog clear event.
    DialogClearEvent => crate::host::event::DialogClearEventData
}

define_event! {
    /// Bedrock form response event.
    BedrockFormResponseEvent => crate::host::event::BedrockFormResponseEventData
}

define_event! {
    /// Server command event.
    ServerCommandEvent => crate::host::event::ServerCommandEventData
}

define_event! {
    /// Server list ping event.
    ServerListPingEvent => crate::host::event::ServerListPingEventData
}

define_event! {
    /// Server load event.
    ServerLoadEvent => crate::host::event::ServerLoadEventData
}

define_event! {
    /// Spawn change event.
    SpawnChangeEvent => crate::host::event::SpawnChangeEventData
}

define_event! {
    /// Server broadcast event.
    ServerBroadcastEvent => crate::host::event::ServerBroadcastEventData
}

define_event! {
    /// Server tick start event.
    ServerTickStartEvent => crate::host::event::ServerTickStartEventData
}

define_event! {
    /// Server tick end event.
    ServerTickEndEvent => crate::host::event::ServerTickEndEventData
}

define_event! {
    /// Packet received event.
    PacketReceivedEvent => crate::host::event::PacketReceivedEventData
}

define_event! {
    /// Packet sent event.
    PacketSentEvent => crate::host::event::PacketSentEventData
}

define_event! {
    /// Chunk load event.
    ChunkLoadEvent => crate::host::event::ChunkLoadEventData
}

define_event! {
    /// Chunk save event.
    ChunkSaveEvent => crate::host::event::ChunkSaveEventData
}

define_event! {
    /// Chunk send event.
    ChunkSendEvent => crate::host::event::ChunkSendEventData
}

define_event! {
    /// Entity damage event.
    EntityDamageEvent => crate::host::event::EntityDamageEventData
}

define_event! {
    /// Entity death event.
    EntityDeathEvent => crate::host::event::EntityDeathEventData
}

define_event! {
    /// Player death event.
    PlayerDeathEvent => crate::host::event::PlayerDeathEventData
}

define_event! {
    /// Entity spawn event.
    EntitySpawnEvent => crate::host::event::EntitySpawnEventData
}

define_event! {
    /// Entity combust event.
    EntityCombustEvent => crate::host::event::EntityCombustEventData
}

define_event! {
    /// Entity regain health event.
    EntityRegainHealthEvent => crate::host::event::EntityRegainHealthEventData
}

define_event! {
    /// Entity air change event.
    EntityAirChangeEvent => crate::host::event::EntityAirChangeEventData
}

define_event! {
    /// Entity breed event.
    EntityBreedEvent => crate::host::event::EntityBreedEventData
}

define_event! {
    /// Entity dismount event.
    EntityDismountEvent => crate::host::event::EntityDismountEventData
}

define_event! {
    /// Entity dye event.
    EntityDyeEvent => crate::host::event::EntityDyeEventData
}

define_event! {
    /// Entity enter love mode event.
    EntityEnterLoveModeEvent => crate::host::event::EntityEnterLoveModeEventData
}

define_event! {
    /// Entity explode event.
    EntityExplodeEvent => crate::host::event::EntityExplodeEventData
}

define_event! {
    /// Entity mount event.
    EntityMountEvent => crate::host::event::EntityMountEventData
}

define_event! {
    /// Entity pickup item event.
    EntityPickupItemEvent => crate::host::event::EntityPickupItemEventData
}

define_event! {
    /// Entity portal event.
    EntityPortalEvent => crate::host::event::EntityPortalEventData
}

define_event! {
    /// Entity resurrect event.
    EntityResurrectEvent => crate::host::event::EntityResurrectEventData
}

define_event! {
    /// Entity shoot bow event.
    EntityShootBowEvent => crate::host::event::EntityShootBowEventData
}

define_event! {
    /// Entity tame event.
    EntityTameEvent => crate::host::event::EntityTameEventData
}

define_event! {
    /// Entity target event.
    EntityTargetEvent => crate::host::event::EntityTargetEventData
}

define_event! {
    /// Entity teleport event.
    EntityTeleportEvent => crate::host::event::EntityTeleportEventData
}

define_event! {
    /// Entity toggle glide event.
    EntityToggleGlideEvent => crate::host::event::EntityToggleGlideEventData
}

define_event! {
    /// Entity transform event.
    EntityTransformEvent => crate::host::event::EntityTransformEventData
}

define_event! {
    /// Player item consume event.
    PlayerItemConsumeEvent => crate::host::event::PlayerItemConsumeEventData
}

define_event! {
    /// Player item damage event.
    PlayerItemDamageEvent => crate::host::event::PlayerItemDamageEventData
}

define_event! {
    /// Player drop item event.
    PlayerDropItemEvent => crate::host::event::PlayerDropItemEventData
}

define_event! {
    /// Player bed enter event.
    PlayerBedEnterEvent => crate::host::event::PlayerBedEnterEventData
}

define_event! {
    /// Player bed leave event.
    PlayerBedLeaveEvent => crate::host::event::PlayerBedLeaveEventData
}

define_event! {
    /// Player bucket empty event.
    PlayerBucketEmptyEvent => crate::host::event::PlayerBucketEmptyEventData
}

define_event! {
    /// Player bucket fill event.
    PlayerBucketFillEvent => crate::host::event::PlayerBucketFillEventData
}

define_event! {
    /// Block damage event.
    BlockDamageEvent => crate::host::event::BlockDamageEventData
}

define_event! {
    /// Block ignite event.
    BlockIgniteEvent => crate::host::event::BlockIgniteEventData
}

define_event! {
    /// Block from to event.
    BlockFromToEvent => crate::host::event::BlockFromToEventData
}

define_event! {
    /// Block form event.
    BlockFormEvent => crate::host::event::BlockFormEventData
}

define_event! {
    /// Block fade event.
    BlockFadeEvent => crate::host::event::BlockFadeEventData
}

define_event! {
    /// Block dispense event.
    BlockDispenseEvent => crate::host::event::BlockDispenseEventData
}

define_event! {
    /// Block explode event.
    BlockExplodeEvent => crate::host::event::BlockExplodeEventData
}

define_event! {
    /// Block physics event.
    BlockPhysicsEvent => crate::host::event::BlockPhysicsEventData
}

define_event! {
    /// Block piston extend event.
    BlockPistonExtendEvent => crate::host::event::BlockPistonExtendEventData
}

define_event! {
    /// Block piston retract event.
    BlockPistonRetractEvent => crate::host::event::BlockPistonRetractEventData
}

define_event! {
    /// Note play event.
    NotePlayEvent => crate::host::event::NotePlayEventData
}

define_event! {
    /// Sign change event.
    SignChangeEvent => crate::host::event::SignChangeEventData
}

define_event! {
    /// Sponge absorb event.
    SpongeAbsorbEvent => crate::host::event::SpongeAbsorbEventData
}

define_event! {
    /// Tnt prime event.
    TntPrimeEvent => crate::host::event::TntPrimeEventData
}

define_event! {
    /// Weather change event.
    WeatherChangeEvent => crate::host::event::WeatherChangeEventData
}

define_event! {
    /// Thunder change event.
    ThunderChangeEvent => crate::host::event::ThunderChangeEventData
}

define_event! {
    /// World load event.
    WorldLoadEvent => crate::host::event::WorldLoadEventData
}

define_event! {
    /// World unload event.
    WorldUnloadEvent => crate::host::event::WorldUnloadEventData
}

define_event! {
    /// Async structure generate event.
    AsyncStructureGenerateEvent => crate::host::event::AsyncStructureGenerateEventData
}

define_event! {
    /// Async structure spawn event.
    AsyncStructureSpawnEvent => crate::host::event::AsyncStructureSpawnEventData
}

define_event! {
    /// Chunk populate event.
    ChunkPopulateEvent => crate::host::event::ChunkPopulateEventData
}

define_event! {
    /// Chunk unload event.
    ChunkUnloadEvent => crate::host::event::ChunkUnloadEventData
}

define_event! {
    /// Entities load event.
    EntitiesLoadEvent => crate::host::event::EntitiesLoadEventData
}

define_event! {
    /// Entities unload event.
    EntitiesUnloadEvent => crate::host::event::EntitiesUnloadEventData
}

define_event! {
    /// Generic game event.
    GenericGameEvent => crate::host::event::GenericGameEventData
}

define_event! {
    /// Loot generate event.
    LootGenerateEvent => crate::host::event::LootGenerateEventData
}

define_event! {
    /// Portal create event.
    PortalCreateEvent => crate::host::event::PortalCreateEventData
}

define_event! {
    /// Structure grow event.
    StructureGrowEvent => crate::host::event::StructureGrowEventData
}

define_event! {
    /// Time skip event.
    TimeSkipEvent => crate::host::event::TimeSkipEventData
}

define_event! {
    /// World init event.
    WorldInitEvent => crate::host::event::WorldInitEventData
}

define_event! {
    /// World save event.
    WorldSaveEvent => crate::host::event::WorldSaveEventData
}

define_event! {
    /// Inventory open event.
    InventoryOpenEvent => crate::host::event::InventoryOpenEventData
}

define_event! {
    /// Inventory drag event.
    InventoryDragEvent => crate::host::event::InventoryDragEventData
}

define_event! {
    /// Craft item event.
    CraftItemEvent => crate::host::event::CraftItemEventData
}

define_event! {
    /// Furnace smelt event.
    FurnaceSmeltEvent => crate::host::event::FurnaceSmeltEventData
}

define_event! {
    /// Brew event.
    BrewEvent => crate::host::event::BrewEventData
}

define_event! {
    /// Brewing stand fuel event.
    BrewingStandFuelEvent => crate::host::event::BrewingStandFuelEventData
}

define_event! {
    /// Furnace burn event.
    FurnaceBurnEvent => crate::host::event::FurnaceBurnEventData
}

define_event! {
    /// Furnace extract event.
    FurnaceExtractEvent => crate::host::event::FurnaceExtractEventData
}

define_event! {
    /// Furnace start smelt event.
    FurnaceStartSmeltEvent => crate::host::event::FurnaceStartSmeltEventData
}

define_event! {
    /// Hopper inventory search event.
    HopperInventorySearchEvent => crate::host::event::HopperInventorySearchEventData
}

define_event! {
    /// Inventory creative event.
    InventoryCreativeEvent => crate::host::event::InventoryCreativeEventData
}

define_event! {
    /// Inventory interact event.
    InventoryInteractEvent => crate::host::event::InventoryInteractEventData
}

define_event! {
    /// Inventory move item event.
    InventoryMoveItemEvent => crate::host::event::InventoryMoveItemEventData
}

define_event! {
    /// Inventory pickup item event.
    InventoryPickupItemEvent => crate::host::event::InventoryPickupItemEventData
}

define_event! {
    /// Prepare anvil event.
    PrepareAnvilEvent => crate::host::event::PrepareAnvilEventData
}

define_event! {
    /// Prepare grindstone event.
    PrepareGrindstoneEvent => crate::host::event::PrepareGrindstoneEventData
}

define_event! {
    /// Prepare inventory result event.
    PrepareInventoryResultEvent => crate::host::event::PrepareInventoryResultEventData
}

define_event! {
    /// Prepare item craft event.
    PrepareItemCraftEvent => crate::host::event::PrepareItemCraftEventData
}

define_event! {
    /// Prepare smithing event.
    PrepareSmithingEvent => crate::host::event::PrepareSmithingEventData
}

define_event! {
    /// Smith item event.
    SmithItemEvent => crate::host::event::SmithItemEventData
}

define_event! {
    /// Trade select event.
    TradeSelectEvent => crate::host::event::TradeSelectEventData
}

define_event! {
    /// Vehicle block collision event.
    VehicleBlockCollisionEvent => crate::host::event::VehicleBlockCollisionEventData
}

define_event! {
    /// Vehicle collision event.
    VehicleCollisionEvent => crate::host::event::VehicleCollisionEventData
}

define_event! {
    /// Vehicle create event.
    VehicleCreateEvent => crate::host::event::VehicleCreateEventData
}

define_event! {
    /// Vehicle damage event.
    VehicleDamageEvent => crate::host::event::VehicleDamageEventData
}

define_event! {
    /// Vehicle destroy event.
    VehicleDestroyEvent => crate::host::event::VehicleDestroyEventData
}

define_event! {
    /// Vehicle enter event.
    VehicleEnterEvent => crate::host::event::VehicleEnterEventData
}

define_event! {
    /// Vehicle entity collision event.
    VehicleEntityCollisionEvent => crate::host::event::VehicleEntityCollisionEventData
}

define_event! {
    /// Vehicle exit event.
    VehicleExitEvent => crate::host::event::VehicleExitEventData
}

define_event! {
    /// Vehicle move event.
    VehicleMoveEvent => crate::host::event::VehicleMoveEventData
}

define_event! {
    /// Vehicle update event.
    VehicleUpdateEvent => crate::host::event::VehicleUpdateEventData
}

define_event! {
    /// Prepare item enchant event.
    PrepareItemEnchantEvent => crate::host::event::PrepareItemEnchantEventData
}

define_event! {
    /// Enchant item event.
    EnchantItemEvent => crate::host::event::EnchantItemEventData
}

define_event! {
    /// Map initialize event.
    MapInitializeEvent => crate::host::event::MapInitializeEventData
}

define_event! {
    /// Hanging break event.
    HangingBreakEvent => crate::host::event::HangingBreakEventData
}

define_event! {
    /// Hanging break by entity event.
    HangingBreakByEntityEvent => crate::host::event::HangingBreakByEntityEventData
}

define_event! {
    /// Hanging place event.
    HangingPlaceEvent => crate::host::event::HangingPlaceEventData
}

define_event! {
    /// Bell resonate event.
    BellResonateEvent => crate::host::event::BellResonateEventData
}

define_event! {
    /// Bell ring event.
    BellRingEvent => crate::host::event::BellRingEventData
}

define_event! {
    /// Block brush event.
    BlockBrushEvent => crate::host::event::BlockBrushEventData
}

define_event! {
    /// Block cook event.
    BlockCookEvent => crate::host::event::BlockCookEventData
}

define_event! {
    /// Block damage abort event.
    BlockDamageAbortEvent => crate::host::event::BlockDamageAbortEventData
}

define_event! {
    /// Block dispense armor event.
    BlockDispenseArmorEvent => crate::host::event::BlockDispenseArmorEventData
}

define_event! {
    /// Block dispense loot event.
    BlockDispenseLootEvent => crate::host::event::BlockDispenseLootEventData
}

define_event! {
    /// Block drop item event.
    BlockDropItemEvent => crate::host::event::BlockDropItemEventData
}

define_event! {
    /// Block exp event.
    BlockExpEvent => crate::host::event::BlockExpEventData
}

define_event! {
    /// Block fertilize event.
    BlockFertilizeEvent => crate::host::event::BlockFertilizeEventData
}

define_event! {
    /// Block multi place event.
    BlockMultiPlaceEvent => crate::host::event::BlockMultiPlaceEventData
}

define_event! {
    /// Block receive game event.
    BlockReceiveGameEvent => crate::host::event::BlockReceiveGameEventData
}

define_event! {
    /// Block shear entity event.
    BlockShearEntityEvent => crate::host::event::BlockShearEntityEventData
}

define_event! {
    /// Block spread event.
    BlockSpreadEvent => crate::host::event::BlockSpreadEventData
}

define_event! {
    /// Brewing start event.
    BrewingStartEvent => crate::host::event::BrewingStartEventData
}

define_event! {
    /// Campfire start event.
    CampfireStartEvent => crate::host::event::CampfireStartEventData
}

define_event! {
    /// Cauldron level change event.
    CauldronLevelChangeEvent => crate::host::event::CauldronLevelChangeEventData
}

define_event! {
    /// Crafter craft event.
    CrafterCraftEvent => crate::host::event::CrafterCraftEventData
}

define_event! {
    /// Entity block form event.
    EntityBlockFormEvent => crate::host::event::EntityBlockFormEventData
}

define_event! {
    /// Fluid level change event.
    FluidLevelChangeEvent => crate::host::event::FluidLevelChangeEventData
}

define_event! {
    /// Inventory block start event.
    InventoryBlockStartEvent => crate::host::event::InventoryBlockStartEventData
}

define_event! {
    /// Leaves decay event.
    LeavesDecayEvent => crate::host::event::LeavesDecayEventData
}

define_event! {
    /// Moisture change event.
    MoistureChangeEvent => crate::host::event::MoistureChangeEventData
}

define_event! {
    /// Sculk bloom event.
    SculkBloomEvent => crate::host::event::SculkBloomEventData
}

define_event! {
    /// Vault display item event.
    VaultDisplayItemEvent => crate::host::event::VaultDisplayItemEventData
}

define_event! {
    /// Creature spawn event.
    CreatureSpawnEvent => crate::host::event::CreatureSpawnEventData
}

define_event! {
    /// Ender dragon change phase event.
    EnderDragonChangePhaseEvent => crate::host::event::EnderDragonChangePhaseEventData
}

define_event! {
    /// Entity break door event.
    EntityBreakDoorEvent => crate::host::event::EntityBreakDoorEventData
}

define_event! {
    /// Entity change block event.
    EntityChangeBlockEvent => crate::host::event::EntityChangeBlockEventData
}

define_event! {
    /// Entity damage by block event.
    EntityDamageByBlockEvent => crate::host::event::EntityDamageByBlockEventData
}

define_event! {
    /// Entity damage by entity event.
    EntityDamageByEntityEvent => crate::host::event::EntityDamageByEntityEventData
}

define_event! {
    /// Entity drop item event.
    EntityDropItemEvent => crate::host::event::EntityDropItemEventData
}

define_event! {
    /// Entity enter block event.
    EntityEnterBlockEvent => crate::host::event::EntityEnterBlockEventData
}

define_event! {
    /// Entity exhaustion event.
    EntityExhaustionEvent => crate::host::event::EntityExhaustionEventData
}

define_event! {
    /// Entity interact event.
    EntityInteractEvent => crate::host::event::EntityInteractEventData
}

define_event! {
    /// Entity knockback event.
    EntityKnockbackEvent => crate::host::event::EntityKnockbackEventData
}

define_event! {
    /// Entity place event.
    EntityPlaceEvent => crate::host::event::EntityPlaceEventData
}

define_event! {
    /// Entity pose change event.
    EntityPoseChangeEvent => crate::host::event::EntityPoseChangeEventData
}

define_event! {
    /// Entity potion effect event.
    EntityPotionEffectEvent => crate::host::event::EntityPotionEffectEventData
}

define_event! {
    /// Entity spell cast event.
    EntitySpellCastEvent => crate::host::event::EntitySpellCastEventData
}

define_event! {
    /// Entity target living entity event.
    EntityTargetLivingEntityEvent => crate::host::event::EntityTargetLivingEntityEventData
}

define_event! {
    /// Entity toggle sit event.
    EntityToggleSitEvent => crate::host::event::EntityToggleSitEventData
}

define_event! {
    /// Entity toggle swim event.
    EntityToggleSwimEvent => crate::host::event::EntityToggleSwimEventData
}

define_event! {
    /// Explosion prime event.
    ExplosionPrimeEvent => crate::host::event::ExplosionPrimeEventData
}

define_event! {
    /// Firework explode event.
    FireworkExplodeEvent => crate::host::event::FireworkExplodeEventData
}

define_event! {
    /// Food level change event.
    FoodLevelChangeEvent => crate::host::event::FoodLevelChangeEventData
}

define_event! {
    /// Item despawn event.
    ItemDespawnEvent => crate::host::event::ItemDespawnEventData
}

define_event! {
    /// Item merge event.
    ItemMergeEvent => crate::host::event::ItemMergeEventData
}

define_event! {
    /// Item spawn event.
    ItemSpawnEvent => crate::host::event::ItemSpawnEventData
}

define_event! {
    /// Piglin barter event.
    PiglinBarterEvent => crate::host::event::PiglinBarterEventData
}

define_event! {
    /// Projectile hit event.
    ProjectileHitEvent => crate::host::event::ProjectileHitEventData
}

define_event! {
    /// Projectile launch event.
    ProjectileLaunchEvent => crate::host::event::ProjectileLaunchEventData
}

define_event! {
    /// Sheep dye wool event.
    SheepDyeWoolEvent => crate::host::event::SheepDyeWoolEventData
}

define_event! {
    /// Sheep regrow wool event.
    SheepRegrowWoolEvent => crate::host::event::SheepRegrowWoolEventData
}

define_event! {
    /// Slime split event.
    SlimeSplitEvent => crate::host::event::SlimeSplitEventData
}

define_event! {
    /// Strider temperature change event.
    StriderTemperatureChangeEvent => crate::host::event::StriderTemperatureChangeEventData
}

define_event! {
    /// Villager acquire trade event.
    VillagerAcquireTradeEvent => crate::host::event::VillagerAcquireTradeEventData
}

define_event! {
    /// Villager career change event.
    VillagerCareerChangeEvent => crate::host::event::VillagerCareerChangeEventData
}

define_event! {
    /// Villager replenish trade event.
    VillagerReplenishTradeEvent => crate::host::event::VillagerReplenishTradeEventData
}

define_event! {
    /// Warden anger change event.
    WardenAngerChangeEvent => crate::host::event::WardenAngerChangeEventData
}

define_event! {
    /// Area effect cloud apply event.
    AreaEffectCloudApplyEvent => crate::host::event::AreaEffectCloudApplyEventData
}

define_event! {
    /// Arrow body count change event.
    ArrowBodyCountChangeEvent => crate::host::event::ArrowBodyCountChangeEventData
}

define_event! {
    /// Bat toggle sleep event.
    BatToggleSleepEvent => crate::host::event::BatToggleSleepEventData
}

define_event! {
    /// Creeper power event.
    CreeperPowerEvent => crate::host::event::CreeperPowerEventData
}

define_event! {
    /// Entity combust by block event.
    EntityCombustByBlockEvent => crate::host::event::EntityCombustByBlockEventData
}

define_event! {
    /// Entity combust by entity event.
    EntityCombustByEntityEvent => crate::host::event::EntityCombustByEntityEventData
}

define_event! {
    /// Entity knockback by entity event.
    EntityKnockbackByEntityEvent => crate::host::event::EntityKnockbackByEntityEventData
}

define_event! {
    /// Entity portal enter event.
    EntityPortalEnterEvent => crate::host::event::EntityPortalEnterEventData
}

define_event! {
    /// Entity portal exit event.
    EntityPortalExitEvent => crate::host::event::EntityPortalExitEventData
}

define_event! {
    /// Entity remove event.
    EntityRemoveEvent => crate::host::event::EntityRemoveEventData
}

define_event! {
    /// Entity target block event.
    EntityTargetBlockEvent => crate::host::event::EntityTargetBlockEventData
}

define_event! {
    /// Entity unleash event.
    EntityUnleashEvent => crate::host::event::EntityUnleashEventData
}

define_event! {
    /// Exp bottle event.
    ExpBottleEvent => crate::host::event::ExpBottleEventData
}

define_event! {
    /// Horse jump event.
    HorseJumpEvent => crate::host::event::HorseJumpEventData
}

define_event! {
    /// Lingering potion splash event.
    LingeringPotionSplashEvent => crate::host::event::LingeringPotionSplashEventData
}

define_event! {
    /// Pig zap event.
    PigZapEvent => crate::host::event::PigZapEventData
}

define_event! {
    /// Pig zombie anger event.
    PigZombieAngerEvent => crate::host::event::PigZombieAngerEventData
}

define_event! {
    /// Potion splash event.
    PotionSplashEvent => crate::host::event::PotionSplashEventData
}

define_event! {
    /// Spawner spawn event.
    SpawnerSpawnEvent => crate::host::event::SpawnerSpawnEventData
}

define_event! {
    /// Trial spawner spawn event.
    TrialSpawnerSpawnEvent => crate::host::event::TrialSpawnerSpawnEventData
}

define_event! {
    /// Villager reputation change event.
    VillagerReputationChangeEvent => crate::host::event::VillagerReputationChangeEventData
}

define_event! {
    /// Async player chat event.
    AsyncPlayerChatEvent => crate::host::event::AsyncPlayerChatEventData
}

define_event! {
    /// Async player pre login event.
    AsyncPlayerPreLoginEvent => crate::host::event::AsyncPlayerPreLoginEventData
}

define_event! {
    /// Player advancement done event.
    PlayerAdvancementDoneEvent => crate::host::event::PlayerAdvancementDoneEventData
}

define_event! {
    /// Player animation event.
    PlayerAnimationEvent => crate::host::event::PlayerAnimationEventData
}

define_event! {
    /// Player armor stand manipulate event.
    PlayerArmorStandManipulateEvent => crate::host::event::PlayerArmorStandManipulateEventData
}

define_event! {
    /// Player bucket entity event.
    PlayerBucketEntityEvent => crate::host::event::PlayerBucketEntityEventData
}

define_event! {
    /// Player changed world event.
    PlayerChangedWorldEvent => crate::host::event::PlayerChangedWorldEventData
}

define_event! {
    /// Player channel event.
    PlayerChannelEvent => crate::host::event::PlayerChannelEventData
}

define_event! {
    /// Player command preprocess event.
    PlayerCommandPreprocessEvent => crate::host::event::PlayerCommandPreprocessEventData
}

define_event! {
    /// Player edit book event.
    PlayerEditBookEvent => crate::host::event::PlayerEditBookEventData
}

define_event! {
    /// Player elytra boost event.
    PlayerElytraBoostEvent => crate::host::event::PlayerElytraBoostEventData
}

define_event! {
    /// Player exp cooldown change event.
    PlayerExpCooldownChangeEvent => crate::host::event::PlayerExpCooldownChangeEventData
}

define_event! {
    /// Player harvest block event.
    PlayerHarvestBlockEvent => crate::host::event::PlayerHarvestBlockEventData
}

define_event! {
    /// Player hide entity event.
    PlayerHideEntityEvent => crate::host::event::PlayerHideEntityEventData
}

define_event! {
    /// Player item break event.
    PlayerItemBreakEvent => crate::host::event::PlayerItemBreakEventData
}

define_event! {
    /// Player item mend event.
    PlayerItemMendEvent => crate::host::event::PlayerItemMendEventData
}

define_event! {
    /// Player kick event.
    PlayerKickEvent => crate::host::event::PlayerKickEventData
}

define_event! {
    /// Player leash entity event.
    PlayerLeashEntityEvent => crate::host::event::PlayerLeashEntityEventData
}

define_event! {
    /// Player level change event.
    PlayerLevelChangeEvent => crate::host::event::PlayerLevelChangeEventData
}

define_event! {
    /// Player locale change event.
    PlayerLocaleChangeEvent => crate::host::event::PlayerLocaleChangeEventData
}

define_event! {
    /// Player name entity event.
    PlayerNameEntityEvent => crate::host::event::PlayerNameEntityEventData
}

define_event! {
    /// Player open sign event.
    PlayerOpenSignEvent => crate::host::event::PlayerOpenSignEventData
}

define_event! {
    /// Player portal event.
    PlayerPortalEvent => crate::host::event::PlayerPortalEventData
}

define_event! {
    /// Player pre login event.
    PlayerPreLoginEvent => crate::host::event::PlayerPreLoginEventData
}

define_event! {
    /// Player riptide event.
    PlayerRiptideEvent => crate::host::event::PlayerRiptideEventData
}

define_event! {
    /// Player shear entity event.
    PlayerShearEntityEvent => crate::host::event::PlayerShearEntityEventData
}

define_event! {
    /// Player show entity event.
    PlayerShowEntityEvent => crate::host::event::PlayerShowEntityEventData
}

define_event! {
    /// Player spawn change event.
    PlayerSpawnChangeEvent => crate::host::event::PlayerSpawnChangeEventData
}

define_event! {
    /// Player statistic increment event.
    PlayerStatisticIncrementEvent => crate::host::event::PlayerStatisticIncrementEventData
}

define_event! {
    /// Player swap hands event.
    PlayerSwapHandsEvent => crate::host::event::PlayerSwapHandsEventData
}

define_event! {
    /// Player take lectern book event.
    PlayerTakeLecternBookEvent => crate::host::event::PlayerTakeLecternBookEventData
}

define_event! {
    /// Player unleash entity event.
    PlayerUnleashEntityEvent => crate::host::event::PlayerUnleashEntityEventData
}

define_event! {
    /// Player velocity event.
    PlayerVelocityEvent => crate::host::event::PlayerVelocityEventData
}

define_event! {
    /// Player input event.
    PlayerInputEvent => crate::host::event::PlayerInputEventData
}

define_event! {
    /// Player interact at entity event.
    PlayerInteractAtEntityEvent => crate::host::event::PlayerInteractAtEntityEventData
}

define_event! {
    /// Player links send event.
    PlayerLinksSendEvent => crate::host::event::PlayerLinksSendEventData
}

define_event! {
    /// Player pickup arrow event.
    PlayerPickupArrowEvent => crate::host::event::PlayerPickupArrowEventData
}

define_event! {
    /// Player recipe book click event.
    PlayerRecipeBookClickEvent => crate::host::event::PlayerRecipeBookClickEventData
}

define_event! {
    /// Player recipe book settings change event.
    PlayerRecipeBookSettingsChangeEvent => crate::host::event::PlayerRecipeBookSettingsChangeEventData
}

define_event! {
    /// Player recipe discover event.
    PlayerRecipeDiscoverEvent => crate::host::event::PlayerRecipeDiscoverEventData
}

define_event! {
    /// Player register channel event.
    PlayerRegisterChannelEvent => crate::host::event::PlayerRegisterChannelEventData
}

define_event! {
    /// Player resource pack status event.
    PlayerResourcePackStatusEvent => crate::host::event::PlayerResourcePackStatusEventData
}

define_event! {
    /// Player spawn location event.
    PlayerSpawnLocationEvent => crate::host::event::PlayerSpawnLocationEventData
}

define_event! {
    /// Player unregister channel event.
    PlayerUnregisterChannelEvent => crate::host::event::PlayerUnregisterChannelEventData
}

define_event! {
    /// Raid finish event.
    RaidFinishEvent => crate::host::event::RaidFinishEventData
}

define_event! {
    /// Raid spawn wave event.
    RaidSpawnWaveEvent => crate::host::event::RaidSpawnWaveEventData
}

define_event! {
    /// Raid stop event.
    RaidStopEvent => crate::host::event::RaidStopEventData
}

define_event! {
    /// Raid trigger event.
    RaidTriggerEvent => crate::host::event::RaidTriggerEventData
}

define_event! {
    /// Lightning strike event.
    LightningStrikeEvent => crate::host::event::LightningStrikeEventData
}

define_event! {
    /// Server platform ready event.
    ServerPlatformReadyEvent => crate::host::event::ServerPlatformReadyEventData
}
