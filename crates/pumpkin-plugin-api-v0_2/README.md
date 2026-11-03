# Integration branch plugin SDK v0.2

This fork provides `pumpkin-plugin-api-v0_2` alongside the unchanged v0.1
`pumpkin-plugin-api`. The integration server enables both hosts by default.
Existing v0.1 plugin components do not need rebuilding.

The v0.2 SDK is adapted from Phoenixxo's upstream migration, PR #3962:
https://github.com/Pumpkin-MC/Pumpkin/pull/3962
(source revision `0740d3f624d94e35c0e8a16b0839d895f9670a6d`).
It targets this branch's existing v0.2 WIT, includes all current event markers,
and retains generated `Item` and `BlockType` enums without requiring component
composition. No event records or fields are changed by this migration.

## Migrating a plugin

Keep your existing Rust import name by changing the dependency to:

```toml
[dependencies]
pumpkin-plugin-api = { package = "pumpkin-plugin-api-v0_2", git = "https://github.com/vxl-gg/Pumpkin", branch = "integration" }
```

Pin `rev` instead of `branch` when distributing plugins so the SDK and server
use the same WIT revision. The crate name in Rust remains `pumpkin_plugin_api`.

- Make `Plugin::on_load`, `on_unload`, and `handle_ipc_message` async.
  `new` and `metadata` remain synchronous; metadata must not call host imports.
- Make event handlers, commands, suggestions, AI goals and generation callbacks
  async. Await async host calls and SDK helpers. Follow the compiler's signatures:
  resource constructors, command builders, event registration and scheduling
  remain synchronous.
- Event markers are exported from `events`, for example
  `events::{EventHandler, EventData, PlayerChatEvent}`. Event handlers still take
  owned event data and return it. `register_event_handler` now returns a handler
  ID directly, rather than `Result<u32>`.
- Permission checks on command senders no longer take a server argument:
  use `sender.has_permission("node".to_string()).await`.
- Schedule tasks through `scheduler::schedule_delayed_task` or
  `schedule_repeating_task`, passing `|server| async move { ... }`.
- Register AI goals through `ai::register_ai_goal` and generators through
  `worldgen::register_chunk_generator`. Generation callbacks receive the raw
  WIT chunk buffer; await its async methods.
- Use `register_plugin!(YourPlugin)` once in the plugin crate. It exports all
  required v0.2 callbacks, including GameTest callbacks.
- Do not hold mutex guards or mutable borrows across `.await`. Other callbacks
  may run while a host call is suspended. Guest futures do not need `Send`.
- `pumpkin-plugin-utils` on this branch now targets v0.2 as well. Existing
  v0.1 plugin binaries remain supported, but rebuild plugins using the utilities
  with the v0.2 SDK. Do not link both SDK versions into one component.

Build plugins with `cargo build --release --target wasm32-wasip2`. Unoptimized
components can exceed Wasm's locals limit when lifting the large event variant;
for development, set `CARGO_PROFILE_DEV_OPT_LEVEL=1`.

Build the integration server normally with `cargo build --locked -p pumpkin`.
For a v0.1-only server build, use
`cargo build --locked -p pumpkin --no-default-features --features plugin-v0_1`.

## Validation

`cargo test -p pumpkin-plugin-api-v0_2` builds an optimized development component
and checks mixed sync/async exports, suspended callback reentry, event
cancellation, commands and suggestions, tasks, IPC, AI, generation and SDK helper
calls against a Wasmtime test host. It requires the `wasm32-wasip2` target.

Before deploying, load an existing v0.1 plugin alongside a migrated v0.2 plugin
on the integration server. Exercise their commands and a cancellable event,
then stop the server and check both unload callbacks and the log.
