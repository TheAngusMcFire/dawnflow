# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
cargo build
cargo build --features nats       # with NATS backend (default)
cargo build --features in_memory_only

# Test via examples
cargo run --example in_memory --features in_memory_only
cargo run --example nats --features nats   # requires NATS server

# Run NATS server (Nushell)
./etc/start-nats.nu
# or manually: nats-server -c etc/nats-config.conf

# NATS example uses env var:
NATS_CONNECTION_STRING=nats://localhost:4222 cargo run --example nats --features nats
```

Unit tests: `cargo test --features nats,dawnjection` (connection strings, subject tokens, encoding compatibility); the examples serve as integration tests against a NATS server. Delivery semantics and constraints are in `README.md`.

## Architecture

Dawnflow is a Rust async message-passing library that provides a backend-agnostic pub/sub and request/response abstraction. Two backends exist (mutually exclusive via feature flags):

- **`nats`** (default): NATS broker backend using MessagePack serialization with field names (`with_struct_map` / `to_vec_named`)
- **`in_memory_only`**: In-process event bus using `Any` for zero-copy type erasure

### Message patterns

Three communication patterns are supported via `Publisher`:
- **pub_sub**: fire-and-forget broadcast (multiple receivers)
- **pub_cons**: consume/acknowledge (single receiver)
- **pub_req**: request/response with timeout

### Core abstractions

**`Publisher`** (`src/publisher.rs`) — the user-facing API. Wraps a `PublisherBackend` enum (either `InMemory` or `Bytes`) and a `PublisherSerializer` (InMemory / Binary/MessagePack / JSON). Dispatches to the appropriate backend.

**`Handler` trait** (`src/handlers.rs`) — inspired by axum's extractor pattern. Handler functions are arbitrary async fns whose parameters implement `FromRequestBody` or `FromRequestMetadata`. The `impl_handler!` macro generates implementations for tuples up to 16 params. `Req<T>` is the primary body extractor.

**`HandlerRegistry`** (`src/registry.rs`) — maps message type names (via `std::any::type_name::<T>()`, last segment) to boxed handler closures. Separate maps for `subscribers`, `consumers`, and `handlers` (req/resp).

**In-memory backend** (`src/in_memory_publisher_backend.rs`) — uses tokio mpsc channels internally. No serialization; `Box<dyn Any + Send>` is cloned via `AnyCloneFactory` trait to fan out to subscribers.

**NATS backend** (`src/nats/`):
- `publisher_backend.rs`: wraps `async_nats::Client`, publishes to subjects `subscriber.{name}`, `consumer.{name}`, `request.{name}`; requests are bounded by their timeout (default 10 s) as a whole, a reply with the `Dawnflow-Error` header becomes `PublisherError::Remote`
- `connection.rs`: parses the connection string (`url` crate, comma-separated server lists) into `ConnectOptions` — `user:pass@` / `token@` auth (percent-decoded; async-nats itself ignores URL credentials), `tls://` scheme, and query params `tls_ca`, `tls_cert`+`tls_key`, `tls_first`, `creds`. Credentials/query are stripped from the server address handed to async-nats. Used by both publisher backend and dispatcher
- `dispatcher.rs`: `NatsDispatcher` subscribes to all three subject prefixes (`consumer.*` and `request.*` as queue subscribers, the subject being the queue group, so one replica handles each message; `subscriber.*` fans out), routes to the registry, and executes handlers per `DispatcherConfig` / `ExecutionMode`: `Concurrent { max_in_flight }` (task per message, default 10 in flight, unordered) or `Ordered` (single worker per handler type, strict arrival order, panics caught per message via `catch_unwind`). Per subscription, a reader task drains the NATS client buffer into an in-process queue (`queue_capacity`, default unbounded = no client-side drops; backlog growth is logged past `backlog_warn_threshold`). Readers and the async-nats connection handler run on a dedicated small runtime (`dawnflow-ingestion` thread) so CPU-heavy handlers can't starve ingestion. NATS connection events (slow consumer = dropped messages, disconnects) are logged via `event_callback`. Reply publishing for request handlers happens inside the handler task; a failing, panicking or unsendable handler answers an empty reply with the `Dawnflow-Error` header. `DispatcherConfig::on_reconnect` is called after a reconnect. Graceful shutdown via `elegant_departure`: the readers stop (unsubscribe), queued and running handlers finish, and the ingestion runtime (with the connection) is dropped only after the last worker ended (at most 30 s after shutdown), so their replies still go out.

### Message flow

```
Publisher::pub_sub/pub_cons/pub_req
  → serializes payload (MessagePack or type-erased Any)
  → sends to backend (NATS subject or mpsc channel)
  → Dispatcher receives and routes by type name
  → HandlerRegistry looks up handler
  → Handler fn extracts params via From* traits, returns Response
  → (for pub_req) response sent back to caller
```

### Feature flag constraint

Enabling both `in_memory` and `nats` features simultaneously is a compile error (`compile_error!` in `src/lib.rs`).
