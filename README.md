# dawnflow

Typed pub/sub, consumer and request/reply messaging over NATS (or in memory), with axum-style handlers. Architecture and commands: `CLAUDE.md`.

## Delivery

| Pattern | Subject | Receivers |
|---|---|---|
| `pub_sub` | `subscriber.<Type>[.<target>]` | every subscribed process |
| `pub_cons` | `consumer.<Type>[.<target>]` | one process (queue group = subject) |
| `pub_req` | `request.<Type>[.<target>]` | one process (queue group = subject), its reply goes back |

- Core NATS keeps nothing: messages published while a receiver is disconnected are lost. `DispatcherConfig::on_reconnect` is the place to reload cached state.
- A request waits at most its timeout (`PubReqOptions::timeout`, default 10 s), also while NATS is unreachable. A handler that fails, panics, or whose reply can't be sent answers an error reply: `PublisherError::Remote`. Without a responder NATS answers `no responders` right away.
- `ExecutionMode::Concurrent` (default, 10 in flight per subject) does not keep the order of messages; use `Ordered` where order matters.
- The in-process queue is unbounded by default (`queue_capacity`), so requests may wait in it behind slow handlers: give them a deadline of their own.
- On shutdown the readers stop and unsubscribe (other replicas take over), queued and running handlers finish and their replies are still sent, as long as the caller keeps awaiting the dispatcher (its `JoinSet`); while NATS is unreachable the connection is kept at most 30 s for them.
- Replicas on dawnflow before 0.10 subscribe without a queue group: during a rolling upgrade both an old replica and one new replica handle the same request or consumer message (the first reply wins).

## Messages

- Routed by the last segment of the Rust type name: two message types with the same name share one subject. Type names and targets must be valid subject tokens (no `.`, `*`, `>`, whitespace); registration panics, publishing returns `PublisherError::InvalidSubject` otherwise.
- MessagePack with field names (since 0.10; older senders encode structs as arrays, which still decode): fields may be added (with `#[serde(default)]` on the receiving side) and reordered between versions. Renaming or removing a field the receiver still requires and a new enum variant the receiver doesn't know yet don't decode: deploy receivers first. `#[serde(untagged)]` enums decode only from senders on 0.10 or later.
- The in-memory backend neither serializes nor sends error replies: handler errors come back directly and handler panics reach the caller. Test the wire behaviour against NATS.
