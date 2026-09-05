# SAM workspace

SAM is the authoritative System Application Manager. This first increment keeps
the wire protocol, reusable client API, state-machine logic, and executable in
separate crates so transport and policy can evolve independently.

## Crates

- `sam-protocol`: IPC-safe shared types and messages.
- `sam-transport`: framed local IPC over Unix sockets or Windows named pipes.
- `sam-core`: registry, health aggregation, system state, and mode transitions.
- `sam-client`: connected application-facing API.
- `apps/sam`: small executable composition root.

## Current transition protocol

1. SAM starts a transition and sends `PrepareMode`.
2. Each participating application returns `ModeReady` or `ModeRejected`.
3. If all are ready, SAM sends `CommitMode`.
4. Each application returns `ModeCommitted`.
5. SAM makes the new mode authoritative after all commit confirmations arrive.

A rejection aborts the preparation phase. Transition IDs prevent delayed
responses from being applied to a newer transition.

## Local IPC transport

The public transport API is platform neutral:

- Linux and other Unix targets use `tokio::net::UnixStream` and
  `tokio::net::UnixListener`.
- Windows uses Tokio named pipes.
- Each serialized Postcard message is preceded by a four-byte big-endian length.
- Incoming lengths are checked against a 64 KiB limit before allocating memory.
- Client connections can split into independent reader and writer halves, so a
  heartbeat task does not block while another task waits for SAM commands.
- Registration carries protocol version `1`, allowing SAM to reject incompatible
  clients before interpreting later messages.

Default endpoints are `/tmp/sam.sock` on Unix and `\\.\pipe\sam` on Windows.
Production Linux deployments should normally pass a configured path under
`/run`, whose directory permissions can restrict which applications connect.
On Unix, SAM intentionally does not delete an existing socket path. The service
manager should remove a stale socket only after confirming no SAM instance is
running. Windows clients retry briefly while the named pipe is busy or starting.

The current executable accepts and decodes concurrent connections. Wiring those
messages into the registry and state managers is the next orchestration layer.

Postcard encodes enum variants by their declaration order. Treat the existing
variant order as wire ABI: append new variants, do not reorder or remove them
within a protocol version.

## Run the checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
