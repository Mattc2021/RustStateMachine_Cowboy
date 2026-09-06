# SAM workspace

SAM is the authoritative System Application Manager. This first increment keeps
the wire protocol, reusable client API, state-machine logic, and executable in
separate crates so transport and policy can evolve independently.

## Crates

- `sam-protocol`: IPC-safe shared types and messages.
- `sam-transport`: framed local IPC over Unix sockets or Windows named pipes.
- `sam-service`: connection lifecycle and orchestration between IPC and core state.
- `sam-core`: registry, health aggregation, system state, and mode transitions.
- `sam-client`: connected application-facing API.
- `sam-demo-app`: shared mock behavior used only by the example executables.
- `navigation`, `guidance`, and `telemetry`: runnable SAM client processes.
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
On Unix, `LocalListener` removes its socket file when dropped, so a clean
shutdown never leaves a stale path behind. `bind` still refuses to reuse an
existing path rather than assuming it's safe to unlink, since a crash (where
`Drop` never runs) can still leave one behind; the service manager should
remove such a stale socket only after confirming no SAM instance is running.
Windows clients retry briefly while the named pipe is busy or starting.

The service now requires registration as the first message, validates protocol
versions, binds every later message to the registered application identity, and
uses per-connection generation IDs so an old disconnected session cannot remove
a newer replacement session. A bounded outbound queue prevents an unresponsive
client from causing unbounded memory growth.

A 100 ms health-monitor task recalculates aggregate health even when no new
messages arrive, so an expired heartbeat changes system health to `Failed` and
triggers a state broadcast.

`SamService::request_mode` begins a coordinated transition. It broadcasts
`PrepareMode`, routes ready/rejected responses into `ModeManager`, broadcasts
`CommitMode` after every participant is ready, and makes the new system mode
authoritative only after every `ModeCommitted` response arrives.

Postcard encodes enum variants by their declaration order. Treat the existing
variant order as wire ABI: append new variants, do not reorder or remove them
within a protocol version.

## Run the multi-process demonstration

Open four terminals from the workspace root:

```bash
cargo run -p sam
cargo run -p navigation
cargo run -p guidance
cargo run -p telemetry
```

The SAM console accepts:

```text
status
applications
startup
standby
working
help
quit
```

To exercise the rejection path, start telemetry with:

```bash
cargo run -p telemetry -- --reject-working
```

To exercise health aggregation, any demo application accepts `--degraded` or
`--failed`. The reusable application runtime registers, validates the protocol
version, sends a heartbeat every 500 ms, responds to mode messages through a
`ModeHandler`, and reconnects after transient transport failures.

When an application reconnects while SAM is already in another mode, the
runtime calls `ModeHandler::synchronize_mode` before starting heartbeats. The
default implementation prepares and commits the authoritative SAM mode. Until
an application's reported mode matches SAM, it is shown as unsynchronized and
its effective health is `Degraded`.

## Logging

All processes use structured `tracing` output. Normal lifecycle and transition
events are visible at the default `info` level. Increase detail with `RUST_LOG`:

Windows Command Prompt:

```bat
set RUST_LOG=debug
cargo run -p sam
```

PowerShell:

```powershell
$env:RUST_LOG = "debug"
cargo run -p sam
```

Linux or WSL:

```bash
RUST_LOG=debug cargo run -p sam
```

Use `RUST_LOG=trace` only when diagnosing individual heartbeats or wire
messages; it is intentionally verbose.

## Run the checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

After adding or changing dependencies, update the checked-in lockfile with:

```bash
cargo generate-lockfile
```
