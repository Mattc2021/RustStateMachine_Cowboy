# SAM company demo guide

## Before the meeting

Run the full check once from the workspace root:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Then run the showcase once on the same machine and account you will use for
the presentation. Close any older SAM or demo-app processes first so they do
not own the local socket or named pipe.

## One-command presentation

Windows Command Prompt:

```bat
scripts\run_demo.cmd
```

PowerShell:

```powershell
.\scripts\run_demo.ps1
```

Linux or WSL:

```bash
./scripts/run_demo.sh
```

The launcher builds before displaying the showcase banner, so compiler output
does not interrupt the actual presentation sequence.

## Suggested talk track

1. **Startup:** “SAM is the authority for system mode and overall health. The
   three applications are independent processes communicating over local IPC.”
2. **Standby commit:** “Mode changes use prepare, ready, commit, and committed
   messages. No application changes mode merely because it received a request.”
3. **Power loss:** “Telemetry has just disappeared. SAM detects the lost IPC
   session and immediately marks system health Failed.”
4. **Recovery:** “When telemetry returns, it registers as a new connection and
   reconciles to SAM's authoritative Standby mode before reporting healthy.”
5. **Rejected transition:** “Telemetry currently has a simulated safety
   interlock. Its rejection aborts Working for the entire system, including
   applications that had already said they were ready.”
6. **Retry:** “The condition clears, so the same two-phase transition succeeds.
   SAM finishes by commanding a controlled return to Standby.”

## What the final line means

`SHOWCASE PASSED` is printed only if SAM observed and validated all of these:

- Three connected and synchronized applications.
- A committed Standby transition.
- Telemetry disconnected and aggregate health became Failed.
- Telemetry reconnected, synchronized, and health returned to Healthy.
- The first Working transition ended without changing the authoritative mode.
- The retry committed Working across every connected application.
- The final transition committed Standby.

## Useful presentation controls

PowerShell accepts named overrides:

```powershell
.\scripts\run_demo.ps1 `
    -TimeoutSeconds 30 `
    -HoldMilliseconds 1500 `
    -FaultAfterMilliseconds 6000 `
    -PowerOffMilliseconds 2500
```

For more internal detail, set `RUST_LOG=debug` before launching. The default
`info` level is recommended for the presentation because it shows lifecycle,
health, and transition events without printing every heartbeat or wire frame.

## Fast troubleshooting

- **Address already in use / named pipe busy:** close older `sam`,
  `navigation`, `guidance`, and `telemetry` processes, then rerun.
- **Timeout waiting for applications:** increase `TimeoutSeconds`, and check
  that endpoint security did not block one of the executables.
- **Telemetry exits with code 75:** this is expected only for the first
  telemetry process; the launcher recognizes it as the simulated power loss.
- **No `SHOWCASE PASSED`:** treat the demo as failed and use the last SAM stage
  banner plus the adjacent warning/error log as the starting point.
