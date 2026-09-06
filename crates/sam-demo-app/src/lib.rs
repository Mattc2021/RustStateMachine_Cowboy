//! Shared `ModeHandler` and CLI-flag parsing used by the demo application
//! binaries (`navigation`, `guidance`, `telemetry`). Real applications would
//! implement `ModeHandler` themselves; this crate exists purely so the three
//! example executables can share one trivial implementation instead of
//! duplicating it.

use async_trait::async_trait;
use sam_client::ModeHandler;
use sam_protocol::{HealthState, SystemMode, TransitionId};

/// A `ModeHandler` that logs every callback and always agrees to prepare and
/// commit mode changes, unless `reject_working` is set — in which case it
/// rejects any transition into `SystemMode::Working`, to exercise SAM's
/// rejection/abort path in the demo.
pub struct DemoHandler {
    name: &'static str,
    current_mode: SystemMode,
    health: HealthState,
    reject_working: bool,
}

impl DemoHandler {
    pub fn new(name: &'static str, reject_working: bool, health: HealthState) -> Self {
        Self { name, current_mode: SystemMode::Startup, health, reject_working }
    }
}

#[async_trait]
impl ModeHandler for DemoHandler {
    fn health(&self) -> HealthState { self.health }
    fn current_mode(&self) -> SystemMode { self.current_mode }

    async fn prepare_mode(&mut self, requested: SystemMode) -> Result<(), String> {
        println!("{} preparing for {requested:?}", self.name);
        if self.reject_working && requested == SystemMode::Working {
            Err(format!("{} configured to reject Working", self.name))
        } else {
            Ok(())
        }
    }

    async fn commit_mode(&mut self, mode: SystemMode) -> Result<(), String> {
        self.current_mode = mode;
        println!("{} committed {mode:?}", self.name);
        Ok(())
    }

    async fn abort_mode(&mut self, transition_id: TransitionId) {
        println!("{} aborted transition {transition_id:?}", self.name);
    }

    async fn system_state_changed(&mut self, mode: SystemMode, health: HealthState) {
        println!("{} sees system state: {mode:?} + {health:?}", self.name);
    }
}

/// The health this process should heartbeat as, read from its own `argv`:
/// `--failed` wins outright, `--degraded` applies otherwise, and no flag
/// means healthy.
pub fn health_from_args() -> HealthState {
    health_from(std::env::args())
}

/// Whether this process should reject transitions to `Working`, read from
/// its own `argv` (`--reject-working`).
pub fn reject_working_from_args() -> bool {
    reject_working_from(std::env::args())
}

fn health_from(args: impl Iterator<Item = String>) -> HealthState {
    let mut health = HealthState::Healthy;
    for argument in args {
        match argument.as_str() {
            "--failed" => return HealthState::Failed,
            "--degraded" => health = HealthState::Degraded,
            _ => {}
        }
    }
    health
}

fn reject_working_from(mut args: impl Iterator<Item = String>) -> bool {
    args.any(|arg| arg == "--reject-working")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> impl Iterator<Item = String> {
        values.iter().map(|value| value.to_string()).collect::<Vec<_>>().into_iter()
    }

    #[test]
    fn no_flags_is_healthy() {
        assert_eq!(health_from(args(&["binary-name"])), HealthState::Healthy);
    }

    #[test]
    fn degraded_flag_is_degraded() {
        assert_eq!(
            health_from(args(&["binary-name", "--degraded"])),
            HealthState::Degraded
        );
    }

    #[test]
    fn failed_flag_wins_even_after_degraded() {
        assert_eq!(
            health_from(args(&["binary-name", "--degraded", "--failed"])),
            HealthState::Failed
        );
    }

    #[test]
    fn reject_working_flag_is_detected() {
        assert!(!reject_working_from(args(&["binary-name"])));
        assert!(reject_working_from(args(&["binary-name", "--reject-working"])));
    }
}
