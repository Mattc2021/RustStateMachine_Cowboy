use sam_protocol::{HealthState, SystemMode};

/// Snapshot of SAM's overall view of the system: the current mode plus the
/// aggregated health across all registered applications (see
/// `HealthManager::calculate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemState {
    pub mode: SystemMode,
    pub health: HealthState,
}

impl Default for SystemState {
    /// A freshly created system starts in `Startup` mode and is assumed
    /// healthy until an application reports otherwise.
    fn default() -> Self {
        Self {
            mode: SystemMode::Startup,
            health: HealthState::Healthy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_starts_in_startup_and_healthy() {
        let state = SystemState::default();
        assert_eq!(state.mode, SystemMode::Startup);
        assert_eq!(state.health, HealthState::Healthy);
    }

    #[test]
    fn system_state_is_copy_and_value_equal() {
        let a = SystemState::default();
        let b = a;
        assert_eq!(a, b);

        let c = SystemState {
            mode: SystemMode::Working,
            health: HealthState::Degraded,
        };
        assert_ne!(a, c);
    }
}
