use serde::{Deserialize, Serialize};

/// Operating mode of the overall system. Applications and SAM agree on a single
/// `SystemMode` at any given time; transitions between modes are coordinated
/// via the two-phase protocol implemented in `sam_core::ModeManager`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemMode {
    Startup,
    Standby,
    Working,
}

/// Health of a single application, or of the system as a whole once aggregated
/// by `sam_core::HealthManager`. Ordering matters for aggregation: `Failed`
/// dominates `Degraded`, which dominates `Healthy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthState {
    Healthy,
    Degraded,
    Failed,
}

/// Identifier for a single mode transition attempt, issued by `ModeManager`.
/// Applications echo this id back in their responses so SAM can detect and
/// discard responses to a transition that has already been superseded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionId(pub u64);

/// Stable identifier for an application participating in the SAM protocol.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApplicationId(pub String);

impl ApplicationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl From<&str> for ApplicationId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn application_id_new_accepts_string_and_str() {
        assert_eq!(ApplicationId::new("navigation"), ApplicationId::from("navigation"));
        assert_eq!(
            ApplicationId::new(String::from("guidance")),
            ApplicationId::from("guidance")
        );
    }

    #[test]
    fn application_id_from_str_matches_direct_construction() {
        let via_from: ApplicationId = "navigation".into();
        assert_eq!(via_from, ApplicationId::new("navigation"));
    }

    #[test]
    fn application_id_equality_is_value_based() {
        assert_eq!(ApplicationId::from("a"), ApplicationId::from("a"));
        assert_ne!(ApplicationId::from("a"), ApplicationId::from("b"));
    }

    #[test]
    fn application_id_hashable_for_use_in_sets() {
        let mut set: HashSet<ApplicationId> = HashSet::new();
        set.insert(ApplicationId::from("navigation"));
        set.insert(ApplicationId::from("navigation"));
        set.insert(ApplicationId::from("guidance"));

        assert_eq!(set.len(), 2);
        assert!(set.contains(&ApplicationId::from("navigation")));
    }

    #[test]
    fn transition_id_equality_and_copy() {
        let a = TransitionId(1);
        let b = a;
        assert_eq!(a, b);
        assert_ne!(TransitionId(1), TransitionId(2));
    }

    #[test]
    fn system_mode_and_health_state_are_copy_and_comparable() {
        let mode = SystemMode::Startup;
        let mode_copy = mode;
        assert_eq!(mode, mode_copy);
        assert_ne!(SystemMode::Startup, SystemMode::Standby);

        let health = HealthState::Healthy;
        let health_copy = health;
        assert_eq!(health, health_copy);
        assert_ne!(HealthState::Healthy, HealthState::Failed);
    }
}
