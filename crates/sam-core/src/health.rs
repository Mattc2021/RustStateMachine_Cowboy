use std::time::{Duration, Instant};

use sam_protocol::HealthState;

use crate::ApplicationRegistry;

/// Aggregates per-application health into a single system-wide `HealthState`.
#[derive(Debug, Clone, Copy)]
pub struct HealthManager {
    heartbeat_timeout: Duration,
}

impl HealthManager {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self { heartbeat_timeout }
    }

    /// Computes overall system health from every application in `registry`.
    ///
    /// The result is `Failed` as soon as any application is disconnected, has
    /// gone silent for longer than `heartbeat_timeout`, or has self-reported
    /// `Failed` — one bad application fails the whole system, regardless of
    /// the others. Otherwise the result is `Degraded` if any application
    /// reports `Degraded`, and `Healthy` only if every application is
    /// healthy (an empty registry is therefore reported as `Healthy`).
    pub fn calculate(&self, registry: &ApplicationRegistry, now: Instant) -> HealthState {
        let mut overall: HealthState = HealthState::Healthy;

        for (_, application) in registry.iter() {
            if !application.connected
                || now.saturating_duration_since(application.last_heartbeat)
                    > self.heartbeat_timeout
                || application.health == HealthState::Failed
            {
                return HealthState::Failed;
            }

            if application.health == HealthState::Degraded {
                overall = HealthState::Degraded;
            }
        }
        overall
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sam_protocol::{ApplicationId, SystemMode};

    fn app(name: &str) -> ApplicationId {
        ApplicationId::from(name)
    }

    #[test]
    fn empty_registry_is_healthy() {
        let registry = ApplicationRegistry::default();
        let manager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(
            manager.calculate(&registry, Instant::now()),
            HealthState::Healthy
        );
    }

    #[test]
    fn all_healthy_applications_report_healthy() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry.register(app("guidance"), start);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();
        registry
            .update_heartbeat(&app("guidance"), HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();

        let manager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(manager.calculate(&registry, start), HealthState::Healthy);
    }

    #[test]
    fn degraded_application_degrades_overall_health() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Degraded, SystemMode::Startup, start)
            .unwrap();

        let manager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(manager.calculate(&registry, start), HealthState::Degraded);
    }

    #[test]
    fn one_failed_application_fails_overall_health_even_if_others_healthy() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry.register(app("guidance"), start);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Failed, SystemMode::Startup, start)
            .unwrap();
        registry
            .update_heartbeat(&app("guidance"), HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();

        let manager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(manager.calculate(&registry, start), HealthState::Failed);
    }

    #[test]
    fn disconnected_application_fails_health_even_if_report_was_healthy() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();
        registry.mark_disconnected(&app("navigation")).unwrap();

        let manager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(manager.calculate(&registry, start), HealthState::Failed);
    }

    #[test]
    fn heartbeat_exactly_at_timeout_boundary_is_still_healthy() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();

        let manager = HealthManager::new(Duration::from_secs(1));
        // Exactly the timeout duration has elapsed; the check uses strictly
        // greater-than, so this boundary should not yet be considered stale.
        assert_eq!(
            manager.calculate(&registry, start + Duration::from_secs(1)),
            HealthState::Healthy
        );
    }

    #[test]
    fn stale_heartbeat_fails_health() {
        let start: Instant = Instant::now();
        let mut registry: ApplicationRegistry = ApplicationRegistry::default();
        let app: ApplicationId = ApplicationId::from("navigation");
        registry.register(app.clone(), start);
        registry
            .update_heartbeat(&app, HealthState::Healthy, SystemMode::Startup, start)
            .unwrap();

        let manager: HealthManager = HealthManager::new(Duration::from_secs(1));
        assert_eq!(
            manager.calculate(&registry, start + Duration::from_secs(2)),
            HealthState::Failed
        );
    }
}
