use std::{collections::HashMap, time::Instant};

use sam_protocol::{ApplicationId, HealthState, SystemMode};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("application is not registered: {0:?}")]
    NotRegistered(ApplicationId),
}

/// Last-known status of a single registered application.
#[derive(Debug, Clone)]
pub struct ApplicationStatus {
    pub current_mode: SystemMode,
    pub health: HealthState,
    pub last_heartbeat: Instant,
    pub connected: bool,
}

impl ApplicationStatus {
    /// New applications are assumed healthy, in `Startup` mode, and connected
    /// until a heartbeat says otherwise or `mark_disconnected` is called.
    pub fn new(now: Instant) -> Self {
        Self {
            current_mode: SystemMode::Startup,
            health: HealthState::Healthy,
            last_heartbeat: now,
            connected: true,
        }
    }
}

/// Tracks every application SAM knows about, keyed by `ApplicationId`.
/// Feeds `HealthManager::calculate`, which walks `iter()` to derive overall
/// system health.
#[derive(Debug, Default)]
pub struct ApplicationRegistry {
    applications: HashMap<ApplicationId, ApplicationStatus>,
}

impl ApplicationRegistry {
    /// Registers an application, replacing any existing entry for the same
    /// id with a fresh `ApplicationStatus`.
    pub fn register(&mut self, application: ApplicationId, now: Instant) {
        self.applications
            .insert(application, ApplicationStatus::new(now));
    }

    /// Marks a previously registered application as disconnected without
    /// removing it from the registry, so its last-known status remains
    /// visible until it re-registers or heartbeats again.
    pub fn mark_disconnected(&mut self, application: &ApplicationId) -> Result<(), RegistryError> {
        let status = self
            .applications
            .get_mut(application)
            .ok_or_else(|| RegistryError::NotRegistered(application.clone()))?;
        status.connected = false;
        Ok(())
    }

    /// Records a heartbeat from an already-registered application, updating
    /// its health, mode, timestamp, and marking it connected again.
    pub fn update_heartbeat(
        &mut self,
        application: &ApplicationId,
        health: HealthState,
        mode: SystemMode,
        now: Instant,
    ) -> Result<(), RegistryError> {
        let status = self
            .applications
            .get_mut(application)
            .ok_or_else(|| RegistryError::NotRegistered(application.clone()))?;
        status.health = health;
        status.current_mode = mode;
        status.last_heartbeat = now;
        status.connected = true;
        Ok(())
    }

    /// Records a successfully committed mode immediately rather than waiting
    /// for the application's next heartbeat to make the registry consistent.
    pub fn set_mode(
        &mut self,
        application: &ApplicationId,
        mode: SystemMode,
    ) -> Result<(), RegistryError> {
        let status = self
            .applications
            .get_mut(application)
            .ok_or_else(|| RegistryError::NotRegistered(application.clone()))?;
        status.current_mode = mode;
        Ok(())
    }

    pub fn get(&self, application: &ApplicationId) -> Option<&ApplicationStatus> {
        self.applications.get(application)
    }

    pub fn ids(&self) -> impl Iterator<Item = &ApplicationId> {
        self.applications.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ApplicationId, &ApplicationStatus)> {
        self.applications.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.applications.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn app(name: &str) -> ApplicationId {
        ApplicationId::from(name)
    }

    #[test]
    fn new_registry_is_empty() {
        let registry = ApplicationRegistry::default();
        assert!(registry.is_empty());
        assert_eq!(registry.ids().count(), 0);
    }

    #[test]
    fn register_adds_application_with_default_status() {
        let now = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), now);

        assert!(!registry.is_empty());
        let status = registry.get(&app("navigation")).expect("registered");
        assert_eq!(status.current_mode, SystemMode::Startup);
        assert_eq!(status.health, HealthState::Healthy);
        assert!(status.connected);
        assert_eq!(status.last_heartbeat, now);
    }

    #[test]
    fn register_twice_overwrites_previous_status() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry
            .update_heartbeat(
                &app("navigation"),
                HealthState::Degraded,
                SystemMode::Working,
                start + Duration::from_secs(1),
            )
            .unwrap();

        // Re-registering should reset the status back to defaults rather than
        // merge with the previous heartbeat data.
        registry.register(app("navigation"), start + Duration::from_secs(2));
        let status = registry.get(&app("navigation")).unwrap();
        assert_eq!(status.health, HealthState::Healthy);
        assert_eq!(status.current_mode, SystemMode::Startup);
    }

    #[test]
    fn get_unknown_application_returns_none() {
        let registry = ApplicationRegistry::default();
        assert!(registry.get(&app("unknown")).is_none());
    }

    #[test]
    fn update_heartbeat_updates_fields_and_reconnects() {
        let start = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), start);
        registry.mark_disconnected(&app("navigation")).unwrap();

        let later = start + Duration::from_secs(5);
        registry
            .update_heartbeat(&app("navigation"), HealthState::Degraded, SystemMode::Working, later)
            .unwrap();

        let status = registry.get(&app("navigation")).unwrap();
        assert_eq!(status.health, HealthState::Degraded);
        assert_eq!(status.current_mode, SystemMode::Working);
        assert_eq!(status.last_heartbeat, later);
        assert!(status.connected);
    }

    #[test]
    fn update_heartbeat_for_unregistered_application_errors() {
        let mut registry = ApplicationRegistry::default();
        let result = registry.update_heartbeat(
            &app("unknown"),
            HealthState::Healthy,
            SystemMode::Startup,
            Instant::now(),
        );
        assert_eq!(result, Err(RegistryError::NotRegistered(app("unknown"))));
    }

    #[test]
    fn set_mode_updates_mode_without_changing_heartbeat_time() {
        let now = Instant::now();
        let mut registry = ApplicationRegistry::default();
        let navigation = app("navigation");
        registry.register(navigation.clone(), now);

        registry.set_mode(&navigation, SystemMode::Standby).unwrap();
        let status = registry.get(&navigation).unwrap();
        assert_eq!(status.current_mode, SystemMode::Standby);
        assert_eq!(status.last_heartbeat, now);
    }

    #[test]
    fn mark_disconnected_sets_connected_false() {
        let now = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), now);
        registry.mark_disconnected(&app("navigation")).unwrap();

        assert!(!registry.get(&app("navigation")).unwrap().connected);
    }

    #[test]
    fn mark_disconnected_for_unregistered_application_errors() {
        let mut registry = ApplicationRegistry::default();
        let result = registry.mark_disconnected(&app("unknown"));
        assert_eq!(result, Err(RegistryError::NotRegistered(app("unknown"))));
    }

    #[test]
    fn ids_and_iter_reflect_all_registered_applications() {
        let now = Instant::now();
        let mut registry = ApplicationRegistry::default();
        registry.register(app("navigation"), now);
        registry.register(app("guidance"), now);

        let mut ids: Vec<String> = registry.ids().map(|id| id.0.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["guidance".to_string(), "navigation".to_string()]);

        assert_eq!(registry.iter().count(), 2);
    }
}
