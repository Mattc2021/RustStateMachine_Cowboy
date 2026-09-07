use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sam_core::{
    ApplicationRegistry, HealthManager, ModeError, ModeEvent, ModeManager, ModeManagerState,
    SystemState,
};
use sam_protocol::{
    ApplicationId, ApplicationToSam, HealthState, SamToApplication, SystemMode, TransitionId,
    PROTOCOL_VERSION,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

/// Identifies one physical connection for an application, distinct from its
/// `ApplicationId`. An application can reconnect and re-register under the
/// same id; the generation counter here lets a stale, already-superseded
/// connection's disconnect (or messages) be told apart from the current one
/// (see `verify_connection`), so a delayed cleanup from an old session can't
/// tear down a newer replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

/// Point-in-time view of one registered application, for reporting (e.g. an
/// operator console's `applications` command) rather than internal logic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationSnapshot {
    pub application: ApplicationId,
    pub health: sam_protocol::HealthState,
    pub effective_health: HealthState,
    pub current_mode: SystemMode,
    pub connected: bool,
    pub synchronized: bool,
    pub heartbeat_age: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ServiceError {
    #[error("SAM service state is unavailable")]
    StateUnavailable,
    #[error("connection is no longer active for {0:?}")]
    StaleConnection(ApplicationId),
    #[error("message application {actual:?} does not match registered application {expected:?}")]
    ApplicationMismatch {
        expected: ApplicationId,
        actual: ApplicationId,
    },
    #[error("registration is only valid as the first message")]
    DuplicateRegistration,
    #[error("mode transition failed: {0}")]
    Mode(String),
}

/// Shared, thread-safe handle to SAM's live system state: registered
/// applications, aggregated health, and the in-progress mode transition (if
/// any). Cheap to `Clone` — every clone refers to the same underlying state,
/// so one instance is shared across every connection task and the health
/// monitor.
#[derive(Clone)]
pub struct SamService {
    inner: Arc<Mutex<ServiceState>>,
}

struct ServiceState {
    system: SystemState,
    applications: ApplicationRegistry,
    health: HealthManager,
    modes: ModeManager,
    clients: HashMap<ApplicationId, ClientConnection>,
    next_connection_id: u64,
}

struct ClientConnection {
    id: ConnectionId,
    outbound: mpsc::Sender<SamToApplication>,
}

impl SamService {
    pub fn new(heartbeat_timeout: Duration) -> Self {
        let system = SystemState::default();
        Self {
            inner: Arc::new(Mutex::new(ServiceState {
                modes: ModeManager::new(system.mode),
                system,
                applications: ApplicationRegistry::default(),
                health: HealthManager::new(heartbeat_timeout),
                clients: HashMap::new(),
                next_connection_id: 1,
            })),
        }
    }

    /// Registers `application`'s new connection, immediately queuing a
    /// `RegisterAccepted` reply on `outbound` with the current system
    /// snapshot. Replaces any previous connection for the same id — its
    /// `ConnectionId` becomes stale and no longer authorized to act on this
    /// application's behalf (see `verify_connection`).
    pub fn register(
        &self,
        application: ApplicationId,
        outbound: mpsc::Sender<SamToApplication>,
        now: Instant,
    ) -> Result<ConnectionId, ServiceError> {
        let mut state = self.lock()?;
        let id = ConnectionId(state.next_connection_id);
        state.next_connection_id = state.next_connection_id.wrapping_add(1);
        let replacing_connection = state.clients.contains_key(&application);
        state.applications.register(application.clone(), now);
        recalculate_health(&mut state, now);
        outbound
            .try_send(SamToApplication::RegisterAccepted {
                protocol_version: PROTOCOL_VERSION,
                current_mode: state.system.mode,
                system_health: state.system.health,
            })
            .map_err(|_| ServiceError::StateUnavailable)?;
        state
            .clients
            .insert(application.clone(), ClientConnection { id, outbound });
        info!(
            application = %application.0,
            connection_id = id.0,
            replacing_connection,
            mode = ?state.system.mode,
            health = ?state.system.health,
            "application registered"
        );
        Ok(id)
    }

    /// Marks `application` disconnected, but only if `connection_id` is
    /// still its current connection. A stale (already-replaced) connection's
    /// disconnect is silently ignored, so an old session tearing down after
    /// a reconnect can't clobber the new one's registration.
    pub fn disconnect(
        &self,
        application: &ApplicationId,
        connection_id: ConnectionId,
        now: Instant,
    ) -> Result<(), ServiceError> {
        let mut state = self.lock()?;
        let active = state
            .clients
            .get(application)
            .is_some_and(|client| client.id == connection_id);
        if !active {
            debug!(application = %application.0, connection_id = connection_id.0, "ignoring stale disconnect");
            return Ok(());
        }
        state.clients.remove(application);
        let _ = state.applications.mark_disconnected(application);
        let previous_health = state.system.health;
        recalculate_health(&mut state, now);
        info!(application = %application.0, connection_id = connection_id.0, "application disconnected");
        if previous_health != state.system.health {
            warn!(previous = ?previous_health, current = ?state.system.health, "system health changed after disconnect");
        }
        broadcast_state(&state);
        Ok(())
    }

    /// Applies one message from `registered`'s connection to system state.
    ///
    /// `registered` and `connection_id` must match the application's current
    /// connection (see `verify_connection`), and the message's own embedded
    /// `application` field must match `registered` (see
    /// `verify_application`) — a connection can only ever speak for the
    /// identity it registered as, even though the wire messages repeat that
    /// id on every variant.
    pub fn handle_message(
        &self,
        registered: &ApplicationId,
        connection_id: ConnectionId,
        message: ApplicationToSam,
        now: Instant,
    ) -> Result<(), ServiceError> {
        let mut state = self.lock()?;
        verify_connection(&state, registered, connection_id)?;
        match message {
            ApplicationToSam::Register { .. } => return Err(ServiceError::DuplicateRegistration),
            ApplicationToSam::Heartbeat {
                application,
                health,
                current_mode,
            } => {
                verify_application(registered, &application)?;
                trace!(application = %application.0, ?health, ?current_mode, "heartbeat received");
                state
                    .applications
                    .update_heartbeat(&application, health, current_mode, now)
                    .map_err(|error| ServiceError::Mode(error.to_string()))?;
                let previous = state.system.health;
                recalculate_health(&mut state, now);
                if previous != state.system.health {
                    info!(previous = ?previous, current = ?state.system.health, "system health changed");
                    broadcast_state(&state);
                }
            }
            ApplicationToSam::ModeReady {
                application,
                transition_id,
            } => {
                verify_application(registered, &application)?;
                let event = match state.modes.application_ready(&application, transition_id) {
                    Ok(event) => event,
                    Err(error) if is_late_transition_response(&error) => {
                        warn!(application = %application.0, ?transition_id, %error, "ignoring late mode-ready response");
                        return Ok(());
                    }
                    Err(error) => return Err(ServiceError::Mode(error.to_string())),
                };
                info!(application = %application.0, ?transition_id, "application ready for mode transition");
                if let Some(ModeEvent::PreparationComplete {
                    transition_id,
                    target_mode,
                }) = event
                {
                    info!(?transition_id, mode = ?target_mode, "all applications ready; broadcasting commit");
                    broadcast(
                        &state,
                        SamToApplication::CommitMode {
                            transition_id,
                            mode: target_mode,
                        },
                    );
                }
            }
            ApplicationToSam::ModeRejected {
                application,
                transition_id,
                reason,
            } => {
                verify_application(registered, &application)?;
                warn!(application = %application.0, ?transition_id, %reason, "application rejected mode transition");
                if let Err(error) =
                    state
                        .modes
                        .application_rejected(&application, transition_id, reason)
                {
                    if is_late_transition_response(&error) {
                        warn!(application = %application.0, ?transition_id, %error, "ignoring late mode-rejection response");
                        return Ok(());
                    }
                    return Err(ServiceError::Mode(error.to_string()));
                }
                broadcast(&state, SamToApplication::AbortMode { transition_id });
            }
            ApplicationToSam::ModeCommitted {
                application,
                transition_id,
                mode,
            } => {
                verify_application(registered, &application)?;
                let event = match state.modes.application_committed(
                    &application,
                    transition_id,
                    mode,
                ) {
                    Ok(event) => event,
                    Err(error) if is_late_transition_response(&error) => {
                        warn!(application = %application.0, ?transition_id, %error, "ignoring late mode-committed response");
                        return Ok(());
                    }
                    Err(error) => return Err(ServiceError::Mode(error.to_string())),
                };
                state
                    .applications
                    .set_mode(&application, mode)
                    .map_err(|error| ServiceError::Mode(error.to_string()))?;
                info!(application = %application.0, ?transition_id, ?mode, "application confirmed mode commit");
                if let Some(ModeEvent::TransitionComplete { mode, .. }) = event {
                    state.system.mode = mode;
                    recalculate_health(&mut state, now);
                    info!(?transition_id, ?mode, health = ?state.system.health, "system mode transition complete");
                    broadcast_state(&state);
                }
            }
        }
        Ok(())
    }

    /// Begins a coordinated transition to `target` across every currently
    /// connected application, broadcasting `PrepareMode` to all of them.
    pub fn request_mode(&self, target: SystemMode) -> Result<TransitionId, ServiceError> {
        let mut state = self.lock()?;
        let participants: Vec<_> = state.clients.keys().cloned().collect();
        let id = state
            .modes
            .request_transition(target, participants)
            .map_err(|error| ServiceError::Mode(error.to_string()))?;
        info!(transition_id = ?id, from = ?state.system.mode, to = ?target, participants = state.clients.len(), "mode transition requested");
        broadcast(
            &state,
            SamToApplication::PrepareMode {
                transition_id: id,
                requested_mode: target,
            },
        );
        Ok(id)
    }

    /// The current system-wide mode and aggregated health.
    pub fn snapshot(&self) -> Result<SystemState, ServiceError> {
        Ok(self.lock()?.system)
    }

    /// Whether the mode manager is currently collecting prepare or commit
    /// responses. This is useful for automation that must distinguish a
    /// rejected transition from one that is merely still in progress.
    pub fn transition_in_progress(&self) -> Result<bool, ServiceError> {
        Ok(!matches!(
            self.lock()?.modes.state(),
            ModeManagerState::Stable { .. }
        ))
    }

    /// A snapshot of every known application, sorted by id for stable
    /// display output.
    pub fn applications(&self, now: Instant) -> Result<Vec<ApplicationSnapshot>, ServiceError> {
        let state = self.lock()?;
        let mut applications: Vec<_> = state
            .applications
            .iter()
            .map(|(id, status)| {
                let synchronized = status.connected && status.current_mode == state.system.mode;
                let base_health = state.health.calculate_application(status, now);
                ApplicationSnapshot {
                    application: id.clone(),
                    health: status.health,
                    effective_health: effective_application_health(base_health, synchronized),
                    current_mode: status.current_mode,
                    connected: status.connected,
                    synchronized,
                    heartbeat_age: now.saturating_duration_since(status.last_heartbeat),
                }
            })
            .collect();
        applications.sort_by(|left, right| left.application.0.cmp(&right.application.0));
        Ok(applications)
    }

    /// Recomputes aggregated health and broadcasts a `StateBroadcast` if it
    /// changed. Called on a timer by `SamServer::run` so a heartbeat that
    /// simply stops arriving (rather than an explicit disconnect) is still
    /// noticed within one polling interval.
    pub fn refresh_health(&self, now: Instant) -> Result<(), ServiceError> {
        let mut state = self.lock()?;
        let previous = state.system.health;
        recalculate_health(&mut state, now);
        if previous != state.system.health {
            info!(previous = ?previous, current = ?state.system.health, "system health changed during periodic evaluation");
            broadcast_state(&state);
        }
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ServiceState>, ServiceError> {
        self.inner
            .lock()
            .map_err(|_| ServiceError::StateUnavailable)
    }
}

fn verify_connection(
    state: &ServiceState,
    app: &ApplicationId,
    id: ConnectionId,
) -> Result<(), ServiceError> {
    if state.clients.get(app).is_some_and(|client| client.id == id) {
        Ok(())
    } else {
        Err(ServiceError::StaleConnection(app.clone()))
    }
}

fn verify_application(
    expected: &ApplicationId,
    actual: &ApplicationId,
) -> Result<(), ServiceError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ServiceError::ApplicationMismatch {
            expected: expected.clone(),
            actual: actual.clone(),
        })
    }
}

fn is_late_transition_response(error: &ModeError) -> bool {
    matches!(
        error,
        ModeError::InvalidPhase | ModeError::StaleTransition | ModeError::UnknownParticipant(_)
    )
}

fn recalculate_health(state: &mut ServiceState, now: Instant) {
    let reported_health = state.health.calculate(&state.applications, now);
    let mode_mismatch = state.applications.iter().any(|(_, application)| {
        application.connected && application.current_mode != state.system.mode
    });
    state.system.health = if reported_health == HealthState::Failed {
        HealthState::Failed
    } else if reported_health == HealthState::Degraded || mode_mismatch {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    };
}

fn effective_application_health(base_health: HealthState, synchronized: bool) -> HealthState {
    if base_health == HealthState::Failed {
        HealthState::Failed
    } else if !synchronized || base_health == HealthState::Degraded {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    }
}

fn broadcast_state(state: &ServiceState) {
    broadcast(
        state,
        SamToApplication::StateBroadcast {
            mode: state.system.mode,
            health: state.system.health,
        },
    );
}

fn broadcast(state: &ServiceState, message: SamToApplication) {
    for (application, client) in &state.clients {
        if let Err(error) = client.outbound.try_send(message.clone()) {
            warn!(application = %application.0, %error, "could not queue outbound SAM message");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sam_protocol::HealthState;

    fn app(name: &str) -> ApplicationId {
        ApplicationId::from(name)
    }

    #[test]
    fn full_transition_is_broadcast_and_committed() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let nav = app("navigation");
        let guidance = app("guidance");
        let (nav_tx, mut nav_rx) = mpsc::channel(16);
        let (guidance_tx, mut guidance_rx) = mpsc::channel(16);
        let nav_id = service.register(nav.clone(), nav_tx, now).unwrap();
        let guidance_id = service
            .register(guidance.clone(), guidance_tx, now)
            .unwrap();
        nav_rx.try_recv().unwrap();
        guidance_rx.try_recv().unwrap();

        let transition = service.request_mode(SystemMode::Working).unwrap();
        assert!(matches!(
            nav_rx.try_recv().unwrap(),
            SamToApplication::PrepareMode { .. }
        ));
        guidance_rx.try_recv().unwrap();
        service
            .handle_message(
                &nav,
                nav_id,
                ApplicationToSam::ModeReady {
                    application: nav.clone(),
                    transition_id: transition,
                },
                now,
            )
            .unwrap();
        service
            .handle_message(
                &guidance,
                guidance_id,
                ApplicationToSam::ModeReady {
                    application: guidance.clone(),
                    transition_id: transition,
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            nav_rx.try_recv().unwrap(),
            SamToApplication::CommitMode { .. }
        ));
        guidance_rx.try_recv().unwrap();
        service
            .handle_message(
                &nav,
                nav_id,
                ApplicationToSam::ModeCommitted {
                    application: nav.clone(),
                    transition_id: transition,
                    mode: SystemMode::Working,
                },
                now,
            )
            .unwrap();
        service
            .handle_message(
                &guidance,
                guidance_id,
                ApplicationToSam::ModeCommitted {
                    application: guidance.clone(),
                    transition_id: transition,
                    mode: SystemMode::Working,
                },
                now,
            )
            .unwrap();
        assert_eq!(service.snapshot().unwrap().mode, SystemMode::Working);
    }

    #[test]
    fn transition_activity_clears_after_rejection() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let telemetry = app("telemetry");
        let (tx, mut rx) = mpsc::channel(8);
        let connection = service.register(telemetry.clone(), tx, now).unwrap();
        rx.try_recv().unwrap();

        let transition = service.request_mode(SystemMode::Working).unwrap();
        assert!(service.transition_in_progress().unwrap());
        rx.try_recv().unwrap();
        service
            .handle_message(
                &telemetry,
                connection,
                ApplicationToSam::ModeRejected {
                    application: telemetry.clone(),
                    transition_id: transition,
                    reason: "temporary safety interlock".to_owned(),
                },
                now,
            )
            .unwrap();

        assert!(!service.transition_in_progress().unwrap());
        assert_eq!(service.snapshot().unwrap().mode, SystemMode::Startup);
    }

    #[test]
    fn late_ready_after_another_application_rejects_is_ignored() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let navigation = app("navigation");
        let telemetry = app("telemetry");
        let (nav_tx, mut nav_rx) = mpsc::channel(8);
        let (telemetry_tx, mut telemetry_rx) = mpsc::channel(8);
        let nav_connection = service.register(navigation.clone(), nav_tx, now).unwrap();
        let telemetry_connection = service
            .register(telemetry.clone(), telemetry_tx, now)
            .unwrap();
        nav_rx.try_recv().unwrap();
        telemetry_rx.try_recv().unwrap();

        let transition = service.request_mode(SystemMode::Working).unwrap();
        nav_rx.try_recv().unwrap();
        telemetry_rx.try_recv().unwrap();
        service
            .handle_message(
                &telemetry,
                telemetry_connection,
                ApplicationToSam::ModeRejected {
                    application: telemetry.clone(),
                    transition_id: transition,
                    reason: "temporary safety interlock".to_owned(),
                },
                now,
            )
            .unwrap();

        assert!(service
            .handle_message(
                &navigation,
                nav_connection,
                ApplicationToSam::ModeReady {
                    application: navigation.clone(),
                    transition_id: transition,
                },
                now,
            )
            .is_ok());
        assert_eq!(service.snapshot().unwrap().mode, SystemMode::Startup);
    }

    #[test]
    fn application_cannot_impersonate_another_connection() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let nav = app("navigation");
        let (tx, _rx) = mpsc::channel(4);
        let id = service.register(nav.clone(), tx, now).unwrap();
        let result = service.handle_message(
            &nav,
            id,
            ApplicationToSam::Heartbeat {
                application: app("guidance"),
                health: HealthState::Healthy,
                current_mode: SystemMode::Startup,
            },
            now,
        );
        assert!(matches!(
            result,
            Err(ServiceError::ApplicationMismatch { .. })
        ));
    }

    #[test]
    fn stale_disconnect_does_not_remove_replacement_connection() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let nav = app("navigation");
        let (first_tx, _first_rx) = mpsc::channel(4);
        let (second_tx, _second_rx) = mpsc::channel(4);
        let first = service.register(nav.clone(), first_tx, now).unwrap();
        let second = service.register(nav.clone(), second_tx, now).unwrap();
        service.disconnect(&nav, first, now).unwrap();
        assert!(service
            .handle_message(
                &nav,
                second,
                ApplicationToSam::Heartbeat {
                    application: nav.clone(),
                    health: HealthState::Healthy,
                    current_mode: SystemMode::Startup,
                },
                now
            )
            .is_ok());
    }

    #[test]
    fn heartbeat_expiration_broadcasts_failed_health() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let nav = app("navigation");
        let (tx, mut rx) = mpsc::channel(4);
        service.register(nav, tx, now).unwrap();
        rx.try_recv().unwrap();

        service
            .refresh_health(now + Duration::from_secs(3))
            .unwrap();
        assert!(matches!(
            rx.try_recv().unwrap(),
            SamToApplication::StateBroadcast {
                health: HealthState::Failed,
                ..
            }
        ));
        assert_eq!(
            service.applications(now + Duration::from_secs(3)).unwrap()[0].effective_health,
            HealthState::Failed,
        );
    }

    #[test]
    fn mode_mismatch_is_degraded_until_application_synchronizes() {
        let service = SamService::new(Duration::from_secs(2));
        let now = Instant::now();
        let navigation = app("navigation");
        let (tx, mut rx) = mpsc::channel(8);
        let connection = service.register(navigation.clone(), tx, now).unwrap();
        rx.try_recv().unwrap();

        service
            .handle_message(
                &navigation,
                connection,
                ApplicationToSam::Heartbeat {
                    application: navigation.clone(),
                    health: HealthState::Healthy,
                    current_mode: SystemMode::Standby,
                },
                now,
            )
            .unwrap();

        let snapshot = service.applications(now).unwrap();
        assert!(!snapshot[0].synchronized);
        assert_eq!(snapshot[0].health, HealthState::Healthy);
        assert_eq!(snapshot[0].effective_health, HealthState::Degraded);
        assert_eq!(service.snapshot().unwrap().health, HealthState::Degraded);
    }
}
