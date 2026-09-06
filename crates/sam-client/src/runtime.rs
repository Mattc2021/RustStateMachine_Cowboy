use std::time::Duration;

use async_trait::async_trait;
use sam_protocol::{
    ApplicationId, HealthState, SamToApplication, SystemMode, TransitionId, PROTOCOL_VERSION,
};
use sam_transport::{LocalEndpoint, TransportError};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tracing::{debug, info, trace, warn};

use crate::SamClient;

const INBOUND_QUEUE_DEPTH: usize = 32;

/// Application-supplied behavior for the SAM mode-transition protocol.
///
/// `ApplicationRuntime` drives the connection (registration, heartbeats,
/// reconnection) and calls back into a `ModeHandler` only for the decisions
/// that are actually application-specific: whether a requested mode can be
/// prepared for, and what to do once a mode commits.
#[async_trait]
pub trait ModeHandler: Send {
    /// The health to report on the next heartbeat.
    fn health(&self) -> HealthState;
    /// The mode to report on the next heartbeat.
    fn current_mode(&self) -> SystemMode;

    /// Called when SAM requests a transition to `requested`. Returning `Ok`
    /// sends `ModeReady`; returning `Err(reason)` sends `ModeRejected` with
    /// that reason, aborting the transition for every participant.
    async fn prepare_mode(&mut self, requested: SystemMode) -> Result<(), String>;

    /// Called once SAM has committed the transition. Returning `Err` fails
    /// the whole session with `RuntimeError::CommitFailed`, since an
    /// application that can't apply a mode it already agreed was ready has
    /// no well-defined state to continue from.
    async fn commit_mode(&mut self, mode: SystemMode) -> Result<(), String>;

    /// Reconciles a newly connected application with SAM's authoritative
    /// mode before the runtime starts heartbeating. Applications may
    /// override this when restart recovery differs from a normal transition.
    async fn synchronize_mode(&mut self, authoritative_mode: SystemMode) -> Result<(), String> {
        if self.current_mode() == authoritative_mode {
            return Ok(());
        }
        self.prepare_mode(authoritative_mode).await?;
        self.commit_mode(authoritative_mode).await
    }

    /// Called when a transition this application was part of was aborted
    /// (by another participant's rejection). Default is a no-op.
    async fn abort_mode(&mut self, _transition_id: TransitionId) {}

    /// Called whenever SAM broadcasts a new system-wide mode/health snapshot,
    /// including once immediately after a successful registration. Default
    /// is a no-op.
    async fn system_state_changed(&mut self, _mode: SystemMode, _health: HealthState) {}
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub endpoint: LocalEndpoint,
    pub heartbeat_interval: Duration,
    pub reconnect_delay: Duration,
    pub registration_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            endpoint: LocalEndpoint::default(),
            heartbeat_interval: Duration::from_millis(500),
            reconnect_delay: Duration::from_secs(1),
            registration_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("registration timed out")]
    RegistrationTimeout,
    #[error("SAM rejected protocol version {supported_version}: {reason}")]
    RegistrationRejected {
        supported_version: u16,
        reason: String,
    },
    #[error("SAM accepted registration with protocol version {received}; client supports {supported}")]
    ProtocolVersionMismatch { received: u16, supported: u16 },
    #[error("expected registration response, received {0:?}")]
    UnexpectedRegistrationResponse(SamToApplication),
    #[error("SAM connection closed")]
    ConnectionClosed,
    #[error("application failed to commit mode: {0}")]
    CommitFailed(String),
    #[error("application could not synchronize with SAM's {mode:?} mode: {reason}")]
    InitialSynchronizationFailed { mode: SystemMode, reason: String },
}

/// Keeps a `ModeHandler` connected to SAM: registers, heartbeats on
/// `config.heartbeat_interval`, and relays mode-transition messages to and
/// from the handler, reconnecting after `config.reconnect_delay` if the
/// connection drops.
pub struct ApplicationRuntime {
    application: ApplicationId,
    config: RuntimeConfig,
}

impl ApplicationRuntime {
    pub fn new(application: impl Into<ApplicationId>, config: RuntimeConfig) -> Self {
        Self {
            application: application.into(),
            config,
        }
    }

    /// Runs `handler` against SAM forever, reconnecting after every
    /// recoverable failure. Only returns for `RegistrationRejected` or
    /// `ProtocolVersionMismatch`, since those indicate a permanent
    /// incompatibility that retrying can't fix.
    pub async fn run<H: ModeHandler>(&self, handler: &mut H) -> Result<(), RuntimeError> {
        loop {
            match self.run_session(handler).await {
                Err(error @ RuntimeError::RegistrationRejected { .. })
                | Err(error @ RuntimeError::ProtocolVersionMismatch { .. })
                | Err(error @ RuntimeError::InitialSynchronizationFailed { .. }) => {
                    return Err(error)
                }
                Ok(()) => sleep(self.config.reconnect_delay).await,
                Err(error) => {
                    warn!(application = %self.application.0, %error, "SAM session ended; retrying");
                    sleep(self.config.reconnect_delay).await;
                }
            }
        }
    }

    /// Connects, registers, and services one session until the connection
    /// closes or a protocol error occurs. A clean return (`Ok`) still means
    /// the session ended — `run` decides whether/when to reconnect.
    async fn run_session<H: ModeHandler>(&self, handler: &mut H) -> Result<(), RuntimeError> {
        debug!(application = %self.application.0, endpoint = %self.config.endpoint.as_str(), "connecting to SAM");
        let mut client = SamClient::connect(self.application.clone(), &self.config.endpoint).await?;
        debug!(application = %self.application.0, "transport connected; registering");
        client.register().await?;

        let registration = timeout(self.config.registration_timeout, client.next_message())
            .await
            .map_err(|_| RuntimeError::RegistrationTimeout)??;

        match registration {
            SamToApplication::RegisterAccepted {
                protocol_version,
                current_mode,
                system_health,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(RuntimeError::ProtocolVersionMismatch {
                        received: protocol_version,
                        supported: PROTOCOL_VERSION,
                    });
                }
                info!(
                    application = %self.application.0,
                    authoritative_mode = ?current_mode,
                    system_health = ?system_health,
                    "registration accepted"
                );
                if handler.current_mode() != current_mode {
                    info!(
                        application = %self.application.0,
                        local_mode = ?handler.current_mode(),
                        authoritative_mode = ?current_mode,
                        "synchronizing application mode after registration"
                    );
                    handler.synchronize_mode(current_mode).await.map_err(|reason| {
                        RuntimeError::InitialSynchronizationFailed { mode: current_mode, reason }
                    })?;
                    info!(application = %self.application.0, mode = ?current_mode, "initial mode synchronization complete");
                }
                handler
                    .system_state_changed(current_mode, system_health)
                    .await;
            }
            SamToApplication::RegisterRejected {
                supported_protocol_version,
                reason,
            } => {
                return Err(RuntimeError::RegistrationRejected {
                    supported_version: supported_protocol_version,
                    reason,
                })
            }
            other => return Err(RuntimeError::UnexpectedRegistrationResponse(other)),
        }

        // Split so a slow/blocked handler callback in the select loop below
        // never prevents inbound messages from being read off the wire.
        let (mut receiver, mut sender) = client.into_split();
        let (inbound_tx, mut inbound_rx) = mpsc::channel(INBOUND_QUEUE_DEPTH);
        tokio::spawn(async move {
            loop {
                let message = receiver.next_message().await;
                let closed = message.is_err();
                if inbound_tx.send(message).await.is_err() || closed {
                    break;
                }
            }
        });

        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    trace!(
                        application = %self.application.0,
                        mode = ?handler.current_mode(),
                        health = ?handler.health(),
                        "sending heartbeat"
                    );
                    sender.heartbeat(handler.health(), handler.current_mode()).await?;
                }
                incoming = inbound_rx.recv() => {
                    let message = incoming.ok_or(RuntimeError::ConnectionClosed)??;
                    match message {
                        SamToApplication::PrepareMode { transition_id, requested_mode } => {
                            info!(application = %self.application.0, ?transition_id, mode = ?requested_mode, "received mode preparation request");
                            match handler.prepare_mode(requested_mode).await {
                                Ok(()) => {
                                    info!(application = %self.application.0, ?transition_id, "application is ready for transition");
                                    sender.mode_ready(transition_id).await?
                                }
                                Err(reason) => {
                                    warn!(application = %self.application.0, ?transition_id, %reason, "application rejected transition");
                                    sender.mode_rejected(transition_id, reason).await?
                                }
                            }
                        }
                        SamToApplication::CommitMode { transition_id, mode } => {
                            info!(application = %self.application.0, ?transition_id, ?mode, "committing application mode");
                            handler.commit_mode(mode).await.map_err(RuntimeError::CommitFailed)?;
                            sender.mode_committed(transition_id, mode).await?;
                            info!(application = %self.application.0, ?transition_id, ?mode, "mode commit confirmed to SAM");
                        }
                        SamToApplication::AbortMode { transition_id } => {
                            warn!(application = %self.application.0, ?transition_id, "transition aborted by SAM");
                            handler.abort_mode(transition_id).await;
                        }
                        SamToApplication::StateBroadcast { mode, health } => {
                            debug!(application = %self.application.0, ?mode, ?health, "received system state broadcast");
                            handler.system_state_changed(mode, health).await;
                        }
                        SamToApplication::RegisterAccepted { .. }
                        | SamToApplication::RegisterRejected { .. } => {
                            return Err(RuntimeError::UnexpectedRegistrationResponse(message));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sam_protocol::ApplicationToSam;
    use sam_transport::LocalListener;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    };

    fn unique_endpoint(tag: &str) -> LocalEndpoint {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("sam-client-runtime-test-{tag}-{}-{id}", std::process::id());
        #[cfg(windows)]
        {
            LocalEndpoint::new(format!(r"\\.\pipe\{name}"))
        }
        #[cfg(unix)]
        {
            LocalEndpoint::new(
                std::env::temp_dir()
                    .join(format!("{name}.sock"))
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    }

    fn config_for(endpoint: LocalEndpoint) -> RuntimeConfig {
        RuntimeConfig {
            endpoint,
            heartbeat_interval: Duration::from_millis(20),
            reconnect_delay: Duration::from_millis(10),
            registration_timeout: Duration::from_secs(5),
        }
    }

    #[derive(Clone)]
    struct RecordingHandler {
        state_changes: Arc<Mutex<Vec<(SystemMode, HealthState)>>>,
        current_mode: Arc<Mutex<SystemMode>>,
    }

    impl Default for RecordingHandler {
        fn default() -> Self {
            Self {
                state_changes: Arc::new(Mutex::new(Vec::new())),
                current_mode: Arc::new(Mutex::new(SystemMode::Startup)),
            }
        }
    }

    #[async_trait]
    impl ModeHandler for RecordingHandler {
        fn health(&self) -> HealthState {
            HealthState::Healthy
        }

        fn current_mode(&self) -> SystemMode {
            *self.current_mode.lock().unwrap()
        }

        async fn prepare_mode(&mut self, _requested: SystemMode) -> Result<(), String> {
            Ok(())
        }

        async fn commit_mode(&mut self, mode: SystemMode) -> Result<(), String> {
            *self.current_mode.lock().unwrap() = mode;
            Ok(())
        }

        async fn system_state_changed(&mut self, mode: SystemMode, health: HealthState) {
            self.state_changes.lock().unwrap().push((mode, health));
        }
    }

    #[tokio::test]
    async fn registration_rejection_is_terminal_and_not_retried() {
        let endpoint = unique_endpoint("rejected");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            connection.receive::<ApplicationToSam>().await.unwrap();
            connection
                .send(&SamToApplication::RegisterRejected {
                    supported_protocol_version: PROTOCOL_VERSION + 1,
                    reason: "client too old".to_string(),
                })
                .await
                .unwrap();
        });

        let runtime = ApplicationRuntime::new("navigation", config_for(endpoint));
        let mut handler = RecordingHandler::default();
        let result = runtime.run(&mut handler).await;

        server.await.unwrap();
        assert!(matches!(
            result,
            Err(RuntimeError::RegistrationRejected { .. })
        ));
    }

    #[tokio::test]
    async fn protocol_version_mismatch_is_terminal_and_not_retried() {
        let endpoint = unique_endpoint("mismatch");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            connection.receive::<ApplicationToSam>().await.unwrap();
            connection
                .send(&SamToApplication::RegisterAccepted {
                    protocol_version: PROTOCOL_VERSION + 1,
                    current_mode: SystemMode::Startup,
                    system_health: HealthState::Healthy,
                })
                .await
                .unwrap();
        });

        let runtime = ApplicationRuntime::new("navigation", config_for(endpoint));
        let mut handler = RecordingHandler::default();
        let result = runtime.run(&mut handler).await;

        server.await.unwrap();
        assert!(matches!(
            result,
            Err(RuntimeError::ProtocolVersionMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn successful_registration_reports_initial_state_and_sends_heartbeats() {
        let endpoint = unique_endpoint("heartbeat");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            connection.receive::<ApplicationToSam>().await.unwrap();
            connection
                .send(&SamToApplication::RegisterAccepted {
                    protocol_version: PROTOCOL_VERSION,
                    current_mode: SystemMode::Standby,
                    system_health: HealthState::Degraded,
                })
                .await
                .unwrap();

            // Two heartbeats confirm the interval loop actually runs, then
            // the connection drops (ending the scope) to end the runtime's
            // session with a transport error from the broken connection.
            connection.receive::<ApplicationToSam>().await.unwrap();
            connection.receive::<ApplicationToSam>().await.unwrap();
        });

        let runtime = ApplicationRuntime::new("navigation", config_for(endpoint));
        let mut handler = RecordingHandler::default();
        let result = timeout(Duration::from_secs(2), runtime.run_session(&mut handler))
            .await
            .expect("run_session should end once the server drops the connection");

        server.await.unwrap();
        assert!(matches!(result, Err(RuntimeError::Transport(_))));
        assert_eq!(
            *handler.state_changes.lock().unwrap(),
            vec![(SystemMode::Standby, HealthState::Degraded)]
        );
        assert_eq!(*handler.current_mode.lock().unwrap(), SystemMode::Standby);
    }
}
