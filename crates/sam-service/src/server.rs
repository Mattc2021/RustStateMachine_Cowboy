use std::time::{Duration, Instant};

use sam_protocol::{ApplicationToSam, SamToApplication, PROTOCOL_VERSION};
use sam_transport::{
    is_disconnect, FramedConnection, LocalEndpoint, LocalListener, PlatformServerStream,
    TransportError,
};
use thiserror::Error;
use tokio::{sync::mpsc, time::timeout};
use tracing::{debug, info, trace, warn};

use crate::{SamService, ServiceError};

const OUTBOUND_QUEUE_DEPTH: usize = 32;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Service(#[from] ServiceError),
}

/// Binds `endpoint` and services connections against a shared `SamService`
/// forever. Each accepted connection runs on its own task, so one slow or
/// misbehaving application cannot block registration or messages for
/// another.
pub struct SamServer {
    endpoint: LocalEndpoint,
    service: SamService,
    registration_timeout: Duration,
}

impl SamServer {
    pub fn new(endpoint: LocalEndpoint, service: SamService) -> Self {
        Self {
            endpoint,
            service,
            registration_timeout: Duration::from_secs(5),
        }
    }

    /// Binds the endpoint, starts the background health-recalculation loop
    /// (so an expired heartbeat is noticed even with no new messages
    /// arriving), and then accepts connections until `bind` or `accept`
    /// fails.
    pub async fn run(self) -> Result<(), ServerError> {
        let mut listener = LocalListener::bind(&self.endpoint)?;
        info!(endpoint = %self.endpoint.as_str(), "SAM IPC listener started");
        let health_service = self.service.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if health_service.refresh_health(Instant::now()).is_err() {
                    warn!("health monitor stopped because service state is unavailable");
                    break;
                }
            }
        });
        loop {
            let connection = listener.accept().await?;
            debug!("accepted IPC connection; awaiting registration");
            let service = self.service.clone();
            let registration_timeout = self.registration_timeout;
            tokio::spawn(async move {
                if let Err(error) =
                    serve_connection(connection, service, registration_timeout).await
                {
                    warn!(%error, "SAM connection task ended with an error");
                }
            });
        }
    }
}

/// Handles one accepted connection end to end: waits for a `Register` as the
/// first message (rejecting anything else, or an incompatible protocol
/// version), then hands off to `run_registered_session` until it disconnects.
async fn serve_connection(
    mut connection: FramedConnection<PlatformServerStream>,
    service: SamService,
    registration_timeout: Duration,
) -> Result<(), ServerError> {
    let first = match timeout(
        registration_timeout,
        connection.receive::<ApplicationToSam>(),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            warn!("connection closed after registration timeout");
            return Ok(());
        }
    };

    let (application, protocol_version) = match first {
        ApplicationToSam::Register {
            application,
            protocol_version,
        } => (application, protocol_version),
        _ => {
            warn!("rejecting connection because its first message was not Register");
            connection
                .send(&SamToApplication::RegisterRejected {
                    supported_protocol_version: PROTOCOL_VERSION,
                    reason: "first message must be Register".to_owned(),
                })
                .await?;
            return Ok(());
        }
    };

    if protocol_version != PROTOCOL_VERSION {
        warn!(
            application = %application.0,
            received_version = protocol_version,
            supported_version = PROTOCOL_VERSION,
            "rejecting incompatible protocol version"
        );
        connection
            .send(&SamToApplication::RegisterRejected {
                supported_protocol_version: PROTOCOL_VERSION,
                reason: format!("unsupported protocol version {protocol_version}"),
            })
            .await?;
        return Ok(());
    }

    let (mut reader, mut writer) = connection.into_split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_DEPTH);
    let connection_id = service.register(application.clone(), outbound_tx, Instant::now())?;
    info!(application = %application.0, ?connection_id, "registered IPC session is active");

    let session_result = run_registered_session(
        &service,
        &application,
        connection_id,
        &mut reader,
        &mut writer,
        &mut outbound_rx,
    )
    .await;
    // Always mark the application disconnected, even if the session ended
    // with an error, so a crashed/dropped connection doesn't leave a stale
    // "connected" entry behind in the registry.
    let disconnect_result = service.disconnect(&application, connection_id, Instant::now());
    debug!(application = %application.0, ?connection_id, "registered IPC session ended");
    session_result?;
    disconnect_result?;
    Ok(())
}

/// Pumps messages in both directions for one registered connection: forwards
/// `outbound_rx` (messages the service wants to push to this application,
/// e.g. broadcasts) to the socket, and feeds incoming messages into
/// `SamService::handle_message`. Returns once either side closes.
async fn run_registered_session(
    service: &SamService,
    application: &sam_protocol::ApplicationId,
    connection_id: crate::ConnectionId,
    reader: &mut sam_transport::FramedReader<tokio::io::ReadHalf<PlatformServerStream>>,
    writer: &mut sam_transport::FramedWriter<tokio::io::WriteHalf<PlatformServerStream>>,
    outbound_rx: &mut mpsc::Receiver<SamToApplication>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            outgoing = outbound_rx.recv() => match outgoing {
                Some(message) => {
                    trace!(application = %application.0, message = ?message, "sending SAM message");
                    writer.send(&message).await?
                },
                None => break,
            },
            incoming = reader.receive::<ApplicationToSam>() => match incoming {
                Ok(message) => {
                    trace!(application = %application.0, message = ?message, "received application message");
                    service.handle_message(application, connection_id, message, Instant::now())?
                },
                Err(error) if is_disconnect(&error) => {
                    debug!(application = %application.0, "peer disconnected");
                    break
                },
                Err(error) => return Err(error.into()),
            },
        }
    }
    Ok(())
}
