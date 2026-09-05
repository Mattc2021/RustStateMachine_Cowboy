use sam_protocol::{
    ApplicationId, ApplicationToSam, HealthState, SamToApplication, SystemMode, TransitionId,
    PROTOCOL_VERSION,
};
use sam_transport::{
    connect, FramedConnection, FramedReader, FramedWriter, LocalEndpoint, PlatformClientStream,
    TransportError,
};
use tokio::io::{ReadHalf, WriteHalf};

/// A connected, application-facing SAM client.
pub struct SamClient {
    application: ApplicationId,
    connection: FramedConnection<PlatformClientStream>,
}

impl SamClient {
    /// Opens the underlying transport connection to `endpoint`. This does not
    /// itself send `Register` — call `register` afterward to join the SAM
    /// protocol.
    pub async fn connect(
        application: impl Into<ApplicationId>,
        endpoint: &LocalEndpoint,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            application: application.into(),
            connection: connect(endpoint).await?,
        })
    }

    pub fn application(&self) -> &ApplicationId {
        &self.application
    }

    /// Announces this application to SAM, stamping the message with the
    /// protocol version this client was built against (see
    /// `sam_protocol::PROTOCOL_VERSION`).
    pub async fn register(&mut self) -> Result<(), TransportError> {
        self.send(ApplicationToSam::Register {
            application: self.application.clone(),
            protocol_version: PROTOCOL_VERSION,
        })
        .await
    }

    pub async fn heartbeat(
        &mut self,
        health: HealthState,
        current_mode: SystemMode,
    ) -> Result<(), TransportError> {
        self.send(ApplicationToSam::Heartbeat {
            application: self.application.clone(),
            health,
            current_mode,
        })
        .await
    }

    pub async fn mode_ready(&mut self, transition_id: TransitionId) -> Result<(), TransportError> {
        self.send(ApplicationToSam::ModeReady {
            application: self.application.clone(),
            transition_id,
        })
        .await
    }

    pub async fn mode_rejected(
        &mut self,
        transition_id: TransitionId,
        reason: impl Into<String>,
    ) -> Result<(), TransportError> {
        self.send(ApplicationToSam::ModeRejected {
            application: self.application.clone(),
            transition_id,
            reason: reason.into(),
        })
        .await
    }

    pub async fn mode_committed(
        &mut self,
        transition_id: TransitionId,
        mode: SystemMode,
    ) -> Result<(), TransportError> {
        self.send(ApplicationToSam::ModeCommitted {
            application: self.application.clone(),
            transition_id,
            mode,
        })
        .await
    }

    /// Waits for the next message SAM sends this application (e.g. a
    /// `PrepareMode`/`CommitMode`/`AbortMode` request or a `StateBroadcast`).
    pub async fn next_message(&mut self) -> Result<SamToApplication, TransportError> {
        self.connection.receive().await
    }

    /// Splits the client into an independent `SamReceiver`/`SamSender` pair
    /// so one task can wait on `next_message` while another sends responses
    /// (e.g. `mode_ready`) concurrently on the same underlying connection.
    pub fn into_split(self) -> (SamReceiver, SamSender) {
        let (reader, writer) = self.connection.into_split();
        (
            SamReceiver { reader },
            SamSender {
                application: self.application,
                writer,
            },
        )
    }

    async fn send(&mut self, message: ApplicationToSam) -> Result<(), TransportError> {
        self.connection.send(&message).await
    }
}

/// The read half of a split `SamClient`; only receives messages from SAM.
pub struct SamReceiver {
    reader: FramedReader<ReadHalf<PlatformClientStream>>,
}

impl SamReceiver {
    pub async fn next_message(&mut self) -> Result<SamToApplication, TransportError> {
        self.reader.receive().await
    }
}

/// The write half of a split `SamClient`; only sends messages to SAM. Holds
/// the `ApplicationId` (the `SamReceiver` half needs none, since it never
/// stamps outgoing messages) so it can keep building well-formed
/// `ApplicationToSam` messages after the split.
pub struct SamSender {
    application: ApplicationId,
    writer: FramedWriter<WriteHalf<PlatformClientStream>>,
}

impl SamSender {
    pub async fn register(&mut self) -> Result<(), TransportError> {
        self.writer
            .send(&ApplicationToSam::Register {
                application: self.application.clone(),
                protocol_version: PROTOCOL_VERSION,
            })
            .await
    }

    pub async fn heartbeat(
        &mut self,
        health: HealthState,
        current_mode: SystemMode,
    ) -> Result<(), TransportError> {
        self.writer
            .send(&ApplicationToSam::Heartbeat {
                application: self.application.clone(),
                health,
                current_mode,
            })
            .await
    }

    pub async fn mode_ready(&mut self, transition_id: TransitionId) -> Result<(), TransportError> {
        self.writer
            .send(&ApplicationToSam::ModeReady {
                application: self.application.clone(),
                transition_id,
            })
            .await
    }

    pub async fn mode_rejected(
        &mut self,
        transition_id: TransitionId,
        reason: impl Into<String>,
    ) -> Result<(), TransportError> {
        self.writer
            .send(&ApplicationToSam::ModeRejected {
                application: self.application.clone(),
                transition_id,
                reason: reason.into(),
            })
            .await
    }

    pub async fn mode_committed(
        &mut self,
        transition_id: TransitionId,
        mode: SystemMode,
    ) -> Result<(), TransportError> {
        self.writer
            .send(&ApplicationToSam::ModeCommitted {
                application: self.application.clone(),
                transition_id,
                mode,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sam_transport::LocalListener;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Exercises the client against a real local listener rather than mocks,
    /// since the interesting behavior here (message shape, framing, transport
    /// wiring) only shows up end-to-end.
    fn unique_endpoint(tag: &str) -> LocalEndpoint {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("sam-client-test-{tag}-{}-{id}", std::process::id());
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

    #[tokio::test]
    async fn register_sends_application_id_and_protocol_version() {
        let endpoint = unique_endpoint("register");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            connection.receive::<ApplicationToSam>().await.unwrap()
        });

        let mut client = SamClient::connect("navigation", &endpoint).await.unwrap();
        client.register().await.unwrap();

        assert_eq!(
            server.await.unwrap(),
            ApplicationToSam::Register {
                application: ApplicationId::from("navigation"),
                protocol_version: PROTOCOL_VERSION,
            }
        );
    }

    #[tokio::test]
    async fn heartbeat_ready_rejected_and_committed_carry_the_right_fields() {
        let endpoint = unique_endpoint("lifecycle");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            let mut received = Vec::new();
            for _ in 0..4 {
                received.push(connection.receive::<ApplicationToSam>().await.unwrap());
            }
            received
        });

        let mut client = SamClient::connect("guidance", &endpoint).await.unwrap();
        client
            .heartbeat(HealthState::Degraded, SystemMode::Working)
            .await
            .unwrap();
        client.mode_ready(TransitionId(1)).await.unwrap();
        client
            .mode_rejected(TransitionId(1), "solution unavailable")
            .await
            .unwrap();
        client
            .mode_committed(TransitionId(2), SystemMode::Working)
            .await
            .unwrap();

        let application = ApplicationId::from("guidance");
        assert_eq!(
            server.await.unwrap(),
            vec![
                ApplicationToSam::Heartbeat {
                    application: application.clone(),
                    health: HealthState::Degraded,
                    current_mode: SystemMode::Working,
                },
                ApplicationToSam::ModeReady {
                    application: application.clone(),
                    transition_id: TransitionId(1),
                },
                ApplicationToSam::ModeRejected {
                    application: application.clone(),
                    transition_id: TransitionId(1),
                    reason: "solution unavailable".to_string(),
                },
                ApplicationToSam::ModeCommitted {
                    application,
                    transition_id: TransitionId(2),
                    mode: SystemMode::Working,
                },
            ]
        );
    }

    #[tokio::test]
    async fn next_message_receives_what_sam_sends() {
        let endpoint = unique_endpoint("incoming");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            connection
                .send(&SamToApplication::PrepareMode {
                    transition_id: TransitionId(5),
                    requested_mode: SystemMode::Working,
                })
                .await
                .unwrap();
        });

        let mut client = SamClient::connect("navigation", &endpoint).await.unwrap();
        let message = client.next_message().await.unwrap();
        server.await.unwrap();

        assert_eq!(
            message,
            SamToApplication::PrepareMode {
                transition_id: TransitionId(5),
                requested_mode: SystemMode::Working,
            }
        );
    }

    #[tokio::test]
    async fn split_sender_and_receiver_operate_independently() {
        let endpoint = unique_endpoint("split");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            let from_client = connection.receive::<ApplicationToSam>().await.unwrap();
            connection
                .send(&SamToApplication::CommitMode {
                    transition_id: TransitionId(9),
                    mode: SystemMode::Working,
                })
                .await
                .unwrap();
            from_client
        });

        let client = SamClient::connect("navigation", &endpoint).await.unwrap();
        let (mut receiver, mut sender) = client.into_split();

        sender
            .heartbeat(HealthState::Healthy, SystemMode::Startup)
            .await
            .unwrap();
        let pushed = receiver.next_message().await.unwrap();

        assert_eq!(
            server.await.unwrap(),
            ApplicationToSam::Heartbeat {
                application: ApplicationId::from("navigation"),
                health: HealthState::Healthy,
                current_mode: SystemMode::Startup,
            }
        );
        assert_eq!(
            pushed,
            SamToApplication::CommitMode {
                transition_id: TransitionId(9),
                mode: SystemMode::Working,
            }
        );
    }
}
