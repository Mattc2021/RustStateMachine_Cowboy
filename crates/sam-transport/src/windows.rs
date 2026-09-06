use std::{io, time::Duration};

use tokio::{
    net::windows::named_pipe::{ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions},
    time::{sleep, Instant},
};
use tracing::{debug, trace};

use crate::{FramedConnection, LocalEndpoint, TransportError};

const ERROR_FILE_NOT_FOUND: i32 = 2;
const ERROR_PIPE_BUSY: i32 = 231;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_millis(25);

pub type PlatformClientStream = NamedPipeClient;
pub type PlatformServerStream = NamedPipeServer;

/// Listens on a Windows named pipe. Each `NamedPipeServer` instance can serve
/// exactly one client, so the listener keeps one connected-but-not-yet-accepted
/// ("pending") instance around at all times and creates a replacement every
/// time `accept` hands one off, so there is always an instance available for
/// the next client to connect to.
pub struct LocalListener {
    pipe_name: String,
    pending: Option<NamedPipeServer>,
}

impl LocalListener {
    /// Creates the first pipe instance for `endpoint`. `first_pipe_instance`
    /// makes pipe creation fail if another listener is already bound to the
    /// same name, so two SAM servers can't silently share one pipe.
    pub fn bind(endpoint: &LocalEndpoint) -> Result<Self, TransportError> {
        if !endpoint.as_str().starts_with(r"\\.\pipe\") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Windows endpoint must begin with \\\\.\\pipe\\",
            )
            .into());
        }

        debug!(endpoint = %endpoint.as_str(), "binding Windows named pipe");
        let pending = ServerOptions::new()
            .first_pipe_instance(true)
            .create(endpoint.as_str())?;

        Ok(Self {
            pipe_name: endpoint.as_str().to_owned(),
            pending: Some(pending),
        })
    }

    /// Waits for a client to connect to the current pending pipe instance,
    /// then immediately arms a fresh instance so a subsequent client can
    /// connect while this one is served.
    pub async fn accept(
        &mut self,
    ) -> Result<FramedConnection<PlatformServerStream>, TransportError> {
        let server = self.pending.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "named-pipe listener is closed")
        })?;
        server.connect().await?;
        self.pending = Some(ServerOptions::new().create(&self.pipe_name)?);
        Ok(FramedConnection::new(server))
    }
}

/// Connects to a named pipe server, retrying while the pipe either doesn't
/// exist yet (server hasn't started `bind` yet) or is busy (every existing
/// instance is already serving a client), up to `CONNECT_TIMEOUT`.
pub async fn connect(
    endpoint: &LocalEndpoint,
) -> Result<FramedConnection<PlatformClientStream>, TransportError> {
    let deadline = Instant::now() + CONNECT_TIMEOUT;

    loop {
        trace!(endpoint = %endpoint.as_str(), "attempting Windows named-pipe connection");
        match ClientOptions::new().open(endpoint.as_str()) {
            Ok(client) => {
                debug!(endpoint = %endpoint.as_str(), "Windows named-pipe connection established");
                return Ok(FramedConnection::new(client));
            }
            Err(error) if retryable(&error) && Instant::now() < deadline => {
                sleep(RETRY_DELAY).await;
            }
            Err(error) if retryable(&error) => return Err(TransportError::ConnectTimeout),
            Err(error) => return Err(error.into()),
        }
    }
}

fn retryable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(ERROR_FILE_NOT_FOUND) | Some(ERROR_PIPE_BUSY)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Named pipes are a shared OS-wide namespace, so each test needs its own
    /// name to avoid colliding with other tests (or a previous run's
    /// leftovers) when run concurrently.
    fn unique_endpoint(tag: &str) -> LocalEndpoint {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        LocalEndpoint::new(format!(
            r"\\.\pipe\sam-transport-test-{tag}-{}-{id}",
            std::process::id()
        ))
    }

    #[test]
    fn bind_rejects_endpoint_without_pipe_prefix() {
        let endpoint = LocalEndpoint::new(r"C:\not\a\pipe");
        assert!(matches!(
            LocalListener::bind(&endpoint),
            Err(TransportError::Io(_))
        ));
    }

    #[tokio::test]
    async fn accepted_connection_exchanges_frames_with_the_client() {
        let endpoint = unique_endpoint("echo");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let server = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            let message: String = connection.receive().await.unwrap();
            connection.send(&message).await.unwrap();
        });

        let mut client = connect(&endpoint).await.unwrap();
        client.send(&"hello".to_string()).await.unwrap();
        let echoed: String = client.receive().await.unwrap();

        server.await.unwrap();
        assert_eq!(echoed, "hello");
    }

    #[tokio::test]
    async fn listener_accepts_a_second_client_after_the_first_disconnects() {
        let endpoint = unique_endpoint("rearm");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let first_client = connect(&endpoint).await.unwrap();
        let _first_server = listener.accept().await.unwrap();
        drop(first_client);

        // If accept() failed to arm a fresh pipe instance, this second
        // connect would hang until it hits the client's CONNECT_TIMEOUT.
        let second_client = connect(&endpoint).await.unwrap();
        let _second_server = listener.accept().await.unwrap();
        drop(second_client);
    }
}
