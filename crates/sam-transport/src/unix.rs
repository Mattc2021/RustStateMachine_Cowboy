use tokio::net::{UnixListener, UnixStream};

use crate::{FramedConnection, LocalEndpoint, TransportError};

pub type PlatformClientStream = UnixStream;
pub type PlatformServerStream = UnixStream;

/// Listens on a Unix domain socket. Unlike the Windows named-pipe listener,
/// a single bound socket can accept any number of client connections without
/// needing to be re-armed between them.
pub struct LocalListener {
    listener: UnixListener,
}

impl LocalListener {
    /// Binds the socket at `endpoint`'s path. Fails if a file already exists
    /// there (e.g. a stale socket left behind by a previous run that didn't
    /// clean up) — the caller is responsible for removing it first if that's
    /// the desired behavior.
    pub fn bind(endpoint: &LocalEndpoint) -> Result<Self, TransportError> {
        Ok(Self {
            listener: UnixListener::bind(endpoint.as_path())?,
        })
    }

    pub async fn accept(
        &mut self,
    ) -> Result<FramedConnection<PlatformServerStream>, TransportError> {
        let (stream, _) = self.listener.accept().await?;
        Ok(FramedConnection::new(stream))
    }
}

pub async fn connect(
    endpoint: &LocalEndpoint,
) -> Result<FramedConnection<PlatformClientStream>, TransportError> {
    let stream = UnixStream::connect(endpoint.as_path()).await?;
    Ok(FramedConnection::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Removes the socket file on drop so a test never leaves behind state
    /// that could make a later run of `bind` fail with "address in use".
    struct SocketGuard(LocalEndpoint);

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0.as_path());
        }
    }

    fn unique_endpoint(tag: &str) -> (LocalEndpoint, SocketGuard) {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sam-transport-test-{tag}-{}-{id}.sock",
            std::process::id()
        ));
        let endpoint = LocalEndpoint::new(path.to_string_lossy().into_owned());
        let guard = SocketGuard(endpoint.clone());
        (endpoint, guard)
    }

    #[tokio::test]
    async fn accepted_connection_exchanges_frames_with_the_client() {
        let (endpoint, _guard) = unique_endpoint("echo");
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
    async fn listener_accepts_multiple_clients_without_rebinding() {
        let (endpoint, _guard) = unique_endpoint("multi");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let _first_client = connect(&endpoint).await.unwrap();
        let _first_server = listener.accept().await.unwrap();

        let _second_client = connect(&endpoint).await.unwrap();
        let _second_server = listener.accept().await.unwrap();
    }

    #[test]
    fn bind_fails_when_socket_file_already_exists() {
        let (endpoint, _guard) = unique_endpoint("conflict");
        let _first = LocalListener::bind(&endpoint).unwrap();

        assert!(matches!(
            LocalListener::bind(&endpoint),
            Err(TransportError::Io(_))
        ));
    }
}
