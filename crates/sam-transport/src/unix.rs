use std::path::PathBuf;

use tokio::net::{UnixListener, UnixStream};
use tracing::debug;

use crate::{FramedConnection, LocalEndpoint, TransportError};

pub type PlatformClientStream = UnixStream;
pub type PlatformServerStream = UnixStream;

/// Listens on a Unix domain socket. Unlike the Windows named-pipe listener,
/// a single bound socket can accept any number of client connections without
/// needing to be re-armed between them.
///
/// Removes its socket file when dropped, so a clean shutdown never leaves a
/// stale path behind for the next `bind` to trip over. A crash still leaves
/// the file in place (`Drop` doesn't run), which is why `bind` itself refuses
/// to reuse an existing path rather than assuming it's safe to unlink.
pub struct LocalListener {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl LocalListener {
    /// Binds the socket at `endpoint`'s path. Fails if a file already exists
    /// there (e.g. a stale socket left behind by a process that didn't shut
    /// down cleanly) — the caller is responsible for removing it first if
    /// that's the desired behavior.
    pub fn bind(endpoint: &LocalEndpoint) -> Result<Self, TransportError> {
        debug!(endpoint = %endpoint.as_str(), "binding Unix-domain socket");
        Ok(Self {
            listener: UnixListener::bind(endpoint.as_path())?,
            socket_path: endpoint.as_path().to_path_buf(),
        })
    }

    pub async fn accept(
        &mut self,
    ) -> Result<FramedConnection<PlatformServerStream>, TransportError> {
        let (stream, _) = self.listener.accept().await?;
        Ok(FramedConnection::new(stream))
    }
}

impl Drop for LocalListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

pub async fn connect(
    endpoint: &LocalEndpoint,
) -> Result<FramedConnection<PlatformClientStream>, TransportError> {
    debug!(endpoint = %endpoint.as_str(), "connecting Unix-domain socket");
    let stream = UnixStream::connect(endpoint.as_path()).await?;
    Ok(FramedConnection::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn unique_endpoint(tag: &str) -> LocalEndpoint {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sam-transport-test-{tag}-{}-{id}.sock",
            std::process::id()
        ));
        LocalEndpoint::new(path.to_string_lossy().into_owned())
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
    async fn listener_accepts_multiple_clients_without_rebinding() {
        let endpoint = unique_endpoint("multi");
        let mut listener = LocalListener::bind(&endpoint).unwrap();

        let _first_client = connect(&endpoint).await.unwrap();
        let _first_server = listener.accept().await.unwrap();

        let _second_client = connect(&endpoint).await.unwrap();
        let _second_server = listener.accept().await.unwrap();
    }

    #[test]
    fn bind_fails_when_socket_file_already_exists() {
        let endpoint = unique_endpoint("conflict");
        let first = LocalListener::bind(&endpoint).unwrap();

        assert!(matches!(
            LocalListener::bind(&endpoint),
            Err(TransportError::Io(_))
        ));
        drop(first);
    }

    #[test]
    fn dropping_the_listener_removes_its_socket_file() {
        let endpoint = unique_endpoint("cleanup");
        let listener = LocalListener::bind(&endpoint).unwrap();
        assert!(endpoint.as_path().exists());

        drop(listener);
        assert!(!endpoint.as_path().exists());
    }
}
