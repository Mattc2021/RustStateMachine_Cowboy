use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::TransportError;

/// Frames larger than this are rejected outright rather than allocated, on
/// both the send and receive paths. On receive in particular, this bounds
/// how much memory a single (possibly corrupt or hostile) 4-byte length
/// prefix can make us allocate before we've validated anything else.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024;

/// A message-oriented wrapper around a raw byte stream: every `send`/`receive`
/// transmits one postcard-encoded message prefixed with its length as a
/// big-endian `u32`, so message boundaries survive the underlying stream.
pub struct FramedConnection<S> {
    stream: S,
    max_frame_size: usize,
}

impl<S> FramedConnection<S> {
    pub fn new(stream: S) -> Self {
        Self::with_max_frame_size(stream, DEFAULT_MAX_FRAME_SIZE)
    }

    pub fn with_max_frame_size(stream: S, max_frame_size: usize) -> Self {
        Self {
            stream,
            max_frame_size,
        }
    }

    pub fn into_inner(self) -> S {
        self.stream
    }
}

impl<S> FramedConnection<S>
where
    S: AsyncRead + AsyncWrite,
{
    /// Splits the connection into an independent reader and writer (backed by
    /// `tokio::io::split`) so the two halves can be owned by separate tasks
    /// and used concurrently, e.g. one task reading incoming messages while
    /// another sends outgoing ones on the same underlying connection.
    pub fn into_split(self) -> (FramedReader<ReadHalf<S>>, FramedWriter<WriteHalf<S>>) {
        let Self {
            stream,
            max_frame_size,
        } = self;
        let (reader, writer) = split(stream);
        (
            FramedReader::with_max_frame_size(reader, max_frame_size),
            FramedWriter::with_max_frame_size(writer, max_frame_size),
        )
    }
}

pub struct FramedReader<R> {
    reader: R,
    max_frame_size: usize,
}

impl<R> FramedReader<R> {
    pub fn with_max_frame_size(reader: R, max_frame_size: usize) -> Self {
        Self {
            reader,
            max_frame_size,
        }
    }
}

impl<R> FramedReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, TransportError> {
        receive_frame(&mut self.reader, self.max_frame_size).await
    }
}

pub struct FramedWriter<W> {
    writer: W,
    max_frame_size: usize,
}

impl<W> FramedWriter<W> {
    pub fn with_max_frame_size(writer: W, max_frame_size: usize) -> Self {
        Self {
            writer,
            max_frame_size,
        }
    }
}

impl<W> FramedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn send<T: Serialize>(&mut self, message: &T) -> Result<(), TransportError> {
        send_frame(&mut self.writer, self.max_frame_size, message).await
    }
}

impl<S> FramedConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn send<T: Serialize>(&mut self, message: &T) -> Result<(), TransportError> {
        send_frame(&mut self.stream, self.max_frame_size, message).await
    }

    pub async fn receive<T: DeserializeOwned>(&mut self) -> Result<T, TransportError> {
        receive_frame(&mut self.stream, self.max_frame_size).await
    }
}

async fn send_frame<W, T>(writer: &mut W, maximum: usize, message: &T) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = postcard::to_allocvec(message)?;
    validate_size(payload.len(), maximum)?;
    let length = u32::try_from(payload.len()).map_err(|_| TransportError::FrameTooLarge {
        actual: payload.len(),
        maximum,
    })?;
    writer.write_all(&length.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn receive_frame<R, T>(reader: &mut R, maximum: usize) -> Result<T, TransportError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut length_bytes = [0_u8; 4];
    reader.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    // Validate before allocating: `length` comes straight off the wire, so a
    // corrupt or hostile prefix must not be able to force an arbitrarily
    // large allocation ahead of the size check below.
    validate_size(length, maximum)?;
    let mut payload = vec![0_u8; length];
    reader.read_exact(&mut payload).await?;
    Ok(postcard::from_bytes(&payload)?)
}

fn validate_size(actual: usize, maximum: usize) -> Result<(), TransportError> {
    if actual > maximum {
        return Err(TransportError::FrameTooLarge { actual, maximum });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncWriteExt};

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        id: u32,
        text: String,
    }

    fn sample_message() -> TestMessage {
        TestMessage {
            id: 7,
            text: "hello".to_string(),
        }
    }

    #[tokio::test]
    async fn send_then_receive_round_trips_a_message() {
        let (client, server) = duplex(1024);
        let mut client_conn = FramedConnection::new(client);
        let mut server_conn = FramedConnection::new(server);

        client_conn.send(&sample_message()).await.unwrap();
        let received: TestMessage = server_conn.receive().await.unwrap();

        assert_eq!(received, sample_message());
    }

    #[tokio::test]
    async fn multiple_messages_on_the_same_connection_stay_in_order() {
        let (client, server) = duplex(1024);
        let mut client_conn = FramedConnection::new(client);
        let mut server_conn = FramedConnection::new(server);

        let first = TestMessage {
            id: 1,
            text: "first".to_string(),
        };
        let second = TestMessage {
            id: 2,
            text: "second".to_string(),
        };
        client_conn.send(&first).await.unwrap();
        client_conn.send(&second).await.unwrap();

        assert_eq!(server_conn.receive::<TestMessage>().await.unwrap(), first);
        assert_eq!(server_conn.receive::<TestMessage>().await.unwrap(), second);
    }

    #[tokio::test]
    async fn split_reader_and_writer_still_round_trip() {
        let (client, server) = duplex(1024);
        let (mut client_reader, mut client_writer) = FramedConnection::new(client).into_split();
        let mut server_conn = FramedConnection::new(server);

        server_conn.send(&sample_message()).await.unwrap();
        assert_eq!(
            client_reader.receive::<TestMessage>().await.unwrap(),
            sample_message()
        );

        client_writer.send(&sample_message()).await.unwrap();
        assert_eq!(
            server_conn.receive::<TestMessage>().await.unwrap(),
            sample_message()
        );
    }

    #[tokio::test]
    async fn send_rejects_a_payload_larger_than_max_frame_size() {
        let (client, _server) = duplex(1024);
        // The encoded TestMessage below is comfortably larger than 4 bytes.
        let mut client_conn = FramedConnection::with_max_frame_size(client, 4);

        let error = client_conn.send(&sample_message()).await.unwrap_err();
        assert!(matches!(
            error,
            TransportError::FrameTooLarge { maximum: 4, .. }
        ));
    }

    #[tokio::test]
    async fn receive_rejects_a_length_prefix_larger_than_max_frame_size() {
        let (mut client, server) = duplex(1024);
        let mut server_conn = FramedConnection::with_max_frame_size(server, 4);

        // Write a length prefix claiming a huge payload; receive_frame must
        // reject this from the prefix alone, without waiting for that many
        // payload bytes to actually arrive.
        client
            .write_all(&1_000_000_u32.to_be_bytes())
            .await
            .unwrap();

        let error = server_conn.receive::<TestMessage>().await.unwrap_err();
        assert!(matches!(
            error,
            TransportError::FrameTooLarge {
                actual: 1_000_000,
                maximum: 4
            }
        ));
    }

    #[tokio::test]
    async fn receive_on_a_closed_connection_before_any_frame_is_an_io_error() {
        let (client, server) = duplex(1024);
        let mut server_conn = FramedConnection::new(server);
        drop(client);

        let error = server_conn.receive::<TestMessage>().await.unwrap_err();
        assert!(matches!(error, TransportError::Io(_)));
    }

    #[tokio::test]
    async fn receive_on_a_connection_closed_mid_frame_is_an_io_error() {
        let (mut client, server) = duplex(1024);
        let mut server_conn = FramedConnection::new(server);

        // Announce a frame, then disappear before sending its payload.
        client.write_all(&16_u32.to_be_bytes()).await.unwrap();
        drop(client);

        let error = server_conn.receive::<TestMessage>().await.unwrap_err();
        assert!(matches!(error, TransportError::Io(_)));
    }
}
