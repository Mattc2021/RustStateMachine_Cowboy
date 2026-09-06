use thiserror::Error;

/// Failure modes for the framed transport layer: the underlying OS transport
/// (`Io`), message encoding (`Serialization`), a frame that would exceed the
/// configured `max_frame_size` (`FrameTooLarge`), or a client that gave up
/// waiting for the server side to become available (`ConnectTimeout`, only
/// produced by the Windows named-pipe `connect`, since Unix socket connects
/// don't retry).
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("message serialization failed: {0}")]
    Serialization(#[from] postcard::Error),
    #[error("frame size {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("connection attempt timed out")]
    ConnectTimeout,
}

/// Whether `error` represents the peer simply going away (closing the
/// connection, resetting it, or the pipe breaking) rather than a genuine
/// transport failure. Callers typically end a connection's read loop quietly
/// on this instead of logging it as a failure.
pub fn is_disconnect(error: &TransportError) -> bool {
    match error {
        TransportError::Io(io_error) => matches!(
            io_error.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts_via_from_and_formats_its_source() {
        let io_error = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe closed");
        let transport_error: TransportError = io_error.into();
        assert!(matches!(transport_error, TransportError::Io(_)));
        assert_eq!(
            transport_error.to_string(),
            "transport I/O failed: pipe closed"
        );
    }

    #[test]
    fn postcard_error_converts_via_from() {
        // Decoding an empty buffer as a non-trivial type is a reliable way to
        // provoke a postcard::Error without depending on its internals.
        let postcard_error = postcard::from_bytes::<u32>(&[]).unwrap_err();
        let transport_error: TransportError = postcard_error.into();
        assert!(matches!(transport_error, TransportError::Serialization(_)));
    }

    #[test]
    fn frame_too_large_formats_both_sizes() {
        let error = TransportError::FrameTooLarge {
            actual: 100,
            maximum: 64,
        };
        assert_eq!(
            error.to_string(),
            "frame size 100 exceeds configured maximum 64"
        );
    }

    #[test]
    fn connect_timeout_has_a_fixed_message() {
        assert_eq!(
            TransportError::ConnectTimeout.to_string(),
            "connection attempt timed out"
        );
    }

    fn io_error(kind: std::io::ErrorKind) -> TransportError {
        TransportError::Io(std::io::Error::new(kind, "test"))
    }

    #[test]
    fn unexpected_eof_connection_reset_and_broken_pipe_are_disconnects() {
        assert!(is_disconnect(&io_error(std::io::ErrorKind::UnexpectedEof)));
        assert!(is_disconnect(&io_error(
            std::io::ErrorKind::ConnectionReset
        )));
        assert!(is_disconnect(&io_error(std::io::ErrorKind::BrokenPipe)));
    }

    #[test]
    fn other_io_errors_are_not_disconnects() {
        assert!(!is_disconnect(&io_error(
            std::io::ErrorKind::PermissionDenied
        )));
        assert!(!is_disconnect(&io_error(std::io::ErrorKind::InvalidData)));
    }

    #[test]
    fn non_io_errors_are_not_disconnects() {
        assert!(!is_disconnect(&TransportError::ConnectTimeout));
        assert!(!is_disconnect(&TransportError::FrameTooLarge {
            actual: 100,
            maximum: 10,
        }));
    }
}
