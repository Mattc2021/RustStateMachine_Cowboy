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
}
