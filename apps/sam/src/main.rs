use sam_protocol::ApplicationToSam;
use sam_transport::{LocalEndpoint, LocalListener, TransportError};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let endpoint = LocalEndpoint::default();
    let mut listener = LocalListener::bind(&endpoint)?;
    println!("SAM listening on {}", endpoint.as_str());

    loop {
        let mut connection = listener.accept().await?;
        tokio::spawn(async move {
            loop {
                match connection.receive::<ApplicationToSam>().await {
                    Ok(message) => println!("received: {message:?}"),
                    Err(error) if is_disconnect(&error) => break,
                    Err(error) => {
                        eprintln!("connection failed: {error}");
                        break;
                    }
                }
            }
        });
    }
}

/// Whether `error` represents the peer simply going away (closing the
/// connection, resetting it, or the pipe breaking) rather than a genuine
/// transport failure. Such errors end that connection's read loop quietly
/// instead of being logged as a failure.
fn is_disconnect(error: &TransportError) -> bool {
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

    fn io_error(kind: std::io::ErrorKind) -> TransportError {
        TransportError::Io(std::io::Error::new(kind, "test"))
    }

    #[test]
    fn unexpected_eof_connection_reset_and_broken_pipe_are_disconnects() {
        assert!(is_disconnect(&io_error(std::io::ErrorKind::UnexpectedEof)));
        assert!(is_disconnect(&io_error(std::io::ErrorKind::ConnectionReset)));
        assert!(is_disconnect(&io_error(std::io::ErrorKind::BrokenPipe)));
    }

    #[test]
    fn other_io_errors_are_not_disconnects() {
        assert!(!is_disconnect(&io_error(std::io::ErrorKind::PermissionDenied)));
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
