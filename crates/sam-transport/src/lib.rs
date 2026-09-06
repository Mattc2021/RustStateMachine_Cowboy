//! Local IPC transport for SAM: a length-prefixed, postcard-encoded framing
//! layer (`frame`) on top of a platform-specific local channel — Unix domain
//! sockets (`unix`) or Windows named pipes (`windows`) — addressed by a
//! single cross-platform `LocalEndpoint`.

mod endpoint;
mod error;
mod frame;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(not(any(unix, windows)))]
compile_error!("sam-transport currently supports Unix-like systems and Windows");

pub use endpoint::LocalEndpoint;
pub use error::{is_disconnect, TransportError};
pub use frame::{FramedConnection, FramedReader, FramedWriter, DEFAULT_MAX_FRAME_SIZE};

#[cfg(unix)]
pub use unix::{connect, LocalListener, PlatformClientStream, PlatformServerStream};
#[cfg(windows)]
pub use windows::{connect, LocalListener, PlatformClientStream, PlatformServerStream};
