#[cfg(unix)]
use std::path::{Path, PathBuf};

/// Address of a local IPC endpoint: a Unix domain socket path on Unix, or a
/// named pipe path (`\\.\pipe\...`) on Windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalEndpoint(String);

impl LocalEndpoint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The well-known endpoint SAM listens on and applications connect to by
    /// default, in the platform-appropriate form.
    #[cfg(unix)]
    pub fn sam_default() -> Self {
        Self::new("/tmp/sam.sock")
    }

    #[cfg(windows)]
    pub fn sam_default() -> Self {
        Self::new(r"\\.\pipe\sam")
    }

    #[cfg(unix)]
    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }

    #[cfg(unix)]
    pub fn parent(&self) -> Option<PathBuf> {
        self.as_path().parent().map(Path::to_path_buf)
    }
}

impl Default for LocalEndpoint {
    fn default() -> Self {
        Self::sam_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_and_as_str_round_trip() {
        let endpoint = LocalEndpoint::new("some-endpoint");
        assert_eq!(endpoint.as_str(), "some-endpoint");
    }

    #[test]
    fn new_accepts_owned_string_and_str_slice() {
        let from_str = LocalEndpoint::new("endpoint");
        let from_string = LocalEndpoint::new(String::from("endpoint"));
        assert_eq!(from_str, from_string);
    }

    #[test]
    fn default_matches_sam_default() {
        assert_eq!(LocalEndpoint::default(), LocalEndpoint::sam_default());
    }

    #[cfg(windows)]
    #[test]
    fn windows_default_is_a_named_pipe_path() {
        assert_eq!(LocalEndpoint::sam_default().as_str(), r"\\.\pipe\sam");
    }

    #[cfg(unix)]
    #[test]
    fn unix_default_is_a_socket_path() {
        assert_eq!(LocalEndpoint::sam_default().as_str(), "/tmp/sam.sock");
    }

    #[cfg(unix)]
    #[test]
    fn as_path_and_parent_reflect_the_underlying_path() {
        let endpoint = LocalEndpoint::new("/tmp/sam.sock");
        assert_eq!(endpoint.as_path(), Path::new("/tmp/sam.sock"));
        assert_eq!(endpoint.parent(), Some(PathBuf::from("/tmp")));
    }

    #[cfg(unix)]
    #[test]
    fn parent_is_none_for_a_relative_path_with_no_directory() {
        let endpoint = LocalEndpoint::new("sam.sock");
        assert_eq!(endpoint.parent(), Some(PathBuf::new()));
    }
}
