use std::sync::{Arc, RwLock};

/// Observable state of the rclone engine.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineState {
    /// rclone binary not yet downloaded.
    NotInstalled,
    /// Binary is being downloaded. Progress in bytes.
    Downloading { downloaded: u64, total: Option<u64> },
    /// Download complete; verifying checksum and placing binary.
    Installing,
    /// Binary is installed but the rcd daemon is not running.
    Stopped,
    /// Daemon is starting; waiting for health-check.
    Starting,
    /// Daemon is running and accepting rc requests.
    Ready,
    /// A non-recoverable error occurred.
    Error { message: String },
}

impl std::fmt::Display for EngineState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInstalled => write!(f, "NotInstalled"),
            Self::Downloading { downloaded, total } => match total {
                Some(t) => write!(f, "Downloading({downloaded}/{t})"),
                None => write!(f, "Downloading({downloaded}/?)"),
            },
            Self::Installing => write!(f, "Installing"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Starting => write!(f, "Starting"),
            Self::Ready => write!(f, "Ready"),
            Self::Error { message } => write!(f, "Error({message})"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SharedState(Arc<RwLock<EngineState>>);

impl SharedState {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(EngineState::NotInstalled)))
    }

    pub fn get(&self) -> EngineState {
        self.0.read().unwrap().clone()
    }

    pub fn set(&self, s: EngineState) {
        *self.0.write().unwrap() = s;
    }
}
