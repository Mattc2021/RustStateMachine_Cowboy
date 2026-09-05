//! Core state machine logic for SAM (System App Manager): coordinating mode
//! transitions across applications (`mode`), tracking which applications are
//! known and their last-reported status (`registry`), aggregating that into
//! overall system health (`health`), and the resulting system-wide snapshot
//! (`state`).

pub mod health;
pub mod mode;
pub mod registry;
pub mod state;

pub use health::HealthManager;
pub use mode::{ModeError, ModeEvent, ModeManager, ModeManagerState};
pub use registry::{ApplicationRegistry, ApplicationStatus, RegistryError};
pub use state::SystemState;
