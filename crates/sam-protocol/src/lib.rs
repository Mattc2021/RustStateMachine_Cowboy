pub mod messages;
pub mod types;

pub use messages::{ApplicationToSam, SamToApplication};
pub use types::{ApplicationId, HealthState, SystemMode, TransitionId};

/// Version of the `ApplicationToSam`/`SamToApplication` wire protocol spoken
/// by this build. Sent in `Register` and echoed back in `RegisterAccepted` so
/// a future SAM/client mismatch can be detected explicitly at connect time
/// rather than failing confusingly on the first subsequent message.
pub const PROTOCOL_VERSION: u16 = 1;
