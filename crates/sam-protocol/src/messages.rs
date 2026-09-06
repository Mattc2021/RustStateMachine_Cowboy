use serde::{Deserialize, Serialize};

use crate::{ApplicationId, HealthState, SystemMode, TransitionId};

/// Messages an application is allowed to send to SAM.
///
/// These mirror the application-facing half of the two-phase mode transition
/// protocol implemented by `sam_core::ModeManager`: `Register`/`Heartbeat` keep
/// SAM aware of an application's liveness and health, while `ModeReady`,
/// `ModeRejected`, and `ModeCommitted` are responses to a `PrepareMode` /
/// `CommitMode` request sent by SAM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplicationToSam {
    Register {
        application: ApplicationId,
        /// The `PROTOCOL_VERSION` the application was built against.
        protocol_version: u16,
    },
    Heartbeat {
        application: ApplicationId,
        health: HealthState,
        current_mode: SystemMode,
    },
    ModeReady {
        application: ApplicationId,
        transition_id: TransitionId,
    },
    ModeRejected {
        application: ApplicationId,
        transition_id: TransitionId,
        reason: String,
    },
    ModeCommitted {
        application: ApplicationId,
        transition_id: TransitionId,
        mode: SystemMode,
    },
}

/// Messages that SAM sends to applications.
///
/// `PrepareMode` begins a transition (expects `ModeReady`/`ModeRejected` in
/// response), `CommitMode` finalizes it once every application is ready
/// (expects `ModeCommitted` in response), and `AbortMode` cancels a transition
/// that was rejected by a participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SamToApplication {
    RegisterAccepted {
        /// The `PROTOCOL_VERSION` SAM itself is running, echoed back so the
        /// application can detect a mismatch with its own version.
        protocol_version: u16,
        current_mode: SystemMode,
        system_health: HealthState,
    },
    PrepareMode {
        transition_id: TransitionId,
        requested_mode: SystemMode,
    },
    CommitMode {
        transition_id: TransitionId,
        mode: SystemMode,
    },
    AbortMode {
        transition_id: TransitionId,
    },
    StateBroadcast {
        mode: SystemMode,
        health: HealthState,
    },
    /// Added at the end to preserve Postcard's existing enum discriminants.
    RegisterRejected {
        supported_protocol_version: u16,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str) -> ApplicationId {
        ApplicationId::from(name)
    }

    /// Every message variant must survive a JSON serialize/deserialize round
    /// trip unchanged. This doesn't exercise the actual Postcard wire format
    /// (see `sam_transport::frame` for that), but it does verify every field
    /// on every variant serializes and deserializes back to an equal value.
    fn assert_json_round_trip<T>(value: T)
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_string(&value).expect("serialize");
        let decoded: T = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(value, decoded);
    }

    #[test]
    fn application_to_sam_variants_round_trip() {
        assert_json_round_trip(ApplicationToSam::Register {
            application: app("navigation"),
            protocol_version: 0,
        });
        assert_json_round_trip(ApplicationToSam::Heartbeat {
            application: app("navigation"),
            health: HealthState::Degraded,
            current_mode: SystemMode::Working,
        });
        assert_json_round_trip(ApplicationToSam::ModeReady {
            application: app("navigation"),
            transition_id: TransitionId(7),
        });
        assert_json_round_trip(ApplicationToSam::ModeRejected {
            application: app("navigation"),
            transition_id: TransitionId(7),
            reason: "solution unavailable".to_string(),
        });
        assert_json_round_trip(ApplicationToSam::ModeCommitted {
            application: app("navigation"),
            transition_id: TransitionId(7),
            mode: SystemMode::Working,
        });
    }

    #[test]
    fn sam_to_application_variants_round_trip() {
        assert_json_round_trip(SamToApplication::RegisterAccepted {
            current_mode: SystemMode::Startup,
            system_health: HealthState::Healthy,
            protocol_version: 0,
        });
        assert_json_round_trip(SamToApplication::PrepareMode {
            transition_id: TransitionId(1),
            requested_mode: SystemMode::Working,
        });
        assert_json_round_trip(SamToApplication::CommitMode {
            transition_id: TransitionId(1),
            mode: SystemMode::Working,
        });
        assert_json_round_trip(SamToApplication::AbortMode {
            transition_id: TransitionId(1),
        });
        assert_json_round_trip(SamToApplication::StateBroadcast {
            mode: SystemMode::Standby,
            health: HealthState::Failed,
        });
        assert_json_round_trip(SamToApplication::RegisterRejected {
            supported_protocol_version: 1,
            reason: "unsupported protocol version 0".to_string(),
        });
    }

    #[test]
    fn distinct_variants_are_not_equal() {
        let ready = ApplicationToSam::ModeReady {
            application: app("navigation"),
            transition_id: TransitionId(1),
        };
        let rejected = ApplicationToSam::ModeRejected {
            application: app("navigation"),
            transition_id: TransitionId(1),
            reason: "n/a".to_string(),
        };
        assert_ne!(ready, rejected);
    }
}
