use std::collections::HashSet;

use sam_protocol::{ApplicationId, SystemMode, TransitionId};
use thiserror::Error;

/// Phase of the mode transition state machine.
///
/// A transition moves through two rounds of unanimous consent:
/// `Stable` -> `Preparing` (every participant must call `application_ready`
/// or `application_rejected`) -> `Committing` (every participant that was
/// ready must call `application_committed`) -> `Stable` again in the new
/// mode. A single rejection during `Preparing` aborts the whole transition
/// back to `Stable` in the original mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModeManagerState {
    Stable {
        mode: SystemMode,
    },
    Preparing {
        transition_id: TransitionId,
        current_mode: SystemMode,
        target_mode: SystemMode,
        waiting_for: HashSet<ApplicationId>,
        ready: HashSet<ApplicationId>,
    },
    Committing {
        transition_id: TransitionId,
        previous_mode: SystemMode,
        target_mode: SystemMode,
        waiting_for: HashSet<ApplicationId>,
        committed: HashSet<ApplicationId>,
    },
}

/// Notable state transitions produced by `ModeManager`, meant to be relayed
/// to interested applications (e.g. as `SamToApplication::CommitMode` /
/// `AbortMode` broadcasts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModeEvent {
    /// Every participant is ready; SAM should now broadcast `CommitMode`.
    PreparationComplete {
        transition_id: TransitionId,
        target_mode: SystemMode,
    },
    /// Every participant has committed; the system is stable in `mode`.
    TransitionComplete {
        transition_id: TransitionId,
        mode: SystemMode,
    },
    /// A participant rejected the transition during preparation; the system
    /// remains (or has reverted to) its previous mode.
    TransitionAborted {
        transition_id: TransitionId,
        rejected_by: ApplicationId,
        reason: String,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ModeError {
    #[error("a mode transition is already in progress")]
    TransitionInProgress,
    #[error("target mode is already active")]
    AlreadyInMode,
    #[error("no applications are available to approve the transition")]
    NoParticipants,
    #[error("response does not match the active transition")]
    StaleTransition,
    #[error("application is not participating in this transition: {0:?}")]
    UnknownParticipant(ApplicationId),
    #[error("response is invalid during the current transition phase")]
    InvalidPhase,
    #[error("committed mode does not match the requested mode")]
    WrongCommittedMode,
}

/// Drives a single system mode through the two-phase (prepare/commit)
/// transition protocol described on `ModeManagerState`. Only one transition
/// may be in flight at a time.
#[derive(Debug)]
pub struct ModeManager {
    state: ModeManagerState,
    next_transition_id: u64,
}

impl ModeManager {
    pub fn new(initial_mode: SystemMode) -> Self {
        Self {
            state: ModeManagerState::Stable { mode: initial_mode },
            next_transition_id: 1,
        }
    }

    pub fn state(&self) -> &ModeManagerState {
        &self.state
    }

    /// The mode the system is currently operating in (or transitioning away
    /// from, if a transition is in progress). This is only ever the *new*
    /// mode once a transition has fully committed.
    pub fn current_mode(&self) -> SystemMode {
        match &self.state {
            ModeManagerState::Stable { mode } => *mode,
            ModeManagerState::Preparing { current_mode, .. } => *current_mode,
            ModeManagerState::Committing { previous_mode, .. } => *previous_mode,
        }
    }

    /// Begins a new transition to `target_mode`, entering the `Preparing`
    /// phase and waiting on `application_ready`/`application_rejected` from
    /// every id in `participants`.
    ///
    /// Fails with `TransitionInProgress` if a transition is already underway,
    /// `AlreadyInMode` if `target_mode` matches the current mode, and
    /// `NoParticipants` if `participants` is empty (a transition with no one
    /// to confirm it could never complete).
    pub fn request_transition(
        &mut self,
        target_mode: SystemMode,
        participants: impl IntoIterator<Item = ApplicationId>,
    ) -> Result<TransitionId, ModeError> {
        let current_mode = match &self.state {
            ModeManagerState::Stable { mode } => *mode,
            _ => return Err(ModeError::TransitionInProgress),
        };

        if current_mode == target_mode {
            return Err(ModeError::AlreadyInMode);
        }

        let waiting_for: HashSet<_> = participants.into_iter().collect();
        if waiting_for.is_empty() {
            return Err(ModeError::NoParticipants);
        }

        let transition_id = TransitionId(self.next_transition_id);
        self.next_transition_id = self.next_transition_id.wrapping_add(1);
        self.state = ModeManagerState::Preparing {
            transition_id,
            current_mode,
            target_mode,
            waiting_for,
            ready: HashSet::new(),
        };
        Ok(transition_id)
    }

    /// Records that `application` has approved the in-progress transition.
    /// Once every waiting participant has responded, advances the state to
    /// `Committing` and returns `Some(ModeEvent::PreparationComplete)`;
    /// otherwise returns `None` while still waiting on the rest.
    ///
    /// Fails with `InvalidPhase` if no transition is in `Preparing`,
    /// `StaleTransition` if `response_id` doesn't match the active
    /// transition, and `UnknownParticipant` if `application` was not part of
    /// the original participant set (or has already responded).
    pub fn application_ready(
        &mut self,
        application: &ApplicationId,
        response_id: TransitionId,
    ) -> Result<Option<ModeEvent>, ModeError> {
        let (transition_id, previous_mode, target_mode, waiting_for, ready) = match &mut self.state
        {
            ModeManagerState::Preparing {
                transition_id,
                current_mode,
                target_mode,
                waiting_for,
                ready,
            } => (
                *transition_id,
                *current_mode,
                *target_mode,
                waiting_for,
                ready,
            ),
            _ => return Err(ModeError::InvalidPhase),
        };

        if transition_id != response_id {
            return Err(ModeError::StaleTransition);
        }
        if !waiting_for.remove(application) {
            return Err(ModeError::UnknownParticipant(application.clone()));
        }
        ready.insert(application.clone());

        if waiting_for.is_empty() {
            let committed_participants = ready.clone();
            self.state = ModeManagerState::Committing {
                transition_id,
                previous_mode,
                target_mode,
                waiting_for: committed_participants,
                committed: HashSet::new(),
            };
            return Ok(Some(ModeEvent::PreparationComplete {
                transition_id,
                target_mode,
            }));
        }
        Ok(None)
    }

    /// Records that `application` has rejected the in-progress transition
    /// during the `Preparing` phase, immediately aborting it: the state
    /// reverts to `Stable` in the mode the system was already in, and every
    /// other participant's response (ready or otherwise) is discarded.
    ///
    /// `application` may be in either `waiting_for` or `ready` — a
    /// participant that already said it was ready is still allowed to back
    /// out before the transition commits. Fails with `InvalidPhase`,
    /// `StaleTransition`, or `UnknownParticipant` under the same conditions
    /// as `application_ready`.
    pub fn application_rejected(
        &mut self,
        application: &ApplicationId,
        response_id: TransitionId,
        reason: impl Into<String>,
    ) -> Result<ModeEvent, ModeError> {
        let (transition_id, current_mode, is_participant) = match &self.state {
            ModeManagerState::Preparing {
                transition_id,
                current_mode,
                waiting_for,
                ready,
                ..
            } => (
                *transition_id,
                *current_mode,
                waiting_for.contains(application) || ready.contains(application),
            ),
            _ => return Err(ModeError::InvalidPhase),
        };

        if transition_id != response_id {
            return Err(ModeError::StaleTransition);
        }
        if !is_participant {
            return Err(ModeError::UnknownParticipant(application.clone()));
        }

        let event = ModeEvent::TransitionAborted {
            transition_id,
            rejected_by: application.clone(),
            reason: reason.into(),
        };
        self.state = ModeManagerState::Stable { mode: current_mode };
        Ok(event)
    }

    /// Records that `application` has committed to `committed_mode` during
    /// the `Committing` phase. Once every participant that was ready has
    /// committed, advances the state to `Stable` in the new mode and returns
    /// `Some(ModeEvent::TransitionComplete)`; otherwise returns `None`.
    ///
    /// Fails with `InvalidPhase`, `StaleTransition`, or `UnknownParticipant`
    /// under the same conditions as `application_ready`, and with
    /// `WrongCommittedMode` if `committed_mode` doesn't match the transition's
    /// target mode.
    pub fn application_committed(
        &mut self,
        application: &ApplicationId,
        response_id: TransitionId,
        committed_mode: SystemMode,
    ) -> Result<Option<ModeEvent>, ModeError> {
        let (transition_id, target_mode, waiting_for, committed) = match &mut self.state {
            ModeManagerState::Committing {
                transition_id,
                target_mode,
                waiting_for,
                committed,
                ..
            } => (*transition_id, *target_mode, waiting_for, committed),
            _ => return Err(ModeError::InvalidPhase),
        };

        if transition_id != response_id {
            return Err(ModeError::StaleTransition);
        }
        if target_mode != committed_mode {
            return Err(ModeError::WrongCommittedMode);
        }
        if !waiting_for.remove(application) {
            return Err(ModeError::UnknownParticipant(application.clone()));
        }
        committed.insert(application.clone());

        if waiting_for.is_empty() {
            self.state = ModeManagerState::Stable { mode: target_mode };
            return Ok(Some(ModeEvent::TransitionComplete {
                transition_id,
                mode: target_mode,
            }));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(name: &str) -> ApplicationId {
        ApplicationId::from(name)
    }

    #[test]
    fn all_apps_ready_and_committed_complete_transition() {
        let navigation = app("navigation");
        let guidance = app("guidance");
        let mut manager = ModeManager::new(SystemMode::Standby);
        let id = manager
            .request_transition(SystemMode::Working, [navigation.clone(), guidance.clone()])
            .unwrap();

        assert_eq!(manager.application_ready(&navigation, id).unwrap(), None);
        assert!(matches!(
            manager.application_ready(&guidance, id).unwrap(),
            Some(ModeEvent::PreparationComplete { .. })
        ));
        assert_eq!(manager.current_mode(), SystemMode::Standby);

        assert_eq!(
            manager
                .application_committed(&navigation, id, SystemMode::Working)
                .unwrap(),
            None
        );
        assert!(matches!(
            manager
                .application_committed(&guidance, id, SystemMode::Working)
                .unwrap(),
            Some(ModeEvent::TransitionComplete { .. })
        ));
        assert_eq!(manager.current_mode(), SystemMode::Working);
    }

    #[test]
    fn rejection_aborts_and_preserves_previous_mode() {
        let navigation = app("navigation");
        let guidance = app("guidance");
        let mut manager = ModeManager::new(SystemMode::Standby);
        let id = manager
            .request_transition(SystemMode::Working, [navigation.clone(), guidance.clone()])
            .unwrap();

        manager.application_ready(&navigation, id).unwrap();
        let event = manager
            .application_rejected(&guidance, id, "solution unavailable")
            .unwrap();

        assert!(matches!(event, ModeEvent::TransitionAborted { .. }));
        assert_eq!(
            manager.state(),
            &ModeManagerState::Stable {
                mode: SystemMode::Standby
            }
        );
    }

    #[test]
    fn stale_response_cannot_advance_transition() {
        let navigation = app("navigation");
        let mut manager = ModeManager::new(SystemMode::Standby);
        let id = manager
            .request_transition(SystemMode::Working, [navigation.clone()])
            .unwrap();

        assert_eq!(
            manager.application_ready(&navigation, TransitionId(id.0 + 1)),
            Err(ModeError::StaleTransition)
        );
    }
}
