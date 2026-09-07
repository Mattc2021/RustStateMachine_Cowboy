//! State-aware automatic console replacement used by the one-command demo.

use std::time::{Duration, Instant};

use sam_protocol::{HealthState, SystemMode};
use sam_service::{SamService, ServiceError};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, info};

const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoDemoConfig {
    enabled: bool,
    showcase: bool,
    expected_apps: usize,
    timeout: Duration,
    hold: Duration,
}

impl AutoDemoConfig {
    pub fn from_env() -> Result<Self, AutoDemoError> {
        Self::parse(std::env::args().skip(1))
    }

    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, AutoDemoError> {
        let mut config = Self {
            enabled: false,
            showcase: false,
            expected_apps: 3,
            timeout: Duration::from_secs(15),
            hold: Duration::from_millis(1_000),
        };
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--auto-demo" => config.enabled = true,
                "--auto-showcase" => {
                    config.enabled = true;
                    config.showcase = true;
                }
                "--expected-apps" => {
                    config.expected_apps = parse_value(&argument, args.next())?;
                }
                "--auto-timeout-secs" => {
                    let seconds: u64 = parse_value(&argument, args.next())?;
                    config.timeout = Duration::from_secs(seconds);
                }
                "--auto-hold-ms" => {
                    let milliseconds: u64 = parse_value(&argument, args.next())?;
                    config.hold = Duration::from_millis(milliseconds);
                }
                _ => return Err(AutoDemoError::UnknownArgument(argument)),
            }
        }
        Ok(config)
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}

fn parse_value<T>(flag: &str, value: Option<String>) -> Result<T, AutoDemoError>
where
    T: std::str::FromStr,
{
    value
        .ok_or_else(|| AutoDemoError::MissingValue(flag.to_owned()))?
        .parse()
        .map_err(|_| AutoDemoError::InvalidValue(flag.to_owned()))
}

#[derive(Debug, Error)]
pub enum AutoDemoError {
    #[error("unknown SAM argument: {0}")]
    UnknownArgument(String),
    #[error("missing value after {0}")]
    MissingValue(String),
    #[error("invalid numeric value after {0}")]
    InvalidValue(String),
    #[error("timed out waiting for {0}")]
    Timeout(String),
    #[error("showcase validation failed: {0}")]
    UnexpectedState(String),
    #[error(transparent)]
    Service(#[from] ServiceError),
}

pub async fn run_auto_demo(
    service: &SamService,
    config: &AutoDemoConfig,
) -> Result<(), AutoDemoError> {
    info!(
        expected_apps = config.expected_apps,
        timeout_seconds = config.timeout.as_secs(),
        hold_milliseconds = config.hold.as_millis(),
        showcase = config.showcase,
        "automatic SAM demo started"
    );
    wait_for_applications(service, config.expected_apps, config.timeout).await?;

    if config.showcase {
        return run_showcase(service, config).await;
    }

    for target in [
        SystemMode::Standby,
        SystemMode::Working,
        SystemMode::Standby,
    ] {
        transition_to(service, target, config.timeout).await?;
        sleep(config.hold).await;
    }

    info!("automatic SAM demo completed successfully");
    Ok(())
}

async fn run_showcase(service: &SamService, config: &AutoDemoConfig) -> Result<(), AutoDemoError> {
    stage(1, "Nominal startup and synchronized Standby transition");
    transition_to(service, SystemMode::Standby, config.timeout).await?;
    sleep(config.hold).await;

    stage(2, "Simulated telemetry power loss");
    info!("waiting for the launcher to remove power from telemetry");
    wait_for_application(service, "telemetry", false, false, config.timeout).await?;
    wait_for_health(service, HealthState::Failed, config.timeout).await?;
    info!("SAM detected the missing component and marked the system Failed");

    stage(
        3,
        "Component restart, registration, and state reconciliation",
    );
    wait_for_application(service, "telemetry", true, true, config.timeout).await?;
    wait_for_health(service, HealthState::Healthy, config.timeout).await?;
    info!("telemetry recovered and synchronized to SAM's authoritative mode");
    sleep(config.hold).await;

    stage(4, "Application rejects an unsafe Working transition");
    expect_rejected_transition(service, SystemMode::Working, config.timeout).await?;
    info!("SAM aborted the distributed transition; every application remains in Standby");
    sleep(config.hold).await;

    stage(
        5,
        "Condition clears and the same transition succeeds on retry",
    );
    transition_to(service, SystemMode::Working, config.timeout).await?;
    sleep(config.hold).await;

    stage(6, "Controlled return to Standby");
    transition_to(service, SystemMode::Standby, config.timeout).await?;
    info!("\nSHOWCASE COMPLETE: nominal operation, fault detection, recovery, rejection, and retry all passed\n");
    Ok(())
}

fn stage(number: usize, description: &str) {
    info!(stage = number, "\n========== {description} ==========");
}

async fn wait_for_applications(
    service: &SamService,
    expected: usize,
    timeout: Duration,
) -> Result<(), AutoDemoError> {
    let deadline = Instant::now() + timeout;
    loop {
        let applications = service.applications(Instant::now())?;
        let ready = applications
            .iter()
            .filter(|application| application.connected && application.synchronized)
            .count();
        debug!(
            ready,
            expected,
            registered = applications.len(),
            "waiting for demo applications"
        );
        if ready >= expected {
            info!(
                ready,
                "all expected applications connected and synchronized"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AutoDemoError::Timeout(format!(
                "{expected} connected and synchronized applications (found {ready})"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn transition_to(
    service: &SamService,
    target: SystemMode,
    timeout: Duration,
) -> Result<(), AutoDemoError> {
    if service.snapshot()?.mode == target {
        info!(
            ?target,
            "automatic transition skipped; system is already in target mode"
        );
        return Ok(());
    }

    let transition_id = service.request_mode(target)?;
    info!(?transition_id, ?target, "automatic mode command issued");
    let deadline = Instant::now() + timeout;
    loop {
        let state = service.snapshot()?;
        if state.mode == target {
            info!(?transition_id, ?target, health = ?state.health, "automatic mode command completed");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AutoDemoError::Timeout(format!(
                "transition {transition_id:?} to {target:?}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn expect_rejected_transition(
    service: &SamService,
    target: SystemMode,
    timeout: Duration,
) -> Result<(), AutoDemoError> {
    let original_mode = service.snapshot()?.mode;
    let transition_id = service.request_mode(target)?;
    info!(
        ?transition_id,
        ?target,
        "issuing transition expected to be rejected by demo policy"
    );
    let deadline = Instant::now() + timeout;
    while service.transition_in_progress()? {
        if Instant::now() >= deadline {
            return Err(AutoDemoError::Timeout(format!(
                "rejection of transition {transition_id:?}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }

    let final_mode = service.snapshot()?.mode;
    if final_mode != original_mode {
        return Err(AutoDemoError::UnexpectedState(format!(
            "transition {transition_id:?} committed {final_mode:?} instead of being rejected"
        )));
    }
    info!(?transition_id, ?target, retained_mode = ?final_mode, "expected transition rejection confirmed");
    Ok(())
}

async fn wait_for_application(
    service: &SamService,
    name: &str,
    connected: bool,
    synchronized: bool,
    timeout: Duration,
) -> Result<(), AutoDemoError> {
    let deadline = Instant::now() + timeout;
    loop {
        let matches = service
            .applications(Instant::now())?
            .iter()
            .any(|application| {
                application.application.0 == name
                    && application.connected == connected
                    && (!synchronized || application.synchronized)
            });
        if matches {
            info!(
                application = name,
                connected, synchronized, "application reached expected showcase state"
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AutoDemoError::Timeout(format!(
                "application {name} connected={connected}, synchronized={synchronized}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_health(
    service: &SamService,
    expected: HealthState,
    timeout: Duration,
) -> Result<(), AutoDemoError> {
    let deadline = Instant::now() + timeout;
    loop {
        let health = service.snapshot()?.health;
        if health == expected {
            info!(?health, "system reached expected health state");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AutoDemoError::Timeout(format!(
                "system health {expected:?} (currently {health:?})"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_demo_defaults_are_predictable() {
        let config = AutoDemoConfig::parse(["--auto-demo".to_owned()]).unwrap();

        assert!(config.enabled);
        assert!(!config.showcase);
        assert_eq!(config.expected_apps, 3);
        assert_eq!(config.timeout, Duration::from_secs(15));
        assert_eq!(config.hold, Duration::from_secs(1));
    }

    #[test]
    fn showcase_enables_automatic_mode() {
        let config = AutoDemoConfig::parse(["--auto-showcase".to_owned()]).unwrap();

        assert!(config.enabled);
        assert!(config.showcase);
    }

    #[test]
    fn auto_demo_timing_and_application_count_are_configurable() {
        let config = AutoDemoConfig::parse([
            "--auto-demo".to_owned(),
            "--expected-apps".to_owned(),
            "4".to_owned(),
            "--auto-timeout-secs".to_owned(),
            "30".to_owned(),
            "--auto-hold-ms".to_owned(),
            "250".to_owned(),
        ])
        .unwrap();

        assert_eq!(config.expected_apps, 4);
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.hold, Duration::from_millis(250));
    }

    #[test]
    fn unknown_arguments_are_rejected() {
        assert!(matches!(
            AutoDemoConfig::parse(["--surprise".to_owned()]),
            Err(AutoDemoError::UnknownArgument(_))
        ));
    }
}
