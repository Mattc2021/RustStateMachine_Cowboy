//! Demo SAM client application. Registers as `telemetry`, heartbeats on a
//! timer, and responds to mode transitions via `sam_demo_app::DemoHandler`.
//! See the workspace README for the `--degraded`/`--failed`/
//! `--reject-working` flags this binary understands.

use sam_client::{ApplicationRuntime, RuntimeConfig, RuntimeError};
use sam_demo_app::{DemoHandler, health_from_args, init_logging, reject_working_from_args};

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    init_logging();
    let mut handler = DemoHandler::new("telemetry", reject_working_from_args(), health_from_args());
    ApplicationRuntime::new("telemetry", RuntimeConfig::default()).run(&mut handler).await
}
