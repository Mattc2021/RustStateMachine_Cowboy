//! Demo SAM client application. Registers as `guidance`, heartbeats on a
//! timer, and responds to mode transitions via `sam_demo_app::DemoHandler`.
//! See the workspace README for the `--degraded`/`--failed`/
//! `--reject-working` flags this binary understands.

use sam_client::{ApplicationRuntime, RuntimeConfig, RuntimeError};
use sam_demo_app::{DemoHandler, health_from_args, reject_working_from_args};

#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    let mut handler = DemoHandler::new("guidance", reject_working_from_args(), health_from_args());
    ApplicationRuntime::new("guidance", RuntimeConfig::default()).run(&mut handler).await
}

