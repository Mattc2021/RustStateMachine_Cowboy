//! Demo SAM client application. Registers as `telemetry`, heartbeats on a
//! timer, and responds to mode transitions via `sam_demo_app::DemoHandler`.
//! See the workspace README for the `--degraded`/`--failed`/
//! `--reject-working`, `--reject-working-once`, and `--exit-after-ms` flags
//! this binary understands.

use sam_client::{ApplicationRuntime, RuntimeConfig, RuntimeError};
use sam_demo_app::{
    exit_after_from_args, health_from_args, init_logging, reject_working_from_args,
    reject_working_once_from_args, DemoHandler,
};
#[tokio::main]
async fn main() -> Result<(), RuntimeError> {
    init_logging();
    let exit_after = exit_after_from_args().unwrap_or_else(|error| {
        eprintln!("telemetry argument error: {error}");
        std::process::exit(2);
    });
    let mut handler = DemoHandler::new("telemetry", reject_working_from_args(), health_from_args())
        .reject_working_once(reject_working_once_from_args());
    let runtime = ApplicationRuntime::new("telemetry", RuntimeConfig::default());

    if let Some(delay) = exit_after {
        tokio::select! {
            result = runtime.run(&mut handler) => result,
            _ = tokio::time::sleep(delay) => {
                eprintln!(
                    "DEMO FAULT: telemetry component lost power (delay_ms={})",
                    delay.as_millis()
                );
                std::process::exit(75);
            }
        }
    } else {
        runtime.run(&mut handler).await
    }
}
