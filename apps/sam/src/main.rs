//! SAM's executable composition root: an interactive console over a
//! `SamService`/`SamServer` pair, so an operator can inspect system state
//! and drive mode transitions from the terminal while applications connect
//! over local IPC in the background.

use std::{error::Error, io::{self, Write}, time::{Duration, Instant}};

use sam_protocol::SystemMode;
use sam_service::{SamServer, SamService};
use sam_transport::LocalEndpoint;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = LocalEndpoint::default();
    let service = SamService::new(Duration::from_secs(2));
    let mut server_task = tokio::spawn(SamServer::new(endpoint.clone(), service.clone()).run());
    let mut commands = BufReader::new(tokio::io::stdin()).lines();

    println!("SAM listening on {}", endpoint.as_str());
    print_help();
    loop {
        tokio::select! {
            result = &mut server_task => {
                result??;
                break;
            }
            command = commands.next_line() => {
                let Some(command) = command? else { break };
                if !handle_command(command.trim(), &service) { break; }
                print!("sam> ");
                io::stdout().flush()?;
            }
        }
    }

    server_task.abort();
    Ok(())
}

fn handle_command(command: &str, service: &SamService) -> bool {
    match command.to_ascii_lowercase().as_str() {
        "startup" => request_mode(service, SystemMode::Startup),
        "standby" => request_mode(service, SystemMode::Standby),
        "working" => request_mode(service, SystemMode::Working),
        "status" => print_status(service),
        "applications" | "apps" => print_applications(service),
        "help" => print_help(),
        "quit" | "exit" => return false,
        "" => {}
        other => eprintln!("unknown command: {other}"),
    }
    true
}

fn request_mode(service: &SamService, target: SystemMode) {
    match service.request_mode(target) {
        Ok(id) => println!("requested {target:?} as transition {id:?}"),
        Err(error) => eprintln!("mode request rejected: {error}"),
    }
}

fn print_status(service: &SamService) {
    match service.snapshot() {
        Ok(state) => println!("system: {:?} + {:?}", state.mode, state.health),
        Err(error) => eprintln!("status unavailable: {error}"),
    }
}

fn print_applications(service: &SamService) {
    match service.applications(Instant::now()) {
        Ok(applications) if applications.is_empty() => println!("no applications registered"),
        Ok(applications) => {
            for app in applications {
                println!(
                    "{}: mode={:?}, health={:?}, connected={}, heartbeat_age={}ms",
                    app.application.0,
                    app.current_mode,
                    app.health,
                    app.connected,
                    app.heartbeat_age.as_millis(),
                );
            }
        }
        Err(error) => eprintln!("application registry unavailable: {error}"),
    }
}

fn print_help() {
    println!("commands: status | applications | startup | standby | working | help | quit");
    print!("sam> ");
    let _ = io::stdout().flush();
}
