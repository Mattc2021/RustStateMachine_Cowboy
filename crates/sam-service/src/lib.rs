//! Connection lifecycle and orchestration between the local IPC transport
//! and SAM's core state machines: `server` accepts and services individual
//! application connections, while `service` holds the shared system state
//! (registry, health, mode) those connections read from and mutate.

mod server;
mod service;

pub use server::{SamServer, ServerError};
pub use service::{ApplicationSnapshot, ConnectionId, SamService, ServiceError};
