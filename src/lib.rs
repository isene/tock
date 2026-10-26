//! tock as a library: the tock program is `app::run`, and gluon uses
//! the same code.

pub mod app;
pub mod config;
pub mod database;
pub mod ics;
pub mod notifications;
pub mod poller;
pub mod sources;
pub mod weather;

// A helper the other modules call at the crate root.
pub(crate) use app::to_local;
