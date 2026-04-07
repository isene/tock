// Background sync thread for periodic calendar synchronization.
// Fetches events from Google and Outlook calendars, upserts into the
// local database, and triggers UI refresh when new events arrive.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use crate::config::Config;
use crate::database::{now_secs, Database, EventData, SyncResult};
use crate::notifications;

// ---------------------------------------------------------------------------
// Events sent from poller to the main thread
// ---------------------------------------------------------------------------

pub enum PollerEvent {
    NeedsRefresh,
}

// ---------------------------------------------------------------------------
// Poller
// ---------------------------------------------------------------------------

pub struct Poller {
    running: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Poller {
    /// Spawn a background thread that periodically syncs remote calendars.
    pub fn start(
        db: Arc<Database>,
        config: &Config,
        tx: mpsc::Sender<PollerEvent>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let flag = running.clone();

        let sync_interval = config.get_i64("google.sync_interval", 300) as u64;
        let default_alarm = config.get_i64("notifications.default_alarm", 15);

        let handle = thread::spawn(move || {
            poller_loop(&db, sync_interval, default_alarm, &flag, &tx);
        });

        Poller {
            running,
            thread: Some(handle),
        }
    }

    /// Signal the background thread to stop and wait for it to finish.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

fn poller_loop(
    db: &Database,
    interval_secs: u64,
    default_alarm: i64,
    running: &AtomicBool,
    tx: &mpsc::Sender<PollerEvent>,
) {
    // Number of 2-second sleep ticks per half-interval.
    let ticks = (interval_secs / 2).max(1);

    while running.load(Ordering::SeqCst) {
        let any_new = run_sync_cycle(db);

        if any_new {
            let _ = tx.send(PollerEvent::NeedsRefresh);
        }

        // Fire desktop notifications for upcoming events.
        notifications::check_and_notify(db, default_alarm);

        // Sleep in short intervals so we can respond to stop quickly.
        for _ in 0..ticks {
            if !running.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_secs(2));
        }
    }
}

// ---------------------------------------------------------------------------
// Sync cycle: iterate over all remote calendars
// ---------------------------------------------------------------------------

/// Run one full sync cycle across all enabled remote calendars.
/// Returns true if any new events were inserted.
fn run_sync_cycle(db: &Database) -> bool {
    let calendars = match db.get_calendars(true) {
        Ok(c) => c,
        Err(_) => return false,
    };

    let mut any_new = false;

    // 90-day window around today.
    let now = now_secs();
    let range_start = now - 90 * 86400;
    let range_end = now + 90 * 86400;

    for cal in &calendars {
        match cal.source_type.as_str() {
            "google" => {
                if sync_google_calendar(db, cal, range_start, range_end) {
                    any_new = true;
                }
            }
            "outlook" => {
                if sync_outlook_calendar(db, cal, range_start, range_end) {
                    any_new = true;
                }
            }
            // "local" and other types: nothing to sync remotely.
            _ => {}
        }
    }

    any_new
}

// ---------------------------------------------------------------------------
// Google sync stub
// ---------------------------------------------------------------------------

/// Sync a single Google calendar. Returns true if new events were inserted.
///
/// Once `sources::google` is implemented this will call:
///   GoogleCalendar::new(source_config) -> get_access_token -> fetch_events
/// For now this is a no-op placeholder that returns false.
fn sync_google_calendar(
    db: &Database,
    cal: &crate::database::Calendar,
    _range_start: i64,
    _range_end: i64,
) -> bool {
    // TODO: integrate with sources::google once implemented.
    // Expected flow:
    //   1. Parse source_config JSON for client credentials / refresh token
    //   2. Create GoogleCalendar, obtain access token
    //   3. Fetch events in [range_start, range_end]
    //   4. For each event: db.upsert_synced_event(cal.id, &event_data)
    //   5. Persist refreshed token: db.update_calendar_sync(...)
    //   6. Return true if any SyncResult::New

    let _ = (db, cal);
    false
}

// ---------------------------------------------------------------------------
// Outlook sync stub
// ---------------------------------------------------------------------------

/// Sync a single Outlook calendar. Returns true if new events were inserted.
///
/// Once `sources::outlook` is implemented this will call:
///   OutlookCalendar::new(source_config) -> refresh_token -> fetch_events
/// For now this is a no-op placeholder that returns false.
fn sync_outlook_calendar(
    db: &Database,
    cal: &crate::database::Calendar,
    _range_start: i64,
    _range_end: i64,
) -> bool {
    // TODO: integrate with sources::outlook once implemented.
    // Expected flow:
    //   1. Parse source_config JSON for client_id / tenant / refresh token
    //   2. Create OutlookCalendar, refresh token via OAuth
    //   3. Fetch events in [range_start, range_end]
    //   4. For each event: db.upsert_synced_event(cal.id, &event_data)
    //   5. Persist refreshed token: db.update_calendar_sync(...)
    //   6. Return true if any SyncResult::New

    let _ = (db, cal);
    false
}
