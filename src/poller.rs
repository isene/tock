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
    while running.load(Ordering::SeqCst) {
        let any_new = run_sync_cycle(db);

        if any_new {
            let _ = tx.send(PollerEvent::NeedsRefresh);
        }

        notifications::check_and_notify(db, default_alarm);

        // Sleep in 1s ticks for low CPU, fast shutdown response
        for _ in 0..interval_secs {
            if !running.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_secs(1));
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

fn sync_google_calendar(
    db: &Database,
    cal: &crate::database::Calendar,
    range_start: i64,
    range_end: i64,
) -> bool {
    use crate::sources::google::GoogleCalendar;

    let cfg_str = match &cal.source_config {
        Some(s) => s.clone(),
        None => return false,
    };
    let config: serde_json::Value = match serde_json::from_str(&cfg_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let email = match config.get("email").and_then(|v| v.as_str()) {
        Some(e) => e,
        None => return false,
    };
    let safe_dir = config.get("safe_dir").and_then(|v| v.as_str());
    let google_calendar_id = match config.get("google_calendar_id").and_then(|v| v.as_str()) {
        Some(id) => id,
        None => return false,
    };

    let mut gc = GoogleCalendar::new(email, safe_dir);
    if gc.get_access_token().is_none() {
        return false;
    }

    let time_min = ts_to_rfc3339(range_start);
    let time_max = ts_to_rfc3339(range_end);

    let events = match gc.fetch_events(google_calendar_id, &time_min, &time_max) {
        Some(evts) => evts,
        None => return false,
    };

    let mut any_new = false;
    for mut ev in events {
        ev.calendar_id = cal.id;
        match db.upsert_synced_event(cal.id, &ev) {
            Ok(SyncResult::New) => any_new = true,
            Ok(SyncResult::Updated) => any_new = true,
            _ => {}
        }
    }

    let _ = db.update_calendar_sync(cal.id, now_secs(), None);
    any_new
}

fn ts_to_rfc3339(ts: i64) -> String {
    let secs_in_day = 86400_i64;
    let days_raw = ts.div_euclid(secs_in_day);
    let day_secs = ts.rem_euclid(secs_in_day);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    let d2 = days_raw + 719468;
    let era = if d2 >= 0 { d2 } else { d2 - 146096 } / 146097;
    let doe = d2 - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let mon = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if mon <= 2 { 1 } else { 0 };
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mon, day, h, m, s)
}

// ---------------------------------------------------------------------------
// Outlook sync stub
// ---------------------------------------------------------------------------

fn sync_outlook_calendar(
    db: &Database,
    cal: &crate::database::Calendar,
    range_start: i64,
    range_end: i64,
) -> bool {
    use crate::sources::outlook::OutlookCalendar;

    let cfg_str = match &cal.source_config {
        Some(s) => s.clone(),
        None => return false,
    };
    let mut config: serde_json::Value = match serde_json::from_str(&cfg_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let mut oc = OutlookCalendar::new(&config);
    if oc.refresh_access_token().is_none() {
        return false;
    }

    let time_min = ts_to_rfc3339(range_start);
    let time_max = ts_to_rfc3339(range_end);

    let events = match oc.fetch_events(&time_min, &time_max) {
        Some(evts) => evts,
        None => return false,
    };

    let mut any_new = false;
    for mut ev in events {
        ev.calendar_id = cal.id;
        match db.upsert_synced_event(cal.id, &ev) {
            Ok(SyncResult::New) => any_new = true,
            Ok(SyncResult::Updated) => any_new = true,
            _ => {}
        }
    }

    // Persist refreshed tokens back to source_config
    let new_config = if let Some(new_refresh) = oc.get_refresh_token() {
        config["refresh_token"] = serde_json::json!(new_refresh);
        if let Some(access) = oc.get_access_token_cached() {
            config["access_token"] = serde_json::json!(access);
        }
        Some(serde_json::to_string(&config).unwrap_or_default())
    } else {
        None
    };

    let _ = db.update_calendar_sync(cal.id, now_secs(), new_config.as_deref());
    any_new
}
