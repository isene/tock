// Background sync thread for periodic calendar synchronization.
// Fetches events from Google and Outlook calendars, upserts into the
// local database, and triggers UI refresh when new events arrive.

use std::sync::{mpsc, Arc, Condvar, Mutex};
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
    // (stopped flag, wakeup condvar). Notifying the condvar wakes the
    // sleeper instantly so stop() doesn't have to wait for the next
    // 1s sleep tick.
    stopped: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Poller {
    /// Spawn a background thread that periodically syncs remote calendars.
    pub fn start(
        db: Arc<Database>,
        config: &Config,
        tx: mpsc::Sender<PollerEvent>,
    ) -> Self {
        let stopped = Arc::new((Mutex::new(false), Condvar::new()));
        let flag = stopped.clone();

        let sync_interval = config.get_i64("google.sync_interval", 300) as u64;
        let default_alarm = config.get_i64("notifications.default_alarm", 15);

        let handle = thread::spawn(move || {
            poller_loop(&db, sync_interval, default_alarm, &flag, &tx);
        });

        Poller {
            stopped,
            thread: Some(handle),
        }
    }

    /// Signal the background thread to stop and wait for it to finish.
    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stopped;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
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
    stopped: &(Mutex<bool>, Condvar),
    tx: &mpsc::Sender<PollerEvent>,
) {
    loop {
        let any_new = run_sync_cycle(db);

        if any_new {
            let _ = tx.send(PollerEvent::NeedsRefresh);
        }

        notifications::check_and_notify(db, default_alarm);

        // Park on the condvar for the full sync interval. stop() flips the
        // flag and notifies, so shutdown is instant — no per-second ticks.
        let (lock, cvar) = stopped;
        let guard = lock.lock().unwrap();
        let (guard, _) = cvar.wait_timeout_while(
            guard,
            Duration::from_secs(interval_secs),
            |stop| !*stop,
        ).unwrap();
        if *guard {
            return;
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

    let mut any_new = reconcile_deletions(db, cal.id, &events, range_start, range_end);

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

/// Delete events still in tock.db but no longer present in the fresh fetch
/// from the remote source. Google's REST API soft-deletes events to status
/// `cancelled` and excludes them from default `events.list` results — so
/// without this reconcile step, deleted events linger in tock forever.
/// Returns true if any orphans were removed (signals UI refresh).
fn reconcile_deletions(
    db: &Database,
    cal_id: i64,
    fresh: &[EventData],
    range_start: i64,
    range_end: i64,
) -> bool {
    let fresh_ids: std::collections::HashSet<String> = fresh
        .iter()
        .filter_map(|e| e.external_id.clone())
        .collect();
    let existing = match db.external_ids_in_range(cal_id, range_start, range_end) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let mut removed = false;
    for id in existing {
        if !fresh_ids.contains(&id) {
            if db.delete_event_by_external_id(cal_id, &id).is_ok() {
                removed = true;
            }
        }
    }
    removed
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

    let mut any_new = reconcile_deletions(db, cal.id, &events, range_start, range_end);

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
