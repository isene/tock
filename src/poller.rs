// Background sync thread for periodic calendar synchronization.
// Fetches events from Google and Outlook calendars, upserts into the
// local database, and triggers UI refresh when new events arrive.

use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::config::Config;
use crate::database::{now_secs, Database, SyncResult};
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

    /// Signal the background thread to stop, and give it a moment to
    /// notice.
    ///
    /// Not an unbounded join. The loop syncs before it parks, so quitting
    /// while a calendar is mid-fetch used to block on the network — five
    /// and a half seconds of a dead terminal, sometimes more. The flag is
    /// checked between calendars, so a sync that has started finishes the
    /// one it is on and stops; if it is stuck in an HTTP call beyond the
    /// grace period we simply leave it. The process is exiting; the
    /// thread dies with it, and it holds nothing that a half-written
    /// sync could corrupt (every upsert is its own transaction).
    pub fn stop(&mut self) {
        let (lock, cvar) = &*self.stopped;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
        let Some(handle) = self.thread.take() else { return };
        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        while std::time::Instant::now() < deadline {
            if handle.is_finished() {
                let _ = handle.join();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        // Left running on purpose — see above.
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
        let any_new = run_sync_cycle(db, stopped);
        if *stopped.0.lock().unwrap() { return; }

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
///
/// Checks the stop flag between calendars: quitting during a sync should
/// cost the remainder of one calendar, not of all of them.
fn run_sync_cycle(db: &Database, stopped: &(Mutex<bool>, Condvar)) -> bool {
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
        if *stopped.0.lock().unwrap() { break; }
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

    let (events, cancelled) =
        match gc.fetch_events_with_cancellations(google_calendar_id, &time_min, &time_max) {
            Some(pair) => pair,
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

    // Drop any local rows the remote now reports as cancelled. This
    // is how an attendee's calendar learns the organizer cancelled
    // the meeting — without it, the row sits forever showing
    // "Needs response" for an event that no longer exists.
    for ext_id in &cancelled {
        match db.delete_event_by_external_id(cal.id, ext_id) {
            Ok(removed) if removed > 0 => any_new = true,
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

    // Persist the rotated tokens NOW, not after the fetch. Microsoft
    // hands back a new refresh token and may retire the old one, so any
    // gap between "the wire rotated it" and "we wrote it down" is a
    // window where losing the process means re-authenticating by hand.
    // The fetch below is seconds wide; this closes it.
    persist_outlook_tokens(db, cal.id, &mut config, &oc);

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

    // Stamp the sync time; the tokens went in before the fetch.
    let _ = db.update_calendar_sync(cal.id, now_secs(), None);
    any_new
}

/// Write the current access / refresh tokens into the calendar's
/// `source_config`. Cheap, and safe to call more than once — the tokens
/// only change when the provider rotates them.
fn persist_outlook_tokens(
    db: &Database,
    cal_id: i64,
    config: &mut serde_json::Value,
    oc: &crate::sources::outlook::OutlookCalendar,
) {
    let Some(refresh) = oc.get_refresh_token() else { return };
    config["refresh_token"] = serde_json::json!(refresh);
    if let Some(access) = oc.get_access_token_cached() {
        config["access_token"] = serde_json::json!(access);
    }
    let json = serde_json::to_string(config).unwrap_or_default();
    let _ = db.update_calendar_sync(cal_id, now_secs(), Some(&json));
}
