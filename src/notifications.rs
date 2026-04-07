// Desktop notifications for upcoming events via notify-send.

use crate::database::{now_secs, Database};
use serde_json::Value as JsonValue;
use std::process::Command;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Check all events starting within the next hour and fire desktop
/// notifications at the configured alarm offsets.
pub fn check_and_notify(db: &Database, default_alarm: i64) {
    let now = now_secs();
    let events = match db.get_events_in_range(now, now + 3600) {
        Ok(v) => v,
        Err(_) => return,
    };

    // Clean stale log entries (older than 24 h).
    let _ = db.clean_old_notifications();

    for ev in &events {
        if ev.all_day {
            continue;
        }

        let minutes_until = (ev.start_time - now) / 60;
        let offsets = parse_alarm_offsets(&ev.alarms, default_alarm);

        for offset in &offsets {
            // Fire if we are within a 2-minute window around the offset.
            if minutes_until >= offset - 1 && minutes_until <= offset + 1 {
                // Skip if already notified for this (event, offset) pair.
                if db.is_notified(ev.id, *offset).unwrap_or(true) {
                    continue;
                }
                send_notification(&ev.title, ev.start_time, ev.location.as_deref());
                let _ = db.log_notification(ev.id, *offset);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse the JSON alarms field (expected: array of integers representing
/// minutes-before offsets). Falls back to a single-element vec with the
/// default alarm value.
fn parse_alarm_offsets(alarms: &Option<JsonValue>, default_alarm: i64) -> Vec<i64> {
    if let Some(JsonValue::Array(arr)) = alarms {
        let parsed: Vec<i64> = arr
            .iter()
            .filter_map(|v| v.as_i64())
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    vec![default_alarm]
}

/// Format and send a desktop notification via notify-send.
fn send_notification(title: &str, start_time: i64, location: Option<&str>) {
    let now = now_secs();
    let minutes = (start_time - now) / 60;

    // Format the start time as HH:MM.
    let hh = ((start_time % 86400) / 3600) % 24;
    let mm = (start_time % 3600) / 60;
    let time_str = format!("{:02}:{:02}", hh, mm);

    let mut body = if minutes <= 1 {
        format!("Starting now ({})", time_str)
    } else {
        format!("In {} minutes ({})", minutes, time_str)
    };

    if let Some(loc) = location {
        if !loc.is_empty() {
            body.push('\n');
            body.push_str(loc);
        }
    }

    let _ = Command::new("notify-send")
        .args(["-a", "Tock", "-u", "normal", "-i", "calendar", title, &body])
        .spawn();
}
