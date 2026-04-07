// ICS file parser for Tock.
// Ported from Ruby Timely's sources/ics_file.rb.

use crate::config::home_dir;
use crate::database::{Database, EventData};
use regex::Regex;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct IcsEvent {
    pub title: Option<String>,
    pub start_time: i64,
    pub end_time: i64,
    pub all_day: bool,
    pub location: Option<String>,
    pub description: Option<String>,
    pub organizer: Option<String>,
    pub attendees: Option<Vec<String>>,
    pub status: Option<String>,
    pub uid: Option<String>,
    pub rrule: Option<String>,
    pub alarms: Option<Vec<i64>>,
}

pub struct ImportResult {
    pub imported: usize,
    pub skipped: usize,
    pub error: Option<String>,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse ICS content into a list of events.
/// Splits on VEVENT blocks, unfolds continuation lines per RFC 5545,
/// then extracts fields from each block.
pub fn parse(content: &str) -> Vec<IcsEvent> {
    let re = Regex::new(r"(?si)BEGIN:VEVENT(.*?)END:VEVENT").unwrap();
    let mut events = Vec::new();

    for cap in re.captures_iter(content) {
        let block = &cap[1];
        // Unfold continuation lines (RFC 5545: CRLF + space/tab = continuation)
        let unfolded = unfold(block);
        if let Some(evt) = parse_vevent(&unfolded) {
            events.push(evt);
        }
    }
    events
}

/// Unfold RFC 5545 continuation lines.
/// A line that starts with a single space or tab is a continuation of
/// the previous line.
fn unfold(text: &str) -> String {
    let re = Regex::new(r"\r?\n[ \t]").unwrap();
    re.replace_all(text, "").to_string()
}

/// Parse a single VEVENT block into an IcsEvent.
fn parse_vevent(vevent: &str) -> Option<IcsEvent> {
    // SUMMARY
    let title = extract_field(vevent, "SUMMARY");

    // DTSTART
    let (start_time, all_day) = parse_dt(vevent, "DTSTART")?;

    // DTEND (default: +86400 if all-day, +3600 otherwise)
    let end_time = match parse_dt(vevent, "DTEND") {
        Some((et, _)) => et,
        None => {
            if all_day {
                start_time + 86400
            } else {
                start_time + 3600
            }
        }
    };

    // LOCATION
    let location = extract_field(vevent, "LOCATION");

    // DESCRIPTION (may span multiple logical lines after unfolding)
    let description = extract_description(vevent);

    // ORGANIZER: try CN= first, then MAILTO:
    let organizer = extract_organizer(vevent);

    // ATTENDEES: collect all CN= values
    let attendees = extract_attendees(vevent);
    let attendees = if attendees.is_empty() {
        None
    } else {
        Some(attendees)
    };

    // STATUS
    let status = extract_field(vevent, "STATUS").map(|s| s.to_lowercase());

    // UID
    let uid = extract_field(vevent, "UID");

    // RRULE
    let rrule = extract_field(vevent, "RRULE");

    // VALARM TRIGGER
    let alarms = extract_alarm_trigger(vevent);
    let alarms = if alarms.is_empty() { None } else { Some(alarms) };

    Some(IcsEvent {
        title,
        start_time,
        end_time,
        all_day,
        location,
        description,
        organizer,
        attendees,
        status,
        uid,
        rrule,
        alarms,
    })
}

// ---------------------------------------------------------------------------
// Field extraction helpers
// ---------------------------------------------------------------------------

/// Extract a simple ICS property value.
/// Matches `FIELD[;params]:value` at line start (case-insensitive).
fn extract_field(vevent: &str, field: &str) -> Option<String> {
    let pat = format!(r"(?im)^{}[^:]*:(.*)", field);
    let re = Regex::new(&pat).unwrap();
    re.captures(vevent)
        .map(|c| c[1].trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Extract DESCRIPTION, unescape ICS escapes.
/// After unfolding, DESCRIPTION is a single line. We grab everything after
/// the first colon, then unescape ICS sequences.
fn extract_description(vevent: &str) -> Option<String> {
    let raw = extract_field(vevent, "DESCRIPTION")?;
    let desc = raw
        .replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";");
    let desc = desc.trim().to_string();
    if desc.is_empty() {
        None
    } else {
        Some(desc)
    }
}

/// Extract ORGANIZER: prefer CN= parameter, fall back to MAILTO:.
fn extract_organizer(vevent: &str) -> Option<String> {
    // Try CN= first
    let re_cn = Regex::new(r"(?im)^ORGANIZER.*CN=([^;:]+)").unwrap();
    if let Some(cap) = re_cn.captures(vevent) {
        let val = cap[1].trim().to_string();
        if !val.is_empty() {
            return Some(val);
        }
    }
    // Fall back to MAILTO:
    let re_mailto = Regex::new(r"(?im)^ORGANIZER.*MAILTO:(.+)$").unwrap();
    re_mailto
        .captures(vevent)
        .map(|c| c[1].trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Extract all ATTENDEE CN= values.
fn extract_attendees(vevent: &str) -> Vec<String> {
    let re = Regex::new(r"(?im)^ATTENDEE.*CN=([^;:]+)").unwrap();
    re.captures_iter(vevent)
        .map(|c| c[1].trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parse VALARM TRIGGER durations. Returns alarm offsets in minutes.
/// Format: TRIGGER:-P[nD][T[nH][nM]]
fn extract_alarm_trigger(vevent: &str) -> Vec<i64> {
    let re =
        Regex::new(r"(?i)TRIGGER[^:]*:(-?)P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?").unwrap();
    let mut alarms = Vec::new();
    for cap in re.captures_iter(vevent) {
        let days: i64 = cap.get(2).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
        let hours: i64 = cap.get(3).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
        let mins: i64 = cap.get(4).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
        let total_mins = days * 1440 + hours * 60 + mins;
        alarms.push(total_mins);
    }
    alarms
}

// ---------------------------------------------------------------------------
// Date/time parsing
// ---------------------------------------------------------------------------

/// Parse a DTSTART or DTEND value from a VEVENT block.
/// Returns (unix_timestamp, is_all_day).
///
/// Supported formats:
///   FIELD;TZID=...:YYYYMMDDTHHmmss   -> local time
///   FIELD;VALUE=DATE:YYYYMMDD         -> all-day
///   FIELD:YYYYMMDDTHHmmssZ            -> UTC, convert to local
///   FIELD:YYYYMMDDTHHmmss             -> local time
///   FIELD:YYYYMMDD                    -> all-day
fn parse_dt(vevent: &str, field: &str) -> Option<(i64, bool)> {
    // 1. FIELD;TZID=...:YYYYMMDDTHHmmss
    let pat1 = format!(r"(?im)^{};TZID=[^:]*:(\d{{8}})T(\d{{4,6}})", field);
    if let Some(cap) = Regex::new(&pat1).unwrap().captures(vevent) {
        let d = &cap[1];
        let t = &cap[2];
        let ts = local_datetime_to_ts(
            parse_int(&d[0..4]),
            parse_int(&d[4..6]),
            parse_int(&d[6..8]),
            parse_int(&t[0..2]),
            parse_int(&t[2..4]),
            if t.len() >= 6 { parse_int(&t[4..6]) } else { 0 },
        );
        return Some((ts, false));
    }

    // 2. FIELD;VALUE=DATE:YYYYMMDD
    let pat2 = format!(r"(?im)^{};VALUE=DATE:(\d{{8}})", field);
    if let Some(cap) = Regex::new(&pat2).unwrap().captures(vevent) {
        let d = &cap[1];
        let ts = local_datetime_to_ts(
            parse_int(&d[0..4]),
            parse_int(&d[4..6]),
            parse_int(&d[6..8]),
            0,
            0,
            0,
        );
        return Some((ts, true));
    }

    // 3. FIELD:YYYYMMDDTHHmmss[Z]
    let pat3 = format!(r"(?im)^{}:(\d{{8}})T(\d{{4,6}})(Z)?", field);
    if let Some(cap) = Regex::new(&pat3).unwrap().captures(vevent) {
        let d = &cap[1];
        let t = &cap[2];
        let utc = cap.get(3).is_some();
        let (yr, mo, dy) = (parse_int(&d[0..4]), parse_int(&d[4..6]), parse_int(&d[6..8]));
        let (hr, mi) = (parse_int(&t[0..2]), parse_int(&t[2..4]));
        let sc = if t.len() >= 6 { parse_int(&t[4..6]) } else { 0 };
        if utc {
            let utc_ts = utc_datetime_to_ts(yr, mo, dy, hr, mi, sc);
            // The timestamp is already absolute; libc::localtime will
            // handle display. Store as-is (UTC epoch seconds).
            return Some((utc_ts, false));
        } else {
            let ts = local_datetime_to_ts(yr, mo, dy, hr, mi, sc);
            return Some((ts, false));
        }
    }

    // 4. FIELD:YYYYMMDD (all-day, no time component)
    let pat4 = format!(r"(?im)^{}:(\d{{8}})", field);
    if let Some(cap) = Regex::new(&pat4).unwrap().captures(vevent) {
        let d = &cap[1];
        let ts = local_datetime_to_ts(
            parse_int(&d[0..4]),
            parse_int(&d[4..6]),
            parse_int(&d[6..8]),
            0,
            0,
            0,
        );
        return Some((ts, true));
    }

    None
}

fn parse_int(s: &str) -> i32 {
    s.parse::<i32>().unwrap_or(0)
}

/// Convert a local-time date/time to a UNIX timestamp using libc mktime.
fn local_datetime_to_ts(yr: i32, mo: i32, dy: i32, hr: i32, mi: i32, sc: i32) -> i64 {
    #[cfg(unix)]
    {
        use std::mem::zeroed;
        unsafe {
            let mut tm: libc::tm = zeroed();
            tm.tm_year = yr - 1900;
            tm.tm_mon = mo - 1;
            tm.tm_mday = dy;
            tm.tm_hour = hr;
            tm.tm_min = mi;
            tm.tm_sec = sc;
            tm.tm_isdst = -1; // let mktime figure it out
            libc::mktime(&mut tm) as i64
        }
    }
    #[cfg(not(unix))]
    {
        // Fallback: treat as UTC on non-Unix platforms.
        utc_datetime_to_ts(yr, mo, dy, hr, mi, sc)
    }
}

/// Convert a UTC date/time to a UNIX timestamp (pure arithmetic).
fn utc_datetime_to_ts(yr: i32, mo: i32, dy: i32, hr: i32, mi: i32, sc: i32) -> i64 {
    // Howard Hinnant's days-from-civil algorithm.
    let y = if mo <= 2 { yr as i64 - 1 } else { yr as i64 };
    let m = if mo <= 2 { mo as i64 + 9 } else { mo as i64 - 3 };
    let d = dy as i64;

    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    days * 86400 + hr as i64 * 3600 + mi as i64 * 60 + sc as i64
}

// ---------------------------------------------------------------------------
// RRULE expansion
// ---------------------------------------------------------------------------

/// Expand an RRULE into occurrence (start_ts, end_ts) pairs.
/// Supports FREQ=DAILY, WEEKLY, MONTHLY, YEARLY with INTERVAL, COUNT, UNTIL.
/// MONTHLY and YEARLY clamp the day to the month's last day when needed.
pub fn expand_rrule(
    rrule: &str,
    dtstart_ts: i64,
    dtend_ts: i64,
    max_occurrences: usize,
    horizon_days: i64,
) -> Vec<(i64, i64)> {
    // Parse RRULE parts into a map.
    let mut parts = std::collections::HashMap::new();
    for segment in rrule.split(';') {
        if let Some((k, v)) = segment.split_once('=') {
            parts.insert(k.to_string(), v.to_string());
        }
    }

    let freq = match parts.get("FREQ") {
        Some(f) => f.to_uppercase(),
        None => return Vec::new(),
    };
    let interval: i64 = parts
        .get("INTERVAL")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let count: Option<usize> = parts.get("COUNT").and_then(|v| v.parse().ok());
    let until_ts: Option<i64> = parts.get("UNTIL").and_then(|v| parse_until(v));

    let horizon_ts = dtstart_ts + horizon_days * 86400;
    let duration = dtend_ts - dtstart_ts;

    let mut occurrences = Vec::new();
    let mut n: usize = 0;

    // Decompose dtstart into local components for month/year arithmetic.
    let (mut yr, mut mo, mut dy, hr, mi, sc) = ts_to_local(dtstart_ts);

    loop {
        n += 1;

        // Advance to next occurrence based on frequency.
        match freq.as_str() {
            "DAILY" => {
                let new_ts = local_datetime_to_ts(yr, mo, dy, hr, mi, sc) + interval * 86400;
                let parts = ts_to_local(new_ts);
                yr = parts.0;
                mo = parts.1;
                dy = parts.2;
            }
            "WEEKLY" => {
                let new_ts =
                    local_datetime_to_ts(yr, mo, dy, hr, mi, sc) + interval * 7 * 86400;
                let parts = ts_to_local(new_ts);
                yr = parts.0;
                mo = parts.1;
                dy = parts.2;
            }
            "MONTHLY" => {
                mo += interval as i32;
                while mo > 12 {
                    mo -= 12;
                    yr += 1;
                }
                // Clamp day to last day of target month.
                let max_day = days_in_month(yr, mo);
                if dy > max_day {
                    dy = max_day;
                }
            }
            "YEARLY" => {
                yr += interval as i32;
                let max_day = days_in_month(yr, mo);
                if dy > max_day {
                    dy = max_day;
                }
            }
            _ => break,
        }

        let st = local_datetime_to_ts(yr, mo, dy, hr, mi, sc);

        if let Some(c) = count {
            if n >= c {
                break;
            }
        }
        if let Some(ut) = until_ts {
            if st > ut {
                break;
            }
        }
        if st > horizon_ts {
            break;
        }
        if occurrences.len() >= max_occurrences {
            break;
        }

        occurrences.push((st, st + duration));
    }

    occurrences
}

/// Parse an UNTIL value (YYYYMMDD or YYYYMMDDTHHmmssZ) into a timestamp.
fn parse_until(s: &str) -> Option<i64> {
    if s.len() < 8 {
        return None;
    }
    let yr = s[0..4].parse::<i32>().ok()?;
    let mo = s[4..6].parse::<i32>().ok()?;
    let dy = s[6..8].parse::<i32>().ok()?;
    // Use end of day so the UNTIL date itself is included.
    Some(local_datetime_to_ts(yr, mo, dy, 23, 59, 59))
}

/// Decompose a UNIX timestamp into local (year, month, day, hour, min, sec).
fn ts_to_local(ts: i64) -> (i32, i32, i32, i32, i32, i32) {
    #[cfg(unix)]
    {
        unsafe {
            let t = ts as libc::time_t;
            let mut tm: libc::tm = std::mem::zeroed();
            libc::localtime_r(&t, &mut tm);
            (
                tm.tm_year + 1900,
                tm.tm_mon + 1,
                tm.tm_mday,
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec,
            )
        }
    }
    #[cfg(not(unix))]
    {
        // Rough UTC fallback for non-Unix.
        let days = ts / 86400;
        let rem = ts % 86400;
        let hr = (rem / 3600) as i32;
        let mi = ((rem % 3600) / 60) as i32;
        let sc = (rem % 60) as i32;
        let (yr, mo, dy) = days_to_civil(days);
        (yr, mo, dy, hr, mi, sc)
    }
}

/// Number of days in a given month (handles leap years).
fn days_in_month(yr: i32, mo: i32) -> i32 {
    match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if yr % 4 == 0 && (yr % 100 != 0 || yr % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Convert day count (from epoch) to (year, month, day). Used only as a
/// non-Unix fallback.
#[cfg(not(unix))]
fn days_to_civil(days: i64) -> (i32, i32, i32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as i32, d as i32)
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

/// Import events from an ICS file into the database.
/// Deduplicates by UID on the same calendar, and by title+time (60 s
/// tolerance) across all calendars.
pub fn import_file(path: &Path, db: &Database, calendar_id: i64) -> ImportResult {
    let content = match fs::read(path) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Err(e) => {
            return ImportResult {
                imported: 0,
                skipped: 0,
                error: Some(format!("read error: {}", e)),
            };
        }
    };

    let events = parse(&content);
    let mut imported: usize = 0;
    let mut skipped: usize = 0;

    for evt in &events {
        // Deduplicate by UID on same calendar.
        if let Some(ref uid) = evt.uid {
            match db.event_exists(calendar_id, uid) {
                Ok(true) => {
                    skipped += 1;
                    continue;
                }
                Ok(false) => {}
                Err(_) => {}
            }
        }

        // Deduplicate by title + start_time across all calendars.
        let title = evt.title.as_deref().unwrap_or("(No title)");
        match db.event_duplicate(title, evt.start_time) {
            Ok(true) => {
                skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(_) => {}
        }

        // Build attendees JSON array: [{"email": "name"}, ...]
        let attendees_json = evt.attendees.as_ref().map(|list| {
            let arr: Vec<serde_json::Value> = list
                .iter()
                .map(|a| json!({"email": a}))
                .collect();
            serde_json::Value::Array(arr)
        });

        // Build alarms JSON array: [minutes, ...]
        let alarms_json = evt.alarms.as_ref().map(|list| {
            let arr: Vec<serde_json::Value> =
                list.iter().map(|m| json!(m)).collect();
            serde_json::Value::Array(arr)
        });

        let data = EventData {
            id: None,
            calendar_id,
            external_id: evt.uid.clone(),
            title: title.to_string(),
            description: evt.description.clone(),
            location: evt.location.clone(),
            start_time: evt.start_time,
            end_time: evt.end_time,
            all_day: evt.all_day,
            timezone: None,
            recurrence_rule: evt.rrule.clone(),
            series_master_id: None,
            status: evt.status.clone().unwrap_or_else(|| "confirmed".into()),
            organizer: evt.organizer.clone(),
            attendees: attendees_json.clone(),
            my_status: None,
            alarms: alarms_json.clone(),
            metadata: None,
        };

        let master_id = match db.save_event(&data) {
            Ok(id) => id,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        imported += 1;

        // Expand recurring events.
        if let Some(ref rrule) = evt.rrule {
            let occurrences =
                expand_rrule(rrule, evt.start_time, evt.end_time, 365, 365);
            for (st, et) in &occurrences {
                let occ_ext_id = evt
                    .uid
                    .as_ref()
                    .map(|uid| format!("{}_{}", uid, st));

                let occ_data = EventData {
                    id: None,
                    calendar_id,
                    external_id: occ_ext_id,
                    title: title.to_string(),
                    description: evt.description.clone(),
                    location: evt.location.clone(),
                    start_time: *st,
                    end_time: *et,
                    all_day: evt.all_day,
                    timezone: None,
                    recurrence_rule: None,
                    series_master_id: Some(master_id),
                    status: evt.status.clone().unwrap_or_else(|| "confirmed".into()),
                    organizer: evt.organizer.clone(),
                    attendees: attendees_json.clone(),
                    my_status: None,
                    alarms: None,
                    metadata: None,
                };

                if db.save_event(&occ_data).is_ok() {
                    imported += 1;
                }
            }
        }
    }

    ImportResult {
        imported,
        skipped,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Watch incoming
// ---------------------------------------------------------------------------

/// Scan ~/.tock/incoming/*.ics, import each file, move to processed/.
/// Returns the total number of events imported.
pub fn watch_incoming(db: &Database, calendar_id: i64) -> usize {
    let incoming = incoming_dir();
    let processed = incoming.join("processed");

    // Ensure directories exist.
    let _ = fs::create_dir_all(&incoming);
    let _ = fs::create_dir_all(&processed);

    let mut total_imported: usize = 0;

    let entries = match fs::read_dir(&incoming) {
        Ok(e) => e,
        Err(_) => return 0,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        if !ext.eq_ignore_ascii_case("ics") {
            continue;
        }

        let result = import_file(&path, db, calendar_id);
        total_imported += result.imported;

        // Move to processed directory.
        if let Some(name) = path.file_name() {
            let dest = processed.join(name);
            let _ = fs::rename(&path, &dest);
        }
    }

    total_imported
}

/// Path to ~/.tock/incoming
fn incoming_dir() -> PathBuf {
    home_dir().join(".tock").join("incoming")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_event() {
        let ics = "\
BEGIN:VCALENDAR\r\n\
BEGIN:VEVENT\r\n\
SUMMARY:Team standup\r\n\
DTSTART:20250115T090000Z\r\n\
DTEND:20250115T093000Z\r\n\
UID:abc-123\r\n\
LOCATION:Room 42\r\n\
STATUS:CONFIRMED\r\n\
END:VEVENT\r\n\
END:VCALENDAR";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.title.as_deref(), Some("Team standup"));
        assert_eq!(e.uid.as_deref(), Some("abc-123"));
        assert_eq!(e.location.as_deref(), Some("Room 42"));
        assert_eq!(e.status.as_deref(), Some("confirmed"));
        assert!(!e.all_day);
    }

    #[test]
    fn test_parse_all_day() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Holiday\r\n\
DTSTART;VALUE=DATE:20250101\r\n\
UID:hol-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert!(events[0].all_day);
        assert_eq!(events[0].title.as_deref(), Some("Holiday"));
    }

    #[test]
    fn test_parse_all_day_bare() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Bare date\r\n\
DTSTART:20250601\r\n\
UID:bare-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert!(events[0].all_day);
    }

    #[test]
    fn test_default_end_time_allday() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:No end\r\n\
DTSTART;VALUE=DATE:20250301\r\n\
UID:noend-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].end_time - events[0].start_time, 86400);
    }

    #[test]
    fn test_default_end_time_timed() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:No end timed\r\n\
DTSTART:20250301T140000Z\r\n\
UID:noend-2\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].end_time - events[0].start_time, 3600);
    }

    #[test]
    fn test_continuation_lines() {
        let ics = "BEGIN:VEVENT\r\nSUMMARY:Long\r\n title here\r\nDTSTART:20250101T120000Z\r\nUID:fold-1\r\nEND:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].title.as_deref(), Some("Longtitle here"));
    }

    #[test]
    fn test_alarm_trigger() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Alert test\r\n\
DTSTART:20250201T100000Z\r\n\
UID:alarm-1\r\n\
BEGIN:VALARM\r\n\
TRIGGER:-PT15M\r\n\
END:VALARM\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].alarms.as_ref().unwrap(), &[15]);
    }

    #[test]
    fn test_alarm_trigger_hours() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Alert h\r\n\
DTSTART:20250201T100000Z\r\n\
UID:alarm-2\r\n\
BEGIN:VALARM\r\n\
TRIGGER:-PT2H30M\r\n\
END:VALARM\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events[0].alarms.as_ref().unwrap(), &[150]);
    }

    #[test]
    fn test_description_unescape() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Desc test\r\n\
DTSTART:20250301T100000Z\r\n\
DESCRIPTION:Line one\\nLine two\\, with comma\\; and semi\r\n\
UID:desc-1\r\n\
END:VEVENT";

        let events = parse(ics);
        let desc = events[0].description.as_deref().unwrap();
        assert!(desc.contains("Line one\nLine two, with comma; and semi"));
    }

    #[test]
    fn test_organizer_cn() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Org test\r\n\
DTSTART:20250301T100000Z\r\n\
ORGANIZER;CN=Alice Smith:mailto:alice@example.com\r\n\
UID:org-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events[0].organizer.as_deref(), Some("Alice Smith"));
    }

    #[test]
    fn test_organizer_mailto_fallback() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Org test2\r\n\
DTSTART:20250301T100000Z\r\n\
ORGANIZER:MAILTO:bob@example.com\r\n\
UID:org-2\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events[0].organizer.as_deref(), Some("bob@example.com"));
    }

    #[test]
    fn test_attendees() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Meeting\r\n\
DTSTART:20250301T100000Z\r\n\
ATTENDEE;CN=Alice:mailto:alice@example.com\r\n\
ATTENDEE;CN=Bob:mailto:bob@example.com\r\n\
UID:att-1\r\n\
END:VEVENT";

        let events = parse(ics);
        let att = events[0].attendees.as_ref().unwrap();
        assert_eq!(att.len(), 2);
        assert_eq!(att[0], "Alice");
        assert_eq!(att[1], "Bob");
    }

    #[test]
    fn test_tzid_format() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:TZ event\r\n\
DTSTART;TZID=Europe/Oslo:20250615T140000\r\n\
DTEND;TZID=Europe/Oslo:20250615T150000\r\n\
UID:tz-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events.len(), 1);
        assert!(!events[0].all_day);
        // Just verify it parsed without panicking and timestamps differ.
        assert!(events[0].end_time > events[0].start_time);
    }

    #[test]
    fn test_multiple_events() {
        let ics = "\
BEGIN:VCALENDAR\r\n\
BEGIN:VEVENT\r\n\
SUMMARY:First\r\n\
DTSTART:20250101T100000Z\r\n\
UID:m-1\r\n\
END:VEVENT\r\n\
BEGIN:VEVENT\r\n\
SUMMARY:Second\r\n\
DTSTART:20250102T100000Z\r\n\
UID:m-2\r\n\
END:VEVENT\r\n\
BEGIN:VEVENT\r\n\
SUMMARY:Third\r\n\
DTSTART:20250103T100000Z\r\n\
UID:m-3\r\n\
END:VEVENT\r\n\
END:VCALENDAR";

        let events = parse(ics);
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn test_rrule_extraction() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:Weekly\r\n\
DTSTART:20250101T090000Z\r\n\
RRULE:FREQ=WEEKLY;COUNT=4\r\n\
UID:rr-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert_eq!(events[0].rrule.as_deref(), Some("FREQ=WEEKLY;COUNT=4"));
    }

    #[test]
    fn test_expand_rrule_daily() {
        let start = utc_datetime_to_ts(2025, 3, 1, 10, 0, 0);
        let end = start + 3600;
        let occs = expand_rrule("FREQ=DAILY;COUNT=3", start, end, 365, 365);
        assert_eq!(occs.len(), 2); // COUNT=3 means 3 total, 2 additional
    }

    #[test]
    fn test_expand_rrule_weekly() {
        let start = utc_datetime_to_ts(2025, 1, 6, 9, 0, 0); // a Monday
        let end = start + 3600;
        let occs = expand_rrule("FREQ=WEEKLY;COUNT=5", start, end, 365, 365);
        assert_eq!(occs.len(), 4);
    }

    #[test]
    fn test_expand_rrule_monthly_clamp() {
        // Start on Jan 31, monthly should clamp to Feb 28.
        let start = utc_datetime_to_ts(2025, 1, 31, 12, 0, 0);
        let end = start + 3600;
        let occs = expand_rrule("FREQ=MONTHLY;COUNT=3", start, end, 365, 365);
        assert_eq!(occs.len(), 2);
        // Second occurrence: Feb 28, 2025
        let (yr, mo, dy, _, _, _) = ts_to_local(occs[0].0);
        assert_eq!((mo, dy), (2, 28));
    }

    #[test]
    fn test_expand_rrule_yearly() {
        let start = utc_datetime_to_ts(2024, 2, 29, 10, 0, 0); // leap day
        let end = start + 3600;
        let occs = expand_rrule("FREQ=YEARLY;COUNT=3", start, end, 365, 3650);
        assert_eq!(occs.len(), 2);
        // 2025 is not a leap year, so Feb 29 -> Feb 28
        let (_, mo, dy, _, _, _) = ts_to_local(occs[0].0);
        assert_eq!((mo, dy), (2, 28));
    }

    #[test]
    fn test_expand_rrule_horizon() {
        let start = utc_datetime_to_ts(2025, 1, 1, 10, 0, 0);
        let end = start + 3600;
        // No COUNT, horizon limits it.
        let occs = expand_rrule("FREQ=DAILY", start, end, 365, 30);
        // Should produce ~30 occurrences, bounded by horizon.
        assert!(occs.len() <= 31);
        assert!(occs.len() >= 29);
    }

    #[test]
    fn test_days_in_month() {
        assert_eq!(days_in_month(2025, 1), 31);
        assert_eq!(days_in_month(2025, 2), 28);
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2025, 4), 30);
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(1900, 2), 28);
    }

    #[test]
    fn test_empty_content() {
        let events = parse("");
        assert!(events.is_empty());
    }

    #[test]
    fn test_no_dtstart_skipped() {
        let ics = "\
BEGIN:VEVENT\r\n\
SUMMARY:No date\r\n\
UID:nodate-1\r\n\
END:VEVENT";

        let events = parse(ics);
        assert!(events.is_empty());
    }
}
