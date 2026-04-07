// Google Calendar API integration for Tock.
// Ported from Timely's Ruby implementation; uses ureq for HTTP.

use crate::database::EventData;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const CALENDAR_API_BASE: &str = "https://www.googleapis.com/calendar/v3";

// ---------------------------------------------------------------------------
// Public structs
// ---------------------------------------------------------------------------

pub struct GoogleCal {
    pub id: String,
    pub summary: String,
    pub primary: bool,
    pub color: Option<String>,
}

pub struct GoogleCalendar {
    email: String,
    safe_dir: String,
    access_token: Option<String>,
    token_expires_at: i64,
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl GoogleCalendar {
    pub fn new(email: &str, safe_dir: Option<&str>) -> Self {
        let dir = safe_dir
            .map(String::from)
            .unwrap_or_else(|| {
                let home = crate::config::home_dir();
                home.join(".config/timely/credentials")
                    .to_string_lossy()
                    .to_string()
            });
        GoogleCalendar {
            email: email.to_string(),
            safe_dir: dir,
            access_token: None,
            token_expires_at: 0,
            last_error: None,
        }
    }

    // -----------------------------------------------------------------------
    // OAuth token management
    // -----------------------------------------------------------------------

    pub fn get_access_token(&mut self) -> Option<String> {
        // Return cached token if still valid (with 60 s margin).
        if let Some(ref tok) = self.access_token {
            if now_epoch() < self.token_expires_at - 60 {
                return Some(tok.clone());
            }
        }

        let base = expand_tilde(&self.safe_dir);

        // Read client credentials JSON.
        let creds_path = PathBuf::from(&base).join(format!("{}.json", self.email));
        let creds_json = match fs::read_to_string(&creds_path) {
            Ok(s) => s,
            Err(e) => {
                self.last_error = Some(format!("Cannot read credentials: {}", e));
                return None;
            }
        };
        let creds: Value = match serde_json::from_str(&creds_json) {
            Ok(v) => v,
            Err(e) => {
                self.last_error = Some(format!("Invalid credentials JSON: {}", e));
                return None;
            }
        };

        // Try "web" first, then "installed" (Google Cloud Console variants).
        let app = creds.get("web")
            .or_else(|| creds.get("installed"));
        let (client_id, client_secret) = match app {
            Some(a) => {
                let id = a.get("client_id").and_then(Value::as_str);
                let secret = a.get("client_secret").and_then(Value::as_str);
                match (id, secret) {
                    (Some(i), Some(s)) => (i.to_string(), s.to_string()),
                    _ => {
                        self.last_error = Some("Missing client_id or client_secret".into());
                        return None;
                    }
                }
            }
            None => {
                self.last_error = Some("No 'web' or 'installed' key in credentials".into());
                return None;
            }
        };

        // Read refresh token (try {email}.calendar.txt, then {email}.txt).
        let refresh_token = self.read_refresh_token(&base)?;

        // Exchange refresh token for access token.
        let body = format!(
            "client_id={}&client_secret={}&refresh_token={}&grant_type=refresh_token",
            url_encode(&client_id),
            url_encode(&client_secret),
            url_encode(&refresh_token),
        );

        let resp = ureq::post(GOOGLE_TOKEN_URL)
            .set("Content-Type", "application/x-www-form-urlencoded")
            .timeout(std::time::Duration::from_secs(15))
            .send_string(&body);

        match resp {
            Ok(r) => {
                let json: Value = match r.into_json() {
                    Ok(v) => v,
                    Err(e) => {
                        self.last_error = Some(format!("Token response parse error: {}", e));
                        return None;
                    }
                };
                if let Some(tok) = json.get("access_token").and_then(Value::as_str) {
                    let expires_in = json.get("expires_in")
                        .and_then(Value::as_i64)
                        .unwrap_or(3600);
                    self.access_token = Some(tok.to_string());
                    self.token_expires_at = now_epoch() + expires_in;
                    self.last_error = None;
                    Some(tok.to_string())
                } else {
                    self.last_error = Some(format!(
                        "No access_token in response: {}",
                        json
                    ));
                    None
                }
            }
            Err(e) => {
                self.last_error = Some(format!("Token request failed: {}", e));
                None
            }
        }
    }

    fn read_refresh_token(&mut self, base: &str) -> Option<String> {
        let candidates = [
            format!("{}.calendar.txt", self.email),
            format!("{}.txt", self.email),
        ];
        for name in &candidates {
            let p = PathBuf::from(base).join(name);
            if let Ok(contents) = fs::read_to_string(&p) {
                let trimmed = contents.trim().to_string();
                if !trimmed.is_empty() {
                    return Some(trimmed);
                }
            }
        }
        self.last_error = Some("No refresh token file found".into());
        None
    }

    // -----------------------------------------------------------------------
    // Calendar listing
    // -----------------------------------------------------------------------

    pub fn list_calendars(&mut self) -> Vec<GoogleCal> {
        let json = match self.api_get("/calendar/v3/users/me/calendarList") {
            Some(v) => v,
            None => return Vec::new(),
        };

        let items = match json.get("items").and_then(Value::as_array) {
            Some(a) => a,
            None => return Vec::new(),
        };

        items.iter().filter_map(|item| {
            let id = item.get("id").and_then(Value::as_str)?;
            let summary = item.get("summary").and_then(Value::as_str).unwrap_or(id);
            let primary = item.get("primary").and_then(Value::as_bool).unwrap_or(false);
            let color = item.get("backgroundColor")
                .and_then(Value::as_str)
                .map(String::from);
            Some(GoogleCal {
                id: id.to_string(),
                summary: summary.to_string(),
                primary,
                color,
            })
        }).collect()
    }

    // -----------------------------------------------------------------------
    // Event CRUD
    // -----------------------------------------------------------------------

    pub fn fetch_events(
        &mut self,
        calendar_id: &str,
        time_min: &str,
        time_max: &str,
    ) -> Option<Vec<EventData>> {
        let mut all_events: Vec<EventData> = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let mut path = format!(
                "/calendar/v3/calendars/{}/events?singleEvents=true&maxResults=250\
                 &orderBy=startTime&timeMin={}&timeMax={}",
                url_encode(calendar_id),
                url_encode(time_min),
                url_encode(time_max),
            );
            if let Some(ref pt) = page_token {
                path.push_str(&format!("&pageToken={}", url_encode(pt)));
            }

            let json = match self.api_get(&path) {
                Some(v) => v,
                None => return if all_events.is_empty() { None } else { Some(all_events) },
            };

            if let Some(items) = json.get("items").and_then(Value::as_array) {
                for item in items {
                    all_events.push(normalize_event(item, &self.email));
                }
            }

            page_token = json.get("nextPageToken")
                .and_then(Value::as_str)
                .map(String::from);
            if page_token.is_none() {
                break;
            }
        }

        Some(all_events)
    }

    pub fn create_event(
        &mut self,
        calendar_id: &str,
        event_data: &EventData,
    ) -> Option<String> {
        let body = to_google_format(event_data);
        let path = format!(
            "/calendar/v3/calendars/{}/events",
            url_encode(calendar_id),
        );
        let resp = self.api_post(&path, &body)?;
        resp.get("id").and_then(Value::as_str).map(String::from)
    }

    pub fn update_event(
        &mut self,
        calendar_id: &str,
        event_id: &str,
        event_data: &EventData,
    ) {
        let body = to_google_format(event_data);
        let path = format!(
            "/calendar/v3/calendars/{}/events/{}",
            url_encode(calendar_id),
            url_encode(event_id),
        );
        let _ = self.api_put(&path, &body);
    }

    pub fn delete_event(
        &mut self,
        calendar_id: &str,
        event_id: &str,
    ) -> bool {
        let path = format!(
            "/calendar/v3/calendars/{}/events/{}",
            url_encode(calendar_id),
            url_encode(event_id),
        );
        self.api_delete(&path)
    }

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    fn api_get(&mut self, path: &str) -> Option<Value> {
        let token = self.get_access_token()?;
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            format!("https://www.googleapis.com{}", path)
        };

        let resp = ureq::get(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Accept-Encoding", "identity")
            .timeout(std::time::Duration::from_secs(60))
            .call();

        match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.last_error = None;
                    Some(v)
                }
                Err(e) => {
                    self.last_error = Some(format!("JSON parse error: {}", e));
                    None
                }
            },
            Err(e) => {
                self.last_error = Some(format!("GET {} failed: {}", url, e));
                None
            }
        }
    }

    fn api_post(&mut self, path: &str, body: &Value) -> Option<Value> {
        let token = self.get_access_token()?;
        let url = format!("https://www.googleapis.com{}", path);

        let resp = ureq::post(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(60))
            .send_json(body.clone());

        match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.last_error = None;
                    Some(v)
                }
                Err(e) => {
                    self.last_error = Some(format!("JSON parse error: {}", e));
                    None
                }
            },
            Err(e) => {
                self.last_error = Some(format!("POST {} failed: {}", url, e));
                None
            }
        }
    }

    fn api_put(&mut self, path: &str, body: &Value) -> Option<Value> {
        let token = self.get_access_token()?;
        let url = format!("https://www.googleapis.com{}", path);

        let resp = ureq::put(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(60))
            .send_json(body.clone());

        match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.last_error = None;
                    Some(v)
                }
                Err(e) => {
                    self.last_error = Some(format!("JSON parse error: {}", e));
                    None
                }
            },
            Err(e) => {
                self.last_error = Some(format!("PUT {} failed: {}", url, e));
                None
            }
        }
    }

    fn api_delete(&mut self, path: &str) -> bool {
        let token = match self.get_access_token() {
            Some(t) => t,
            None => return false,
        };
        let url = format!("https://www.googleapis.com{}", path);

        let resp = ureq::delete(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .timeout(std::time::Duration::from_secs(60))
            .call();

        match resp {
            Ok(_) => {
                self.last_error = None;
                true
            }
            Err(e) => {
                self.last_error = Some(format!("DELETE {} failed: {}", url, e));
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event normalization (Google -> EventData)
// ---------------------------------------------------------------------------

fn normalize_event(item: &Value, self_email: &str) -> EventData {
    let external_id = item.get("id")
        .and_then(Value::as_str)
        .map(String::from);

    let title = item.get("summary")
        .and_then(Value::as_str)
        .unwrap_or("(no title)")
        .to_string();

    let description = item.get("description")
        .and_then(Value::as_str)
        .map(String::from);

    let location = item.get("location")
        .and_then(Value::as_str)
        .map(String::from);

    // Start time: prefer dateTime, fall back to date (all-day).
    let (start_time, all_day) = parse_google_time(item.get("start"));
    let (end_time, _) = parse_google_time(item.get("end"));

    let timezone = item.get("start")
        .and_then(|s| s.get("timeZone"))
        .and_then(Value::as_str)
        .map(String::from);

    let recurrence_rule = item.get("recurrence")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .and_then(Value::as_str)
        .map(String::from);

    let status = item.get("status")
        .and_then(Value::as_str)
        .unwrap_or("confirmed")
        .to_string();

    let organizer = item.get("organizer")
        .and_then(|o| o.get("email"))
        .and_then(Value::as_str)
        .map(String::from);

    // Attendees array (kept as JSON).
    let attendees = item.get("attendees")
        .cloned();

    // Determine my own RSVP status from the attendees list.
    let my_status = item.get("attendees")
        .and_then(Value::as_array)
        .and_then(|arr| {
            arr.iter().find(|a| {
                a.get("self").and_then(Value::as_bool).unwrap_or(false)
                    || a.get("email").and_then(Value::as_str) == Some(self_email)
            })
        })
        .and_then(|a| a.get("responseStatus"))
        .and_then(Value::as_str)
        .map(String::from);

    EventData {
        id: None,
        calendar_id: 0, // Caller sets this after matching.
        external_id,
        title,
        description,
        location,
        start_time,
        end_time,
        all_day,
        timezone,
        recurrence_rule,
        series_master_id: None,
        status,
        organizer,
        attendees,
        my_status,
        alarms: None,
        metadata: None,
    }
}

/// Parse a Google Calendar start/end object into (unix_timestamp, is_all_day).
fn parse_google_time(obj: Option<&Value>) -> (i64, bool) {
    let obj = match obj {
        Some(v) => v,
        None => return (0, false),
    };

    // dateTime: "2024-01-15T10:00:00+01:00"
    if let Some(dt) = obj.get("dateTime").and_then(Value::as_str) {
        return (parse_rfc3339(dt), false);
    }
    // date: "2024-01-15" (all-day event)
    if let Some(d) = obj.get("date").and_then(Value::as_str) {
        return (parse_date_str(d), true);
    }
    (0, false)
}

// ---------------------------------------------------------------------------
// EventData -> Google format
// ---------------------------------------------------------------------------

fn to_google_format(event_data: &EventData) -> Value {
    let mut ev = json!({});

    ev["summary"] = json!(event_data.title);

    if let Some(ref desc) = event_data.description {
        ev["description"] = json!(desc);
    }
    if let Some(ref loc) = event_data.location {
        ev["location"] = json!(loc);
    }

    if event_data.all_day {
        ev["start"] = json!({ "date": ts_to_date_str(event_data.start_time) });
        ev["end"] = json!({ "date": ts_to_date_str(event_data.end_time) });
    } else {
        let tz = event_data.timezone.as_deref().unwrap_or("UTC");
        ev["start"] = json!({
            "dateTime": ts_to_rfc3339(event_data.start_time),
            "timeZone": tz,
        });
        ev["end"] = json!({
            "dateTime": ts_to_rfc3339(event_data.end_time),
            "timeZone": tz,
        });
    }

    if let Some(ref att) = event_data.attendees {
        ev["attendees"] = att.clone();
    }

    ev["status"] = json!(event_data.status);

    ev
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Minimal RFC 3339 parser (enough for Google Calendar responses).
/// Handles "2024-01-15T10:00:00Z" and "2024-01-15T10:00:00+01:00".
fn parse_rfc3339(s: &str) -> i64 {
    // Strip fractional seconds if present.
    let s = if let Some(dot) = s.find('.') {
        // Find the end of fractional part (next non-digit).
        let rest = &s[dot + 1..];
        let frac_end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        format!("{}{}", &s[..dot], &rest[frac_end..])
    } else {
        s.to_string()
    };

    // Expected: "YYYY-MM-DDTHH:MM:SS" possibly followed by Z or +/-HH:MM.
    if s.len() < 19 {
        return 0;
    }
    let year: i64 = s[0..4].parse().unwrap_or(0);
    let month: i64 = s[5..7].parse().unwrap_or(0);
    let day: i64 = s[8..10].parse().unwrap_or(0);
    let hour: i64 = s[11..13].parse().unwrap_or(0);
    let min: i64 = s[14..16].parse().unwrap_or(0);
    let sec: i64 = s[17..19].parse().unwrap_or(0);

    // Days from civil (Howard Hinnant).
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    let mut ts = days * 86400 + hour * 3600 + min * 60 + sec;

    // Timezone offset.
    let tz_part = &s[19..];
    if tz_part.starts_with('+') || tz_part.starts_with('-') {
        let sign: i64 = if tz_part.starts_with('-') { 1 } else { -1 };
        let cleaned = tz_part[1..].replace(':', "");
        if cleaned.len() >= 4 {
            let oh: i64 = cleaned[0..2].parse().unwrap_or(0);
            let om: i64 = cleaned[2..4].parse().unwrap_or(0);
            ts += sign * (oh * 3600 + om * 60);
        }
    }
    // "Z" means UTC, no adjustment needed.

    ts
}

/// Parse "YYYY-MM-DD" to a UNIX timestamp at midnight UTC.
fn parse_date_str(s: &str) -> i64 {
    if s.len() < 10 {
        return 0;
    }
    parse_rfc3339(&format!("{}T00:00:00Z", &s[..10]))
}

/// Format a UNIX timestamp as "YYYY-MM-DDTHH:MM:SSZ".
fn ts_to_rfc3339(ts: i64) -> String {
    let secs_in_day = 86400_i64;
    let mut days = ts.div_euclid(secs_in_day);
    let day_secs = ts.rem_euclid(secs_in_day);

    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;

    // Civil from days (Howard Hinnant, inverse).
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mon = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if mon <= 2 { 1 } else { 0 };

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mon, d, h, m, s)
}

/// Format a UNIX timestamp as "YYYY-MM-DD".
fn ts_to_date_str(ts: i64) -> String {
    let full = ts_to_rfc3339(ts);
    full[..10].to_string()
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

/// Percent-encode a string for use in URLs.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

/// Expand a leading ~ to the user's home directory.
fn expand_tilde(path: &str) -> String {
    if path.starts_with("~/") || path == "~" {
        let home = crate::config::home_dir();
        format!("{}{}", home.display(), &path[1..])
    } else {
        path.to_string()
    }
}
