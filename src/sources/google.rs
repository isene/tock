// Google Calendar API integration for Tock.
// Ported from Timely's Ruby implementation; uses ureq for HTTP.
// Some CRUD / listing entry points are ported ahead of being wired into
// the TUI; allow the unused surface rather than dropping the integration.
#![allow(dead_code)]

use crate::database::EventData;
use serde_json::{json, Value};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const CALENDAR_SCOPE: &str = "https://www.googleapis.com/auth/calendar";
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

/// What the OAuth client file from Google Cloud Console holds.
struct Client {
    id: String,
    secret: String,
    /// A Desktop client ("installed"): Google sends the answer to any
    /// port on 127.0.0.1.
    desktop: bool,
    /// The return addresses registered for it; a web client may only use
    /// one of these, exactly.
    redirects: Vec<String>,
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
                home.join(".config/tock/credentials")
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
        let c = self.read_client(&base)?;
        let (client_id, client_secret) = (c.id, c.secret);

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

    /// The OAuth client, from `<email>.json` in the safe dir: the file
    /// Google Cloud Console hands out, "installed" or "web".
    fn read_client(&mut self, base: &str) -> Option<Client> {
        let creds_path = PathBuf::from(base).join(format!("{}.json", self.email));
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
        let Some(app) = creds.get("web").or_else(|| creds.get("installed")) else {
            self.last_error = Some("No 'web' or 'installed' key in credentials".into());
            return None;
        };
        let desktop = creds.get("installed").is_some();
        let redirects = app.get("redirect_uris").and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect())
            .unwrap_or_default();
        match (app.get("client_id").and_then(Value::as_str), app.get("client_secret").and_then(Value::as_str)) {
            (Some(i), Some(s)) => Some(Client { id: i.to_string(), secret: s.to_string(), desktop, redirects }),
            _ => {
                self.last_error = Some("Missing client_id or client_secret".into());
                None
            }
        }
    }

    /// Sign in through the browser and keep the refresh token, written to
    /// `<email>.calendar.txt` in the safe dir. `open` shows Google's page.
    ///
    /// Google's answer carries a code, sent to the client's return
    /// address. A Desktop client may use any port on 127.0.0.1, and a
    /// web client one registered on this machine: tock listens there and
    /// nothing is pasted. A web client registered elsewhere (a helper
    /// page) sends the browser there, and `paste` asks for the address it
    /// ends on, or just the code. Gives up after five minutes.
    pub fn authorize(&mut self, open: &dyn Fn(&str), paste: &mut dyn FnMut(&str) -> String) -> Result<(), String> {
        let base = expand_tilde(&self.safe_dir);
        let client = self.read_client(&base)
            .ok_or_else(|| self.last_error.clone().unwrap_or_default())?;
        // Where Google sends the answer, and a port to catch it on when
        // that is this machine.
        let local = client.redirects.iter().find_map(|r| local_port(r).map(|p| (r.clone(), p)));
        let (redirect, listener) = match (client.desktop, local) {
            (true, _) => {
                let l = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
                let port = l.local_addr().map_err(|e| e.to_string())?.port();
                (format!("http://127.0.0.1:{port}"), Some(l))
            }
            (false, Some((r, port))) => {
                let l = std::net::TcpListener::bind(("127.0.0.1", port))
                    .map_err(|e| format!("port {port} for {r}: {e}"))?;
                (r, Some(l))
            }
            (false, None) => match client.redirects.first() {
                Some(r) => (r.clone(), None),
                None => return Err("the client file lists no return address (redirect_uris)".into()),
            },
        };
        // Ties Google's answer to this sign-in, so no other page can
        // hand tock a code.
        let state = format!("{:x}{:x}", now_nanos(), std::process::id());
        let url = format!(
            "{}?client_id={}&redirect_uri={}&response_type=code&scope={}&access_type=offline&prompt=consent&state={}",
            GOOGLE_AUTH_URL, url_encode(&client.id), url_encode(&redirect), url_encode(CALENDAR_SCOPE), state
        );
        open(&url);
        let code = match &listener {
            Some(l) => wait_for_code(l, &state, std::time::Duration::from_secs(300))?,
            None => code_from_paste(&paste(" Sign in, then paste the address the browser ends on (or the code): "), &state)?,
        };

        let body = format!(
            "code={}&client_id={}&client_secret={}&redirect_uri={}&grant_type=authorization_code",
            url_encode(&code), url_encode(&client.id), url_encode(&client.secret), url_encode(&redirect)
        );
        let json: Value = ureq::post(GOOGLE_TOKEN_URL)
            .set("Content-Type", "application/x-www-form-urlencoded")
            .timeout(std::time::Duration::from_secs(15))
            .send_string(&body)
            .map_err(|e| match e {
                ureq::Error::Status(_, r) => {
                    // Google's answer is a JSON object; its own words are
                    // in error_description, else error.
                    let v: Value = r.into_json().unwrap_or(Value::Null);
                    let why = v.get("error_description").or_else(|| v.get("error"))
                        .and_then(Value::as_str).unwrap_or("no reason given");
                    format!("Google refused the code: {why}")
                }
                e => format!("token request failed: {e}"),
            })?
            .into_json()
            .map_err(|e| format!("token answer unreadable: {e}"))?;
        let refresh = json.get("refresh_token").and_then(Value::as_str)
            .ok_or_else(|| format!("Google sent no refresh token: {json}"))?;
        let path = PathBuf::from(&base).join(format!("{}.calendar.txt", self.email));
        write_private(&path, refresh)?;
        if let Some(tok) = json.get("access_token").and_then(Value::as_str) {
            self.access_token = Some(tok.to_string());
            self.token_expires_at = now_epoch() + json.get("expires_in").and_then(Value::as_i64).unwrap_or(3600);
        }
        self.last_error = None;
        Ok(())
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
        let (events, _) = self.fetch_events_with_cancellations(
            calendar_id, time_min, time_max
        )?;
        Some(events)
    }

    /// Same as `fetch_events` but also returns the external_ids of
    /// any events Google has marked `status: "cancelled"` so the
    /// caller can delete the local rows. We always pass
    /// `showDeleted=true` — Google filters cancelled events out by
    /// default and tock would otherwise have no signal that an
    /// organizer killed a meeting on the attendee's calendar.
    pub fn fetch_events_with_cancellations(
        &mut self,
        calendar_id: &str,
        time_min: &str,
        time_max: &str,
    ) -> Option<(Vec<EventData>, Vec<String>)> {
        let mut all_events: Vec<EventData> = Vec::new();
        let mut cancelled: Vec<String> = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let mut path = format!(
                "/calendar/v3/calendars/{}/events?singleEvents=true&showDeleted=true&maxResults=250\
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
                None => return if all_events.is_empty() && cancelled.is_empty() {
                    None
                } else {
                    Some((all_events, cancelled))
                },
            };

            if let Some(items) = json.get("items").and_then(Value::as_array) {
                for item in items {
                    let status = item.get("status").and_then(Value::as_str).unwrap_or("");
                    if status == "cancelled" {
                        if let Some(id) = item.get("id").and_then(Value::as_str) {
                            cancelled.push(id.to_string());
                        }
                        continue;
                    }
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

        Some((all_events, cancelled))
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

    /// Update the user's own `responseStatus` on an event and notify
    /// the organizer (`sendUpdates=all`). Returns true on a
    /// successful PATCH. The attendee is matched by case-insensitive
    /// email or, failing that, by the entry flagged `"self": true`
    /// (Google sets this for whichever account the API is auth'd as).
    pub fn respond_to_event(
        &mut self,
        calendar_id: &str,
        event_id: &str,
        my_email: &str,
        response: &str,
    ) -> bool {
        let google_status = match response {
            "accept" | "accepted" => "accepted",
            "decline" | "declined" => "declined",
            "tentative" | "tentativelyAccepted" => "tentative",
            _ => {
                self.last_error = Some(format!("Unknown response: {}", response));
                return false;
            }
        };
        // Google PATCH replaces the attendees array wholesale, so
        // we must round-trip: GET → mutate self entry → PATCH back.
        let get_path = format!(
            "/calendar/v3/calendars/{}/events/{}",
            url_encode(calendar_id),
            url_encode(event_id),
        );
        let mut ev = match self.api_get(&get_path) {
            Some(v) => v,
            None    => return false,
        };
        let attendees = match ev.get_mut("attendees").and_then(|v| v.as_array_mut()) {
            Some(a) => a,
            None => {
                self.last_error = Some("Event has no attendees".into());
                return false;
            }
        };
        let my_lc = my_email.to_ascii_lowercase();
        let mut matched = false;
        for a in attendees.iter_mut() {
            let email = a.get("email").and_then(Value::as_str).unwrap_or("").to_ascii_lowercase();
            let is_self = a.get("self").and_then(Value::as_bool).unwrap_or(false);
            if (!my_lc.is_empty() && email == my_lc) || is_self {
                a["responseStatus"] = Value::String(google_status.to_string());
                matched = true;
                break;
            }
        }
        if !matched {
            self.last_error = Some(format!("Not an attendee: {}", my_email));
            return false;
        }
        let body = serde_json::json!({ "attendees": attendees });
        let patch_path = format!(
            "/calendar/v3/calendars/{}/events/{}?sendUpdates=all",
            url_encode(calendar_id),
            url_encode(event_id),
        );
        self.api_patch(&patch_path, &body).is_some()
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

    fn api_patch(&mut self, path: &str, body: &Value) -> Option<Value> {
        let token = self.get_access_token()?;
        let url = format!("https://www.googleapis.com{}", path);

        let resp = ureq::request("PATCH", &url)
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
                self.last_error = Some(format!("PATCH {} failed: {}", url, e));
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

/// Format a UNIX timestamp as "YYYY-MM-DDTHH:MM:SSZ" (public for use in manual sync).
pub fn ts_to_rfc3339_pub(ts: i64) -> String { ts_to_rfc3339(ts) }

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
fn now_nanos() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
}

/// A file only its owner can read, for a secret.
pub fn write_private(path: &std::path::Path, text: &str) -> Result<(), String> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
    }
    let mut f = fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600)
        .open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    std::io::Write::write_all(&mut f, text.as_bytes()).map_err(|e| e.to_string())
}

/// Wait on the port for the browser to come back from Google, answer it
/// with a line the user can read, and give back the code it carried.
/// Other requests (a favicon) are answered and ignored. Looks for a
/// connection five times a second, only while the sign-in lasts.
fn wait_for_code(listener: &std::net::TcpListener, state: &str, limit: std::time::Duration) -> Result<String, String> {
    use std::io::{Read, Write};
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let until = std::time::Instant::now() + limit;
    while std::time::Instant::now() < until {
        let (mut conn, _) = match listener.accept() {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(200));
                continue;
            }
            Err(e) => return Err(e.to_string()),
        };
        let _ = conn.set_nonblocking(false);
        let _ = conn.set_read_timeout(Some(std::time::Duration::from_secs(5)));
        let mut buf = [0u8; 8192];
        let n = conn.read(&mut buf).unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        let params = request_params(&request);
        let (reply, result) = match (params.get("code"), params.get("error")) {
            (Some(code), _) if params.get("state").map(String::as_str) == Some(state) => {
                ("tock has Google's answer. You can close this tab and go back to tock.", Some(Ok(code.clone())))
            }
            (Some(_), _) => ("This answer was not for tock's sign-in; nothing was kept.", None),
            (None, Some(err)) => ("Google said no; nothing was kept. tock shows why.", Some(Err(format!("Google: {err}")))),
            (None, None) => ("", None),
        };
        let page = format!("<!doctype html><meta charset=utf-8><title>tock</title><p style=\"font:1.2em sans-serif\">{reply}</p>");
        let _ = write!(conn, "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}", page.len());
        if let Some(r) = result {
            return r;
        }
    }
    Err("no answer from Google within five minutes".into())
}

/// The port of a return address on this machine, as a web client must
/// name it: `http://localhost:8080/` or `http://127.0.0.1:8080`.
fn local_port(redirect: &str) -> Option<u16> {
    let rest = redirect.strip_prefix("http://localhost:").or_else(|| redirect.strip_prefix("http://127.0.0.1:"))?;
    rest.split(['/', '?']).next()?.parse().ok()
}

/// The code from what the user pasted: the whole address the browser
/// ended on (its state must be this sign-in's), or the code alone.
fn code_from_paste(pasted: &str, state: &str) -> Result<String, String> {
    let pasted = pasted.trim();
    if pasted.is_empty() {
        return Err("nothing pasted".into());
    }
    if !pasted.contains("code=") && !pasted.contains("error=") {
        return Ok(pasted.to_string());
    }
    let params = request_params(&format!("GET {} HTTP/1.1", pasted));
    if let Some(err) = params.get("error") {
        return Err(format!("Google: {err}"));
    }
    match (params.get("code"), params.get("state")) {
        (Some(_), Some(s)) if s != state => Err("that address is from another sign-in".into()),
        (Some(c), _) => Ok(c.clone()),
        (None, _) => Err("no code in what was pasted".into()),
    }
}

/// The query parameters of an HTTP request's first line, decoded.
fn request_params(request: &str) -> std::collections::HashMap<String, String> {
    let target = request.lines().next().and_then(|l| l.split_whitespace().nth(1)).unwrap_or("");
    let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
    query.split('&').filter_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        Some((url_decode(k), url_decode(v)))
    }).collect()
}

fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = |c: u8| (c as char).to_digit(16);
                match (hex(b[i + 1]), hex(b[i + 2])) {
                    (Some(h), Some(l)) => { out.push((h * 16 + l) as u8); i += 2; }
                    _ => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// A browser coming back to the port: the request it sends, and the
    /// page it gets.
    fn browse(port: u16, target: &str) -> String {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(s, "GET {target} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
        let mut page = String::new();
        let _ = s.read_to_string(&mut page);
        page
    }

    #[test]
    fn the_port_takes_the_code_meant_for_it() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || {
            let icon = browse(port, "/favicon.ico");
            let stray = browse(port, "/?state=other&code=nope");
            let page = browse(port, "/?state=s1&code=4%2F0Ab_x&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcalendar");
            (icon, stray, page)
        });
        let code = wait_for_code(&l, "s1", std::time::Duration::from_secs(10));
        let (icon, stray, page) = browser.join().unwrap();
        assert_eq!(code.as_deref(), Ok("4/0Ab_x"));
        assert!(icon.starts_with("HTTP/1.1 200"), "a favicon is answered too");
        assert!(stray.contains("not for tock"));
        assert!(page.contains("has Google"));
    }

    #[test]
    fn a_refusal_comes_back_as_the_reason() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let browser = std::thread::spawn(move || browse(port, "/?error=access_denied&state=s1"));
        let got = wait_for_code(&l, "s1", std::time::Duration::from_secs(10));
        browser.join().unwrap();
        assert_eq!(got, Err("Google: access_denied".to_string()));
    }

    #[test]
    fn a_pasted_address_or_code_gives_the_code() {
        assert_eq!(code_from_paste(" https://helper.example.com/?state=s1&code=4%2F0Ab&scope=x ", "s1"), Ok("4/0Ab".into()));
        assert_eq!(code_from_paste("4/0AbCd", "s1"), Ok("4/0AbCd".into()));
        assert!(code_from_paste("https://helper.example.com/?state=old&code=4%2F0Ab", "s1").is_err());
        assert_eq!(code_from_paste("https://helper.example.com/?error=access_denied&state=s1", "s1"), Err("Google: access_denied".into()));
        assert!(code_from_paste("  ", "s1").is_err());
    }

    #[test]
    fn a_return_address_here_gives_its_port() {
        assert_eq!(local_port("http://localhost:8080/"), Some(8080));
        assert_eq!(local_port("http://127.0.0.1:9004"), Some(9004));
        assert_eq!(local_port("http://localhost"), None);
        assert_eq!(local_port("https://helper.example.com/"), None);
    }

    #[test]
    fn percent_escapes_decode() {
        assert_eq!(url_decode("a%2Fb+c%3d%zz%"), "a/b c=%zz%");
        assert_eq!(url_decode("%C3%B8"), "ø");
    }
}
