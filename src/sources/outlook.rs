// Outlook / Microsoft 365 Graph API integration for Tock.
// Ported from Timely's Ruby implementation; uses ureq for HTTP.
// Some CRUD / listing entry points are ported ahead of being wired into
// the TUI; allow the unused surface rather than dropping the integration.
#![allow(dead_code)]

use crate::database::EventData;
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};

const GRAPH_BASE: &str = "https://graph.microsoft.com/v1.0";
const AUTH_BASE: &str = "https://login.microsoftonline.com";
const SCOPES: &str = "Calendars.ReadWrite offline_access";

// ---------------------------------------------------------------------------
// Public structs
// ---------------------------------------------------------------------------

pub struct OutlookCal {
    pub id: String,
    pub name: String,
    pub color: Option<String>,
    pub can_edit: bool,
}

pub struct TokenResult {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// One row of the `/me/calendar/getSchedule` response: an email and its
/// compact availability string (one character per slot).
pub struct ScheduleEntry {
    pub email: String,
    pub availability_view: String,
}

pub struct OutlookCalendar {
    client_id: String,
    tenant_id: String,
    access_token: Option<String>,
    refresh_token: Option<String>,
    token_expires_at: i64,
    /// Where the Graph API answers. A test points it at a local listener.
    base: String,
    pub last_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl OutlookCalendar {
    pub fn new(config: &Value) -> Self {
        OutlookCalendar {
            client_id: config.get("client_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            tenant_id: config.get("tenant_id")
                .and_then(Value::as_str)
                .unwrap_or("common")
                .to_string(),
            access_token: config.get("access_token")
                .and_then(Value::as_str)
                .map(String::from),
            refresh_token: config.get("refresh_token")
                .and_then(Value::as_str)
                .map(String::from),
            token_expires_at: 0,
            base: GRAPH_BASE.to_string(),
            last_error: None,
        }
    }

    pub fn get_refresh_token(&self) -> Option<&str> {
        self.refresh_token.as_deref()
    }

    pub fn get_access_token_cached(&self) -> Option<&str> {
        self.access_token.as_deref()
    }

    fn auth_url(&self, endpoint: &str) -> String {
        format!("{}/{}/oauth2/v2.0/{}", AUTH_BASE, self.tenant_id, endpoint)
    }

    // -----------------------------------------------------------------------
    // Device-code flow
    // -----------------------------------------------------------------------

    /// Initiate the device authorization flow. Returns the JSON response
    /// containing user_code, verification_uri, and device_code.
    pub fn start_device_auth(&mut self) -> Option<Value> {
        let url = self.auth_url("devicecode");
        let body = format!(
            "client_id={}&scope={}",
            url_encode(&self.client_id),
            url_encode(SCOPES),
        );

        let resp = ureq::post(&url)
            .set("Content-Type", "application/x-www-form-urlencoded")
            .timeout(std::time::Duration::from_secs(10))
            .send_string(&body);

        match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.last_error = None;
                    Some(v)
                }
                Err(e) => {
                    self.last_error = Some(format!("Device auth parse error: {}", e));
                    None
                }
            },
            Err(e) => {
                self.last_error = Some(format!("Device auth request failed: {}", e));
                None
            }
        }
    }

    /// Poll for token after device authorization. Blocks until the user
    /// completes auth, the code expires, or an error occurs.
    pub fn poll_for_token(&mut self, device_code: &str) -> Option<TokenResult> {
        let url = self.auth_url("token");
        let body = format!(
            "grant_type=urn:ietf:params:oauth:grant-type:device_code\
             &client_id={}&device_code={}",
            url_encode(&self.client_id),
            url_encode(device_code),
        );

        loop {
            let resp = ureq::post(&url)
                .set("Content-Type", "application/x-www-form-urlencoded")
                .timeout(std::time::Duration::from_secs(10))
                .send_string(&body);

            match resp {
                Ok(r) => {
                    let json: Value = match r.into_json() {
                        Ok(v) => v,
                        Err(e) => {
                            self.last_error = Some(format!("Token parse error: {}", e));
                            return None;
                        }
                    };
                    if let Some(tok) = json.get("access_token").and_then(Value::as_str) {
                        let rt = json.get("refresh_token")
                            .and_then(Value::as_str)
                            .map(String::from);
                        let expires_in = json.get("expires_in")
                            .and_then(Value::as_i64)
                            .unwrap_or(3600);
                        self.access_token = Some(tok.to_string());
                        self.refresh_token = rt.clone();
                        self.token_expires_at = now_epoch() + expires_in;
                        self.last_error = None;
                        return Some(TokenResult {
                            access_token: tok.to_string(),
                            refresh_token: rt,
                        });
                    }
                    // Handle pending / slow_down errors.
                    let err = json.get("error").and_then(Value::as_str).unwrap_or("");
                    match err {
                        "authorization_pending" => {
                            std::thread::sleep(std::time::Duration::from_secs(5));
                        }
                        "slow_down" => {
                            std::thread::sleep(std::time::Duration::from_secs(10));
                        }
                        _ => {
                            let desc = json.get("error_description")
                                .and_then(Value::as_str)
                                .unwrap_or("Unknown error");
                            self.last_error = Some(format!("Token poll error: {}", desc));
                            return None;
                        }
                    }
                }
                Err(ureq::Error::Status(_, resp)) => {
                    // 4xx responses during polling carry the pending/slow_down
                    // errors in the JSON body.
                    let json: Value = match resp.into_json() {
                        Ok(v) => v,
                        Err(_) => {
                            self.last_error = Some("Token poll: unparseable error response".into());
                            return None;
                        }
                    };
                    let err = json.get("error").and_then(Value::as_str).unwrap_or("");
                    match err {
                        "authorization_pending" => {
                            std::thread::sleep(std::time::Duration::from_secs(5));
                        }
                        "slow_down" => {
                            std::thread::sleep(std::time::Duration::from_secs(10));
                        }
                        _ => {
                            let desc = json.get("error_description")
                                .and_then(Value::as_str)
                                .unwrap_or("Unknown error");
                            self.last_error = Some(format!("Token poll error: {}", desc));
                            return None;
                        }
                    }
                }
                Err(e) => {
                    self.last_error = Some(format!("Token poll transport error: {}", e));
                    return None;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Token refresh
    // -----------------------------------------------------------------------

    pub fn refresh_access_token(&mut self) -> Option<String> {
        // Return cached token if still valid (with 60 s margin).
        if let Some(ref tok) = self.access_token {
            if now_epoch() < self.token_expires_at - 60 {
                return Some(tok.clone());
            }
        }

        let rt = match &self.refresh_token {
            Some(t) => t.clone(),
            None => {
                self.last_error = Some("No refresh token available".into());
                return None;
            }
        };

        let url = self.auth_url("token");
        let body = format!(
            "client_id={}&scope={}&refresh_token={}&grant_type=refresh_token",
            url_encode(&self.client_id),
            url_encode(SCOPES),
            url_encode(&rt),
        );

        let resp = ureq::post(&url)
            .set("Content-Type", "application/x-www-form-urlencoded")
            .timeout(std::time::Duration::from_secs(10))
            .send_string(&body);

        match resp {
            Ok(r) => {
                let json: Value = match r.into_json() {
                    Ok(v) => v,
                    Err(e) => {
                        self.last_error = Some(format!("Refresh parse error: {}", e));
                        return None;
                    }
                };
                if let Some(tok) = json.get("access_token").and_then(Value::as_str) {
                    let expires_in = json.get("expires_in")
                        .and_then(Value::as_i64)
                        .unwrap_or(3600);
                    self.access_token = Some(tok.to_string());
                    self.token_expires_at = now_epoch() + expires_in;
                    // Update refresh token if rotated.
                    if let Some(new_rt) = json.get("refresh_token").and_then(Value::as_str) {
                        self.refresh_token = Some(new_rt.to_string());
                    }
                    self.last_error = None;
                    Some(tok.to_string())
                } else {
                    self.last_error = Some(format!(
                        "No access_token in refresh response: {}",
                        json,
                    ));
                    None
                }
            }
            Err(ureq::Error::Status(code, resp)) => {
                // Microsoft says why in the body. Keep `error` and the first
                // sentence of its description, so an expired sign-in reaches
                // the screen as "press O" instead of a bare 400.
                let body = resp.into_string().unwrap_or_default();
                let why = serde_json::from_str::<Value>(&body).ok().map(|j| format!("{}: {}",
                    j.get("error").and_then(Value::as_str).unwrap_or(""),
                    j.get("error_description").and_then(Value::as_str).unwrap_or("")
                        .split('.').next().unwrap_or("").trim())).unwrap_or_default();
                self.last_error = Some(format!("refresh failed ({}) {}", code, why));
                None
            }
            Err(e) => {
                self.last_error = Some(format!("Refresh request failed: {}", e));
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Calendar listing
    // -----------------------------------------------------------------------

    pub fn list_calendars(&mut self) -> Vec<OutlookCal> {
        let json = match self.api_get("/me/calendars") {
            Some(v) => v,
            None => return Vec::new(),
        };

        let items = match json.get("value").and_then(Value::as_array) {
            Some(a) => a,
            None => return Vec::new(),
        };

        items.iter().filter_map(|item| {
            let id = item.get("id").and_then(Value::as_str)?;
            let name = item.get("name").and_then(Value::as_str).unwrap_or("(unnamed)");
            let color = item.get("hexColor")
                .or_else(|| item.get("color"))
                .and_then(Value::as_str)
                .map(String::from);
            let can_edit = item.get("canEdit")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            Some(OutlookCal {
                id: id.to_string(),
                name: name.to_string(),
                color,
                can_edit,
            })
        }).collect()
    }

    // -----------------------------------------------------------------------
    // Event CRUD
    // -----------------------------------------------------------------------

    pub fn fetch_events(
        &mut self,
        time_min: &str,
        time_max: &str,
    ) -> Option<Vec<EventData>> {
        let mut all_events: Vec<EventData> = Vec::new();
        let mut url = Some(format!(
            "/me/calendarView?startDateTime={}&endDateTime={}&$top=250\
             &$orderby=start/dateTime",
            url_encode(time_min),
            url_encode(time_max),
        ));

        while let Some(ref path) = url {
            let json = match self.api_get(path) {
                Some(v) => v,
                None => return if all_events.is_empty() { None } else { Some(all_events) },
            };

            if let Some(items) = json.get("value").and_then(Value::as_array) {
                for item in items {
                    all_events.push(normalize_event(item));
                }
            }

            // Follow @odata.nextLink for pagination.
            url = json.get("@odata.nextLink")
                .and_then(Value::as_str)
                .map(String::from);
        }

        Some(all_events)
    }

    /// Make the event in Outlook and give back the id Outlook gave it.
    /// Outlook mails an invitation to every guest at once.
    pub fn create_event(&mut self, event_data: &EventData) -> Option<String> {
        let body = to_outlook_format(event_data);
        let resp = self.api_post("/me/events", &body)?;
        let id = resp.get("id").and_then(Value::as_str).map(String::from);
        if id.is_none() {
            self.last_error = Some("Outlook answered without an event id".into());
        }
        id
    }

    /// Send an edit to Outlook. Only a field with a new value goes out;
    /// an edit that changed nothing sends no request.
    pub fn update_event(&mut self, event_id: &str, before: &EventData, after: &EventData) -> bool {
        let body = changed_fields(before, after);
        if body.as_object().is_some_and(|o| o.is_empty()) {
            self.last_error = None;
            return true;
        }
        let path = format!("/me/events/{}", event_id);
        self.api_patch(&path, &body).is_some()
    }

    pub fn delete_event(&mut self, event_id: &str) -> bool {
        let path = format!("/me/events/{}", event_id);
        self.api_delete(&path)
    }

    /// Microsoft Graph `/me/calendar/getSchedule`. Returns one entry per
    /// email with an `availability_view` string — a char per slot where
    /// '0' = free, '1' = tentative, '2' = busy, '3' = out-of-office,
    /// '4' = working elsewhere. Callers render as a grid.
    ///
    /// `start`/`end` are local wall-clock RFC3339 strings without offset
    /// (Graph interprets them in the supplied `time_zone`). `interval`
    /// is the slot width in minutes (typically 30 or 60).
    pub fn get_schedule(
        &mut self,
        emails: &[String],
        start: &str,
        end: &str,
        time_zone: &str,
        interval_minutes: u32,
    ) -> Option<Vec<ScheduleEntry>> {
        let body = json!({
            "schedules": emails,
            "startTime": { "dateTime": start, "timeZone": time_zone },
            "endTime":   { "dateTime": end,   "timeZone": time_zone },
            "availabilityViewInterval": interval_minutes,
        });
        let resp = self.api_post("/me/calendar/getSchedule", &body)?;
        let arr = resp.get("value")?.as_array()?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            let email = item.get("scheduleId").and_then(Value::as_str).unwrap_or("").to_string();
            let view = item.get("availabilityView").and_then(Value::as_str).unwrap_or("").to_string();
            out.push(ScheduleEntry { email, availability_view: view });
        }
        Some(out)
    }

    pub fn respond_to_event(&mut self, event_id: &str, response: &str) -> bool {
        let action = match response {
            "accept" | "accepted" => "accept",
            "decline" | "declined" => "decline",
            "tentative" | "tentativelyAccepted" => "tentativelyAccept",
            _ => {
                self.last_error = Some(format!("Unknown response type: {}", response));
                return false;
            }
        };
        let path = format!("/me/events/{}/{}", event_id, action);
        let body = json!({ "sendResponse": true });
        self.api_post(&path, &body).is_some()
    }

    // -----------------------------------------------------------------------
    // HTTP helpers
    // -----------------------------------------------------------------------

    fn ensure_token(&mut self) -> Option<String> {
        if let Some(ref tok) = self.access_token {
            if now_epoch() < self.token_expires_at - 60 {
                return Some(tok.clone());
            }
        }
        self.refresh_access_token()
    }

    fn api_get(&mut self, path: &str) -> Option<Value> {
        let token = self.ensure_token()?;
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}{}", self.base, path)
        };

        let resp = ureq::get(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Accept", "application/json")
            .timeout(std::time::Duration::from_secs(30))
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
        let token = self.ensure_token()?;
        let url = format!("{}{}", self.base, path);

        let resp = ureq::post(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(30))
            .send_json(body.clone());

        match resp {
            Ok(r) => match r.into_json::<Value>() {
                Ok(v) => {
                    self.last_error = None;
                    Some(v)
                }
                Err(_) => {
                    // Some POST endpoints (e.g. accept/decline) return empty body.
                    self.last_error = None;
                    Some(json!({}))
                }
            },
            Err(e) => {
                self.last_error = Some(format!("POST {} failed: {}", url, e));
                None
            }
        }
    }

    fn api_patch(&mut self, path: &str, body: &Value) -> Option<Value> {
        let token = self.ensure_token()?;
        let url = format!("{}{}", self.base, path);

        let resp = ureq::request("PATCH", &url)
            .set("Authorization", &format!("Bearer {}", token))
            .set("Content-Type", "application/json")
            .timeout(std::time::Duration::from_secs(30))
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

    fn api_delete(&mut self, path: &str) -> bool {
        let token = match self.ensure_token() {
            Some(t) => t,
            None => return false,
        };
        let url = format!("{}{}", self.base, path);

        let resp = ureq::delete(&url)
            .set("Authorization", &format!("Bearer {}", token))
            .timeout(std::time::Duration::from_secs(30))
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
// Event normalization (Outlook -> EventData)
// ---------------------------------------------------------------------------

fn normalize_event(item: &Value) -> EventData {
    let external_id = item.get("id")
        .and_then(Value::as_str)
        .map(String::from);

    let title = item.get("subject")
        .and_then(Value::as_str)
        .unwrap_or("(no subject)")
        .to_string();

    // Prefer the full body.content — bodyPreview is Microsoft Graph's
    // 255-char truncated summary and cuts mid-word on anything longer.
    // clean_description at render time strips HTML so storing raw HTML
    // here is fine.
    let description = item.get("body")
        .and_then(|b| b.get("content"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            item.get("bodyPreview")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from)
        });

    let location = item.get("location")
        .and_then(|l| l.get("displayName"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from);

    let all_day = item.get("isAllDay")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let timezone = item.get("start")
        .and_then(|s| s.get("timeZone"))
        .and_then(Value::as_str)
        .map(String::from);

    let start_time = parse_outlook_time(item.get("start"));
    let end_time = parse_outlook_time(item.get("end"));

    let status = match item.get("showAs").and_then(Value::as_str) {
        Some("free") => "free",
        Some("tentative") => "tentative",
        Some("oof") | Some("workingElsewhere") => "busy",
        _ => "confirmed",
    }.to_string();

    let organizer = item.get("organizer")
        .and_then(|o| o.get("emailAddress"))
        .and_then(|e| e.get("address"))
        .and_then(Value::as_str)
        .map(String::from);

    let attendees = item.get("attendees").cloned();

    let my_status = item.get("responseStatus")
        .and_then(|r| r.get("response"))
        .and_then(Value::as_str)
        .map(String::from);

    let recurrence_rule = item.get("recurrence")
        .filter(|v| !v.is_null())
        .map(|v| v.to_string());

    EventData {
        id: None,
        calendar_id: 0,
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

/// Parse an Outlook start/end object { "dateTime": "...", "timeZone": "..." }.
fn parse_outlook_time(obj: Option<&Value>) -> i64 {
    let obj = match obj {
        Some(v) => v,
        None => return 0,
    };
    let dt = match obj.get("dateTime").and_then(Value::as_str) {
        Some(s) => s,
        None => return 0,
    };
    // Outlook returns ISO 8601 without offset (assumes timeZone field).
    // Append Z to parse as UTC; caller should handle timezone conversion.
    if dt.contains('Z') || dt.contains('+') || dt.contains('-') && dt.len() > 19 {
        parse_rfc3339(dt)
    } else {
        parse_rfc3339(&format!("{}Z", dt))
    }
}

// ---------------------------------------------------------------------------
// EventData -> Outlook format
// ---------------------------------------------------------------------------

fn to_outlook_format(event_data: &EventData) -> Value {
    let mut ev = json!({});

    ev["subject"] = json!(event_data.title);

    if let Some(desc) = event_data.description.as_deref().map(str::trim).filter(|d| !d.is_empty()) {
        ev["body"] = json!({
            "contentType": "text",
            "content": desc,
        });
    }

    if let Some(loc) = event_data.location.as_deref().map(str::trim).filter(|l| !l.is_empty()) {
        ev["location"] = json!({ "displayName": loc });
    }

    ev["isAllDay"] = json!(event_data.all_day);

    // The stored times count seconds in UTC, so UTC is the zone to name,
    // whatever zone the event was first written in.
    ev["start"] = json!({
        "dateTime": ts_to_iso(event_data.start_time),
        "timeZone": "UTC",
    });
    ev["end"] = json!({
        "dateTime": ts_to_iso(event_data.end_time),
        "timeZone": "UTC",
    });

    let guests = guest_addresses(event_data.attendees.as_ref());
    if !guests.is_empty() {
        ev["attendees"] = guests.iter().map(|a| guest(a, "required")).collect();
    }

    if wants_teams(event_data) {
        ev["isOnlineMeeting"] = json!(true);
        ev["onlineMeetingProvider"] = json!("teamsForBusiness");
    }

    ev
}

fn guest(address: &str, kind: &str) -> Value {
    json!({ "emailAddress": { "address": address }, "type": kind })
}

/// True for an event the user asked to be a Teams meeting. The new-event
/// dialog writes the wish into the metadata; nothing else carries it.
fn wants_teams(event_data: &EventData) -> bool {
    event_data.metadata.as_ref()
        .and_then(|m| m.get("teams"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// The guests' addresses, lower case and sorted. A guest typed in tock is
/// `{"email": ...}`; one that came from Outlook is
/// `{"emailAddress": {"address": ...}}`.
pub fn guest_addresses(attendees: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = attendees
        .and_then(Value::as_array)
        .map(|list| list.iter().filter_map(guest_address).collect())
        .unwrap_or_default();
    out.sort();
    out.dedup();
    out
}

fn guest_address(entry: &Value) -> Option<String> {
    entry.get("email")
        .or_else(|| entry.get("emailAddress").and_then(|e| e.get("address")))
        .and_then(Value::as_str)
        .map(|a| a.trim().to_lowercase())
        .filter(|a| !a.is_empty())
}

/// What an edit changed, as the body of the PATCH request.
///
/// A field goes out only when it has a new value. The description never
/// does: the body of a meeting holds its Teams join block, and a plain
/// text copy sent back would wipe that block out. A field the user
/// emptied stays as it is in Outlook, since an emptied guest list would
/// cancel the meeting for everyone on it.
fn changed_fields(before: &EventData, after: &EventData) -> Value {
    let old = to_outlook_format(before);
    let new = to_outlook_format(after);
    let mut out = json!({});
    for key in ["subject", "isAllDay", "start", "end", "location", "attendees"] {
        if let Some(value) = new.get(key) {
            if old.get(key) != Some(value) {
                out[key] = value.clone();
            }
        }
    }
    if out.get("attendees").is_some() {
        // A guest who was optional stays optional.
        let kinds = before.attendees.as_ref().and_then(Value::as_array).cloned().unwrap_or_default();
        out["attendees"] = guest_addresses(after.attendees.as_ref()).iter().map(|address| {
            let kind = kinds.iter()
                .find(|e| guest_address(e).as_deref() == Some(address))
                .and_then(|e| e.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("required");
            guest(address, kind)
        }).collect();
    }
    out
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

/// Minimal RFC 3339 / ISO 8601 parser.
fn parse_rfc3339(s: &str) -> i64 {
    // Strip fractional seconds if present.
    let s = if let Some(dot) = s.find('.') {
        let rest = &s[dot + 1..];
        let frac_end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        format!("{}{}", &s[..dot], &rest[frac_end..])
    } else {
        s.to_string()
    };

    if s.len() < 19 {
        return 0;
    }
    let year: i64 = s[0..4].parse().unwrap_or(0);
    let month: i64 = s[5..7].parse().unwrap_or(0);
    let day: i64 = s[8..10].parse().unwrap_or(0);
    let hour: i64 = s[11..13].parse().unwrap_or(0);
    let min: i64 = s[14..16].parse().unwrap_or(0);
    let sec: i64 = s[17..19].parse().unwrap_or(0);

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

    ts
}

/// Format a UNIX timestamp as "YYYY-MM-DDTHH:MM:SS" (no trailing Z; Outlook
/// expects the timezone in a separate field).
fn ts_to_iso(ts: i64) -> String {
    let secs_in_day = 86400_i64;
    let mut days = ts.div_euclid(secs_in_day);
    let day_secs = ts.rem_euclid(secs_in_day);

    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;

    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mon = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if mon <= 2 { 1 } else { 0 };

    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, mon, d, h, m, s)
}

// ---------------------------------------------------------------------------
// Misc helpers
// ---------------------------------------------------------------------------

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn event(title: &str) -> EventData {
        EventData {
            id: None,
            calendar_id: 9,
            external_id: None,
            title: title.into(),
            description: None,
            location: None,
            // 2026-10-12 09:00 to 10:00 UTC.
            start_time: 1_791_795_600,
            end_time: 1_791_799_200,
            all_day: false,
            timezone: None,
            recurrence_rule: None,
            series_master_id: None,
            status: "confirmed".into(),
            organizer: None,
            attendees: None,
            my_status: None,
            alarms: None,
            metadata: None,
        }
    }

    /// A stand-in for Graph on a local port. Answers `count` requests with
    /// `answer` and gives back what it was sent, one string per request.
    fn graph(count: usize, answer: &'static str) -> (OutlookCalendar, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for _ in 0..count {
                let (mut stream, _) = listener.accept().unwrap();
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    raw.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&raw).to_string();
                    let Some(head_end) = text.find("\r\n\r\n") else { continue };
                    let length = text.lines()
                        .find_map(|l| l.to_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap()))
                        .unwrap_or(0);
                    if n == 0 || raw.len() >= head_end + 4 + length {
                        break;
                    }
                }
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    answer.len(), answer);
                stream.write_all(reply.as_bytes()).unwrap();
                seen.push(String::from_utf8_lossy(&raw).to_string());
            }
            seen
        });
        let mut oc = OutlookCalendar::new(&json!({ "access_token": "test-token" }));
        oc.base = base;
        oc.token_expires_at = now_epoch() + 3600;
        (oc, seen)
    }

    fn body_of(request: &str) -> Value {
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap()
    }

    #[test]
    fn a_new_event_goes_to_outlook_as_one_post() {
        let (mut oc, seen) = graph(1, r#"{"id":"AAMk-new"}"#);
        let mut ev = event("Planning");
        ev.description = Some("  Bring the numbers ".into());
        ev.attendees = Some(json!([{ "email": " Bob@Example.com" }, { "email": "alice@example.com" }]));
        ev.metadata = Some(json!({ "teams": true }));

        assert_eq!(oc.create_event(&ev).as_deref(), Some("AAMk-new"));

        let seen = seen.join().unwrap();
        assert!(seen[0].starts_with("POST /me/events HTTP/1.1\r\n"), "{}", seen[0]);
        assert!(seen[0].contains("Authorization: Bearer test-token\r\n"), "{}", seen[0]);
        let body = body_of(&seen[0]);
        assert_eq!(body["subject"], "Planning");
        assert_eq!(body["body"], json!({ "contentType": "text", "content": "Bring the numbers" }));
        assert_eq!(body["start"], json!({ "dateTime": "2026-10-12T09:00:00", "timeZone": "UTC" }));
        assert_eq!(body["end"], json!({ "dateTime": "2026-10-12T10:00:00", "timeZone": "UTC" }));
        assert_eq!(body["attendees"], json!([
            { "emailAddress": { "address": "alice@example.com" }, "type": "required" },
            { "emailAddress": { "address": "bob@example.com" }, "type": "required" },
        ]));
        assert_eq!(body["isOnlineMeeting"], true);
        assert_eq!(body["onlineMeetingProvider"], "teamsForBusiness");
    }

    #[test]
    fn a_plain_event_asks_for_no_teams_meeting_and_no_guests() {
        let body = to_outlook_format(&event("Dentist"));
        assert!(body.get("isOnlineMeeting").is_none());
        assert!(body.get("attendees").is_none());
        assert!(body.get("body").is_none());
    }

    /// The event as Outlook hands it back: an HTML body with the Teams
    /// block, and guests in Outlook's own shape.
    fn meeting_from_outlook() -> EventData {
        let mut ev = event("Planning");
        ev.external_id = Some("AAMk-1".into());
        ev.description = Some("<html><body>Bring the numbers<div>Join the Teams meeting</div></body></html>".into());
        ev.location = Some("Room 2".into());
        ev.timezone = Some("UTC".into());
        ev.attendees = Some(json!([
            { "emailAddress": { "address": "Alice@Example.com", "name": "Alice" }, "type": "optional",
              "status": { "response": "accepted" } },
            { "emailAddress": { "address": "bob@example.com", "name": "Bob" }, "type": "required",
              "status": { "response": "none" } },
        ]));
        ev
    }

    #[test]
    fn an_edit_sends_only_what_changed() {
        let before = meeting_from_outlook();

        // A new title and a later hour; the dialog hands the guests back
        // in tock's own shape, and the place was left empty.
        let mut after = before.clone();
        after.title = "Planning, round two".into();
        after.start_time += 3600;
        after.end_time += 3600;
        after.location = None;
        after.description = Some("typed over".into());
        after.attendees = Some(json!([{ "email": "bob@example.com" }, { "email": "alice@example.com" }]));

        assert_eq!(changed_fields(&before, &after), json!({
            "subject": "Planning, round two",
            "start": { "dateTime": "2026-10-12T10:00:00", "timeZone": "UTC" },
            "end": { "dateTime": "2026-10-12T11:00:00", "timeZone": "UTC" },
        }));
    }

    #[test]
    fn a_new_guest_is_added_and_the_old_ones_stay_as_they_were() {
        let before = meeting_from_outlook();
        let mut after = before.clone();
        after.attendees = Some(json!([
            { "email": "alice@example.com" }, { "email": "bob@example.com" }, { "email": "carol@example.com" },
        ]));
        assert_eq!(changed_fields(&before, &after), json!({ "attendees": [
            { "emailAddress": { "address": "alice@example.com" }, "type": "optional" },
            { "emailAddress": { "address": "bob@example.com" }, "type": "required" },
            { "emailAddress": { "address": "carol@example.com" }, "type": "required" },
        ]}));
    }

    #[test]
    fn an_emptied_guest_list_is_never_sent() {
        let before = meeting_from_outlook();
        let mut after = before.clone();
        after.attendees = None;
        assert_eq!(changed_fields(&before, &after), json!({}));
    }

    #[test]
    fn an_edit_that_changed_nothing_sends_no_request() {
        // Nothing listens on port 1, so a request would fail.
        let mut oc = OutlookCalendar::new(&json!({ "access_token": "test-token" }));
        oc.base = "http://127.0.0.1:1".into();
        oc.token_expires_at = now_epoch() + 3600;
        let ev = meeting_from_outlook();
        assert!(oc.update_event("AAMk-1", &ev, &ev.clone()));
    }

    #[test]
    fn an_edit_is_a_patch_and_a_delete_is_a_delete() {
        let (mut oc, seen) = graph(2, r#"{"id":"AAMk-1"}"#);
        let before = meeting_from_outlook();
        let mut after = before.clone();
        after.location = Some("Room 5".into());

        assert!(oc.update_event("AAMk-1", &before, &after));
        assert!(oc.delete_event("AAMk-1"));

        let seen = seen.join().unwrap();
        assert!(seen[0].starts_with("PATCH /me/events/AAMk-1 HTTP/1.1\r\n"), "{}", seen[0]);
        assert_eq!(body_of(&seen[0]), json!({ "location": { "displayName": "Room 5" } }));
        assert!(seen[1].starts_with("DELETE /me/events/AAMk-1 HTTP/1.1\r\n"), "{}", seen[1]);
    }
}
