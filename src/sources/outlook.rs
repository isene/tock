// Outlook / Microsoft 365 Graph API integration for Tock.
// Ported from Timely's Ruby implementation; uses ureq for HTTP.

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

pub struct OutlookCalendar {
    client_id: String,
    tenant_id: String,
    access_token: Option<String>,
    refresh_token: Option<String>,
    token_expires_at: i64,
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
            last_error: None,
        }
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

    pub fn create_event(&mut self, event_data: &EventData) -> Option<String> {
        let body = to_outlook_format(event_data);
        let resp = self.api_post("/me/events", &body)?;
        resp.get("id").and_then(Value::as_str).map(String::from)
    }

    pub fn update_event(
        &mut self,
        event_id: &str,
        event_data: &EventData,
    ) {
        let body = to_outlook_format(event_data);
        let path = format!("/me/events/{}", event_id);
        let _ = self.api_patch(&path, &body);
    }

    pub fn delete_event(&mut self, event_id: &str) -> bool {
        let path = format!("/me/events/{}", event_id);
        self.api_delete(&path)
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
            format!("{}{}", GRAPH_BASE, path)
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
        let url = format!("{}{}", GRAPH_BASE, path);

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
        let url = format!("{}{}", GRAPH_BASE, path);

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
        let url = format!("{}{}", GRAPH_BASE, path);

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

    let description = item.get("bodyPreview")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            item.get("body")
                .and_then(|b| b.get("content"))
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

    if let Some(ref desc) = event_data.description {
        ev["body"] = json!({
            "contentType": "text",
            "content": desc,
        });
    }

    if let Some(ref loc) = event_data.location {
        ev["location"] = json!({ "displayName": loc });
    }

    ev["isAllDay"] = json!(event_data.all_day);

    let tz = event_data.timezone.as_deref().unwrap_or("UTC");
    ev["start"] = json!({
        "dateTime": ts_to_iso(event_data.start_time),
        "timeZone": tz,
    });
    ev["end"] = json!({
        "dateTime": ts_to_iso(event_data.end_time),
        "timeZone": tz,
    });

    if let Some(ref att) = event_data.attendees {
        ev["attendees"] = att.clone();
    }

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
