//! CalDAV: calendars on iCloud, Fastmail, Nextcloud, Radicale and any
//! other server that speaks it, read and written with a user name and a
//! password (for iCloud an app-specific one, made at appleid.apple.com).
//!
//! The server finds the calendars (current-user-principal, then
//! calendar-home-set, then the collections in it), sends the events of
//! a time window with repeats already expanded, and takes an event as
//! one `.ics` file per event, put and deleted by its address.

use crate::database::EventData;
use regex::Regex;
use std::time::Duration;

const CURRENT_PRINCIPAL: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>"#;

const HOME_SET: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><c:calendar-home-set/></d:prop></d:propfind>"#;

const LIST: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav" xmlns:a="http://apple.com/ns/ical/">
<d:prop><d:resourcetype/><d:displayname/><c:supported-calendar-component-set/><a:calendar-color/></d:prop>
</d:propfind>"#;

const CTAG: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cs="http://calendarserver.org/ns/"><d:prop><cs:getctag/><d:sync-token/></d:prop></d:propfind>"#;

/// One calendar on the server.
pub struct Collection {
    pub url: String,
    pub name: String,
    /// `#RRGGBB` (Apple's calendar-color), when the server has one.
    pub color: Option<String>,
}

/// One event file on the server: its address, its version tag, and the
/// iCalendar text it holds.
pub struct Resource {
    pub href: String,
    pub ics: String,
}

pub struct CalDav {
    user: String,
    password: String,
    agent: ureq::Agent,
}

impl CalDav {
    pub fn new(user: &str, password: &str) -> Self {
        // Redirects by hand: ureq would turn a redirected PROPFIND into
        // a GET.
        let agent = ureq::AgentBuilder::new()
            .redirects(0)
            .timeout(Duration::from_secs(30))
            .build();
        CalDav { user: user.to_string(), password: password.to_string(), agent }
    }

    /// A WebDAV request: the address it ended at, the status, the body.
    fn request(&self, method: &str, url: &str, depth: Option<&str>, body: &str, extra: &[(&str, &str)])
        -> Result<(String, u16, String), String>
    {
        let auth = format!("Basic {}", crust::base64_encode(format!("{}:{}", self.user, self.password).as_bytes()));
        let mut url = url.to_string();
        for _ in 0..5 {
            let mut req = self.agent.request(method, &url).set("Authorization", &auth);
            if !body.is_empty() {
                req = req.set("Content-Type", if body.starts_with("BEGIN:VCALENDAR") {
                    "text/calendar; charset=utf-8"
                } else {
                    "application/xml; charset=utf-8"
                });
            }
            if let Some(d) = depth {
                req = req.set("Depth", d);
            }
            for (k, v) in extra {
                req = req.set(k, v);
            }
            let resp = if body.is_empty() { req.call() } else { req.send_string(body) };
            let r = match resp {
                Ok(r) => r,
                Err(ureq::Error::Status(_, r)) => r,
                Err(e) => return Err(format!("{url}: {e}")),
            };
            let status = r.status();
            if (300..400).contains(&status) {
                if let Some(to) = r.header("Location") {
                    url = resolve(&url, to);
                    continue;
                }
            }
            if status == 401 {
                return Err("the server refused the user name or password".into());
            }
            return Ok((url, status, r.into_string().unwrap_or_default()));
        }
        Err(format!("{url}: too many redirects"))
    }

    /// The address in `prop` (an href inside it) of the resource at `url`.
    fn href_of(&self, url: &str, body: &str, prop: &str) -> Result<String, String> {
        let (at, status, xml) = self.request("PROPFIND", url, Some("0"), body, &[])?;
        if status >= 400 {
            return Err(format!("{at}: HTTP {status}"));
        }
        elements(&xml, prop).into_iter()
            .find_map(|p| elements(p, "href").into_iter().next().map(|h| resolve(&at, &text(h))))
            .ok_or_else(|| format!("{at}: no {prop}"))
    }

    /// The calendars the account holds that take events, from the
    /// server's address (`https://caldav.icloud.com`, say, or a
    /// Nextcloud's `.../remote.php/dav`).
    pub fn discover(&self, server: &str) -> Result<Vec<Collection>, String> {
        let principal = self.href_of(server, CURRENT_PRINCIPAL, "current-user-principal")
            .or_else(|first| {
                let known = format!("{}/.well-known/caldav", origin(server));
                self.href_of(&known, CURRENT_PRINCIPAL, "current-user-principal").map_err(|_| first)
            })?;
        let home = self.href_of(&principal, HOME_SET, "calendar-home-set")?;
        let (at, status, xml) = self.request("PROPFIND", &home, Some("1"), LIST, &[])?;
        if status >= 400 {
            return Err(format!("{at}: HTTP {status}"));
        }
        let mut out = Vec::new();
        for r in elements(&xml, "response") {
            let Some(href) = elements(r, "href").into_iter().next().map(text) else { continue };
            let is_calendar = elements(r, "resourcetype").iter().any(|t| has_tag(t, "calendar"));
            // A calendar that names its kinds must name VEVENT; one that
            // names none takes everything.
            let kinds = comp_names(r);
            if !is_calendar || (!kinds.is_empty() && !kinds.iter().any(|k| k == "VEVENT")) {
                continue;
            }
            let url = resolve(&at, &href);
            let name = elements(r, "displayname").into_iter().next().map(text)
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| href.trim_end_matches('/').rsplit('/').next().unwrap_or("CalDAV").to_string());
            let color = elements(r, "calendar-color").into_iter().next().map(text)
                .filter(|c| c.starts_with('#') && c.len() >= 7)
                .map(|c| c[..7].to_string());
            out.push(Collection { url, name, color });
        }
        Ok(out)
    }

    /// The calendar's change tag: it changes whenever any event in it
    /// does, so an unchanged tag means nothing to fetch.
    pub fn ctag(&self, url: &str) -> Option<String> {
        let (_, status, xml) = self.request("PROPFIND", url, Some("0"), CTAG, &[]).ok()?;
        if status >= 400 {
            return None;
        }
        ["getctag", "sync-token"].iter()
            .find_map(|p| elements(&xml, p).into_iter().next().map(text))
            .filter(|t| !t.is_empty())
    }

    /// The events between `start` and `end` (unix seconds), repeats
    /// expanded by the server where it can.
    pub fn events(&self, url: &str, start: i64, end: i64) -> Result<Vec<Resource>, String> {
        let (s, e) = (utc_stamp(start), utc_stamp(end));
        let body = format!(r#"<?xml version="1.0" encoding="utf-8"?>
<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
<d:prop><d:getetag/><c:calendar-data><c:expand start="{s}" end="{e}"/></c:calendar-data></d:prop>
<c:filter><c:comp-filter name="VCALENDAR"><c:comp-filter name="VEVENT"><c:time-range start="{s}" end="{e}"/></c:comp-filter></c:comp-filter></c:filter>
</c:calendar-query>"#);
        let (at, status, xml) = self.request("REPORT", url, Some("1"), &body, &[])?;
        if status >= 400 {
            return Err(format!("{at}: HTTP {status}"));
        }
        Ok(elements(&xml, "response").into_iter().filter_map(|r| {
            let href = resolve(&at, &text(elements(r, "href").into_iter().next()?));
            let ics = text(elements(r, "calendar-data").into_iter().next()?);
            if ics.is_empty() { None } else { Some(Resource { href, ics }) }
        }).collect())
    }

    /// Write one event file: a new one only where none is (`create`), else
    /// over the one there.
    pub fn put(&self, href: &str, ics: &str, create: bool) -> Result<(), String> {
        let extra: &[(&str, &str)] = if create { &[("If-None-Match", "*")] } else { &[] };
        let (at, status, _) = self.request("PUT", href, None, ics, extra)?;
        if (200..300).contains(&status) { Ok(()) } else { Err(format!("{at}: HTTP {status}")) }
    }

    pub fn delete(&self, href: &str) -> Result<(), String> {
        let (at, status, _) = self.request("DELETE", href, None, "", &[])?;
        // Gone already counts as done.
        if (200..300).contains(&status) || status == 404 { Ok(()) } else { Err(format!("{at}: HTTP {status}")) }
    }
}

// ---------------------------------------------------------------------------
// Events to and from a server
// ---------------------------------------------------------------------------

/// The rows one event file makes: each event in it, and each repeat of
/// one that still carries its rule (a server that did not expand), within
/// `start..end`. A row's id is the file's address for a lone event, and
/// the address and the start for one of several, so a repeat is never
/// written back as the whole series.
pub fn rows(res: &Resource, calendar_id: i64, start: i64, end: i64) -> Vec<EventData> {
    let evs = crate::ics::parse(&res.ics);
    let mut spans: Vec<(crate::ics::IcsEvent, i64, i64)> = Vec::new();
    for ev in evs {
        match &ev.rrule {
            Some(rule) => {
                // expand_rrule gives the repeats after the first date.
                spans.push((ev.clone(), ev.start_time, ev.end_time));
                let horizon = (end - ev.start_time) / 86400 + 1;
                for (s, e) in crate::ics::expand_rrule(rule, ev.start_time, ev.end_time, 1000, horizon.max(1)) {
                    spans.push((ev.clone(), s, e));
                }
            }
            None => {
                let (s, e) = (ev.start_time, ev.end_time);
                spans.push((ev, s, e));
            }
        }
    }
    spans.retain(|(_, s, e)| *e > start && *s < end);
    let single = spans.len() == 1;
    spans.into_iter().map(|(ev, s, e)| EventData {
        id: None,
        calendar_id,
        external_id: Some(if single { res.href.clone() } else { format!("{}#{}", res.href, s) }),
        title: ev.title.clone().unwrap_or_else(|| "(No title)".into()),
        description: ev.description.clone(),
        location: ev.location.clone(),
        start_time: s,
        end_time: e,
        all_day: ev.all_day,
        timezone: None,
        recurrence_rule: None,
        series_master_id: None,
        status: ev.status.clone().unwrap_or_else(|| "confirmed".into()).to_lowercase(),
        organizer: ev.organizer.clone(),
        attendees: ev.attendees.as_ref().map(|l| serde_json::Value::Array(
            l.iter().map(|a| serde_json::json!({"email": a})).collect())),
        my_status: None,
        alarms: ev.alarms.as_ref().map(|l| serde_json::Value::Array(l.iter().map(|m| serde_json::json!(m)).collect())),
        metadata: ev.uid.as_ref().map(|u| serde_json::json!({"ics_uid": u})),
    }).collect()
}

/// Whether a row's id stands for one repeat of a series, which tock
/// does not write back.
pub fn is_repeat(external_id: &str) -> bool {
    external_id.rsplit_once('#').is_some_and(|(_, t)| t.parse::<i64>().is_ok())
}

/// A new event's UID and its file's address in the calendar at `url`.
pub fn new_href(url: &str, uid: &str) -> String {
    let safe: String = uid.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '-' }).collect();
    format!("{}/{}.ics", url.trim_end_matches('/'), safe)
}

/// A UID nobody else has: the time to the nanosecond and the process.
pub fn new_uid() -> String {
    let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("tock-{n:x}-{:x}", std::process::id())
}

/// The event as an iCalendar file of one VEVENT.
pub fn to_ics(ev: &EventData, uid: &str) -> String {
    let mut lines = vec![
        "BEGIN:VCALENDAR".to_string(),
        "VERSION:2.0".to_string(),
        "PRODID:-//isene//tock//EN".to_string(),
        "BEGIN:VEVENT".to_string(),
        format!("UID:{uid}"),
        format!("DTSTAMP:{}", utc_stamp(now())),
    ];
    if ev.all_day {
        // All-day events sit at UTC midnight, and the end is the day after.
        lines.push(format!("DTSTART;VALUE=DATE:{}", &utc_stamp(ev.start_time)[..8]));
        lines.push(format!("DTEND;VALUE=DATE:{}", &utc_stamp(ev.end_time.max(ev.start_time + 86400))[..8]));
    } else {
        lines.push(format!("DTSTART:{}", utc_stamp(ev.start_time)));
        lines.push(format!("DTEND:{}", utc_stamp(ev.end_time.max(ev.start_time))));
    }
    lines.push(format!("SUMMARY:{}", escape(&ev.title)));
    if let Some(l) = ev.location.as_deref().filter(|l| !l.is_empty()) {
        lines.push(format!("LOCATION:{}", escape(l)));
    }
    if let Some(d) = ev.description.as_deref().filter(|d| !d.is_empty()) {
        lines.push(format!("DESCRIPTION:{}", escape(d)));
    }
    for m in ev.alarms.as_ref().and_then(|a| a.as_array()).into_iter().flatten().filter_map(|m| m.as_i64()) {
        lines.extend([
            "BEGIN:VALARM".to_string(),
            "ACTION:DISPLAY".to_string(),
            format!("DESCRIPTION:{}", escape(&ev.title)),
            format!("TRIGGER:-PT{m}M"),
            "END:VALARM".to_string(),
        ]);
    }
    lines.extend(["END:VEVENT".to_string(), "END:VCALENDAR".to_string()]);
    lines.iter().map(|l| fold(l)).collect::<Vec<_>>().join("\r\n") + "\r\n"
}

/// Text as iCalendar wants it: backslash, comma, semicolon and newline
/// escaped.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace(',', "\\,").replace(';', "\\;").replace("\r\n", "\\n").replace('\n', "\\n")
}

/// A line cut at 75 bytes, never inside a letter, each part after the
/// first starting with a space (RFC 5545).
fn fold(line: &str) -> String {
    let mut out = String::new();
    let mut n = 0;
    for c in line.chars() {
        if n + c.len_utf8() > 75 {
            out.push_str("\r\n ");
            n = 1;
        }
        out.push(c);
        n += c.len_utf8();
    }
    out
}

fn now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

/// `YYYYMMDDTHHMMSSZ` for a unix time.
fn utc_stamp(ts: i64) -> String {
    crate::sources::google::ts_to_rfc3339_pub(ts).replace(['-', ':'], "")
}

// ---------------------------------------------------------------------------
// Addresses and XML
// ---------------------------------------------------------------------------

/// `scheme://host[:port]` of an address.
fn origin(url: &str) -> String {
    let after = url.find("://").map(|i| i + 3).unwrap_or(0);
    match url[after..].find('/') {
        Some(i) => url[..after + i].to_string(),
        None => url.to_string(),
    }
}

/// `href` as a full address, read against the address it came from.
fn resolve(base: &str, href: &str) -> String {
    let href = href.trim();
    if href.starts_with("http://") || href.starts_with("https://") {
        href.to_string()
    } else if href.starts_with('/') {
        format!("{}{}", origin(base), href)
    } else {
        let dir = base.rsplit_once('/').map(|(d, _)| d).unwrap_or(base);
        format!("{dir}/{href}")
    }
}

/// The insides of every element called `name`, whatever its namespace
/// prefix. Elements of one name are not nested in a multistatus answer.
fn elements<'a>(xml: &'a str, name: &str) -> Vec<&'a str> {
    let open = Regex::new(&format!(r"(?i)<(?:[A-Za-z0-9_.-]+:)?{}(?:\s[^>]*)?>", regex::escape(name))).unwrap();
    let close = Regex::new(&format!(r"(?i)</(?:[A-Za-z0-9_.-]+:)?{}\s*>", regex::escape(name))).unwrap();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(m) = open.find_at(xml, from) {
        if m.as_str().ends_with("/>") {
            from = m.end();
            continue;
        }
        match close.find_at(xml, m.end()) {
            Some(c) => {
                out.push(&xml[m.end()..c.start()]);
                from = c.end();
            }
            None => break,
        }
    }
    out
}

/// Whether an element called `name` is there, empty or not.
fn has_tag(xml: &str, name: &str) -> bool {
    Regex::new(&format!(r"(?i)<(?:[A-Za-z0-9_.-]+:)?{}[\s/>]", regex::escape(name))).unwrap().is_match(xml)
}

/// The kinds a calendar takes, from supported-calendar-component-set.
fn comp_names(xml: &str) -> Vec<String> {
    let re = Regex::new(r#"(?i)<(?:[A-Za-z0-9_.-]+:)?comp\s[^>]*name\s*=\s*["']([A-Za-z]+)["']"#).unwrap();
    elements(xml, "supported-calendar-component-set").iter()
        .flat_map(|s| re.captures_iter(s).map(|c| c[1].to_uppercase()).collect::<Vec<_>>())
        .collect()
}

/// An element's text: a CDATA block as it is, else with the XML escapes
/// undone.
fn text(inner: &str) -> String {
    let t = inner.trim();
    if let Some(body) = t.strip_prefix("<![CDATA[").and_then(|b| b.strip_suffix("]]>")) {
        return body.to_string();
    }
    let re = Regex::new(r"&(#x[0-9a-fA-F]+|#[0-9]+|lt|gt|amp|quot|apos);").unwrap();
    re.replace_all(t, |c: &regex::Captures| {
        let e = &c[1];
        match e {
            "lt" => "<".to_string(),
            "gt" => ">".to_string(),
            "amp" => "&".to_string(),
            "quot" => "\"".to_string(),
            "apos" => "'".to_string(),
            _ => {
                let n = if let Some(h) = e.strip_prefix("#x") { u32::from_str_radix(h, 16).ok() } else { e[1..].parse().ok() };
                n.and_then(char::from_u32).map(String::from).unwrap_or_default()
            }
        }
    }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A calendar-home listing in the shape iCloud answers with: default
    /// namespace, absolute hrefs on another host, a VTODO list among the
    /// calendars.
    const ICLOUD_LIST: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<multistatus xmlns="DAV:">
<response><href>/123/calendars/</href><propstat><prop><resourcetype><collection/></resourcetype></prop><status>HTTP/1.1 200 OK</status></propstat></response>
<response><href>/123/calendars/home/</href><propstat><prop><resourcetype><collection/><calendar xmlns="urn:ietf:params:xml:ns:caldav"/></resourcetype><displayname>Home</displayname><supported-calendar-component-set xmlns="urn:ietf:params:xml:ns:caldav"><comp name="VEVENT"/></supported-calendar-component-set><calendar-color xmlns="http://apple.com/ns/ical/">#1BADF8FF</calendar-color></prop><status>HTTP/1.1 200 OK</status></propstat></response>
<response><href>/123/calendars/tasks/</href><propstat><prop><resourcetype><collection/><calendar xmlns="urn:ietf:params:xml:ns:caldav"/></resourcetype><displayname>Reminders</displayname><supported-calendar-component-set xmlns="urn:ietf:params:xml:ns:caldav"><comp name="VTODO"/></supported-calendar-component-set></prop><status>HTTP/1.1 200 OK</status></propstat></response>
<response><href>/123/calendars/work%20stuff/</href><propstat><prop><resourcetype><collection/><C:calendar xmlns:C="urn:ietf:params:xml:ns:caldav"/></resourcetype><displayname>Work &amp; more</displayname></prop><status>HTTP/1.1 200 OK</status></propstat></response>
</multistatus>"##;

    #[test]
    fn calendars_are_read_from_a_listing() {
        let rs = elements(ICLOUD_LIST, "response");
        assert_eq!(rs.len(), 4);
        let calendars: Vec<(String, bool, Vec<String>)> = rs.iter().map(|r| (
            text(elements(r, "href")[0]),
            elements(r, "resourcetype").iter().any(|t| has_tag(t, "calendar")),
            comp_names(r),
        )).collect();
        assert_eq!(calendars[0].1, false, "the home itself is no calendar");
        assert_eq!(calendars[1], ("/123/calendars/home/".into(), true, vec!["VEVENT".into()]));
        assert_eq!(calendars[2].2, vec!["VTODO".to_string()]);
        assert!(calendars[3].1 && calendars[3].2.is_empty(), "prefixed calendar tag, no kinds named");
        assert_eq!(text(elements(rs[3], "displayname")[0]), "Work & more");
        assert_eq!(text(elements(rs[1], "calendar-color")[0]), "#1BADF8FF");
    }

    #[test]
    fn addresses_resolve() {
        assert_eq!(origin("https://p42-caldav.icloud.com:443/123/x/"), "https://p42-caldav.icloud.com:443");
        assert_eq!(resolve("https://a.example/dav/cal/", "/dav/cal/e.ics"), "https://a.example/dav/cal/e.ics");
        assert_eq!(resolve("https://a.example/dav/cal/", "https://b.example/x"), "https://b.example/x");
        assert_eq!(resolve("https://a.example/dav/cal/", "e.ics"), "https://a.example/dav/cal/e.ics");
    }

    #[test]
    fn calendar_data_comes_out_whole() {
        let xml = r#"<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
<d:response><d:href>/dav/cal/a.ics</d:href><d:propstat><d:prop><d:getetag>"1"</d:getetag>
<cal:calendar-data>BEGIN:VCALENDAR&#13;
BEGIN:VEVENT&#13;
UID:a&#13;
DTSTART:20260926T100000Z&#13;
DTEND:20260926T110000Z&#13;
SUMMARY:Tea &amp; cake&#13;
END:VEVENT&#13;
END:VCALENDAR&#13;
</cal:calendar-data></d:prop></d:propstat></d:response>
<d:response><d:href>/dav/cal/b.ics</d:href><d:propstat><d:prop><cal:calendar-data><![CDATA[BEGIN:VCALENDAR
BEGIN:VEVENT
UID:b
DTSTART;VALUE=DATE:20260927
DTEND;VALUE=DATE:20260928
SUMMARY:A <b> day
END:VEVENT
END:VCALENDAR]]></cal:calendar-data></d:prop></d:propstat></d:response>
</d:multistatus>"#;
        let rs = elements(xml, "response");
        let a = Resource { href: text(elements(rs[0], "href")[0]), ics: text(elements(rs[0], "calendar-data")[0]) };
        let b = Resource { href: text(elements(rs[1], "href")[0]), ics: text(elements(rs[1], "calendar-data")[0]) };
        let ra = rows(&a, 7, 0, i64::MAX / 2);
        assert_eq!(ra.len(), 1);
        assert_eq!(ra[0].title, "Tea & cake");
        assert_eq!(ra[0].external_id.as_deref(), Some("/dav/cal/a.ics"));
        let rb = rows(&b, 7, 0, i64::MAX / 2);
        assert!(rb[0].all_day && rb[0].title == "A <b> day");
    }

    #[test]
    fn a_rule_the_server_left_is_expanded_here() {
        let res = Resource { href: "https://x/cal/r.ics".into(), ics: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:r\r\nDTSTART:20260901T080000Z\r\nDTEND:20260901T083000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n".into() };
        // 2026-09-01 .. 2026-09-30: the Tuesdays 1, 8, 15, 22, 29.
        let start = 1_788_220_800; // 2026-09-01 00:00 UTC
        let rs = rows(&res, 1, start, start + 30 * 86400);
        assert_eq!(rs.len(), 5);
        assert!(rs.iter().all(|r| is_repeat(r.external_id.as_deref().unwrap())));
        assert!(!is_repeat("https://x/cal/one.ics"));
    }

    /// Against a real server, when TOCK_CALDAV (server), TOCK_CALDAV_USER
    /// and TOCK_CALDAV_PASS are set: `cargo test -- --ignored caldav`.
    #[test]
    #[ignore]
    fn round_trip_on_a_server() {
        let (Ok(server), Ok(user), Ok(pass)) = (std::env::var("TOCK_CALDAV"), std::env::var("TOCK_CALDAV_USER"), std::env::var("TOCK_CALDAV_PASS")) else {
            return;
        };
        let dav = CalDav::new(&user, &pass);
        let cals = dav.discover(&server).expect("discover");
        println!("calendars: {:?}", cals.iter().map(|c| (&c.name, &c.url)).collect::<Vec<_>>());
        assert_eq!(cals.len(), 1, "the task list is left out");
        let url = &cals[0].url;
        let (start, end) = (now() - 86400, now() + 60 * 86400);
        let rows_now = |dav: &CalDav| -> Vec<EventData> {
            dav.events(url, start, end).expect("events").iter().flat_map(|r| rows(r, 1, start, end)).collect()
        };
        let before = rows_now(&dav);
        for r in &before { println!("  {} {} {:?}", r.start_time, r.title, r.external_id); }
        assert!(before.iter().any(|r| r.title == "Dentist, Oslo"));
        assert_eq!(before.iter().filter(|r| r.title == "Standup").count(), 4, "a weekly event, four times");
        let tag = dav.ctag(url).expect("ctag");

        let ev = EventData {
            id: None, calendar_id: 1, external_id: None, title: "From tock".into(),
            description: None, location: None, start_time: now() + 7 * 86400, end_time: now() + 7 * 86400 + 1800,
            all_day: false, timezone: None, recurrence_rule: None, series_master_id: None,
            status: "confirmed".into(), organizer: None, attendees: None, my_status: None, alarms: None, metadata: None,
        };
        let uid = new_uid();
        let href = new_href(url, &uid);
        dav.put(&href, &to_ics(&ev, &uid), true).expect("put");
        assert!(dav.put(&href, &to_ics(&ev, &uid), true).is_err(), "a second create of the same file is refused");
        let after = rows_now(&dav);
        assert!(after.iter().any(|r| r.title == "From tock" && r.external_id.as_deref() == Some(href.as_str())));
        assert_ne!(dav.ctag(url).expect("ctag"), tag, "the change tag moved");
        dav.delete(&href).expect("delete");
        assert!(!rows_now(&dav).iter().any(|r| r.title == "From tock"));
    }

    #[test]
    fn an_event_goes_out_as_ics() {
        let ev = EventData {
            id: None, calendar_id: 1, external_id: None,
            title: "Lunch, with Ann; long ".to_string() + &"x".repeat(80),
            description: Some("two\nlines".into()), location: None,
            start_time: 1_790_000_000, end_time: 1_790_003_600, all_day: false,
            timezone: None, recurrence_rule: None, series_master_id: None,
            status: "confirmed".into(), organizer: None, attendees: None, my_status: None,
            alarms: Some(serde_json::json!([15])), metadata: None,
        };
        let ics = to_ics(&ev, "u1");
        assert!(ics.contains("SUMMARY:Lunch\\, with Ann\\; long"));
        assert!(ics.contains("DESCRIPTION:two\\nlines"));
        assert!(ics.contains("TRIGGER:-PT15M"));
        assert!(ics.lines().all(|l| l.trim_end_matches('\r').len() <= 75), "lines folded");
        let back = crate::ics::parse(&ics);
        assert_eq!(back.len(), 1);
        assert_eq!((back[0].start_time, back[0].end_time), (1_790_000_000, 1_790_003_600));
        assert!(back[0].title.as_deref().unwrap().starts_with("Lunch, with Ann; long"));
    }
}
