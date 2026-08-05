// Tock - Terminal calendar TUI
// Feature and visual clone of Timely (Ruby), built on Crust.


mod config;
mod database;
mod ics;
mod notifications;
mod poller;
mod sources;
mod weather;

use crust::{display_width, strip_ansi, Crust, Cursor, Input, Pane, style};
use database::{Database, Event, EventData};
use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::path::Path;

// =========================================================================
// Date arithmetic helpers
// =========================================================================

fn days_in_month(year: i32, month: u32) -> u32 {
    orbit::days_in_month(year, month)
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// ISO weekday: Monday=1, Sunday=7
fn cwday(year: i32, month: u32, day: u32) -> u32 {
    // Tomohiko Sakamoto's algorithm
    let t = [0i32, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let y = if month < 3 { year - 1 } else { year };
    let d = ((y + y / 4 - y / 100 + y / 400 + t[(month - 1) as usize] + day as i32)
        % 7) as u32;
    if d == 0 { 7 } else { d }
}

/// ISO week number
fn cweek(year: i32, month: u32, day: u32) -> u32 {
    let doy = day_of_year(year, month, day) as i32;
    let dow = cwday(year, month, day) as i32;
    let _jan1_dow = cwday(year, 1, 1) as i32;
    let mut wk = (doy - dow + 10) / 7;
    if wk < 1 {
        // Belongs to last week of previous year
        let prev_jan1 = cwday(year - 1, 1, 1) as i32;
        let prev_dec31 = if is_leap(year - 1) { 366 } else { 365 };
        wk = (prev_dec31 - cwday(year - 1, 12, 31) as i32 + 10) / 7;
        let _ = prev_jan1; // silence warning
    } else if wk > 52 {
        let dec31_dow = cwday(year, 12, 31) as i32;
        if dec31_dow < 4 {
            wk = 1;
        }
    }
    wk.max(1) as u32
}

fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let mut doy = 0;
    for m in 1..month {
        doy += days_in_month(year, m);
    }
    doy + day
}

fn add_months(date: (i32, u32, u32), n: i32) -> (i32, u32, u32) {
    let (y, m, d) = date;
    let total = (y * 12 + m as i32 - 1) + n;
    let ny = total.div_euclid(12);
    let nm = (total.rem_euclid(12) + 1) as u32;
    let max_d = days_in_month(ny, nm);
    (ny, nm, d.min(max_d))
}

fn date_to_ts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> i64 {
    // Hinnant's algorithm for days from civil
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let m = if month <= 2 { month + 9 } else { month - 3 } as i64;
    let d = day as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64
}

/// `(start, end)` for an all-day event covering `days` whole days from
/// `(y,m,d)`. Stored as **UTC midnight** — the convention the renderer
/// reads (it takes the UTC date of `start_time`). Local-midnight
/// storage (`date_to_ts - tz`) put the event one offset before UTC
/// midnight, so its UTC date fell on the previous day and it rendered
/// on two days. `end` is exclusive (UTC midnight after the last day).
fn all_day_range(y: i32, m: u32, d: u32, days: i64) -> (i64, i64) {
    let start = date_to_ts(y, m, d, 0, 0, 0);
    (start, start + days.max(1) * 86400)
}

/// Parse `YYYY-MM-DD` → `(y, m, d)`. `None` on malformed input.
fn parse_ymd(s: &str) -> Option<(i32, u32, u32)> {
    let p: Vec<&str> = s.trim().split('-').collect();
    if p.len() != 3 { return None; }
    let y = p[0].parse::<i32>().ok()?;
    let m = p[1].parse::<u32>().ok()?;
    let d = p[2].parse::<u32>().ok()?;
    if (1..=12).contains(&m) && (1..=31).contains(&d) { Some((y, m, d)) } else { None }
}

fn ts_to_parts(ts: i64) -> (i32, u32, u32, u32, u32, u32) {
    let secs = ts.rem_euclid(86400);
    let days = ts.div_euclid(86400);
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
    (
        y as i32,
        m as u32,
        d as u32,
        (secs / 3600) as u32,
        ((secs % 3600) / 60) as u32,
        (secs % 60) as u32,
    )
}

fn today() -> (i32, u32, u32) {
    let now = database::now_secs();
    // Apply local timezone offset
    let tz_offset = local_tz_offset_secs();
    let local = now + tz_offset;
    let (y, m, d, _, _, _) = ts_to_parts(local);
    (y, m, d)
}

fn now_slot() -> i32 {
    let now = database::now_secs();
    let tz_offset = local_tz_offset_secs();
    let local = now + tz_offset;
    let (_, _, _, h, m, _) = ts_to_parts(local);
    (h * 2 + if m >= 30 { 1 } else { 0 }) as i32
}

/// Get local timezone offset in seconds from UTC via libc
fn local_tz_offset_secs() -> i64 {
    unsafe {
        let now = database::now_secs() as libc::time_t;
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&now, &mut tm);
        tm.tm_gmtoff as i64
    }
}

/// Format a weekday name from ISO weekday number
fn weekday_short(wd: u32) -> &'static str {
    match wd {
        1 => "Mon", 2 => "Tue", 3 => "Wed", 4 => "Thu",
        5 => "Fri", 6 => "Sat", 7 => "Sun", _ => "???",
    }
}

fn weekday_long(wd: u32) -> &'static str {
    match wd {
        1 => "Monday", 2 => "Tuesday", 3 => "Wednesday", 4 => "Thursday",
        5 => "Friday", 6 => "Saturday", 7 => "Sunday", _ => "Unknown",
    }
}

fn month_name(m: u32) -> &'static str {
    match m {
        1 => "January", 2 => "February", 3 => "March", 4 => "April",
        5 => "May", 6 => "June", 7 => "July", 8 => "August",
        9 => "September", 10 => "October", 11 => "November", 12 => "December",
        _ => "?",
    }
}

fn month_short(m: u32) -> &'static str {
    match m {
        1 => "Jan", 2 => "Feb", 3 => "Mar", 4 => "Apr",
        5 => "May", 6 => "Jun", 7 => "Jul", 8 => "Aug",
        9 => "Sep", 10 => "Oct", 11 => "Nov", 12 => "Dec",
        _ => "?",
    }
}

fn format_date_long(y: i32, m: u32, d: u32) -> String {
    let wd = cwday(y, m, d);
    format!("{}, {} {:02}, {}", weekday_long(wd), month_name(m), d, y)
}

fn format_date_short(y: i32, m: u32, d: u32) -> String {
    let wd = cwday(y, m, d);
    format!("{} {} {:02}, {}", weekday_short(wd), month_short(m), d, y)
}

// =========================================================================
// App
// =========================================================================

struct App {
    db: Arc<Database>,
    config: config::Config,
    running: bool,
    selected_date: (i32, u32, u32),
    selected_slot: i32,
    slot_offset: i32,
    selected_event_index: usize,
    events_by_date: HashMap<(i32, u32, u32), Vec<Event>>,
    weather_forecast: HashMap<String, weather::DayForecast>,
    weather_fetched_at: i64,
    /// A forecast fetch is already in flight.
    weather_refreshing: bool,

    info: Pane,
    top: Pane,
    mid: Pane,
    bottom: Pane,
    status: Pane,

    rows: u16,
    cols: u16,

    cached_planets_date: Option<(i32, u32, u32)>,
    cached_planets: Vec<String>,
    allday_count_date: Option<(i32, u32, u32)>,
    allday_count: usize,

    syncing: bool,
    poller_rx: mpsc::Receiver<poller::PollerEvent>,
    _poller_tx: mpsc::Sender<poller::PollerEvent>,
}

impl App {
    fn new() -> Self {
        let db = Arc::new(Database::new(None).expect("Failed to open database"));
        let cfg = config::Config::new();
        let (cols, rows) = Crust::terminal_size();

        let top_h: u16 = 10;
        let bottom_h = ((rows as f64 * 0.2) as u16).max(5);
        let mid_h = rows.saturating_sub(2 + top_h + bottom_h).max(4);
        let bottom_h = if 2 + top_h + mid_h + bottom_h > rows {
            rows.saturating_sub(2 + top_h + mid_h).max(3)
        } else {
            bottom_h
        };

        let info_bg = cfg.get_i64("colors.info_bg", 235) as u16;
        let status_bg = cfg.get_i64("colors.status_bg", 235) as u16;

        let mut info = Pane::new(1, 1, cols, 1, 255, info_bg);
        info.border = false;
        info.scroll = false;

        let mut top = Pane::new(1, 2, cols, top_h, 255, 0);
        top.border = false;
        top.scroll = false;

        let mut mid = Pane::new(1, 2 + top_h, cols, mid_h, 255, 0);
        mid.border = false;
        mid.scroll = false;

        let mut bottom = Pane::new(1, 2 + top_h + mid_h, cols, bottom_h, 255, 0);
        bottom.border = false;
        bottom.scroll = false;

        let mut status = Pane::new(1, rows, cols, 1, 252, status_bg);
        status.border = false;
        status.scroll = false;

        let (tx, rx) = mpsc::channel();

        let slot = now_slot();

        App {
            db,
            config: cfg,
            running: true,
            selected_date: today(),
            selected_slot: slot,
            slot_offset: (slot - 5).max(0),
            selected_event_index: 0,
            events_by_date: HashMap::new(),
            weather_forecast: HashMap::new(),
            weather_fetched_at: 0,
            weather_refreshing: false,
            info,
            top,
            mid,
            bottom,
            status,
            rows,
            cols,
            cached_planets_date: None,
            cached_planets: Vec::new(),
            allday_count_date: None,
            allday_count: 0,
            syncing: false,
            poller_rx: rx,
            _poller_tx: tx,
        }
    }

    // =====================================================================
    // Pane recreation
    // =====================================================================

    fn recreate_panes(&mut self) {
        let (cols, rows) = Crust::terminal_size();
        self.cols = cols;
        self.rows = rows;

        let top_h: u16 = 10;
        let mut bottom_h = ((rows as f64 * 0.2) as u16).max(5);
        let mid_h = rows.saturating_sub(2 + top_h + bottom_h).max(4);
        if 2 + top_h + mid_h + bottom_h > rows {
            bottom_h = rows.saturating_sub(2 + top_h + mid_h).max(3);
        }

        let info_bg = self.config.get_i64("colors.info_bg", 235) as u16;
        let status_bg = self.config.get_i64("colors.status_bg", 235) as u16;

        self.info = Pane::new(1, 1, cols, 1, 255, info_bg);
        self.info.border = false;
        self.info.scroll = false;

        self.top = Pane::new(1, 2, cols, top_h, 255, 0);
        self.top.border = false;
        self.top.scroll = false;

        self.mid = Pane::new(1, 2 + top_h, cols, mid_h, 255, 0);
        self.mid.border = false;
        self.mid.scroll = false;

        self.bottom = Pane::new(1, 2 + top_h + mid_h, cols, bottom_h, 255, 0);
        self.bottom.border = false;
        self.bottom.scroll = false;

        self.status = Pane::new(1, rows, cols, 1, 252, status_bg);
        self.status.border = false;
        self.status.scroll = false;
    }

    // =====================================================================
    // All-day count (max across visible week, cached per date)
    // =====================================================================

    fn allday_count(&mut self) -> usize {
        if self.allday_count_date == Some(self.selected_date) {
            return self.allday_count;
        }
        let (sy, sm, sd) = self.selected_date;
        let wd = cwday(sy, sm, sd);
        let mut max = 0usize;
        for i in 0..7 {
            let offset = i as i32 - (wd as i32 - 1);
            let d = add_days(self.selected_date, offset);
            let n = self.events_by_date.get(&d)
                .map(|evts| evts.iter().filter(|e| e.all_day).count())
                .unwrap_or(0);
            if n > max { max = n; }
        }
        self.allday_count = max;
        self.allday_count_date = Some(self.selected_date);
        max
    }

    fn min_slot(&mut self) -> i32 {
        let n = self.allday_count() as i32;
        if n > 0 { -n } else { 0 }
    }

    // =====================================================================
    // Slot navigation
    // =====================================================================

    fn adjust_slot_offset(&mut self) {
        if self.selected_slot < 0 { return; }
        let ac = self.allday_count();
        let extra = if ac > 0 { ac + 1 } else { 0 };
        let available = (self.mid.h as i32 - 3 - extra as i32).max(1);
        let scrolloff = 2;
        if self.selected_slot - self.slot_offset >= available - scrolloff {
            self.slot_offset = (self.selected_slot - available + scrolloff + 1)
                .min((48 - available).max(0));
        } else if self.selected_slot - self.slot_offset < scrolloff {
            self.slot_offset = (self.selected_slot - scrolloff).max(0);
        }
    }

    fn move_slot_down(&mut self) {
        let ms = self.min_slot();
        self.selected_slot = if self.selected_slot >= 47 { ms } else { self.selected_slot + 1 };
        if self.selected_slot == ms { self.slot_offset = 0; }
        self.adjust_slot_offset();
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn move_slot_up(&mut self) {
        let ms = self.min_slot();
        if self.selected_slot <= ms {
            self.selected_slot = 47;
            let ac = self.allday_count();
            let extra = if ac > 0 { ac + 1 } else { 0 };
            let available = (self.mid.h as i32 - 3 - extra as i32).max(1);
            self.slot_offset = (48 - available).max(0);
        } else {
            self.selected_slot -= 1;
        }
        self.adjust_slot_offset();
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn page_slots_down(&mut self) {
        let ms = self.min_slot();
        self.selected_slot = (self.selected_slot + 10).min(47).max(ms);
        self.adjust_slot_offset();
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn page_slots_up(&mut self) {
        let ms = self.min_slot();
        self.selected_slot = (self.selected_slot - 10).max(ms);
        self.adjust_slot_offset();
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn go_slot_top(&mut self) {
        self.selected_slot = self.min_slot();
        self.slot_offset = 0;
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn go_slot_bottom(&mut self) {
        self.selected_slot = 47;
        let ac = self.allday_count();
        let extra = if ac > 0 { ac + 1 } else { 0 };
        let available = (self.mid.h as i32 - 3 - extra as i32).max(1);
        self.slot_offset = (48 - available).max(0);
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    // =====================================================================
    // Date/event state changes
    // =====================================================================

    fn date_changed(&mut self) {
        self.selected_event_index = 0;
        self.allday_count_date = None;
        self.load_events_for_range();
        // If slot is in all-day area but no event there, jump out
        if self.selected_slot < 0 && self.event_at_selected_slot().is_none() {
            let events = self.events_on_selected_day();
            if let Some(first_timed) = events.iter().find(|e| !e.all_day) {
                let tz = local_tz_offset_secs();
                let local = first_timed.start_time + tz;
                let (_, _, _, h, m, _) = ts_to_parts(local);
                self.selected_slot = h as i32 * 2 + if m >= 30 { 1 } else { 0 };
            } else {
                self.selected_slot = now_slot();
            }
            self.slot_offset = (self.selected_slot - 5).max(0);
        }
        self.render_all();
    }

    fn safe_date(y: i32, m: u32, d: u32) -> (i32, u32, u32) {
        let m = m.clamp(1, 12);
        let max_d = days_in_month(y, m);
        (y, m, d.min(max_d))
    }

    fn events_on_selected_day(&self) -> Vec<Event> {
        self.events_by_date.get(&self.selected_date).cloned().unwrap_or_default()
    }

    fn event_at_selected_slot(&mut self) -> Option<Event> {
        let events = self.events_on_selected_day();

        if self.selected_slot < 0 {
            let ac = self.allday_count();
            let allday: Vec<&Event> = events.iter().filter(|e| e.all_day).collect();
            let idx = ac as i32 - self.selected_slot.abs();
            if idx >= 0 && (idx as usize) < allday.len() {
                return Some(allday[idx as usize].clone());
            }
            return None;
        }

        let hour = self.selected_slot / 2;
        let minute = (self.selected_slot % 2) * 30;
        let (sy, sm, sd) = self.selected_date;
        let tz = local_tz_offset_secs();
        let slot_start = date_to_ts(sy, sm, sd, hour as u32, minute as u32, 0) - tz;
        let slot_end = slot_start + 1800;

        let overlaps = |e: &Event| !e.all_day && e.start_time < slot_end && e.end_time > slot_start;

        // When several events share this slot, return the one the user
        // has cycled to with e/E (selected_event_index) — not always the
        // first — so every event sharing a slot is reachable for view
        // and edit. Falls back to the first overlapping event when the
        // index points elsewhere (e.g. after arrow-key navigation).
        if let Some(sel) = events.get(self.selected_event_index) {
            if overlaps(sel) {
                return Some(sel.clone());
            }
        }
        events.iter().find(|e| overlaps(e)).cloned()
    }

    fn select_next_event_on_day(&mut self) {
        let events = self.events_on_selected_day();
        if events.is_empty() { return; }
        self.selected_event_index = (self.selected_event_index + 1) % events.len();
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn select_prev_event_on_day(&mut self) {
        let events = self.events_on_selected_day();
        if events.is_empty() { return; }
        self.selected_event_index = if self.selected_event_index == 0 {
            events.len() - 1
        } else {
            self.selected_event_index - 1
        };
        self.render_mid_pane();
        self.render_bottom_pane();
    }

    fn move_slot_to_event(&mut self, evt: &Event) {
        if evt.all_day {
            let ac = self.allday_count();
            let events = self.events_on_selected_day();
            let allday: Vec<&Event> = events.iter().filter(|e| e.all_day).collect();
            let idx = allday.iter().position(|e| e.id == evt.id).unwrap_or(0);
            self.selected_slot = -(ac as i32 - idx as i32);
        } else {
            let tz = local_tz_offset_secs();
            let local = evt.start_time + tz;
            let (_, _, _, h, m, _) = ts_to_parts(local);
            self.selected_slot = h as i32 * 2 + if m >= 30 { 1 } else { 0 };
            self.slot_offset = (self.selected_slot - 5).max(0);
        }
    }

    fn jump_to_next_event(&mut self) {
        let events = self.events_on_selected_day();
        if !events.is_empty() && self.selected_event_index < events.len() - 1 {
            self.selected_event_index += 1;
            let evt = events[self.selected_event_index].clone();
            self.move_slot_to_event(&evt);
            self.render_mid_pane();
            self.render_bottom_pane();
            return;
        }

        for offset in 1..=365 {
            let d = add_days(self.selected_date, offset);
            if let Ok(day_events) = self.db.get_events_for_date(d.0, d.1, d.2) {
                if !day_events.is_empty() {
                    self.selected_date = d;
                    self.selected_event_index = 0;
                    self.allday_count_date = None;
                    self.load_events_for_range();
                    let events = self.events_on_selected_day();
                    if let Some(first) = events.first() {
                        let first = first.clone();
                        self.move_slot_to_event(&first);
                    }
                    self.render_all();
                    return;
                }
            }
        }
        self.show_feedback("No more events found within the next year", 245);
    }

    fn jump_to_prev_event(&mut self) {
        let events = self.events_on_selected_day();
        if !events.is_empty() && self.selected_event_index > 0 {
            self.selected_event_index -= 1;
            let evt = events[self.selected_event_index].clone();
            self.move_slot_to_event(&evt);
            self.render_mid_pane();
            self.render_bottom_pane();
            return;
        }

        for offset in 1..=365 {
            let d = add_days(self.selected_date, -(offset as i32));
            if let Ok(day_events) = self.db.get_events_for_date(d.0, d.1, d.2) {
                if !day_events.is_empty() {
                    self.selected_date = d;
                    self.allday_count_date = None;
                    self.load_events_for_range();
                    let events = self.events_on_selected_day();
                    self.selected_event_index = events.len().saturating_sub(1);
                    if let Some(last) = events.last() {
                        let last = last.clone();
                        self.move_slot_to_event(&last);
                    }
                    self.render_all();
                    return;
                }
            }
        }
        self.show_feedback("No earlier events found within the past year", 245);
    }

    // =====================================================================
    // Data loading
    // =====================================================================

    /// Refresh the forecast on a background thread, then nudge the main
    /// loop to redraw through the poller's own channel. One at a time —
    /// a stale cache is read on every range load, and each would
    /// otherwise start its own fetch.
    fn refresh_weather_bg(&mut self) {
        if self.weather_refreshing { return; }
        self.weather_refreshing = true;
        let db = self.db.clone();
        let tx = self._poller_tx.clone();
        let lat = self.config.get_f64("location.lat", 59.9139);
        let lon = self.config.get_f64("location.lon", 10.7522);
        std::thread::spawn(move || {
            if weather::refresh(lat, lon, &db) {
                let _ = tx.send(poller::PollerEvent::NeedsRefresh);
            }
        });
    }

    fn load_events_for_range(&mut self) {
        let (sy, sm, _) = self.selected_date;
        let range_start = add_months((sy, sm, 1), -3);
        let range_end_m = add_months((sy, sm, 1), 3);
        let range_end = (range_end_m.0, range_end_m.1, days_in_month(range_end_m.0, range_end_m.1));

        let tz = local_tz_offset_secs();
        let start_ts = date_to_ts(range_start.0, range_start.1, range_start.2, 0, 0, 0) - tz;
        let end_ts = date_to_ts(range_end.0, range_end.1, range_end.2, 23, 59, 59) - tz;

        let raw_events = self.db.get_events_in_range(start_ts, end_ts).unwrap_or_default();

        self.events_by_date.clear();
        for evt in &raw_events {
            // All-day events are stored as UTC midnight per the iCal/Google/Outlook
            // floating-date convention — they are DATES, not timestamps. Applying a
            // local-TZ shift pushes the (exclusive) end into the next day; don't.
            // Timed events do need the shift so their wall-clock date is right.
            let (sy2, sm2, sd2) = if evt.all_day {
                let (y, m, d, _, _, _) = ts_to_parts(evt.start_time);
                (y, m, d)
            } else {
                let (y, m, d, _, _, _) = ts_to_parts(evt.start_time + tz);
                (y, m, d)
            };
            let (ey, em, ed) = if evt.all_day {
                // Exclusive end → subtract 1 second before extracting the date,
                // so a May 23 → May 28 (exclusive) event ends on May 27.
                let end_ts = if evt.end_time > evt.start_time { evt.end_time - 1 } else { evt.start_time };
                let (y, m, d, _, _, _) = ts_to_parts(end_ts);
                (y, m, d)
            } else {
                let (y, m, d, _, _, _) = ts_to_parts(evt.end_time + tz);
                (y, m, d)
            };

            let mut cur = (sy2, sm2, sd2);
            let end_date = (ey, em, ed);
            loop {
                if cur >= range_start && cur <= range_end {
                    self.events_by_date.entry(cur).or_default().push(evt.clone());
                }
                if cur >= end_date { break; }
                cur = add_days(cur, 1);
                // Safety: don't loop more than 366 days
                if day_diff(cur, (sy2, sm2, sd2)) > 366 { break; }
            }
        }

        // Sort each day's events
        for evts in self.events_by_date.values_mut() {
            evts.sort_by_key(|e| e.start_time);
        }

        // Clamp selected event index
        let events = self.events_on_selected_day();
        if events.is_empty() {
            self.selected_event_index = 0;
        } else if self.selected_event_index >= events.len() {
            self.selected_event_index = events.len() - 1;
        }

        // Weather, from the cache only. Fetching here put an HTTP round
        // trip to met.no in front of the first paint every time the
        // six-hour cache expired — the "black for several seconds" at
        // startup. Show what the cache has, however old, and refresh
        // behind the UI.
        let now = database::now_secs();
        if self.weather_forecast.is_empty() || (now - self.weather_fetched_at) > 21600 {
            let (cached, stale) = weather::cached_or_stale(&self.db);
            if !cached.is_empty() {
                self.weather_forecast = cached;
                self.weather_fetched_at = now;
            }
            if stale { self.refresh_weather_bg(); }
        }

        // Invalidate allday cache
        self.allday_count_date = None;
    }

    // =====================================================================
    // Rendering
    // =====================================================================

    fn render_all(&mut self) {
        // Check for resize
        let (cols, rows) = Crust::terminal_size();
        if cols != self.cols || rows != self.rows {
            Crust::clear_screen();
            self.recreate_panes();
        }

        // Set terminal title
        let events = self.events_on_selected_day();
        let (sy, sm, sd) = self.selected_date;
        let mut title = format!("Tock: {}", format_date_short(sy, sm, sd));
        if !events.is_empty() {
            title.push_str(&format!(" ({} event{})", events.len(),
                if events.len() == 1 { "" } else { "s" }));
        }
        Crust::set_title(&title);

        self.render_info_bar();
        self.render_top_pane();
        self.render_mid_pane();
        self.render_bottom_pane();
        self.render_status_bar();
    }

    // ----- Info bar -----

    fn render_info_bar(&mut self) {
        let (sy, sm, sd) = self.selected_date;
        let title = style::bold(" Tock");
        let date_str = format!("  {}", format_date_long(sy, sm, sd));

        let phase = orbit::moon_phase(sy, sm, sd);
        let moon_color = body_color("moon");
        // orbit returns the U+1F311–18 emoji moon set; those render
        // as 2-cell colour icons in most font stacks and pick up a
        // grey fallback glyph next to them in some terminals — the
        // "two moons" effect. Use the same text-presentation glyph
        // tock already uses for the moon body (☾ + VS-15); the phase
        // name + illumination carry the phase info.
        let moon = format!("  {} {} ({}%)",
            style::fg_rgb("\u{263E}\u{FE0E}", &moon_color),
            phase.phase_name,
            (phase.illumination * 100.0).round() as i32);

        let lat = self.config.get_f64("location.lat", 59.9139);
        let lon = self.config.get_f64("location.lon", 10.7522);
        let tz = self.config.get_f64("timezone_offset", 1.0);

        // Harmonised header pattern: `<body glyph> <up/down arrow>
        // <time>` for sun AND moon, so both rise/set pairs read the
        // same. One body glyph each, one direction-arrow set across
        // both. The arrows are U+2191 (↑) and U+2193 (↓) which are
        // unambiguously text in every terminal font we care about.
        // VS-15 (U+FE0E) on the body glyphs keeps emoji-presentation
        // renderers from blowing them up to 2-cell colour icons.
        let moon_rs = match orbit::moon_times(sy, sm, sd, lat, lon, tz) {
            Some((rise, set)) => {
                let mc = body_color("moon");
                // ☾ U+263E LAST QUARTER MOON — used as the generic
                // moon body glyph here, neutral on waxing/waning so
                // the symbol describes the body, not its phase.
                format!("  {}\u{2191}{}  {}\u{2193}{}",
                    style::fg_rgb("\u{263E}\u{FE0E}", &mc), rise,
                    style::fg_rgb("\u{263E}\u{FE0E}", &mc), set)
            }
            None => String::new(),
        };
        let sun_str = match orbit::sun_times(sy, sm, sd, lat, lon, tz) {
            Some((rise, set)) => {
                let sc = body_color("sun");
                // ☀ U+2600 BLACK SUN WITH RAYS — same sun for both,
                // arrow tells which is which. Avoids the earlier
                // `☼ + ☀` pair which rendered as `✷ + ☀` in some
                // fonts and didn't read as "matched set".
                format!("  {}\u{2191}{}  {}\u{2193}{}",
                    style::fg_rgb("\u{2600}\u{FE0E}", &sc), rise,
                    style::fg_rgb("\u{2600}\u{FE0E}", &sc), set)
            }
            None => String::new(),
        };

        // Visible planets (cached per date). We override orbit's
        // astrological glyphs with two-letter Latin abbreviations
        // (Me / Ve / Ma / Ju / Sa) so the row reads uniformly in
        // every terminal. Venus and Mars are on Unicode's default-
        // emoji list and many font stacks ignore VS-15 — those two
        // would blow up to 2-cell colour icons next to small-text
        // Mercury / Jupiter / Saturn and the row would never sit
        // flush. Colour from `p.color` still distinguishes them at
        // a glance. Nova / astro keep the orbit glyphs since they
        // render in different contexts.
        if self.cached_planets_date != Some(self.selected_date) {
            let planets = orbit::visible_planets(sy, sm, sd, lat, lon, tz);
            self.cached_planets = planets.iter().map(|p| {
                let abbrev = planet_abbrev(p.name);
                style::fg_rgb(abbrev, p.color)
            }).collect();
            self.cached_planets_date = Some(self.selected_date);
        }
        let planet_str = if !self.cached_planets.is_empty() {
            format!("  {}", self.cached_planets.join(" "))
        } else {
            String::new()
        };

        let text = format!("{}{}{}{}{}{}", title, date_str, moon, moon_rs, sun_str, planet_str);
        self.info.set_text(&text);
        self.info.refresh();
    }

    // ----- Status bar -----

    fn render_status_bar(&mut self) {
        let keys = "d/D:Day  w/W:Week  m/M:Month  y/Y:Year  e/E:Event  n:New  g:GoTo  t:Today  i:Import  G:Google  O:Outlook  S:Sync  C:Cal  P:Prefs  ?:Help  q:Quit";
        let version = format!("tock v{}", env!("CARGO_PKG_VERSION"));
        let w = self.cols as usize;
        if self.syncing {
            let sync_ind = style::fg(" Syncing...", 226);
            let used = keys.len() + 12 + version.len() + 2;
            let pad_len = w.saturating_sub(used).max(1);
            let text = format!(" {}{}{} {}", keys, " ".repeat(pad_len), sync_ind, version);
            self.status.set_text(&text);
        } else {
            let used = keys.len() + version.len() + 3;
            let pad_len = w.saturating_sub(used).max(1);
            let text = format!(" {}{}{}", keys, " ".repeat(pad_len), version);
            self.status.set_text(&text);
        }
        self.status.refresh();
    }

    // ----- Top pane (mini months) -----

    fn render_top_pane(&mut self) {
        let (sy, sm, sd) = self.selected_date;
        let t = today();
        let month_width = 26usize; // 25 + 1 separator
        let months_visible = (self.cols as usize / month_width).max(1);
        let offset = 3; // Selected month is 4th from left

        let mut month_data: Vec<(i32, u32)> = Vec::new();
        for i in 0..months_visible {
            let m_off = i as i32 - offset as i32;
            let d = add_months((sy, sm, 1), m_off);
            month_data.push((d.0, d.1));
        }

        let current_month_bg = self.config.get_i64("colors.current_month_bg", 233);
        let today_bg = self.config.get_i64("colors.today_bg", 246) as u8;

        let rendered: Vec<Vec<String>> = month_data.iter().map(|&(year, month)| {
            let sel_day = if year == sy && month == sm { Some(sd) } else { None };
            let is_current = year == sy && month == sm;
            let lines = self.render_mini_month(year, month, sel_day, t, today_bg);
            if is_current {
                lines.iter().map(|l| style::bg(l, current_month_bg as u8)).collect()
            } else {
                lines
            }
        }).collect();

        let max_lines = rendered.iter().map(|m| m.len()).max().unwrap_or(0);
        let mut combined: Vec<String> = vec![String::new()]; // 1 row top padding

        for row in 0..max_lines {
            let mut parts: Vec<String> = Vec::new();
            for month_lines in &rendered {
                let line = month_lines.get(row).cloned().unwrap_or_default();
                let pure_len = display_width(&line);
                let pad = (month_width - 1).saturating_sub(pure_len);
                parts.push(format!("{}{}", line, " ".repeat(pad)));
            }
            combined.push(format!(" {}", parts.join(" ")));
        }

        while combined.len() < self.top.h as usize {
            combined.push(String::new());
        }

        self.top.set_text(&combined.join("\n"));
        self.top.full_refresh();
    }

    fn render_mini_month(&self, year: i32, month: u32, sel_day: Option<u32>,
                          today_date: (i32, u32, u32), today_bg: u8) -> Vec<String> {
        let mut lines = Vec::new();

        // Title
        let title = format!("{} {}", month_name(month), year);
        let pad = (25usize.saturating_sub(title.len())) / 2;
        lines.push(format!("{}{}", " ".repeat(pad.max(1)), style::bold(&title)));

        // Weekday header
        let days = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"];
        let hdr: Vec<String> = days.iter().enumerate().map(|(i, d)| {
            let s = format!("{:>2}", d);
            match i {
                5 => style::fg(&s, 208),
                6 => style::fg(&s, 167),
                _ => style::fg(&s, 245),
            }
        }).collect();
        lines.push(format!("    {}", hdr.join(" ")));

        // Build weeks
        let first_wd = cwday(year, month, 1);
        let last_day = days_in_month(year, month);
        let mut week: Vec<Option<u32>> = Vec::new();
        for _ in 0..(first_wd - 1) { week.push(None); }

        for day in 1..=last_day {
            week.push(Some(day));
            if week.len() == 7 {
                lines.push(self.format_mini_week(&week, year, month, sel_day, today_date, today_bg));
                week.clear();
            }
        }
        if !week.is_empty() {
            while week.len() < 7 { week.push(None); }
            lines.push(self.format_mini_week(&week, year, month, sel_day, today_date, today_bg));
        }

        // Pad to 8 lines
        while lines.len() < 8 {
            lines.push(" ".repeat(25));
        }
        lines
    }

    fn format_mini_week(&self, week: &[Option<u32>], year: i32, month: u32,
                         sel_day: Option<u32>, today_date: (i32, u32, u32), today_bg: u8) -> String {
        let first_day = week.iter().flatten().next().copied().unwrap_or(1);
        let wn = cweek(year, month, first_day);
        let wn_str = style::fg(&format!("{:2}", wn), 238);

        let cells: Vec<String> = week.iter().enumerate().map(|(i, day_opt)| {
            match day_opt {
                None => "  ".to_string(),
                Some(day) => {
                    let day = *day;
                    let date = (year, month, day);
                    let is_today = date == today_date;
                    let is_selected = sel_day == Some(day);
                    let events = self.events_by_date.get(&date);
                    let has_events = events.map(|e| !e.is_empty()).unwrap_or(false);

                    let base_color: Option<u8> = if has_events {
                        Some(events.unwrap().first().map(|e| e.calendar_color as u8).unwrap_or(39))
                    } else if i == 6 { // Sunday
                        Some(167)
                    } else if i == 5 { // Saturday
                        Some(208)
                    } else {
                        None
                    };

                    let d = format!("{:2}", day);
                    if is_selected && is_today {
                        let s = if let Some(c) = base_color {
                            style::fg(&d, c)
                        } else { d };
                        style::bg(&style::underline(&style::bold(&s)), today_bg)
                    } else if is_selected {
                        let s = if let Some(c) = base_color {
                            style::fg(&d, c)
                        } else { d };
                        style::underline(&style::bold(&s))
                    } else if is_today {
                        let s = if let Some(c) = base_color {
                            style::fg(&d, c)
                        } else { d };
                        style::bg(&s, today_bg)
                    } else if let Some(c) = base_color {
                        style::fg(&d, c)
                    } else {
                        d
                    }
                }
            }
        }).collect();

        format!("{} {}", wn_str, cells.join(" "))
    }

    // ----- Mid pane (week view) -----

    fn render_mid_pane(&mut self) {
        let (sy, sm, sd) = self.selected_date;
        let wd = cwday(sy, sm, sd);
        let week_start = add_days(self.selected_date, -(wd as i32 - 1));

        let time_col = 6usize; // "HH:MM "
        let gap = 1usize;
        let day_col = ((self.cols as usize).saturating_sub(time_col + gap * 6) / 7).max(8);

        let sel_alt_a = self.config.get_i64("colors.selected_bg_a", 235) as u8;
        let sel_alt_b = self.config.get_i64("colors.selected_bg_b", 234) as u8;
        let alt_bg_a = self.config.get_i64("colors.alt_bg_a", 233) as u8;
        let alt_bg_b = self.config.get_i64("colors.alt_bg_b", 0) as u8;
        let slot_sel_bg = self.config.get_i64("colors.slot_selected_bg", 237) as u8;
        let today_bg = self.config.get_i64("colors.today_bg", 246) as u8;
        let today_fg = self.config.get_i64("colors.today_fg", 232) as u8;
        let sat_color = self.config.get_i64("colors.saturday", 208) as u8;
        let sun_color = self.config.get_i64("colors.sunday", 167) as u8;
        let t = today();
        let tz = local_tz_offset_secs();

        let mut lines: Vec<String> = Vec::new();

        // Weather row
        let mut weather_parts = vec![" ".repeat(time_col)];
        for i in 0..7 {
            let day = add_days(week_start, i);
            let w_str = weather::short_for_date(&self.weather_forecast, day.0, day.1, day.2)
                .unwrap_or_default();
            let pure_len = display_width(&w_str);
            let pad = day_col.saturating_sub(pure_len);
            weather_parts.push(format!("{}{}", style::fg(&w_str, 245), " ".repeat(pad)));
        }
        lines.push(weather_parts.join(" "));

        // Day headers
        let wk = cweek(week_start.0, week_start.1, week_start.2);
        let wk_label = format!("W{}", wk);
        let wk_str = style::fg(&wk_label, 238);
        let wk_pad = time_col.saturating_sub(wk_label.len()).max(1);
        let mut header_parts = vec![format!("{}{}", wk_str, " ".repeat(wk_pad))];

        for i in 0..7 {
            let day = add_days(week_start, i);
            let day_wd = cwday(day.0, day.1, day.2);
            let header_text = format!("{} {}", weekday_short(day_wd), day.2);
            let is_sel = day == self.selected_date;
            let is_today = day == t;

            let base_color: u8 = if day_wd == 7 { sun_color }
                else if day_wd == 6 { sat_color }
                else { 245 };

            let pure_len = header_text.len();
            let pad = day_col.saturating_sub(pure_len);
            let (header, pad_str) = if is_sel && is_today {
                let h = style::bg(&style::fg(&style::underline(&style::bold(&header_text)), today_fg), today_bg);
                (h, style::bg(&" ".repeat(pad), today_bg))
            } else if is_sel {
                let h = style::bg(&style::fg(&style::underline(&style::bold(&header_text)), base_color), sel_alt_a);
                (h, style::bg(&" ".repeat(pad), sel_alt_a))
            } else if is_today {
                let h = style::bg(&style::fg(&style::bold(&header_text), today_fg), today_bg);
                (h, style::bg(&" ".repeat(pad), today_bg))
            } else {
                (style::fg(&header_text, base_color), " ".repeat(pad))
            };
            header_parts.push(format!("{}{}", header, pad_str));
        }
        lines.push(header_parts.join(" "));

        // Separator
        let sep = style::fg(&"-".repeat(self.cols as usize), 238);
        lines.push(sep.clone());

        // All-day events
        let mut week_allday: Vec<Vec<Event>> = Vec::new();
        let mut week_events: Vec<Vec<Event>> = Vec::new();
        for i in 0..7 {
            let day = add_days(week_start, i);
            let all = self.events_by_date.get(&day).cloned().unwrap_or_default();
            week_allday.push(all.iter().filter(|e| e.all_day).cloned().collect());
            week_events.push(all.iter().filter(|e| !e.all_day).cloned().collect());
        }

        let max_allday = week_allday.iter().map(|v| v.len()).max().unwrap_or(0);
        if max_allday > 0 {
            for row in 0..max_allday {
                let allday_slot = -(max_allday as i32 - row as i32);
                let is_row_selected = self.selected_slot == allday_slot;
                let label = if is_row_selected {
                    format!("{} ", style::bold(&style::fg("  All", 255)))
                } else {
                    " ".repeat(time_col)
                };
                let mut parts = vec![label];

                for col in 0..7 {
                    let day = add_days(week_start, col);
                    let is_sel = day == self.selected_date;
                    let is_at = is_sel && is_row_selected;
                    let cell_bg = if is_at { Some(slot_sel_bg) }
                        else if is_sel { Some(sel_alt_a) }
                        else { None };

                    let evt_opt = week_allday[col as usize].get(row);
                    let cell = if let Some(evt) = evt_opt {
                        let title = if evt.title.is_empty() { "(No title)" } else { &evt.title };
                        let color = evt.calendar_color as u8;
                        let marker = if is_at { ">" } else { " " };
                        let rsvp = rsvp_marker(evt.my_status.as_deref());
                        let labeled = if rsvp.is_empty() { title.to_string() } else { format!("{} {}", rsvp, title) };
                        let entry = format!("{}{}", marker, truncate_str(&labeled, day_col.saturating_sub(1)));
                        if let Some(bg_c) = cell_bg {
                            style::bg(&style::bold(&style::fg(&entry, color)), bg_c)
                        } else {
                            style::fg(&entry, color)
                        }
                    } else if let Some(bg) = cell_bg {
                        style::bg(" ", bg)
                    } else {
                        " ".to_string()
                    };

                    let pure_len = display_width(&cell);
                    let pad = day_col.saturating_sub(pure_len);
                    let pad_str = if is_sel {
                        style::bg(&" ".repeat(pad), sel_alt_a)
                    } else {
                        " ".repeat(pad)
                    };
                    parts.push(format!("{}{}", cell, pad_str));
                }
                lines.push(parts.join(" "));
            }
            lines.push(style::fg(&"-".repeat(self.cols as usize), 238));
        }

        // Time grid
        let _work_start = self.config.get_i64("work_hours.start", 8) as i32;
        let extra_rows = if max_allday > 0 { max_allday + 1 } else { 0 };
        let available = (self.mid.h as i32 - 3 - extra_rows as i32).max(1);

        // Default offset
        if self.slot_offset < 0 { self.slot_offset = 0; }
        let max_offset = (48 - available).max(0);
        if self.slot_offset > max_offset { self.slot_offset = max_offset; }

        let end_slot = (self.slot_offset + available).min(48);
        // Id of the event the user has cycled to (e/E). Lets a slot
        // shared by several events show the selected one in the grid;
        // the rest are flagged with a "+N" badge and reachable via e/E.
        let sel_event_id: Option<i64> = self.events_on_selected_day()
            .get(self.selected_event_index).map(|e| e.id);
        for slot_idx in self.slot_offset..end_slot {
            let hour = slot_idx / 2;
            let minute = (slot_idx % 2) * 30;
            let row_num = (slot_idx - self.slot_offset) as usize;
            let is_slot_selected = self.selected_slot == slot_idx;
            let row_bg = if row_num % 2 == 0 { alt_bg_a } else { alt_bg_b };

            let time_label = format!("{:02}:{:02} ", hour, minute);
            let tl = if is_slot_selected {
                style::bold(&style::fg(&time_label, 255))
            } else {
                style::fg(&time_label, 238)
            };

            let mut parts = vec![tl];
            for col in 0..7 {
                let day = add_days(week_start, col);
                let is_sel = day == self.selected_date;
                let cell_bg = if is_sel && is_slot_selected {
                    slot_sel_bg
                } else if is_sel {
                    if row_num % 2 == 0 { sel_alt_a } else { sel_alt_b }
                } else {
                    row_bg
                };

                let day_ts_start = date_to_ts(day.0, day.1, day.2,
                    hour as u32, minute as u32, 0) - tz;
                let day_ts_end = day_ts_start + 1800;

                let overlapping: Vec<&Event> = week_events[col as usize].iter()
                    .filter(|e| e.start_time < day_ts_end && e.end_time > day_ts_start)
                    .collect();
                // At the selected day+slot, draw the event the user has
                // cycled to so events sharing the slot are each reachable;
                // elsewhere the first.
                let evt_opt: Option<&Event> = if is_sel && is_slot_selected {
                    sel_event_id
                        .and_then(|id| overlapping.iter().find(|e| e.id == id).copied())
                        .or_else(|| overlapping.first().copied())
                } else {
                    overlapping.first().copied()
                };
                let extra = overlapping.len().saturating_sub(1);

                let cell = if let Some(evt) = evt_opt {
                    let is_at_slot = is_sel && is_slot_selected;
                    let marker = if is_at_slot { ">" } else { " " };
                    let color = evt.calendar_color as u8;
                    // The title only renders in ONE row per event:
                    // either the slot where the event starts (so a
                    // 09:00 event lands on row 09:00), or — when the
                    // event began before the visible window — at the
                    // top of the visible scroll so the user always
                    // sees the name once. Continuation slots show a
                    // thin left-edge bar in the calendar's colour so
                    // the span is still obvious without the title
                    // being printed 13 times.
                    let starts_here = evt.start_time >= day_ts_start && evt.start_time < day_ts_end;
                    let is_first_visible = slot_idx == self.slot_offset
                        && evt.start_time < day_ts_start;
                    let show_title = starts_here || is_first_visible;
                    let ends_here = evt.end_time > day_ts_start && evt.end_time <= day_ts_end;

                    if show_title {
                        let title = if evt.title.is_empty() { "(No title)" } else { &evt.title };
                        let rsvp = rsvp_marker(evt.my_status.as_deref());
                        let labeled = if rsvp.is_empty() {
                            title.to_string()
                        } else {
                            format!("{} {}", rsvp, title)
                        };
                        // Flag a slot shared by multiple events so the
                        // hidden ones are discoverable (cycle with e/E).
                        let badge = if extra > 0 { format!(" +{}", extra) } else { String::new() };
                        let mut entry = format!("{}{}{}", marker, labeled, badge);
                        if entry.len() > day_col {
                            entry = format!("{}.", truncate_str(&entry, day_col.saturating_sub(1)));
                        }
                        if is_at_slot {
                            style::bg(&style::bold(&style::fg(&entry, color)), cell_bg)
                        } else {
                            style::bg(&style::fg(&entry, color), cell_bg)
                        }
                    } else {
                        // Continuation row. `▕` (U+2595 RIGHT ONE
                        // EIGHTH BLOCK) draws a single thin bar on
                        // the left edge in the event's colour; the
                        // rest of the cell stays on the slot bg so
                        // it reads as quiet continuation rather than
                        // a heavy block. The last slot of a multi-
                        // slot event additionally prints the end
                        // time (e.g. "▕ -15:30") so the GM knows
                        // exactly where it stops without scrolling.
                        let bar = if is_at_slot {
                            style::bold(&style::fg("\u{2595}", color))
                        } else {
                            style::fg("\u{2595}", color)
                        };
                        let tail = if ends_here {
                            let end_local = evt.end_time + tz;
                            let hh = ((end_local.rem_euclid(86400)) / 3600) as i32;
                            let mm = ((end_local.rem_euclid(3600)) / 60) as i32;
                            format!(" -{:02}:{:02}", hh, mm)
                        } else {
                            String::new()
                        };
                        let tail_styled = if tail.is_empty() {
                            String::new()
                        } else {
                            style::fg(&tail, color)
                        };
                        style::bg(&format!("{}{}", bar, tail_styled), cell_bg)
                    }
                } else {
                    style::bg(" ", cell_bg)
                };

                let pure_len = display_width(&cell);
                let pad = day_col.saturating_sub(pure_len);
                parts.push(format!("{}{}", cell, style::bg(&" ".repeat(pad), cell_bg)));
            }
            lines.push(parts.join(" "));
        }

        while lines.len() < self.mid.h as usize {
            lines.push(String::new());
        }

        self.mid.set_text(&lines.join("\n"));
        self.mid.full_refresh();
    }

    // ----- Bottom pane -----

    fn render_bottom_pane(&mut self) {
        let mut lines: Vec<String> = Vec::new();
        let (sy, sm, sd) = self.selected_date;
        let events = self.events_on_selected_day();
        let w = self.cols as usize;

        // Separator
        lines.push(style::fg(&"-".repeat(w), 238));

        let evt = self.event_at_selected_slot();
        if let Some(evt) = evt {
            // Kastrup-style detail: a coloured title, a full-width rule, then
            // aligned "Label: value" rows (When / Where / Organizer /
            // Attendees / Calendar / Status / Join), then the human part of
            // the description with meeting boilerplate stripped out.
            let color = evt.calendar_color as u8;
            let tz = local_tz_offset_secs();
            const LBL: u8 = 73;   // field label (teal)
            const VAL: u8 = 252;  // primary value
            const DIM: u8 = 245;  // secondary value
            let label_w = 10;     // pads "Attendees:" so colons align
            let max_val = w.saturating_sub(label_w + 3);
            let rule = || style::fg(&"\u{2500}".repeat(w.saturating_sub(1)), 238);

            // Title (bold, calendar colour, RSVP marker prefix)
            let title_only = if evt.title.is_empty() { "(No title)".to_string() } else { evt.title.clone() };
            let rsvp = rsvp_marker(evt.my_status.as_deref());
            let title = if rsvp.is_empty() { title_only } else { format!("{} {}", rsvp, title_only) };
            lines.push(format!(" {}", style::bold(&style::fg(&truncate_str(&title, w.saturating_sub(2)), color))));
            lines.push(rule());

            // When
            let when = if evt.all_day {
                format!("{}-{:02}-{:02}  All day", sy, sm, sd)
            } else {
                let (_, _, _, sh, smn, _) = ts_to_parts(evt.start_time + tz);
                let (_, _, _, eh, emn, _) = ts_to_parts(evt.end_time + tz);
                format!("{} {}-{:02}-{:02}  {:02}:{:02} - {:02}:{:02}",
                    weekday_short(cwday(sy, sm, sd)), sy, sm, sd, sh, smn, eh, emn)
            };
            if let Some(l) = fmt_field("When", label_w, &when, max_val, LBL, VAL) { lines.push(l); }
            if let Some(ref loc) = evt.location {
                if let Some(l) = fmt_field("Where", label_w, loc.trim(), max_val, LBL, VAL) { lines.push(l); }
            }
            if let Some(ref org) = evt.organizer {
                if let Some(l) = fmt_field("Organizer", label_w, org.trim(), max_val, LBL, DIM) { lines.push(l); }
            }
            if let Some(ref att) = evt.attendees {
                if let Some(line) = attendee_line(att, max_val) {
                    let lbl = format!("{:<width$}", "Attendees:", width = label_w);
                    lines.push(format!(" {} {}", style::fg(&lbl, LBL), line));
                }
            }
            if let Some(l) = fmt_field("Calendar", label_w, &evt.calendar_name, max_val, LBL, DIM) { lines.push(l); }
            // Status: my RSVP, plus the event status unless it's the boring default.
            let mut st: Vec<String> = Vec::new();
            if let Some(ref ms) = evt.my_status { st.push(humanize_status(ms).to_string()); }
            if !evt.status.is_empty() && !evt.status.eq_ignore_ascii_case("confirmed") {
                st.push(evt.status.clone());
            }
            if let Some(l) = fmt_field("Status", label_w, &st.join("  |  "), max_val, LBL, DIM) { lines.push(l); }
            // Join link extracted from the description (or location).
            let join = evt.description.as_deref().and_then(extract_meeting_link)
                .or_else(|| evt.location.as_deref().and_then(extract_meeting_link));
            if let Some(ref url) = join {
                let lbl = format!("{:<width$}", "Join:", width = label_w);
                lines.push(format!(" {} {}", style::fg(&lbl, 40), style::fg(&truncate_str(url, max_val), 39)));
            }

            // Description — boilerplate-stripped, word-wrapped to full width.
            if let Some(ref desc) = evt.description {
                let cleaned = clean_meeting_desc(desc);
                if !cleaned.is_empty() {
                    lines.push(rule());
                    let max_lines = self.bottom.h as usize;
                    let mut line = " ".to_string();
                    for word in cleaned.split_whitespace() {
                        if line.len() + word.len() + 1 > w.saturating_sub(2) {
                            lines.push(style::fg(&line, 248));
                            if lines.len() >= max_lines { break; }
                            line = format!(" {}", word);
                        } else {
                            if line == " " {
                                line.push_str(word);
                            } else {
                                line.push(' ');
                                line.push_str(word);
                            }
                        }
                    }
                    if lines.len() < max_lines && line.trim().len() > 0 {
                        lines.push(style::fg(&line, 248));
                    }
                }
            }
        } else {
            // Day summary
            lines.push(style::bold(&format!(" {}", format_date_long(sy, sm, sd))));

            // Astronomical events
            let astro = orbit::astro_events_for_year(sy, sm, sd);
            for a in &astro {
                lines.push(style::fg(&format!(" {}", a), 180));
            }

            lines.push(String::new());
            if !events.is_empty() {
                let allday = events.iter().filter(|e| e.all_day).count();
                let timed = events.len() - allday;
                let mut parts: Vec<String> = Vec::new();
                if timed > 0 { parts.push(format!("{} timed", timed)); }
                if allday > 0 { parts.push(format!("{} all-day", allday)); }
                let plural = if events.len() == 1 { "" } else { "s" };
                lines.push(style::fg(&format!(" {} event{} today", parts.join(", "), plural), 240));
            } else {
                lines.push(style::fg(" No events scheduled", 240));
            }
        }

        while lines.len() < self.bottom.h as usize {
            lines.push(String::new());
        }

        self.bottom.set_text(&lines.join("\n"));
        self.bottom.full_refresh();
    }

    // =====================================================================
    // Input handling
    // =====================================================================

    fn handle_input(&mut self, key: &str) {
        match key {
            "y" => {
                let (y, m, d) = self.selected_date;
                self.selected_date = Self::safe_date(y + 1, m, d);
                self.date_changed();
            }
            "Y" => {
                let (y, m, d) = self.selected_date;
                self.selected_date = Self::safe_date(y - 1, m, d);
                self.date_changed();
            }
            "m" => {
                self.selected_date = add_months(self.selected_date, 1);
                self.date_changed();
            }
            "M" => {
                self.selected_date = add_months(self.selected_date, -1);
                self.date_changed();
            }
            "w" => {
                self.selected_date = add_days(self.selected_date, 7);
                self.date_changed();
            }
            "W" => {
                self.selected_date = add_days(self.selected_date, -7);
                self.date_changed();
            }
            "d" | "l" | "RIGHT" => {
                self.selected_date = add_days(self.selected_date, 1);
                self.date_changed();
            }
            "D" | "h" | "LEFT" => {
                self.selected_date = add_days(self.selected_date, -1);
                self.date_changed();
            }
            "DOWN" => self.move_slot_down(),
            "UP" => self.move_slot_up(),
            "PgDOWN" => self.page_slots_down(),
            "PgUP" => self.page_slots_up(),
            "HOME" => self.go_slot_top(),
            "END" => self.go_slot_bottom(),
            "j" => self.select_next_event_on_day(),
            "k" => self.select_prev_event_on_day(),
            "e" => self.jump_to_next_event(),
            "E" => self.jump_to_prev_event(),
            "t" => {
                self.selected_date = today();
                self.selected_event_index = 0;
                self.selected_slot = now_slot();
                self.slot_offset = (self.selected_slot - 5).max(0);
                self.date_changed();
            }
            "g" => self.go_to_date(),
            "n" => self.create_event(),
            "ENTER" => self.edit_event(),
            "x" | "DEL" => self.delete_event(),
            "C-Y" => self.copy_event_to_clipboard(),
            "v" => self.view_event_popup(),
            "a" => self.accept_invite(),
            "A" => self.decline_invite(),
            "T" => self.tentative_invite(),
            "F" => self.show_free_busy(),
            "J" => self.join_meeting(),
            "r" => self.reply_via_kastrup(),
            "i" => self.import_ics_file(),
            "G" => self.setup_google_calendar(),
            "O" => self.setup_outlook_calendar(),
            "S" => self.manual_sync(),
            "C" => self.show_calendars(),
            "C-R" => {
                self.cached_planets_date = None;
                self.weather_forecast.clear();
                self.weather_fetched_at = 0;
                self.load_events_for_range();
                self.render_all();
            }
            "C-L" => {
                Crust::clear_screen();
                self.recreate_panes();
                self.render_all();
            }
            "P" => self.show_preferences(),
            "?" => self.show_help(),
            "q" => self.running = false,
            _ => {}
        }
    }

    // =====================================================================
    // Actions
    // =====================================================================

    fn go_to_date(&mut self) {
        self.blank_bottom("");
        let input = self.bottom_ask("Go to: ", "");
        if input.is_empty() { self.render_all(); return; }

        let input = input.trim().to_string();
        if let Some(parsed) = self.parse_go_to_input(&input) {
            self.selected_date = parsed;
            self.selected_event_index = 0;
            self.date_changed();
        } else {
            self.show_feedback(&format!("Could not parse date: {}", input), 196);
        }
    }

    fn parse_go_to_input(&self, input: &str) -> Option<(i32, u32, u32)> {
        let lower = input.to_lowercase();
        if lower == "today" { return Some(today()); }

        // yyyy-mm-dd
        if input.len() >= 8 && input.contains('-') {
            let parts: Vec<&str> = input.split('-').collect();
            if parts.len() == 3 {
                let y: i32 = parts[0].parse().ok()?;
                let m: u32 = parts[1].parse().ok()?;
                let d: u32 = parts[2].parse().ok()?;
                if m >= 1 && m <= 12 && d >= 1 && d <= days_in_month(y, m) {
                    return Some((y, m, d));
                }
            }
        }

        // Year only
        if input.len() == 4 {
            if let Ok(y) = input.parse::<i32>() {
                return Some((y, 1, 1));
            }
        }

        // Month name
        let months = ["jan", "feb", "mar", "apr", "may", "jun",
                       "jul", "aug", "sep", "oct", "nov", "dec"];
        for (i, m) in months.iter().enumerate() {
            if lower.starts_with(m) {
                let (sy, _, _) = self.selected_date;
                return Some((sy, (i + 1) as u32, 1));
            }
        }

        // Day number
        if let Ok(d) = input.parse::<u32>() {
            if d >= 1 && d <= 31 {
                let (sy, sm, _) = self.selected_date;
                let max_d = days_in_month(sy, sm);
                return Some((sy, sm, d.min(max_d)));
            }
        }

        None
    }

    /// If `cal` is a remote-source calendar (google / outlook), push the
    /// just-saved event up. For new events (no external_id yet), the
    /// remote create returns an id which we persist back. Returns true
    /// when a remote push happened (success or attempted), false when
    /// the calendar is local-only.
    fn push_event_remote(&mut self, cal: &crate::database::Calendar,
                          local_id: i64, mut data: EventData) -> bool {
        match cal.source_type.as_str() {
            "google" => {
                let cfg_str = match &cal.source_config { Some(s) => s.clone(), None => return true };
                let cfg: serde_json::Value = match serde_json::from_str(&cfg_str) {
                    Ok(v) => v, Err(_) => return true,
                };
                let email = cfg.get("email").and_then(|v| v.as_str()).unwrap_or("");
                let safe_dir = cfg.get("safe_dir").and_then(|v| v.as_str());
                let google_cal_id = cfg.get("google_calendar_id").and_then(|v| v.as_str()).unwrap_or("");
                if email.is_empty() || google_cal_id.is_empty() {
                    self.show_feedback("Google config incomplete (email/google_calendar_id)",
                        214);
                    return true;
                }
                let mut gc = crate::sources::google::GoogleCalendar::new(email, safe_dir);
                if gc.get_access_token().is_none() {
                    self.show_feedback("Google auth failed - run S to re-auth", 196);
                    return true;
                }
                if let Some(ref existing_id) = data.external_id.clone() {
                    // UPDATE existing remote event
                    gc.update_event(google_cal_id, existing_id, &data);
                } else {
                    // CREATE new remote event; persist returned id back locally.
                    if let Some(remote_id) = gc.create_event(google_cal_id, &data) {
                        data.id = Some(local_id);
                        data.external_id = Some(remote_id);
                        let _ = self.db.save_event(&data);
                    } else {
                        self.show_feedback(
                            &format!("Saved locally, Google push failed: {}",
                                gc.last_error.as_deref().unwrap_or("unknown")),
                            196);
                    }
                }
                true
            }
            "outlook" => {
                // Outlook write-back not yet wired; warn user.
                self.show_feedback("Saved locally; Outlook write-back not implemented",
                    214);
                true
            }
            _ => false, // local calendar - no remote action
        }
    }

    /// Mirror of push_event_remote for delete: if the calendar is remote
    /// and we have an external id, delete it on the remote side.
    fn delete_event_remote(&mut self, cal: &crate::database::Calendar,
                            external_id: &str) {
        match cal.source_type.as_str() {
            "google" => {
                let cfg_str = match &cal.source_config { Some(s) => s.clone(), None => return };
                let cfg: serde_json::Value = match serde_json::from_str(&cfg_str) {
                    Ok(v) => v, Err(_) => return,
                };
                let email = cfg.get("email").and_then(|v| v.as_str()).unwrap_or("");
                let safe_dir = cfg.get("safe_dir").and_then(|v| v.as_str());
                let google_cal_id = cfg.get("google_calendar_id").and_then(|v| v.as_str()).unwrap_or("");
                if email.is_empty() || google_cal_id.is_empty() { return; }
                let mut gc = crate::sources::google::GoogleCalendar::new(email, safe_dir);
                if gc.get_access_token().is_none() { return; }
                let _ = gc.delete_event(google_cal_id, external_id);
            }
            _ => {}
        }
    }

    fn create_event(&mut self) {
        let (sy, sm, sd) = self.selected_date;
        let default_time = if self.selected_slot >= 0 {
            format!("{:02}:{:02}", self.selected_slot / 2, (self.selected_slot % 2) * 30)
        } else {
            "09:00".to_string()
        };

        let calendars = self.db.get_calendars(false).unwrap_or_default();
        let default_cal_id = self.config.get_i64("default_calendar", 1);
        let cal = calendars.iter().find(|c| c.id == default_cal_id)
            .or(calendars.first());
        let cal = match cal {
            Some(c) => c.clone(),
            None => { self.show_feedback("No calendars configured", 196); return; }
        };
        let mut cal_id = cal.id;
        let mut cal_color = cal.color as u8;

        // Calendar picker
        if calendars.len() > 1 {
            let cal_list: String = calendars.iter().enumerate()
                .map(|(i, c)| format!("{}:{}", i + 1, c.name))
                .collect::<Vec<_>>().join("  ");
            let default_idx = calendars.iter().position(|c| c.id == cal_id).unwrap_or(0);
            self.blank_bottom(&style::bold(&style::fg(" New Event", cal_color)));
            let pick = self.bottom_ask(&format!(" Calendar ({}): ", cal_list),
                &format!("{}", default_idx + 1));
            if pick.is_empty() { self.render_all(); return; }
            if let Ok(idx) = pick.trim().parse::<usize>() {
                if idx >= 1 && idx <= calendars.len() {
                    cal_id = calendars[idx - 1].id;
                    cal_color = calendars[idx - 1].color as u8;
                }
            }
        }

        self.blank_bottom(&style::bold(&style::fg(
            &format!(" New Event on {}", format_date_long(sy, sm, sd)), cal_color)));
        let title = self.bottom_ask(" Title: ", "");
        if title.trim().is_empty() { self.render_all(); return; }
        let title = title.trim().to_string();

        self.blank_bottom(&style::bold(&style::fg(&format!(" {}", title), cal_color)));
        let time_str = self.bottom_ask(" Start time (HH:MM or 'all day'): ", &default_time);
        if time_str.is_empty() { self.render_all(); return; }

        let all_day = time_str.trim().to_lowercase() == "all day";
        let tz = local_tz_offset_secs();

        let (start_ts, end_ts) = if all_day {
            // Multi-day all-day: ask how many whole days it spans.
            self.blank_bottom(&style::bold(&style::fg(
                &format!(" {} (all day)", title), cal_color)));
            let days_str = self.bottom_ask(" Number of days: ", "1");
            if days_str.is_empty() { self.render_all(); return; }
            let days: i64 = days_str.trim().parse().unwrap_or(1).max(1);
            all_day_range(sy, sm, sd, days)
        } else {
            let parts: Vec<&str> = time_str.trim().split(':').collect();
            let hour: u32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(9);
            let minute: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            let s = date_to_ts(sy, sm, sd, hour, minute, 0) - tz;

            self.blank_bottom(&style::bold(&style::fg(
                &format!(" {} at {}", title, time_str.trim()), cal_color)));
            let dur_str = self.bottom_ask(" Duration in minutes: ", "60");
            if dur_str.is_empty() { self.render_all(); return; }
            let duration: i64 = dur_str.trim().parse().unwrap_or(60).max(1);
            (s, s + duration * 60)
        };

        // Location
        self.blank_bottom(&style::bold(&style::fg(&format!(" {}", title), cal_color)));
        let location = self.bottom_ask(" Location (Enter to skip): ", "");
        let location = if location.trim().is_empty() { None } else { Some(location.trim().to_string()) };

        // Invitees
        self.blank_bottom(&style::bold(&style::fg(&format!(" {}", title), cal_color)));
        let invitees_str = self.bottom_ask(" Invite (comma-separated emails, Enter to skip): ", "");
        let attendees = if invitees_str.trim().is_empty() {
            None
        } else {
            let arr: Vec<serde_json::Value> = invitees_str.split(',')
                .map(|e| serde_json::json!({"email": e.trim()}))
                .collect();
            Some(serde_json::Value::Array(arr))
        };

        // Description
        self.blank_bottom(&style::bold(&style::fg(&format!(" {}", title), cal_color)));
        let desc_str = self.bottom_ask(" Description (Enter to skip): ", "");
        let description = if desc_str.trim().is_empty() {
            None
        } else {
            Some(desc_str.trim().to_string())
        };

        let data = EventData {
            id: None,
            calendar_id: cal_id,
            external_id: None,
            title: title.clone(),
            description,
            location,
            start_time: start_ts,
            end_time: end_ts,
            all_day,
            timezone: None,
            recurrence_rule: None,
            series_master_id: None,
            status: "confirmed".to_string(),
            organizer: None,
            attendees,
            my_status: None,
            alarms: None,
            metadata: None,
        };

        let local_id = match self.db.save_event(&data) {
            Ok(id) => id,
            Err(e) => {
                self.show_feedback(&format!("Save failed: {}", e), 196);
                return;
            }
        };

        // Push to remote source if applicable (Google / Outlook).
        let pushed = self.push_event_remote(&cal, local_id, data);

        self.load_events_for_range();
        self.render_all();
        let suffix = if pushed { " (synced)" } else { "" };
        self.show_feedback(&format!("Event created: {}{}", title, suffix), cal_color);
    }

    fn edit_event(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };
        let tz = local_tz_offset_secs();

        // Current date/time of the event for prefills: UTC date for
        // all-day (matches storage), local wall-clock for timed.
        let (cy, cm, cd, csh, csmn, _) = if evt.all_day {
            ts_to_parts(evt.start_time)
        } else {
            ts_to_parts(evt.start_time + tz)
        };
        let span_secs = (evt.end_time - evt.start_time).max(0);

        // Title
        self.blank_bottom(&style::bold(" Edit Event"));
        let new_title = self.bottom_ask(" Title: ", &evt.title);
        if new_title.trim().is_empty() { self.render_all(); return; }
        let new_title = new_title.trim().to_string();

        // Date
        self.blank_bottom(&style::bold(&format!(" {} — date", new_title)));
        let date_def = format!("{:04}-{:02}-{:02}", cy, cm, cd);
        let date_in = self.bottom_ask(" Date (YYYY-MM-DD): ", &date_def);
        if date_in.is_empty() { self.render_all(); return; }
        let (dy, dm, dd) = parse_ymd(&date_in).unwrap_or((cy, cm, cd));

        // Time / all-day
        let time_def = if evt.all_day { "all day".to_string() }
            else { format!("{:02}:{:02}", csh, csmn) };
        self.blank_bottom(&style::bold(&format!(" {} — time", new_title)));
        let time_str = self.bottom_ask(" Start time (HH:MM or 'all day'): ", &time_def);
        if time_str.is_empty() { self.render_all(); return; }
        let all_day = time_str.trim().to_lowercase() == "all day";

        let (start_ts, end_ts) = if all_day {
            let cur_days = (span_secs / 86400).max(1);
            self.blank_bottom(&style::bold(&format!(" {} (all day)", new_title)));
            let days_str = self.bottom_ask(" Number of days: ", &cur_days.to_string());
            let days: i64 = days_str.trim().parse().unwrap_or(cur_days).max(1);
            all_day_range(dy, dm, dd, days)
        } else {
            let parts: Vec<&str> = time_str.trim().split(':').collect();
            let hour: u32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(9);
            let minute: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            let s = date_to_ts(dy, dm, dd, hour, minute, 0) - tz;
            let cur_dur = if evt.all_day || span_secs == 0 { 60 } else { (span_secs / 60).max(1) };
            self.blank_bottom(&style::bold(&format!(" {} at {}", new_title, time_str.trim())));
            let dur_str = self.bottom_ask(" Duration in minutes: ", &cur_dur.to_string());
            let duration: i64 = dur_str.trim().parse().unwrap_or(cur_dur).max(1);
            (s, s + duration * 60)
        };

        // Location
        let loc_def = evt.location.clone().unwrap_or_default();
        self.blank_bottom(&style::bold(&format!(" {} — location", new_title)));
        let loc_in = self.bottom_ask(" Location (Enter to skip): ", &loc_def);
        let location = if loc_in.trim().is_empty() { None } else { Some(loc_in.trim().to_string()) };

        // Invitees (prefill from current attendees' emails)
        let inv_def = evt.attendees.as_ref()
            .and_then(|a| a.as_array())
            .map(|arr| arr.iter()
                .filter_map(|e| e.get("email").and_then(|v| v.as_str()))
                .collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        self.blank_bottom(&style::bold(&format!(" {} — invitees", new_title)));
        let inv_in = self.bottom_ask(" Invite (comma emails, Enter to skip): ", &inv_def);
        let attendees = if inv_in.trim().is_empty() { None } else {
            let arr: Vec<serde_json::Value> = inv_in.split(',')
                .map(|e| serde_json::json!({"email": e.trim()}))
                .collect();
            Some(serde_json::Value::Array(arr))
        };

        // Description
        let desc_def = evt.description.clone().unwrap_or_default();
        self.blank_bottom(&style::bold(&format!(" {} — description", new_title)));
        let desc_in = self.bottom_ask(" Description (Enter to skip): ", &desc_def);
        let description = if desc_in.trim().is_empty() { None } else { Some(desc_in.trim().to_string()) };

        let data = EventData {
            id: Some(evt.id),
            calendar_id: evt.calendar_id,
            external_id: evt.external_id.clone(),
            title: new_title,
            description,
            location,
            start_time: start_ts,
            end_time: end_ts,
            all_day,
            timezone: evt.timezone.clone(),
            recurrence_rule: evt.recurrence_rule.clone(),
            series_master_id: evt.series_master_id,
            status: evt.status.clone(),
            organizer: evt.organizer.clone(),
            attendees,
            my_status: evt.my_status.clone(),
            alarms: evt.alarms.clone(),
            metadata: evt.metadata.clone(),
        };

        let local_id = match self.db.save_event(&data) {
            Ok(id) => id,
            Err(e) => {
                self.show_feedback(&format!("Save failed: {}", e), 196);
                return;
            }
        };

        // Push update to remote if the calendar is a remote source.
        let cal_opt = self.db.get_calendars(false).ok()
            .and_then(|cs| cs.into_iter().find(|c| c.id == evt.calendar_id));
        let pushed = if let Some(cal) = cal_opt {
            self.push_event_remote(&cal, local_id, data)
        } else { false };

        self.load_events_for_range();
        self.render_all();
        let suffix = if pushed { " (synced)" } else { "" };
        self.show_feedback(&format!("Event updated{}", suffix), 156);
    }

    fn delete_event(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        // Is this part of a recurring series? Either it's the master
        // (recurrence_rule set) or an expanded occurrence (series_master_id set).
        let is_series = evt.recurrence_rule.as_ref().map(|r| !r.is_empty()).unwrap_or(false)
            || evt.series_master_id.is_some();

        self.blank_bottom(&style::bold(" Delete Event"));
        if is_series {
            let choice = self.bottom_ask(
                &format!(" Delete '{}': (o)ccurrence / (s)eries / (n)o: ", evt.title),
                "",
            );
            match choice.trim().to_lowercase().as_str() {
                "o" | "occurrence" => {
                    let _ = self.db.delete_event(evt.id);
                    self.load_events_for_range();
                    self.render_all();
                    self.show_feedback("Occurrence deleted", 156);
                }
                "s" | "series" => {
                    match self.db.delete_event_series(evt.id) {
                        Ok(n) => {
                            self.load_events_for_range();
                            self.render_all();
                            self.show_feedback(&format!("Series deleted ({} row(s))", n), 156);
                        }
                        Err(e) => {
                            self.render_all();
                            self.show_feedback(&format!("Delete failed: {}", e), 196);
                        }
                    }
                }
                _ => { self.render_all(); self.show_feedback("Cancelled", 245); }
            }
            return;
        }

        let confirm = self.bottom_ask(&format!(" Delete '{}'? (y/n): ", evt.title), "");
        if confirm.trim().to_lowercase() != "y" { self.render_all(); return; }

        // Push delete to remote first (before we lose the external_id).
        let cal_opt = self.db.get_calendars(false).ok()
            .and_then(|cs| cs.into_iter().find(|c| c.id == evt.calendar_id));
        if let (Some(cal), Some(ref ext)) = (cal_opt, evt.external_id.as_ref()) {
            self.delete_event_remote(&cal, ext);
        }

        let _ = self.db.delete_event(evt.id);
        self.load_events_for_range();
        self.render_all();
        self.show_feedback("Event deleted", 156);
    }

    fn copy_event_to_clipboard(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        let (sy, sm, sd) = self.selected_date;
        let tz = local_tz_offset_secs();
        let mut text_lines: Vec<String> = Vec::new();
        text_lines.push(evt.title.clone());

        if evt.all_day {
            text_lines.push(format!("{}  All day", format_date_long(sy, sm, sd)));
        } else {
            let local_s = evt.start_time + tz;
            let (_, _, _, sh, smn, _) = ts_to_parts(local_s);
            let local_e = evt.end_time + tz;
            let (_, _, _, eh, emn, _) = ts_to_parts(local_e);
            text_lines.push(format!("{}  {:02}:{:02} - {:02}:{:02}",
                format_date_long(sy, sm, sd), sh, smn, eh, emn));
        }

        if let Some(ref loc) = evt.location {
            if !loc.trim().is_empty() { text_lines.push(format!("Location: {}", loc.trim())); }
        }
        if let Some(ref org) = evt.organizer {
            if !org.trim().is_empty() { text_lines.push(format!("Organizer: {}", org.trim())); }
        }
        text_lines.push(format!("Calendar: {}", evt.calendar_name));
        if let Some(ref ms) = evt.my_status {
            text_lines.push(format!("My status: {}", humanize_status(ms)));
        }
        if let Some(ref desc) = evt.description {
            let desc = clean_description(desc);
            if !desc.is_empty() {
                text_lines.push(String::new());
                text_lines.push(desc);
            }
        }

        let text = text_lines.join("\n");
        crust::clipboard_copy(&text, "clipboard");
        crust::clipboard_copy(&text, "primary");
        self.show_feedback("Event copied to clipboard", 156);
    }

    fn view_event_popup(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        let pw = (self.cols.saturating_sub(10) as usize).min(80).max(50) as u16;
        let ph = (self.rows.saturating_sub(6) as usize).min(30) as u16;
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;

        let mut popup = Pane::new(px, py, pw, ph, 252, 0);
        popup.border = true;
        popup.scroll = true;

        let (sy, sm, sd) = self.selected_date;
        let color = evt.calendar_color as u8;
        let tz = local_tz_offset_secs();
        let mut lines: Vec<String> = Vec::new();

        lines.push(String::new());
        let title = if evt.title.is_empty() { "(No title)" } else { &evt.title };
        lines.push(format!("  {}", style::bold(&style::fg(title, color))));
        lines.push(String::new());

        let when_label = style::fg("When:", 51);
        if evt.all_day {
            lines.push(format!("  {}  {}  All day",
                when_label, format_date_long(sy, sm, sd)));
        } else {
            let local_s = evt.start_time + tz;
            let (_, _, _, sh, smn, _) = ts_to_parts(local_s);
            let local_e = evt.end_time + tz;
            let (_, _, _, eh, emn, _) = ts_to_parts(local_e);
            lines.push(format!("  {}      {}  {:02}:{:02} - {:02}:{:02}",
                when_label, format_date_long(sy, sm, sd), sh, smn, eh, emn));
        }

        if let Some(ref loc) = evt.location {
            if !loc.trim().is_empty() {
                lines.push(format!("  {}  {}", style::fg("Location:", 51), loc.trim()));
            }
        }
        if let Some(ref org) = evt.organizer {
            if !org.trim().is_empty() {
                lines.push(format!("  {} {}", style::fg("Organizer:", 51), org.trim()));
            }
        }
        lines.push(format!("  {}  {}", style::fg("Calendar:", 51), evt.calendar_name));

        let mut status_parts: Vec<String> = Vec::new();
        if !evt.status.is_empty() { status_parts.push(format!("Status: {}", evt.status)); }
        if let Some(ref ms) = evt.my_status {
            status_parts.push(format!("My status: {}", humanize_status(ms)));
        }
        if !status_parts.is_empty() {
            lines.push(style::fg(&format!("  {}", status_parts.join("  |  ")), 245));
        }

        // Attendees
        if let Some(ref att) = evt.attendees {
            if let Some(arr) = att.as_array() {
                if !arr.is_empty() {
                    lines.push(String::new());
                    lines.push(format!("  {}", style::fg("Attendees:", 51)));
                    for a in arr {
                        // attendee_name_status normalises Google/Outlook/manual
                        // shapes; the RSVP marker (✓/✗/?/·) leads each name.
                        let Some((name, status)) = attendee_name_status(a) else { continue };
                        let (marker, mcol) = rsvp_short(&status);
                        lines.push(format!("    {} {}", style::fg(marker, mcol), style::fg(&name, 252)));
                    }
                }
            }
        }

        // Join link (extracted from the description / location)
        let join = evt.description.as_deref().and_then(extract_meeting_link)
            .or_else(|| evt.location.as_deref().and_then(extract_meeting_link));
        if let Some(ref url) = join {
            lines.push(String::new());
            lines.push(format!("  {} {}", style::fg("Join:", 40), style::fg(url, 39)));
        }

        // Description (meeting boilerplate stripped — join link is shown above)
        if let Some(ref desc) = evt.description {
            let desc = clean_meeting_desc(desc);
            if !desc.is_empty() {
                lines.push(String::new());
                let sep_w = (pw as usize).saturating_sub(6).max(1);
                lines.push(format!("  {}", style::fg(&"-".repeat(sep_w), 238)));
                // Push each logical line; crust's pane word-wraps long
                // lines on refresh (wrap/word_wrap default true). The old
                // manual byte-slice wrap panicked on UTF-8 boundaries
                // (Nordic å/ø/æ in Dualog descriptions).
                for dline in desc.split('\n') {
                    lines.push(style::fg(&format!("  {}", dline), 248));
                }
            }
        }

        lines.push(String::new());
        lines.push(format!("  {}", style::fg("UP/DOWN:scroll  C-Y:copy  ESC/q:close", 245)));

        popup.set_text(&lines.join("\n"));
        popup.full_refresh(); // full_refresh draws the border; refresh() alone doesn't

        loop {
            let k = Input::getchr(None);
            match k.as_deref() {
                Some("ESC") | Some("q") | Some("v") => break,
                Some("DOWN") | Some("j") => popup.linedown(),
                Some("UP") | Some("k") => popup.lineup(),
                Some("PgDOWN") => popup.pagedown(),
                Some("PgUP") => popup.pageup(),
                Some("C-Y") => {
                    let clean: Vec<String> = lines.iter()
                        .map(|l| strip_ansi(l)).collect();
                    let text = clean.join("\n");
                    crust::clipboard_copy(&text, "clipboard");
                    crust::clipboard_copy(&text, "primary");
                    if let Some(last) = lines.last_mut() {
                        *last = format!("  {}", style::fg("Copied to clipboard", 156));
                    }
                    popup.set_text(&lines.join("\n"));
                    popup.refresh();
                }
                _ => {}
            }
        }

        Crust::clear_screen();
        self.recreate_panes();
        self.render_all();
    }

    #[allow(dead_code)]
    fn accept_invite(&mut self)    { self.rsvp_invite("accept"); }
    fn decline_invite(&mut self)   { self.rsvp_invite("decline"); }
    fn tentative_invite(&mut self) { self.rsvp_invite("tentative"); }

    /// Update the local RSVP state AND notify the organizer via the
    /// calendar backend when possible. Currently propagates for Outlook
    /// (Microsoft Graph `/me/events/{id}/{accept|decline|tentativelyAccept}`);
    /// Google and local calendars only update the local my_status for now.
    fn rsvp_invite(&mut self, response: &str) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        let (verb_ing, my_status_value) = match response {
            "accept"    => ("Accepting",  "accepted"),
            "decline"   => ("Declining",  "declined"),
            "tentative" => ("Tentatively accepting", "tentativelyAccepted"),
            _ => { self.show_feedback(&format!("Unknown RSVP: {}", response), 196); return; }
        };
        self.show_feedback(&format!("{} '{}'...", verb_ing, evt.title), 226);

        // Resolve the calendar so we know which backend to talk to.
        let cal_source_type = self.db.get_calendars(false).ok()
            .and_then(|cals| cals.into_iter().find(|c| c.id == evt.calendar_id))
            .map(|c| (c.source_type, c.source_config));

        let mut graph_ok = None::<bool>;
        if let Some((ref stype, Some(ref cfg_str))) = cal_source_type {
            if stype == "outlook" {
                if let Ok(config) = serde_json::from_str::<serde_json::Value>(cfg_str) {
                    let mut oc = crate::sources::outlook::OutlookCalendar::new(&config);
                    if oc.refresh_access_token().is_some() {
                        if let Some(ref ext) = evt.external_id {
                            graph_ok = Some(oc.respond_to_event(ext, response));
                        } else {
                            graph_ok = Some(false);
                        }
                    } else {
                        graph_ok = Some(false);
                    }
                }
            } else if stype == "google" {
                // Google Calendar RSVP: PATCH the event with the user's
                // updated responseStatus and `sendUpdates=all` so the
                // organizer's mail client sees the reply. The user's
                // identity for this calendar lives in source_config.email;
                // the actual calendar id (usually identical) is in
                // google_calendar_id.
                if let Ok(config) = serde_json::from_str::<serde_json::Value>(cfg_str) {
                    let email = config.get("email").and_then(|v| v.as_str()).unwrap_or("");
                    let cal_id = config.get("google_calendar_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(email);
                    let safe_dir = config.get("safe_dir").and_then(|v| v.as_str());
                    if !email.is_empty() && !cal_id.is_empty() {
                        let mut gc = crate::sources::google::GoogleCalendar::new(email, safe_dir);
                        if gc.get_access_token().is_some() {
                            if let Some(ref ext) = evt.external_id {
                                graph_ok = Some(gc.respond_to_event(cal_id, ext, email, response));
                            } else {
                                graph_ok = Some(false);
                            }
                        } else {
                            graph_ok = Some(false);
                        }
                    }
                }
            }
        }

        let data = EventData {
            id: Some(evt.id),
            calendar_id: evt.calendar_id,
            external_id: evt.external_id.clone(),
            title: evt.title.clone(),
            description: evt.description.clone(),
            location: evt.location.clone(),
            start_time: evt.start_time,
            end_time: evt.end_time,
            all_day: evt.all_day,
            timezone: evt.timezone.clone(),
            recurrence_rule: evt.recurrence_rule.clone(),
            series_master_id: evt.series_master_id,
            status: evt.status.clone(),
            organizer: evt.organizer.clone(),
            attendees: evt.attendees.clone(),
            my_status: Some(my_status_value.to_string()),
            alarms: evt.alarms.clone(),
            metadata: evt.metadata.clone(),
        };
        let _ = self.db.save_event(&data);
        self.load_events_for_range();
        self.render_all();

        let msg = match graph_ok {
            Some(true)  => format!("{} (organizer notified)", humanize_status(my_status_value)),
            Some(false) => format!("{} locally — RSVP propagation failed", humanize_status(my_status_value)),
            None        => format!("{} locally — no propagation for this calendar", humanize_status(my_status_value)),
        };
        let color = if matches!(graph_ok, Some(false)) { 196 } else { 156 };
        self.show_feedback(&msg, color);
    }

    /// Free/busy overlay: prompt for comma-separated emails, call Microsoft
    /// Graph getSchedule against the first configured Outlook calendar, and
    /// render a compact grid in the bottom pane. One row per person, one
    /// column per 30-min slot from 08:00 to 18:00 on the currently selected
    /// day (so 20 cells per person). Cell legend: · free, ~ tentative,
    /// █ busy, ◎ OOF, e elsewhere.
    fn show_free_busy(&mut self) {
        let emails_raw = self.bottom_ask(" Emails (comma-separated): ", "");
        let emails_raw = emails_raw.trim().to_string();
        if emails_raw.is_empty() {
            self.render_all();
            return;
        }
        let emails: Vec<String> = emails_raw
            .split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        if emails.is_empty() {
            self.show_feedback("No emails provided", 245);
            return;
        }

        // First Outlook calendar (getSchedule is a per-user API; any auth works).
        let oc_config = match self.db.get_calendars(false).ok().and_then(|cals| {
            cals.into_iter()
                .find(|c| c.source_type == "outlook")
                .and_then(|c| c.source_config)
        }) {
            Some(cfg) => cfg,
            None => { self.show_feedback("No Outlook calendar configured (need it for getSchedule)", 196); return; }
        };
        let config: serde_json::Value = match serde_json::from_str(&oc_config) {
            Ok(v) => v,
            Err(e) => { self.show_feedback(&format!("Bad Outlook config: {}", e), 196); return; }
        };
        let mut oc = crate::sources::outlook::OutlookCalendar::new(&config);
        if oc.refresh_access_token().is_none() {
            self.show_feedback(&format!("Graph auth failed: {}",
                oc.last_error.as_deref().unwrap_or("unknown")), 196);
            return;
        }

        // Window: 08:00 to 18:00 local on the selected day, 30-min slots.
        let (sy, sm, sd) = self.selected_date;
        let start = format!("{:04}-{:02}-{:02}T08:00:00", sy, sm, sd);
        let end   = format!("{:04}-{:02}-{:02}T18:00:00", sy, sm, sd);
        let tz = std::env::var("TZ").unwrap_or_else(|_| "Europe/Oslo".to_string());

        self.show_feedback(&format!("Fetching schedule for {} people...", emails.len()), 226);
        let schedules = match oc.get_schedule(&emails, &start, &end, &tz, 30) {
            Some(s) => s,
            None => { self.show_feedback(&format!("getSchedule failed: {}",
                oc.last_error.as_deref().unwrap_or("no response")), 196); return; }
        };

        // Render grid. 20 half-hour slots from 08:00 to 18:00.
        // Columns: time header (08:00 marker every 2 slots).
        let mut lines: Vec<String> = Vec::new();
        lines.push(style::bold(&format!(
            " Availability for {:04}-{:02}-{:02}  08:00-18:00  (30-min slots)",
            sy, sm, sd)));
        lines.push(String::new());
        // Hour-tick header.
        let mut header = String::from(format!(" {:>28}  ", ""));
        for slot in 0..20 {
            if slot % 2 == 0 {
                let hour = 8 + (slot / 2);
                header.push_str(&format!("{:02}", hour));
            } else {
                header.push_str("  ");
            }
        }
        lines.push(style::fg(&header, 240));

        for entry in &schedules {
            let mut row = format!(" {:>28}  ", truncate_str(&entry.email, 28));
            for ch in entry.availability_view.chars() {
                let glyph = match ch {
                    '0' => style::fg("·", 108),  // free (muted green)
                    '1' => style::fg("~", 220),  // tentative
                    '2' => style::fg("\u{2588}", 167), // busy (red block)
                    '3' => style::fg("\u{25CE}", 208), // OOF
                    '4' => style::fg("e", 117),        // working elsewhere
                    _   => "?".to_string(),
                };
                // Display a single column per slot + padding inside header.
                row.push_str(&glyph);
                row.push(' ');
            }
            lines.push(row);
        }
        lines.push(String::new());
        lines.push(style::fg(
            "  ·=free  ~=tentative  █=busy  ◎=OOF  e=elsewhere    (F again to refresh, any key to dismiss)",
            240));

        self.bottom.say(&lines.join("\n"));
        let _ = crust::Input::getchr(None);
        self.render_all();
    }

    /// `J` (Join) — open the selected event's meeting URL.
    ///
    /// Launcher selection is config-driven via `meeting_handlers:` in
    /// `~/.tock/config.yml`. Each entry maps a host suffix to a
    /// command, e.g.
    ///
    /// ```yaml
    /// meeting_handlers:
    ///   teams.microsoft.com: teams-for-linux
    ///   zoom.us: ""             # empty = force xdg-open
    /// ```
    ///
    /// Host matching is suffix-based (so `us02web.zoom.us` matches a
    /// `zoom.us` entry). Empty values, missing entries, and missing
    /// binaries all fall back to `xdg-open` — which hands the URL to
    /// the user's default browser. Detached spawn so tock stays in
    /// the foreground.
    fn join_meeting(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };
        let haystack = {
            let desc = evt.description.as_deref().unwrap_or("");
            let loc  = evt.location.as_deref().unwrap_or("");
            format!("{}\n{}", desc, loc)
        };
        let Some(url) = extract_meeting_url(&haystack) else {
            self.show_feedback(
                "No meeting URL found in description or location", 245);
            return;
        };

        // Walk meeting_handlers from longest suffix to shortest so
        // `teams.microsoft.com` wins over a hypothetical bare
        // `microsoft.com` entry.
        let host = url_host(&url).unwrap_or_default();
        let handlers = self.config.get(
            "meeting_handlers",
            serde_yaml::Value::Mapping(Default::default()),
        );
        let mut configured: Option<String> = None;
        if let serde_yaml::Value::Mapping(m) = handlers {
            let mut entries: Vec<(String, String)> = m.into_iter()
                .filter_map(|(k, v)| {
                    let key = k.as_str()?.to_string();
                    let val = v.as_str().unwrap_or("").to_string();
                    Some((key, val))
                })
                .collect();
            entries.sort_by_key(|(k, _)| std::cmp::Reverse(k.len()));
            for (suffix, cmd) in entries {
                if host == suffix || host.ends_with(&format!(".{}", suffix)) {
                    if !cmd.trim().is_empty() {
                        configured = Some(cmd);
                    }
                    break;
                }
            }
        }

        // Resolve the launcher. If the config says use `foo` but `foo`
        // isn't on PATH, drop down to xdg-open with a status note so
        // the user knows what happened.
        let (launcher, fallback) = match configured {
            Some(cmd) if which_on_path(&cmd) => (cmd, false),
            Some(cmd) => {
                self.show_feedback(
                    &format!("{} not in PATH, opening in browser…", cmd), 245);
                ("xdg-open".to_string(), true)
            }
            None => ("xdg-open".to_string(), false),
        };

        // GUI launchers like teams-for-linux (Electron) trust
        // XDG_SESSION_TYPE when picking a display backend. Some
        // display managers (gdm/sddm on hybrid systems) export
        // `wayland` even on X11 sessions where WAYLAND_DISPLAY is
        // unset; the launcher then tries Wayland, fails to connect,
        // and exits silently — leaving a zombie child behind.
        // Force the child's session type back to x11 in that case.
        let mut cmd = std::process::Command::new(&launcher);
        cmd.arg(&url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if std::env::var_os("WAYLAND_DISPLAY")
            .map(|v| v.is_empty()).unwrap_or(true)
        {
            cmd.env("XDG_SESSION_TYPE", "x11");
            cmd.env("ELECTRON_OZONE_PLATFORM_HINT", "x11");
        }
        let spawned = cmd.spawn();
        match spawned {
            Ok(_) if !fallback => self.show_feedback(
                &format!("Joining via {}…", launcher), 156),
            Ok(_) => {}  // already showed the fallback note above
            Err(e) => self.show_feedback(
                &format!("Couldn't launch {}: {}", launcher, e), 196),
        }
    }

    fn reply_via_kastrup(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };
        let organizer = evt.organizer.as_deref().unwrap_or("");
        if organizer.is_empty() {
            self.show_feedback("No organizer to reply to", 245);
            return;
        }
        // Launch kastrup with compose-to argument
        Crust::cleanup();
        let _ = std::process::Command::new("kastrup")
            .arg("--compose-to")
            .arg(organizer)
            .arg("--subject")
            .arg(&format!("Re: {}", evt.title))
            .status();
        Crust::init();
        Crust::set_app_identity("Tock");
        Crust::clear_screen();
        self.recreate_panes();
        self.render_all();
    }

    fn import_ics_file(&mut self) {
        self.blank_bottom(&style::bold(" Import ICS File"));
        let path = self.bottom_ask(" File path: ", "");
        if path.trim().is_empty() { self.render_all(); return; }

        let expanded = shellexpand(&path.trim());
        let p = Path::new(&expanded);
        if !p.exists() {
            self.show_feedback(&format!("File not found: {}", expanded), 196);
            return;
        }

        let cal_id = self.config.get_i64("default_calendar", 1);
        let result = ics::import_file(p, &self.db, cal_id);
        self.load_events_for_range();
        self.render_all();
        let mut msg = format!("Imported {} event(s)", result.imported);
        if result.skipped > 0 { msg.push_str(&format!(", skipped {}", result.skipped)); }
        let color = if result.error.is_some() { 196u8 } else { 156u8 };
        if let Some(ref err) = result.error { msg.push_str(&format!(" ({})", err)); }
        self.show_feedback(&msg, color);
    }

    fn setup_google_calendar(&mut self) {
        self.blank_bottom(&style::bold(&style::fg(" Google Calendar Setup", 39)));
        let email = self.bottom_ask(" Google email: ", "");
        if email.trim().is_empty() { self.render_all(); return; }
        let email = email.trim().to_string();

        let safe_dir = self.config.get_str("google.safe_dir", "~/.config/tock/credentials");
        self.show_feedback("Connecting to Google Calendar...", 226);

        let _google = sources::google::GoogleCalendar::new(&email, Some(&safe_dir));
        // Google calendar setup is complex; show instructions
        self.show_feedback("Google Calendar: see credentials setup documentation", 245);
    }

    /// Outlook device-code auth / re-auth. Microsoft Conditional-Access
    /// policies cap refresh-token lifetime (e.g. Dualog: 90 days), after
    /// which sync silently stops with AADSTS70043 — this is how the user
    /// renews it without leaving the TUI. Existing Outlook calendars are
    /// re-authenticated in place (their tokens updated); the client_id /
    /// tenant default to whatever those calendars already use.
    ///
    /// Note: `poll_for_token` blocks the UI until the user finishes the
    /// browser sign-in (or the device code expires). That's acceptable
    /// for a deliberate, infrequent action.
    fn setup_outlook_calendar(&mut self) {
        self.blank_bottom(&style::bold(&style::fg(" Outlook/365 Calendar Re-auth", 33)));

        // Existing Outlook calendars seed the client_id / tenant so a
        // re-auth reuses the same Azure app registration.
        let existing: Vec<crate::database::Calendar> = self.db.get_calendars(false)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.source_type == "outlook")
            .collect();
        let (def_cid, def_tenant) = existing.first()
            .and_then(|c| c.source_config.as_deref())
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .map(|cfg| (
                cfg.get("client_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                cfg.get("tenant_id").and_then(|v| v.as_str()).unwrap_or("common").to_string(),
            ))
            .unwrap_or_else(|| (
                self.config.get_str("outlook.client_id", ""),
                self.config.get_str("outlook.tenant_id", "common"),
            ));

        let client_id = self.bottom_ask(" Azure App client_id: ", &def_cid);
        if client_id.trim().is_empty() { self.render_all(); return; }
        let client_id = client_id.trim().to_string();
        let tenant_in = self.bottom_ask(
            &format!(" Tenant ID (Enter for '{}'): ", def_tenant), &def_tenant);
        let tenant_id = if tenant_in.trim().is_empty() { def_tenant.clone() } else { tenant_in.trim().to_string() };

        self.config.set("outlook.client_id", serde_yaml::Value::String(client_id.clone()));
        self.config.set("outlook.tenant_id", serde_yaml::Value::String(tenant_id.clone()));
        let _ = self.config.save();

        // Kick off the device-code flow.
        let auth_cfg = serde_json::json!({ "client_id": client_id, "tenant_id": tenant_id });
        let mut oc = sources::outlook::OutlookCalendar::new(&auth_cfg);
        let dev = match oc.start_device_auth() {
            Some(v) => v,
            None => {
                self.show_feedback(&format!("Device auth failed: {}",
                    oc.last_error.clone().unwrap_or_default()), 196);
                self.render_all();
                return;
            }
        };
        let user_code = dev.get("user_code").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let uri = dev.get("verification_uri").and_then(|v| v.as_str())
            .unwrap_or("https://microsoft.com/devicelogin").to_string();
        let device_code = dev.get("device_code").and_then(|v| v.as_str()).unwrap_or("").to_string();

        // Show the code + URL, then block on the poll. The bottom line
        // stays put while the UI is frozen during sign-in.
        self.blank_bottom(&style::bold(&style::fg(
            &format!(" Open {}  —  enter code  {}   (signing in… UI waits)", uri, user_code), 46)));

        let tok = match oc.poll_for_token(&device_code) {
            Some(t) => t,
            None => {
                self.show_feedback(&format!("Auth failed/expired: {}",
                    oc.last_error.clone().unwrap_or_default()), 196);
                self.render_all();
                return;
            }
        };

        if existing.is_empty() {
            self.show_feedback(
                "Authenticated, but no existing Outlook calendar to attach (provision one first).", 220);
            self.render_all();
            return;
        }

        // Re-auth in place: write the fresh tokens into every Outlook
        // calendar's source_config (keeping client_id / tenant / the
        // outlook_calendar_id already stored there).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64).unwrap_or(0);
        let mut updated = 0;
        for c in &existing {
            let mut cfg: serde_json::Value = c.source_config.as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            cfg["client_id"] = serde_json::json!(client_id);
            cfg["tenant_id"] = serde_json::json!(tenant_id);
            cfg["access_token"] = serde_json::json!(tok.access_token);
            if let Some(rt) = &tok.refresh_token {
                cfg["refresh_token"] = serde_json::json!(rt);
            }
            let s = serde_json::to_string(&cfg).unwrap_or_default();
            if self.db.update_calendar_sync(c.id, now, Some(&s)).is_ok() {
                updated += 1;
            }
        }
        self.show_feedback(
            &format!("Outlook re-authenticated ({} calendar(s)). Press S to sync.", updated), 46);
        self.render_all();
    }

    fn manual_sync(&mut self) {
        let google_cals: Vec<_> = self.db.get_calendars(true).unwrap_or_default()
            .into_iter().filter(|c| c.source_type == "google").collect();
        let outlook_cals: Vec<_> = self.db.get_calendars(true).unwrap_or_default()
            .into_iter().filter(|c| c.source_type == "outlook").collect();

        if google_cals.is_empty() && outlook_cals.is_empty() {
            self.show_feedback("No remote calendars configured. Press G (Google) or O (Outlook) to set up.", 245);
            return;
        }

        self.syncing = true;
        self.render_status_bar();
        self.show_feedback("Syncing...", 156);

        // Run sync directly on main thread for immediate feedback
        let now = crate::database::now_secs();
        let range_start = now - 90 * 86400;
        let range_end = now + 90 * 86400;
        let mut any_new = false;

        for cal in &google_cals {
            let cfg_str = match &cal.source_config { Some(s) => s.clone(), None => continue };
            let config: serde_json::Value = match serde_json::from_str(&cfg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let email = config.get("email").and_then(|v| v.as_str()).unwrap_or("");
            let safe_dir = config.get("safe_dir").and_then(|v| v.as_str());
            let gcid = config.get("google_calendar_id").and_then(|v| v.as_str()).unwrap_or("");
            let mut gc = crate::sources::google::GoogleCalendar::new(email, safe_dir);
            if gc.get_access_token().is_none() {
                let err = gc.last_error.as_deref().unwrap_or("auth failed");
                self.show_feedback(&format!("Google sync {}: {}", cal.name, err), 196);
                continue;
            }
            let tmin = crate::sources::google::ts_to_rfc3339_pub(range_start);
            let tmax = crate::sources::google::ts_to_rfc3339_pub(range_end);
            if let Some(events) = gc.fetch_events(gcid, &tmin, &tmax) {
                for mut ev in events {
                    ev.calendar_id = cal.id;
                    if let Ok(crate::database::SyncResult::New | crate::database::SyncResult::Updated) =
                        self.db.upsert_synced_event(cal.id, &ev)
                    {
                        any_new = true;
                    }
                }
                let _ = self.db.update_calendar_sync(cal.id, now, None);
            }
        }

        for cal in &outlook_cals {
            let cfg_str = match &cal.source_config { Some(s) => s.clone(), None => continue };
            let mut config: serde_json::Value = match serde_json::from_str(&cfg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let mut oc = crate::sources::outlook::OutlookCalendar::new(&config);
            if oc.refresh_access_token().is_none() {
                let err = oc.last_error.as_deref().unwrap_or("auth failed");
                self.show_feedback(&format!("Outlook sync {}: {}", cal.name, err), 196);
                continue;
            }
            let tmin = crate::sources::google::ts_to_rfc3339_pub(range_start);
            let tmax = crate::sources::google::ts_to_rfc3339_pub(range_end);
            if let Some(events) = oc.fetch_events(&tmin, &tmax) {
                for mut ev in events {
                    ev.calendar_id = cal.id;
                    if let Ok(crate::database::SyncResult::New | crate::database::SyncResult::Updated) =
                        self.db.upsert_synced_event(cal.id, &ev)
                    {
                        any_new = true;
                    }
                }
                let new_cfg = if let Some(rt) = oc.get_refresh_token() {
                    config["refresh_token"] = serde_json::json!(rt);
                    if let Some(at) = oc.get_access_token_cached() {
                        config["access_token"] = serde_json::json!(at);
                    }
                    Some(serde_json::to_string(&config).unwrap_or_default())
                } else { None };
                let _ = self.db.update_calendar_sync(cal.id, now, new_cfg.as_deref());
            }
        }

        self.syncing = false;
        if any_new {
            self.load_events_for_range();
            self.render_all();
            self.show_feedback("Sync complete, new events loaded", 156);
        } else {
            self.render_status_bar();
            self.show_feedback("Sync complete, no changes", 245);
        }
    }

    fn show_calendars(&mut self) {
        let mut calendars = self.db.get_calendars(false).unwrap_or_default();
        if calendars.is_empty() {
            self.show_feedback("No calendars configured", 245);
            return;
        }

        let pw = (self.cols.saturating_sub(16) as usize).min(64).max(50) as u16;
        let ph = (calendars.len() as u16 + 7).min(self.rows.saturating_sub(6));
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;

        let mut popup = Pane::new(px, py, pw, ph, 252, 0);
        popup.border = true;
        popup.scroll = false;

        let mut sel = 0usize;

        let build = |calendars: &[database::Calendar], sel: usize, popup: &mut Pane, pw: u16| {
            popup.full_refresh();
            let mut lines = Vec::new();
            lines.push(String::new());
            lines.push(format!("  {}", style::bold("Calendars")));
            let sep_w = (pw as usize).saturating_sub(6).max(1);
            lines.push(format!("  {}", style::fg(&"-".repeat(sep_w), 238)));

            for (i, cal) in calendars.iter().enumerate() {
                let color = cal.color as u8;
                let swatch = style::fg("\u{2588}\u{2588}", color);
                let status = if cal.enabled {
                    style::fg("on", 35)
                } else {
                    style::fg("off", 196)
                };
                let src = &cal.source_type;
                let name = &cal.name;
                let name_trunc = truncate_str(name, 22);
                let display = format!("  {} {:<22} {}  [{}]", swatch, name_trunc, status, src);
                if i == sel {
                    lines.push(style::bold(&style::fg(&display, 39)));
                } else {
                    lines.push(display);
                }
            }

            lines.push(String::new());
            lines.push(format!("  {}", style::fg("j/k:nav  c:color  ENTER:toggle  x:remove  q:close", 245)));
            popup.set_text(&lines.join("\n"));
            popup.ix = 0;
            popup.refresh();
        };

        build(&calendars, sel, &mut popup, pw);

        loop {
            let k = Input::getchr(None);
            match k.as_deref() {
                Some("ESC") | Some("q") => break,
                Some("k") | Some("UP") => {
                    sel = if sel == 0 { calendars.len() - 1 } else { sel - 1 };
                    build(&calendars, sel, &mut popup, pw);
                }
                Some("j") | Some("DOWN") => {
                    sel = (sel + 1) % calendars.len();
                    build(&calendars, sel, &mut popup, pw);
                }
                Some("c") => {
                    let cal = &calendars[sel];
                    if let Some(new_color) = self.pick_color(cal.color as u8) {
                        let _ = self.db.update_calendar_color(calendars[sel].id, new_color as i64);
                        calendars[sel].color = new_color as i64;
                    }
                    // Recreate popup after color picker
                    popup = Pane::new(px, py, pw, ph, 252, 0);
                    popup.border = true;
                    popup.scroll = false;
                    build(&calendars, sel, &mut popup, pw);
                }
                Some("ENTER") => {
                    let _ = self.db.toggle_calendar_enabled(calendars[sel].id);
                    calendars[sel].enabled = !calendars[sel].enabled;
                    build(&calendars, sel, &mut popup, pw);
                }
                Some("x") => {
                    let name = calendars[sel].name.clone();
                    let confirm = popup.ask(&format!(" Remove '{}'? (y/n): ", name), "");
                    if confirm.trim().to_lowercase() == "y" {
                        let _ = self.db.delete_calendar_with_events(calendars[sel].id);
                        calendars.remove(sel);
                        if calendars.is_empty() { break; }
                        if sel >= calendars.len() { sel = calendars.len() - 1; }
                    }
                    build(&calendars, sel, &mut popup, pw);
                }
                _ => {}
            }
        }

        Crust::clear_screen();
        self.recreate_panes();
        self.load_events_for_range();
        self.render_all();
    }

    fn pick_color(&mut self, current: u8) -> Option<u8> {
        let pw = 52u16;
        let ph = 20u16;
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;

        let mut popup = Pane::new(px, py, pw, ph, 252, 0);
        popup.border = true;
        popup.scroll = false;

        let mut sel = current as u16;

        let build = |sel: u16, popup: &mut Pane| {
            popup.full_refresh();
            let mut lines = Vec::new();
            lines.push(String::new());
            lines.push(format!("  {}  current: {} {}",
                style::bold("Pick Color"),
                style::fg("\u{2588}\u{2588}", sel as u8),
                sel));
            lines.push(String::new());

            for row in 0..16u16 {
                let mut line = " ".to_string();
                for col in 0..16u16 {
                    let c = row * 16 + col;
                    if c == sel {
                        line.push_str(&style::bold(&style::fg(&style::bg("X ", c as u8), 255)));
                    } else {
                        line.push_str(&style::bg("  ", c as u8));
                    }
                    line.push(' ');
                }
                lines.push(line);
            }
            lines.push(String::new());
            lines.push(format!("  {}", style::fg("Arrows:move  ENTER:select  ESC:cancel", 245)));
            popup.set_text(&lines.join("\n"));
            popup.ix = 0;
            popup.refresh();
        };

        build(sel, &mut popup);

        let result;
        loop {
            let k = Input::getchr(None);
            match k.as_deref() {
                Some("ESC") | Some("q") => { result = None; break; }
                Some("ENTER") => { result = Some(sel as u8); break; }
                Some("RIGHT") | Some("l") => { sel = (sel + 1) % 256; build(sel, &mut popup); }
                Some("LEFT") | Some("h") => { sel = (sel + 255) % 256; build(sel, &mut popup); }
                Some("DOWN") | Some("j") => { sel = (sel + 16) % 256; build(sel, &mut popup); }
                Some("UP") | Some("k") => { sel = (sel + 240) % 256; build(sel, &mut popup); }
                _ => {}
            }
        }

        Crust::clear_screen();
        self.recreate_panes();
        self.render_all();
        result
    }

    fn show_preferences(&mut self) {
        let pw = (self.cols.saturating_sub(20) as usize).min(56).max(48) as u16;
        let ph = 19u16;
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;

        let mut popup = Pane::new(px, py, pw, ph, 252, 0);
        popup.border = true;
        popup.scroll = false;

        let pref_keys: Vec<(&str, &str, i64)> = vec![
            ("colors.selected_bg_a",   "Sel. alt bg A",     235),
            ("colors.selected_bg_b",   "Sel. alt bg B",     234),
            ("colors.alt_bg_a",        "Row alt bg A",      233),
            ("colors.alt_bg_b",        "Row alt bg B",      0),
            ("colors.current_month_bg","Current month bg",  233),
            ("colors.saturday",        "Saturday color",    208),
            ("colors.sunday",          "Sunday color",      167),
            ("colors.today_fg",        "Today fg",          232),
            ("colors.today_bg",        "Today bg",          246),
            ("colors.slot_selected_bg","Slot selected bg",  237),
            ("colors.info_bg",         "Info bar bg",       235),
            ("colors.status_bg",       "Status bar bg",     235),
            ("work_hours.start",       "Work hours start",  8),
            ("work_hours.end",         "Work hours end",    17),
            ("default_calendar",       "Default calendar",  1),
        ];

        let mut sel = 0usize;

        let is_color = |key: &str| -> bool { key.starts_with("colors.") };

        let build = |sel: usize, popup: &mut Pane, config: &config::Config, db: &Database, pw: u16,
                     pref_keys: &[(&str, &str, i64)]| {
            popup.full_refresh();
            let inner_w = pw as usize - 4;
            let mut lines = Vec::new();
            lines.push(String::new());
            lines.push(format!("  {}", style::bold("Preferences")));
            lines.push(format!("  {}", style::fg(&"\u{2500}".repeat(inner_w.saturating_sub(3).max(1)), 238)));

            for (i, &(key, label, default)) in pref_keys.iter().enumerate() {
                let val = config.get_i64(key, default);
                let display = if key.starts_with("colors.") {
                    let swatch = if key.contains("bg") {
                        style::bg("  ", val as u8)
                    } else {
                        style::fg("\u{2588}\u{2588}", val as u8)
                    };
                    format!("  {:<18} {:>3} {}", label, val, swatch)
                } else if key == "default_calendar" {
                    let cal_name = db.get_calendars(false).ok()
                        .and_then(|cals| cals.iter().find(|c| c.id == val).map(|c| c.name.clone()))
                        .unwrap_or_default();
                    let extra = if cal_name.is_empty() { String::new() }
                        else { format!(" ({})", cal_name) };
                    format!("  {:<18} {}{}", label, val, extra)
                } else {
                    format!("  {:<18} {}", label, val)
                };

                if i == sel {
                    lines.push(style::bold(&style::fg(&display, 39)));
                } else {
                    lines.push(display);
                }
            }

            lines.push(String::new());
            let (key, _, _) = pref_keys[sel];
            if key.starts_with("colors.") {
                lines.push(format!("  {}", style::fg("j/k:navigate  h/l:adjust  H/L:x10  ENTER:type  q:close", 245)));
            } else {
                lines.push(format!("  {}", style::fg("j/k:navigate  ENTER:edit  q/ESC:close", 245)));
            }

            popup.set_text(&lines.join("\n"));
            popup.ix = 0;
            popup.refresh();
        };

        build(sel, &mut popup, &self.config, &self.db, pw, &pref_keys);

        loop {
            let k = Input::getchr(None);
            match k.as_deref() {
                Some("ESC") | Some("q") => break,
                Some("k") | Some("UP") => {
                    sel = if sel == 0 { pref_keys.len() - 1 } else { sel - 1 };
                    build(sel, &mut popup, &self.config, &self.db, pw, &pref_keys);
                }
                Some("j") | Some("DOWN") => {
                    sel = (sel + 1) % pref_keys.len();
                    build(sel, &mut popup, &self.config, &self.db, pw, &pref_keys);
                }
                Some("h") | Some("LEFT") | Some("l") | Some("RIGHT") | Some("H") | Some("L") => {
                    let (key, _, default) = pref_keys[sel];
                    if is_color(key) {
                        let delta: i64 = match k.as_deref() {
                            Some("h") | Some("LEFT") => -1,
                            Some("l") | Some("RIGHT") => 1,
                            Some("H") => -10,
                            Some("L") => 10,
                            _ => 0,
                        };
                        let val = (self.config.get_i64(key, default) + delta).clamp(0, 255);
                        self.config.set(key, serde_yaml::Value::Number(serde_yaml::Number::from(val)));
                        let _ = self.config.save();
                        build(sel, &mut popup, &self.config, &self.db, pw, &pref_keys);
                    }
                }
                Some("ENTER") => {
                    let (key, label, default) = pref_keys[sel];
                    if is_color(key) {
                        let current = self.config.get_i64(key, default);
                        if let Some(new_color) = self.pick_color(current as u8) {
                            self.config.set(key, serde_yaml::Value::Number(
                                serde_yaml::Number::from(new_color as i64)));
                            let _ = self.config.save();
                        }
                        popup = Pane::new(px, py, pw, ph, 252, 0);
                        popup.border = true;
                        popup.scroll = false;
                    } else {
                        let current = self.config.get_i64(key, default);
                        let result = popup.ask(&format!("{}: ", label), &current.to_string());
                        if !result.trim().is_empty() {
                            if let Ok(val) = result.trim().parse::<i64>() {
                                self.config.set(key, serde_yaml::Value::Number(
                                    serde_yaml::Number::from(val)));
                                let _ = self.config.save();
                            }
                        }
                    }
                    build(sel, &mut popup, &self.config, &self.db, pw, &pref_keys);
                }
                _ => {}
            }
        }

        Crust::clear_screen();
        self.recreate_panes();
        self.render_all();
    }

    fn show_help(&mut self) {
        let pw = (self.cols.saturating_sub(16) as usize).min(68).max(56) as u16;
        let ph = 24u16;
        let px = (self.cols.saturating_sub(pw)) / 2;
        let py = (self.rows.saturating_sub(ph)) / 2;

        let mut popup = Pane::new(px, py, pw, ph, 252, 0);
        popup.border = true;
        popup.scroll = false;

        let k = |s: &str| -> String { style::fg(s, 51) };
        let d = |s: &str| -> String { style::fg(s, 252) };
        let sep_w = (pw as usize).saturating_sub(6).max(1);
        let sep = format!("  {}", style::fg(&"-".repeat(sep_w), 238));

        let mut lines = Vec::new();
        lines.push(String::new());
        lines.push(format!("  {}", style::bold(&style::fg("Tock - Terminal Calendar", 156))));
        lines.push(sep.clone());
        lines.push(format!("  {}", style::bold(&style::fg("Navigation", 156))));
        lines.push(format!("  {}  {}        {}  {}", k("d/RIGHT"), d("Next day"), k("D/LEFT"), d("Prev day")));
        lines.push(format!("  {}        {}       {}       {}", k("w"), d("Next week"), k("W"), d("Prev week")));
        lines.push(format!("  {}        {}      {}       {}", k("m"), d("Next month"), k("M"), d("Prev month")));
        lines.push(format!("  {}        {}       {}       {}", k("y"), d("Next year"), k("Y"), d("Prev year")));
        lines.push(format!("  {}  {}", k("UP/DOWN"), d("Select time slot (scrolls at edges)")));
        lines.push(format!("  {}  {}   {}    {}", k("PgUp/Dn"), d("Jump 10 slots"), k("HOME"), d("Top/all-day")));
        lines.push(format!("  {}      {}  {}     {}", k("END"), d("Bottom (23:30)"), k("j/k"), d("Cycle events")));
        lines.push(format!("  {}      {}", k("e/E"), d("Jump to event (next/prev)")));
        lines.push(format!("  {}        {}           {}       {}", k("t"), d("Today"), k("g"), d("Go to (date, Mon, yyyy)")));
        lines.push(sep.clone());
        lines.push(format!("  {}", style::bold(&style::fg("Events", 156))));
        lines.push(format!("  {}        {}       {}   {}", k("n"), d("New event"), k("ENTER"), d("Edit event")));
        lines.push(format!("  {}    {}    {}       {}", k("x/DEL"), d("Delete event"), k("a"), d("Accept invite")));
        lines.push(format!("  {}        {}", k("v"), d("View event details (scrollable popup)")));
        lines.push(format!("  {}        {}", k("r"), d("Reply via Heathrow")));
        lines.push(format!("  {}        {}", k("J"), d("Join meeting (per-host handler from config, else browser)")));
        lines.push(sep.clone());
        lines.push(format!("  {}  {}   {}  {}   {}  {}", k("i"), d("Import ICS"), k("G"), d("Google setup"), k("O"), d("Outlook setup")));
        lines.push(format!("  {}  {}     {}  {}      {}  {}", k("S"), d("Sync now"), k("C"), d("Calendars"), k("P"), d("Preferences")));
        lines.push(format!("  {}  {}", k("q"), d("Quit")));
        lines.push(String::new());
        lines.push(format!("  {}", style::fg("Press any key to close...", 245)));

        popup.set_text(&lines.join("\n"));
        popup.refresh();
        let _ = Input::getchr(None);
        Crust::clear_screen();
        self.recreate_panes();
        self.render_all();
    }

    // =====================================================================
    // Helper methods
    // =====================================================================

    fn show_feedback(&mut self, message: &str, color: u8) {
        let w = self.cols as usize;
        let mut lines = vec![
            style::fg(&"-".repeat(w), 238),
            style::fg(&format!(" {}", message), color),
        ];
        while lines.len() < self.bottom.h as usize {
            lines.push(String::new());
        }
        self.bottom.set_text(&lines.join("\n"));
        self.bottom.full_refresh();
    }

    fn blank_bottom(&mut self, header: &str) {
        let w = self.cols as usize;
        let mut lines = vec![style::fg(&"-".repeat(w), 238)];
        lines.push(String::new());
        if !header.is_empty() { lines.push(header.to_string()); }
        while lines.len() < self.bottom.h as usize {
            lines.push(String::new());
        }
        self.bottom.set_text(&lines.join("\n"));
        self.bottom.full_refresh();
    }

    fn bottom_ask(&mut self, prompt: &str, default: &str) -> String {
        let prompt_y = self.bottom.y + 3;
        let mut prompt_pane = Pane::new(1, prompt_y, self.cols, 1, 255, 0);
        prompt_pane.border = false;
        prompt_pane.scroll = false;
        prompt_pane.ask(prompt, default)
    }

    fn check_heathrow_goto(&mut self) {
        let goto_file = config::tock_home().join("goto");
        if !goto_file.exists() { return; }
        if let Ok(content) = std::fs::read_to_string(&goto_file) {
            let _ = std::fs::remove_file(&goto_file);
            // kastrup writes goto immediately after dropping an ICS in
            // ~/.tock/incoming/, so a fresh goto is also the wake
            // signal that the inbox has something new. Drain incoming
            // before navigating so the event the user is about to look
            // at actually exists in the DB. Free piggyback — no new
            // polling, no new syscalls in the no-goto path (goto stat
            // is the gate).
            let cal_id = self.config.get_i64("default_calendar", 1);
            let imported = ics::watch_incoming(&self.db, cal_id);
            if imported > 0 { self.load_events_for_range(); }

            let content = content.trim().to_string();
            if content.is_empty() {
                if imported > 0 { self.render_all(); }
                return;
            }
            if let Some(parsed) = self.parse_go_to_input(&content) {
                self.selected_date = parsed;
                self.selected_event_index = 0;
                self.load_events_for_range();
                self.render_all();
            } else if imported > 0 {
                self.render_all();
            }
        }
    }
}

// =========================================================================
// Free functions
// =========================================================================

fn add_days(date: (i32, u32, u32), n: i32) -> (i32, u32, u32) {
    let (y, m, d) = date;
    // Use noon to avoid DST boundary issues
    let ts = date_to_ts(y, m, d, 12, 0, 0) + (n as i64) * 86400;
    let (ny, nm, nd, _, _, _) = ts_to_parts(ts);
    (ny, nm, nd)
}

fn day_diff(a: (i32, u32, u32), b: (i32, u32, u32)) -> i64 {
    let ts_a = date_to_ts(a.0, a.1, a.2, 0, 0, 0);
    let ts_b = date_to_ts(b.0, b.1, b.2, 0, 0, 0);
    ((ts_a - ts_b) / 86400).abs()
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.len() <= max { s.to_string() }
    else { s.chars().take(max).collect() }
}

fn body_color(name: &str) -> String {
    for &(n, c) in orbit::BODY_COLORS {
        if n == name { return c.to_string(); }
    }
    "888888".to_string()
}

/// Two-letter Latin abbreviation for the planet name returned by
/// `orbit::visible_planets`. Used by the tock header so the planet
/// row renders uniformly across terminals — the astrological glyphs
/// from orbit don't all honour VS-15 on every font stack (Venus and
/// Mars in particular tend to upgrade to emoji), and a mixed row of
/// some-text some-emoji looks worse than this clean letter set.
fn planet_abbrev(name: &str) -> &'static str {
    match name {
        "Mercury" => "Me",
        "Venus"   => "Ve",
        "Mars"    => "Ma",
        "Jupiter" => "Ju",
        "Saturn"  => "Sa",
        "Uranus"  => "Ur",
        "Neptune" => "Ne",
        _ => "?",
    }
}

/// Subtle single-glyph RSVP marker: a dot for "accepted" / "organizer",
/// "?" for tentative, "×" for declined. Empty for unresponded or missing.
fn rsvp_marker(my_status: Option<&str>) -> &'static str {
    match my_status {
        Some("accepted")                                 => "•",
        Some("organizer")                                => "∘",
        Some("tentative") | Some("tentativelyAccepted") => "?",
        Some("declined")                                 => "×",
        _                                                => "",
    }
}

fn humanize_status(status: &str) -> &str {
    match status {
        "needsAction" => "Needs response",
        "accepted" => "Accepted",
        "declined" => "Declined",
        "tentative" | "tentativelyAccepted" => "Tentative",
        "confirmed" => "Confirmed",
        "cancelled" => "Cancelled",
        _ => status,
    }
}

fn clean_description(desc: &str) -> String {
    let desc = desc.to_string();
    // HTML → text (Outlook / Word descriptions). Order matters:
    //   1) drop <style>/<head>/<script> blocks and comments outright;
    //   2) collapse source whitespace — Word wraps the HTML with `\r\n\t`
    //      every ~76 cols, which is NOT content (HTML ignores it);
    //   3) turn block/break tags into real newlines;
    //   4) strip remaining inline tags and decode entities.
    // The old code replaced every tag with a space, which DROPPED the
    // real <p>/<br> breaks while KEEPING the source wrapping as newlines —
    // exactly backwards, producing fragmented text ("Need\nhelp?").
    let desc = if desc.trim_start().starts_with('<') {
        let blocks = regex::Regex::new(
            r"(?is)<!--.*?-->|<style[^>]*>.*?</style>|<head[^>]*>.*?</head>|<script[^>]*>.*?</script>"
        ).unwrap();
        let desc = blocks.replace_all(&desc, " ");
        let ws = regex::Regex::new(r"\s+").unwrap();
        let desc = ws.replace_all(&desc, " ");
        let brk = regex::Regex::new(
            r"(?i)<br\s*/?>|</(?:p|div|li|tr|h[1-6]|ul|ol|table|blockquote)\s*>"
        ).unwrap();
        let desc = brk.replace_all(&desc, "\n");
        let tags = regex::Regex::new(r"<[^>]+>").unwrap();
        tags.replace_all(&desc, "")
            .replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#39;", "'")
            .replace("&quot;", "\"")
            .to_string()
    } else {
        desc
    };

    // Clean garbage patterns
    let re_color = regex::Regex::new(r"BC\d+-Color:\s*-?\d+\s*").unwrap();
    let re_meet = regex::Regex::new(r"(?s)-::~:~::~:~.*$").unwrap();
    let re_underscore = regex::Regex::new(r"_{3,}").unwrap();
    let re_dashes = regex::Regex::new(r"-{5,}").unwrap();
    let re_box = regex::Regex::new(r"[\u{2501}\u{2550}\u{2500}]{3,}").unwrap();
    let re_blanks = regex::Regex::new(r"\n{3,}").unwrap();

    let desc = re_color.replace_all(&desc, "");
    let desc = re_meet.replace_all(&desc, "");
    let desc = re_underscore.replace_all(&desc, "");
    let desc = re_dashes.replace_all(&desc, "");
    let desc = re_box.replace_all(&desc, "");
    let desc = re_blanks.replace_all(&desc, "\n\n");
    desc.trim().to_string()
}

/// One "Label:    value" detail row, kastrup-style: the label is padded
/// to `label_w`, coloured `lcol`; the value is colour `vcol`, truncated to
/// the remaining width. None when the value is blank (so empty fields are
/// skipped rather than printing a bare label).
fn fmt_field(label: &str, label_w: usize, value: &str, max_val: usize, lcol: u8, vcol: u8) -> Option<String> {
    if value.trim().is_empty() { return None; }
    let lbl = format!("{:<width$}", format!("{}:", label), width = label_w);
    Some(format!(" {} {}", style::fg(&lbl, lcol), style::fg(&truncate_str(value, max_val), vcol)))
}

/// Attendee RSVP → (marker, colour). Covers Google (`responseStatus`) and
/// Outlook (`status.response`) vocabularies.
fn rsvp_short(status: &str) -> (&'static str, u8) {
    match status.to_ascii_lowercase().as_str() {
        "accepted"                            => ("\u{2713}", 40),  // ✓ green
        "declined"                            => ("\u{2717}", 167), // ✗ red
        "tentative" | "tentativelyaccepted"   => ("?", 179),        // amber
        _                                     => ("\u{00B7}", 244), // · grey (no response)
    }
}

/// Normalise one attendee object into (display name, raw status), handling
/// the three shapes tock stores: Outlook `{emailAddress:{address,name},
/// status:{response}}`, Google `{email,displayName,responseStatus}`, and
/// the manual `{email}`. Display prefers a real name, else the email's
/// local part for compactness.
fn attendee_name_status(a: &serde_json::Value) -> Option<(String, String)> {
    let (name, email) = match a.get("emailAddress") {
        Some(ea) => (
            ea.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            ea.get("address").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ),
        None => (
            a.get("displayName").and_then(|v| v.as_str())
                .or_else(|| a.get("name").and_then(|v| v.as_str())).unwrap_or("").to_string(),
            a.get("email").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ),
    };
    let disp = if !name.trim().is_empty() { name.trim().to_string() }
        else if !email.is_empty() { email.split('@').next().unwrap_or(&email).to_string() }
        else { return None };
    let status = a.get("responseStatus").and_then(|v| v.as_str())
        .or_else(|| a.get("status").and_then(|s| s.get("response")).and_then(|v| v.as_str()))
        .unwrap_or("").to_string();
    Some((disp, status))
}

/// Render the attendee list as `Name ✓  Name ?  …  (total)`, coloured by
/// RSVP, fitted to `max_w` plain columns (overflow collapses to `+N`).
fn attendee_line(attendees: &serde_json::Value, max_w: usize) -> Option<String> {
    let arr = attendees.as_array()?;
    if arr.is_empty() { return None; }
    let total = arr.len();
    let mut parts: Vec<String> = Vec::new();
    let mut used = 0usize;
    let mut shown = 0usize;
    for a in arr {
        let Some((name, status)) = attendee_name_status(a) else { continue };
        let (marker, mcol) = rsvp_short(&status);
        let plain = format!("{} {}", name, marker);
        if shown > 0 && used + plain.len() + 2 > max_w {
            parts.push(style::fg(&format!("+{}", total - shown), 245));
            break;
        }
        parts.push(format!("{} {}", style::fg(&name, 250), style::fg(marker, mcol)));
        used += plain.len() + 2;
        shown += 1;
    }
    if parts.is_empty() { return None; }
    Some(format!("{}  {}", parts.join("  "), style::fg(&format!("({})", total), 245)))
}

/// Pull the first video-meeting join URL out of a description/location
/// (Teams, Zoom, Google Meet, Whereby, Webex). The "Bli med:/Join:" short
/// link sorts first in Teams invites, so the first match is the clean one.
fn extract_meeting_link(text: &str) -> Option<String> {
    let re = regex::Regex::new(
        r"https://[^\s<>|)\]]*(?:teams\.microsoft\.com/(?:meet|l/meetup-join)|zoom\.us/j/|meet\.google\.com/|whereby\.com/|webex\.com)[^\s<>|)\]]*"
    ).ok()?;
    re.find(text).map(|m| m.as_str().trim_end_matches(['>', ')', ']', '.', ',']).to_string())
}

/// Strip auto-generated meeting boilerplate (the Teams/Zoom join block,
/// dial-in numbers, passcodes, help links, logo images) so the detail pane
/// shows only the human-written description. The join URL is surfaced
/// separately as its own field, so dropping it here is intentional.
fn clean_meeting_desc(desc: &str) -> String {
    let base = clean_description(desc);
    let img_re = regex::Regex::new(r"\[[^\]]*?(?:https?://|\.(?:png|jpg|jpeg|gif|svg))[^\]]*\]").unwrap();
    const NOISE: &[&str] = &[
        "teams.microsoft.com", "microsoft teams", "bli med", "join the meeting",
        "møte-id", "meeting id", "passord", "passcode", "aka.ms", "pexip",
        "videokonferanse", "video conference", "videokonferanseenhet",
        "leiers nøkkel", "tenant key", "video-id", "video id",
        "mer informasjon", "more info", "møtealternativer", "meeting options",
        "zoom.us", "meet.google.com", "whereby.com", "webex.com",
        "trenger du hjelp", "need help", "systemreferanse", "for arrangører",
        "for organizers", "________",
    ];
    let mut out: Vec<String> = Vec::new();
    for raw in base.lines() {
        let line = img_re.replace_all(raw, "");
        let l = line.trim();
        if l.is_empty() { out.push(String::new()); continue; }
        let low = l.to_lowercase();
        if NOISE.iter().any(|n| low.contains(n)) { continue; }
        out.push(l.to_string());
    }
    let joined = out.join("\n");
    let re_blanks = regex::Regex::new(r"\n{2,}").unwrap();
    re_blanks.replace_all(joined.trim(), "\n").to_string()
}

fn shellexpand(path: &str) -> String {
    if path.starts_with('~') {
        let home = config::home_dir();
        format!("{}{}", home.display(), &path[1..])
    } else {
        path.to_string()
    }
}

/// Pick the most useful meeting URL out of an event's description /
/// location blob. Priority order:
///   1. Modern Teams meet links     `https://teams.microsoft.com/meet/<id>?p=…`
///   2. Classic Teams meetup-join   `https://teams.microsoft.com/l/meetup-join/19%3a…`
///   3. Any Teams URL (msteams: scheme included)
///   4. Zoom join links             `https://*.zoom.us/j/<id>` (with optional ?pwd=)
///   5. Google Meet                 `https://meet.google.com/<id>`
///   6. Any https?:// URL — last-resort fallback
///
/// Returns the first hit; the calling site picks the launcher
/// (teams-for-linux for Teams URLs, xdg-open for the rest).
fn extract_meeting_url(haystack: &str) -> Option<String> {
    use regex::Regex;
    // Patterns anchor URLs against terminating characters (whitespace,
    // `>`, `"`, `'`, `<`, `]`) so we don't capture trailing markup
    // from html-formatted descriptions. The `tail` placeholder gets
    // substituted into each pattern.
    let tail = r#"[^\s<>"'\]]+"#;
    let patterns: [String; 7] = [
        format!(r"https://teams\.microsoft\.com/meet/{}", tail),
        format!(r"https://teams\.microsoft\.com/l/meetup-join/{}", tail),
        r#"msteams:[^\s<>"']+"#.to_string(),
        format!(r"https://teams\.microsoft\.com/{}", tail),
        format!(r"https://[a-zA-Z0-9.-]+\.zoom\.us/j/{}", tail),
        format!(r"https://meet\.google\.com/[a-z0-9-]+(?:\?{})?", tail),
        format!(r"https?://{}", tail),
    ];
    for p in &patterns {
        let re = Regex::new(p).ok()?;
        if let Some(m) = re.find(haystack) {
            // Strip a trailing `)` / `.` / `,` that often closes an
            // inline link in Markdown or Outlook-flavoured plain text.
            let mut s = m.as_str().to_string();
            while s.ends_with(')') || s.ends_with('.') || s.ends_with(',') {
                s.pop();
            }
            return Some(s);
        }
    }
    None
}

/// Pull the host out of a URL string. Returns the part between
/// `://` and the first `/` (or end-of-string). Lowercased so
/// host-suffix matching works regardless of how the URL was cased.
/// `msteams:` and other schemes without `://` return None.
fn url_host(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    let host = after_scheme.split('/').next()?;
    // Strip optional `user:pass@` and trailing `:port`.
    let host = host.rsplit_once('@').map(|(_, h)| h).unwrap_or(host);
    let host = host.split_once(':').map(|(h, _)| h).unwrap_or(host);
    Some(host.to_ascii_lowercase())
}

/// Cheap, dependency-free `which`: probe each `:`-separated PATH dir
/// for an executable file named `cmd`. We don't try to follow
/// symlinks or check exec bits exhaustively — `is_file()` plus the
/// path's existence is enough for the "is teams-for-linux installed"
/// gate the join flow needs.
fn which_on_path(cmd: &str) -> bool {
    let path = match std::env::var_os("PATH") { Some(p) => p, None => return false };
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(cmd);
        if candidate.is_file() { return true; }
    }
    false
}

/// Flush any pending bytes on stdin
fn flush_stdin() {
    use std::io::Read;
    unsafe {
        let flags = libc::fcntl(0, libc::F_GETFL);
        libc::fcntl(0, libc::F_SETFL, flags | libc::O_NONBLOCK);
        let mut buf = [0u8; 256];
        while std::io::stdin().read(&mut buf).unwrap_or(0) > 0 {}
        libc::fcntl(0, libc::F_SETFL, flags);
    }
}

// =========================================================================
// Main
// =========================================================================

/// One-shot: push a single local-only event to its remote (Google) calendar
/// and persist the returned external_id back. Returns process exit code.
fn push_event_oneshot(event_id: i64) -> i32 {
    let db = match Database::new(None) {
        Ok(d) => d,
        Err(e) => { eprintln!("could not open tock.db: {}", e); return 1; }
    };
    let evt = match db.get_event(event_id) {
        Ok(Some(e)) => e,
        Ok(None) => { eprintln!("event {} not found", event_id); return 1; }
        Err(e) => { eprintln!("db error: {}", e); return 1; }
    };
    let data = EventData {
        id: Some(evt.id),
        calendar_id: evt.calendar_id,
        external_id: evt.external_id.clone(),
        title: evt.title.clone(),
        description: evt.description.clone(),
        location: evt.location.clone(),
        start_time: evt.start_time,
        end_time: evt.end_time,
        all_day: evt.all_day,
        timezone: evt.timezone.clone(),
        recurrence_rule: evt.recurrence_rule.clone(),
        series_master_id: evt.series_master_id,
        status: evt.status.clone(),
        organizer: evt.organizer.clone(),
        attendees: evt.attendees.clone(),
        my_status: evt.my_status.clone(),
        alarms: evt.alarms.clone(),
        metadata: evt.metadata.clone(),
    };
    if data.external_id.is_some() {
        eprintln!("event {} already has external_id={:?}; skipping",
            evt.id, data.external_id);
        return 0;
    }
    let cal = match db.get_calendars(false).ok()
        .and_then(|cs| cs.into_iter().find(|c| c.id == data.calendar_id))
    {
        Some(c) => c,
        None => { eprintln!("calendar {} not found", data.calendar_id); return 1; }
    };
    if cal.source_type != "google" {
        eprintln!("calendar source_type={} (only google is supported here)", cal.source_type);
        return 1;
    }
    let cfg_str = match cal.source_config.as_ref() {
        Some(s) => s,
        None => { eprintln!("calendar source_config missing"); return 1; }
    };
    let cfg: serde_json::Value = match serde_json::from_str(cfg_str) {
        Ok(v) => v,
        Err(e) => { eprintln!("source_config parse: {}", e); return 1; }
    };
    let email = cfg.get("email").and_then(|v| v.as_str()).unwrap_or("");
    let safe_dir = cfg.get("safe_dir").and_then(|v| v.as_str());
    let google_cal_id = cfg.get("google_calendar_id").and_then(|v| v.as_str()).unwrap_or("");
    if email.is_empty() || google_cal_id.is_empty() {
        eprintln!("source_config missing email or google_calendar_id");
        return 1;
    }
    let mut gc = sources::google::GoogleCalendar::new(email, safe_dir);
    if gc.get_access_token().is_none() {
        eprintln!("Google auth failed: {:?}", gc.last_error);
        return 1;
    }
    println!("pushing event {} ('{}') to calendar {}...", evt.id, evt.title, google_cal_id);
    match gc.create_event(google_cal_id, &data) {
        Some(remote_id) => {
            let mut updated = data;
            updated.id = Some(evt.id);
            updated.external_id = Some(remote_id.clone());
            if let Err(e) = db.save_event(&updated) {
                eprintln!("created on Google ({}) but failed to persist external_id locally: {}",
                    remote_id, e);
                return 1;
            }
            println!("created on Google, id={} (persisted locally)", remote_id);
            0
        }
        None => {
            eprintln!("push failed: {:?}", gc.last_error);
            1
        }
    }
}

fn main() {
    // --help and --version answer before the TUI touches the terminal.
    // A tool that asks what this is — the fe2o3 launcher's ? popup, a
    // packaging script, a curious shell — should get an answer, not a
    // screen paint.
    if std::env::args().skip(1).any(|a| a == "-h" || a == "--help") {
        println!("tock — Calendar with ephemeris (Fe2O3 suite)");
        println!();
        println!("Usage: tock [OPTIONS]");
        println!();
        println!("  --compose-to ADDR   open a compose window to ADDR (via kastrup)");
        println!("  --subject TEXT      subject for --compose-to");
        println!("  --push-event FILE   import an ICS file and exit");
        println!();
        println!("Day / week / month / year views, sun, moon and planets, Google and Outlook");
        println!("sync. Drop .ics files in ~/.tock/incoming/ to import them.");
        return;
    }
    if std::env::args().skip(1).any(|a| a == "-v" || a == "--version") {
        println!("tock {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    // CLI: `tock --push-event <id>` pushes a single local-only event to its
    // remote calendar (one-shot, no TUI). Used to backfill events that were
    // created before remote write-back was wired.
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--push-event") {
        if let Some(id_str) = args.get(pos + 1) {
            if let Ok(id) = id_str.parse::<i64>() {
                std::process::exit(push_event_oneshot(id));
            }
        }
        eprintln!("usage: tock --push-event <event_id>");
        std::process::exit(2);
    }

    Crust::init();
    Crust::set_app_identity("Tock");
    Crust::clear_screen();
    Cursor::hide();

    let mut app = App::new();

    app.load_events_for_range();

    // Watch incoming ICS files
    let cal_id = app.config.get_i64("default_calendar", 1);
    let incoming_count = ics::watch_incoming(&app.db, cal_id);
    if incoming_count > 0 {
        app.load_events_for_range();
    }

    app.render_all();

    // Start background poller
    let poller = poller::Poller::start(
        app.db.clone(),
        &app.config,
        app._poller_tx.clone(),
    );

    flush_stdin();

    let mut weather_date = today();
    // Notification alarms fire on minute precision (±1min window). No need
    // to recheck on every idle wake.
    let mut last_notify_minute: i64 = -1;

    while app.running {
        let key = Input::getchr(Some(2));
        if let Some(ref k) = key {
            app.handle_input(k);
        } else {
            // Idle: check poller
            if let Ok(poller::PollerEvent::NeedsRefresh) = app.poller_rx.try_recv() {
                app.load_events_for_range();
                app.render_all();
            }

            // Check notifications only when the wall-clock minute changes.
            let cur_minute = crate::database::now_secs() / 60;
            if cur_minute != last_notify_minute {
                last_notify_minute = cur_minute;
                let default_alarm = app.config.get_i64("notifications.default_alarm", 15);
                notifications::check_and_notify(&app.db, default_alarm);
            }

            // Refresh weather on new day
            let t = today();
            if weather_date != t {
                weather_date = t;
                app.weather_forecast.clear();
                app.weather_fetched_at = 0;
                app.load_events_for_range();
                app.render_all();
            }

            // Check heathrow goto file
            app.check_heathrow_goto();
        }
    }

    drop(poller);
    Cursor::show();
    Crust::cleanup();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_modern_teams_meet() {
        let body = "Join: https://teams.microsoft.com/meet/31274339366591?p=10ubU0wL4gdnhyeK9w\nMeeting ID: 312";
        let url = extract_meeting_url(body).unwrap();
        assert!(url.starts_with("https://teams.microsoft.com/meet/31274339366591"));
        assert!(url.contains("?p=10ubU0wL4gdnhyeK9w"));
    }

    #[test]
    fn extract_classic_meetup_join() {
        let body = "System reference<https://teams.microsoft.com/l/meetup-join/19%3ameeting_OGNh@thread.v2/0?context=%7b%22Tid%22%3a%22012%22%7d>";
        let url = extract_meeting_url(body).unwrap();
        assert!(url.contains("/l/meetup-join/19%3ameeting_OGNh"));
        // Must not include the trailing > from the html-style link.
        assert!(!url.ends_with('>'));
    }

    #[test]
    fn modern_meet_link_beats_classic_when_both_present() {
        // Real outlook descriptions ship the modern `meet/` URL up
        // top and the classic `l/meetup-join` further down. The
        // first hit wins so we get the human-readable one.
        let body = "
            Join: https://teams.microsoft.com/meet/31274339366591?p=10ubU0w
            ...
            System reference: https://teams.microsoft.com/l/meetup-join/19%3a@thread.v2/0
        ";
        let url = extract_meeting_url(body).unwrap();
        assert!(url.contains("/meet/31274339366591"));
    }

    #[test]
    fn extract_zoom() {
        let body = "Please join: https://us02web.zoom.us/j/12345678?pwd=abc def";
        let url = extract_meeting_url(body).unwrap();
        assert!(url.starts_with("https://us02web.zoom.us/j/12345678"));
    }

    #[test]
    fn returns_none_when_no_url() {
        assert!(extract_meeting_url("Lunch at the canteen, no link").is_none());
        assert!(extract_meeting_url("").is_none());
    }

    #[test]
    fn url_host_extracts_lowercased_host() {
        assert_eq!(url_host("https://teams.microsoft.com/meet/123?p=x"),
                   Some("teams.microsoft.com".into()));
        assert_eq!(url_host("HTTPS://Teams.Microsoft.Com/x"),
                   Some("teams.microsoft.com".into()));
        assert_eq!(url_host("https://us02web.zoom.us/j/123"),
                   Some("us02web.zoom.us".into()));
        assert_eq!(url_host("https://user:pass@host.example.com:8080/p"),
                   Some("host.example.com".into()));
        assert_eq!(url_host("msteams:foo"), None);
        assert_eq!(url_host("not a url"), None);
    }
}
