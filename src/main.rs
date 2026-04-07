// Tock - Terminal calendar TUI
// Feature and visual clone of Timely (Ruby), built on Crust.

mod astronomy;
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
    astronomy::days_in_month(year, month)
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

        events.iter().find(|e| {
            if e.all_day { return false; }
            e.start_time < slot_end && e.end_time > slot_start
        }).cloned()
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
            let st_local = evt.start_time + tz;
            let et_local = evt.end_time + tz;
            let (sy2, sm2, sd2, _, _, _) = ts_to_parts(st_local);
            let (ey, em, ed, _, _, _) = ts_to_parts(et_local);

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

        // Load weather (cached)
        let now = database::now_secs();
        if self.weather_forecast.is_empty() || (now - self.weather_fetched_at) > 21600 {
            let lat = self.config.get_f64("location.lat", 59.9139);
            let lon = self.config.get_f64("location.lon", 10.7522);
            self.weather_forecast = weather::fetch_cached(lat, lon, &self.db);
            self.weather_fetched_at = now;
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

        let phase = astronomy::moon_phase(sy, sm, sd);
        let moon_color = body_color("moon");
        let moon = format!("  {} {} ({}%)",
            style::fg_rgb(phase.symbol, &moon_color), phase.phase_name,
            (phase.illumination * 100.0).round() as i32);

        let lat = self.config.get_f64("location.lat", 59.9139);
        let lon = self.config.get_f64("location.lon", 10.7522);
        let tz = self.config.get_f64("timezone_offset", 1.0);

        // Moon rise/set
        let moon_rs = match astronomy::moon_times(sy, sm, sd, lat, lon, tz) {
            Some((rise, set)) => {
                let mc = body_color("moon");
                format!("  {}\u{2191}{}  {}\u{2193}{}",
                    style::fg_rgb("\u{263D}", &mc), rise,
                    style::fg_rgb("\u{263D}", &mc), set)
            }
            None => String::new(),
        };

        // Sun rise/set
        let sun_str = match astronomy::sun_times(sy, sm, sd, lat, lon, tz) {
            Some((rise, set)) => {
                let sc = body_color("sun");
                format!("  {}\u{2191}{}  {}\u{2193}{}",
                    style::fg_rgb("\u{2600}", &sc), rise,
                    style::fg_rgb("\u{2600}", &sc), set)
            }
            None => String::new(),
        };

        // Visible planets (cached per date)
        if self.cached_planets_date != Some(self.selected_date) {
            let planets = astronomy::visible_planets(sy, sm, sd, lat, lon, tz);
            self.cached_planets = planets.iter().map(|p| {
                style::fg_rgb(p.symbol, p.color)
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
                        let entry = format!("{}{}", marker, truncate_str(title, day_col.saturating_sub(1)));
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

                let evt_opt = week_events[col as usize].iter().find(|e| {
                    e.start_time < day_ts_end && e.end_time > day_ts_start
                });

                let cell = if let Some(evt) = evt_opt {
                    let is_at_slot = is_sel && is_slot_selected;
                    let marker = if is_at_slot { ">" } else { " " };
                    let title = if evt.title.is_empty() { "(No title)" } else { &evt.title };
                    let mut entry = format!("{}{}", marker, title);
                    if entry.len() > day_col {
                        entry = format!("{}.", truncate_str(&entry, day_col.saturating_sub(1)));
                    }
                    let color = evt.calendar_color as u8;
                    if is_at_slot {
                        style::bg(&style::bold(&style::fg(&entry, color)), cell_bg)
                    } else {
                        style::bg(&style::fg(&entry, color), cell_bg)
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
            let color = evt.calendar_color as u8;
            let title = if evt.title.is_empty() { "(No title)".to_string() } else { evt.title.clone() };
            let tz = local_tz_offset_secs();

            let time_info = if evt.all_day {
                format!("{}-{:02}-{:02}  All day", sy, sm, sd)
            } else {
                let local_s = evt.start_time + tz;
                let (_, _, _, sh, smn, _) = ts_to_parts(local_s);
                let local_e = evt.end_time + tz;
                let (_, _, _, eh, emn, _) = ts_to_parts(local_e);
                let swd = cwday(sy, sm, sd);
                format!("{} {}-{:02}-{:02}  {:02}:{:02} - {:02}:{:02}",
                    weekday_short(swd), sy, sm, sd, sh, smn, eh, emn)
            };

            lines.push(format!(" {}  {}",
                style::bold(&style::fg(&title, color)),
                style::fg(&time_info, 252)));

            // Details line
            let mut details: Vec<String> = Vec::new();
            if let Some(ref loc) = evt.location {
                let loc = loc.trim();
                if !loc.is_empty() { details.push(format!("Location: {}", loc)); }
            }
            if let Some(ref org) = evt.organizer {
                let org = org.trim();
                if !org.is_empty() { details.push(format!("Organizer: {}", org)); }
            }
            details.push(format!("Calendar: {}", evt.calendar_name));
            let detail_line = format!(" {}", details.join("  |  "));
            let detail_line = truncate_str(&detail_line, w.saturating_sub(2));
            lines.push(style::fg(&detail_line, 245));

            // Status
            let mut status_parts: Vec<String> = Vec::new();
            if !evt.status.is_empty() {
                status_parts.push(format!("Status: {}", evt.status));
            }
            if let Some(ref ms) = evt.my_status {
                status_parts.push(format!("My status: {}", humanize_status(ms)));
            }
            if !status_parts.is_empty() {
                lines.push(style::fg(&format!(" {}", status_parts.join("  |  ")), 245));
            }

            // Description
            if let Some(ref desc) = evt.description {
                let desc = clean_description(desc);
                if !desc.is_empty() {
                    let desc_flat = desc.replace('\n', " ").replace('\r', "");
                    lines.push(String::new());
                    let max_lines = 50;
                    let mut line = " ".to_string();
                    for word in desc_flat.split_whitespace() {
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
            let astro = astronomy::astro_events_for_year(sy, sm, sd);
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
            "r" => self.show_feedback("Reply via Heathrow: not yet implemented", 226),
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
            let s = date_to_ts(sy, sm, sd, 0, 0, 0) - tz;
            (s, s + 86400)
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

        let data = EventData {
            id: None,
            calendar_id: cal_id,
            external_id: None,
            title: title.clone(),
            description: None,
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

        let _ = self.db.save_event(&data);
        self.load_events_for_range();
        self.render_all();
        let msg = format!("Event created: {}", title);
        self.show_feedback(&msg, cal_color);
    }

    fn edit_event(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        self.blank_bottom(&style::bold(" Edit Event"));
        let new_title = self.bottom_ask(" Title: ", &evt.title);
        if new_title.is_empty() { self.render_all(); return; }

        let data = EventData {
            id: Some(evt.id),
            calendar_id: evt.calendar_id,
            external_id: evt.external_id.clone(),
            title: new_title.trim().to_string(),
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

        let _ = self.db.save_event(&data);
        self.load_events_for_range();
        self.render_all();
        self.show_feedback("Event updated", 156);
    }

    fn delete_event(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        self.blank_bottom(&style::bold(" Delete Event"));
        let confirm = self.bottom_ask(&format!(" Delete '{}'? (y/n): ", evt.title), "");
        if confirm.trim().to_lowercase() != "y" { self.render_all(); return; }

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
                        let name = a.get("name").or(a.get("email")).or(a.get("displayName"))
                            .and_then(|v| v.as_str()).unwrap_or("?");
                        let status = a.get("status").or(a.get("responseStatus"))
                            .and_then(|v| v.as_str()).unwrap_or("");
                        let status_str = if status.is_empty() { String::new() }
                            else { style::fg(&format!("  ({})", status), 245) };
                        lines.push(format!("    {}{}", style::fg(name, 252), status_str));
                    }
                }
            }
        }

        // Description
        if let Some(ref desc) = evt.description {
            let desc = clean_description(desc);
            if !desc.is_empty() {
                lines.push(String::new());
                let sep_w = (pw as usize).saturating_sub(6).max(1);
                lines.push(format!("  {}", style::fg(&"-".repeat(sep_w), 238)));
                for dline in desc.split('\n') {
                    let mut remaining = dline.to_string();
                    let max_w = pw as usize - 6;
                    while remaining.len() > max_w {
                        lines.push(style::fg(&format!("  {}", &remaining[..max_w]), 248));
                        remaining = remaining[max_w..].to_string();
                    }
                    lines.push(style::fg(&format!("  {}", remaining), 248));
                }
            }
        }

        lines.push(String::new());
        lines.push(format!("  {}", style::fg("UP/DOWN:scroll  C-Y:copy  ESC/q:close", 245)));

        popup.set_text(&lines.join("\n"));
        popup.refresh();

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

    fn accept_invite(&mut self) {
        let evt = match self.event_at_selected_slot() {
            Some(e) => e,
            None => { self.show_feedback("No event at this time slot", 245); return; }
        };

        self.show_feedback(&format!("Accepting '{}'...", evt.title), 226);

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
            my_status: Some("accepted".to_string()),
            alarms: evt.alarms.clone(),
            metadata: evt.metadata.clone(),
        };

        let _ = self.db.save_event(&data);
        self.load_events_for_range();
        self.render_all();
        self.show_feedback("Invite accepted", 156);
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

        let safe_dir = self.config.get_str("google.safe_dir", "~/.config/timely/credentials");
        self.show_feedback("Connecting to Google Calendar...", 226);

        let _google = sources::google::GoogleCalendar::new(&email, Some(&safe_dir));
        // Google calendar setup is complex; show instructions
        self.show_feedback("Google Calendar: see credentials setup documentation", 245);
    }

    fn setup_outlook_calendar(&mut self) {
        self.blank_bottom(&style::bold(&style::fg(" Outlook/365 Calendar Setup", 33)));
        let default_client_id = self.config.get_str("outlook.client_id", "");
        let client_id = self.bottom_ask(" Azure App client_id: ", &default_client_id);
        if client_id.trim().is_empty() { self.render_all(); return; }

        let default_tenant = self.config.get_str("outlook.tenant_id", "common");
        let tenant_id = self.bottom_ask(
            &format!(" Tenant ID (Enter for '{}'): ", default_tenant), &default_tenant);
        let tenant_id = if tenant_id.trim().is_empty() { default_tenant } else { tenant_id.trim().to_string() };

        self.config.set("outlook.client_id", serde_yaml::Value::String(client_id.trim().to_string()));
        self.config.set("outlook.tenant_id", serde_yaml::Value::String(tenant_id.clone()));
        let _ = self.config.save();

        self.show_feedback("Outlook Calendar: device code auth not yet integrated in Tock", 245);
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

        // Sync runs in background via poller; trigger a refresh
        self.show_feedback("Sync triggered. Background poller will refresh.", 156);
        self.syncing = false;
        self.render_status_bar();
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
        let goto_file = config::timely_home().join("goto");
        if !goto_file.exists() { return; }
        if let Ok(content) = std::fs::read_to_string(&goto_file) {
            let _ = std::fs::remove_file(&goto_file);
            let content = content.trim().to_string();
            if content.is_empty() { return; }
            if let Some(parsed) = self.parse_go_to_input(&content) {
                self.selected_date = parsed;
                self.selected_event_index = 0;
                self.load_events_for_range();
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
    for &(n, c) in astronomy::BODY_COLORS {
        if n == name { return c.to_string(); }
    }
    "888888".to_string()
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
    // Strip HTML tags if it looks like HTML
    let desc = if desc.trim_start().starts_with('<') {
        let re = regex::Regex::new(r"<[^>]+>").unwrap();
        re.replace_all(&desc, " ")
            .replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
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

fn shellexpand(path: &str) -> String {
    if path.starts_with('~') {
        let home = config::home_dir();
        format!("{}{}", home.display(), &path[1..])
    } else {
        path.to_string()
    }
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

fn main() {
    Crust::init();
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

            // Check notifications
            let default_alarm = app.config.get_i64("notifications.default_alarm", 15);
            notifications::check_and_notify(&app.db, default_alarm);

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
