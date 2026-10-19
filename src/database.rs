//! Where tock keeps its calendars and events: ferrite, the Fe₂O₃
//! database that keeps its tables in memory, in `~/.tock/tock.ferrite/`.
//!
//! Only one program may open the store, so tock also writes
//! `~/.tock/calendars` for others to read (kastrup picks a calendar
//! from it).

use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use ferrite::Value;

// ---------------------------------------------------------------------------
// Structs & enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Event {
    pub id: i64,
    pub calendar_id: i64,
    pub external_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start_time: i64,
    pub end_time: i64,
    pub all_day: bool,
    pub timezone: Option<String>,
    pub recurrence_rule: Option<String>,
    pub series_master_id: Option<i64>,
    pub status: String,
    pub organizer: Option<String>,
    pub attendees: Option<JsonValue>,
    pub my_status: Option<String>,
    pub alarms: Option<JsonValue>,
    pub metadata: Option<JsonValue>,
    pub calendar_name: String,
    pub calendar_color: i64,
}

impl Event {
    /// The event as the shape that gets saved and sent to a server.
    pub fn data(&self) -> EventData {
        EventData {
            id: Some(self.id),
            calendar_id: self.calendar_id,
            external_id: self.external_id.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            location: self.location.clone(),
            start_time: self.start_time,
            end_time: self.end_time,
            all_day: self.all_day,
            timezone: self.timezone.clone(),
            recurrence_rule: self.recurrence_rule.clone(),
            series_master_id: self.series_master_id,
            status: self.status.clone(),
            organizer: self.organizer.clone(),
            attendees: self.attendees.clone(),
            my_status: self.my_status.clone(),
            alarms: self.alarms.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Calendar {
    pub id: i64,
    pub name: String,
    pub source_type: String,
    pub source_config: Option<String>,
    pub color: i64,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct EventData {
    pub id: Option<i64>,
    pub calendar_id: i64,
    pub external_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub location: Option<String>,
    pub start_time: i64,
    pub end_time: i64,
    pub all_day: bool,
    pub timezone: Option<String>,
    pub recurrence_rule: Option<String>,
    pub series_master_id: Option<i64>,
    pub status: String,
    pub organizer: Option<String>,
    pub attendees: Option<JsonValue>,
    pub my_status: Option<String>,
    pub alarms: Option<JsonValue>,
    pub metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncResult {
    New,
    Updated,
    Skipped,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    Ferrite(ferrite::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Ferrite(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ferrite::Error> for Error {
    fn from(e: ferrite::Error) -> Self { Error::Ferrite(e) }
}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

struct Store(ferrite::Db);

impl Store {
    /// Run a statement that changes something. Gives how many rows.
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<usize> {
        Ok(self.0.execute(sql, params)?.changed())
    }

    /// Run a statement that reads, and give every row back.
    fn rows(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        Ok(self.0.query(sql, params)?.rows().to_vec())
    }

    /// The first row, if there is one.
    fn row(&mut self, sql: &str, params: &[Value]) -> Result<Option<Vec<Value>>> {
        Ok(self.rows(sql, params)?.into_iter().next())
    }

    /// One number: the first value of the first row.
    fn count(&mut self, sql: &str, params: &[Value]) -> Result<i64> {
        Ok(self.row(sql, params)?.and_then(|r| r.first().and_then(Value::as_int)).unwrap_or(0))
    }

    fn batch(&mut self, sql: &str) -> Result<()> {
        Ok(self.0.execute_batch(sql)?)
    }

    fn last_id(&self) -> i64 {
        self.0.last_insert_key()
    }
}

/// Anything that can go into a `?` slot.
trait ToVal {
    fn to_val(&self) -> Value;
}

impl ToVal for i64 {
    fn to_val(&self) -> Value { Value::Int(*self) }
}
impl ToVal for bool {
    fn to_val(&self) -> Value { Value::Int(*self as i64) }
}
impl ToVal for str {
    fn to_val(&self) -> Value { Value::Text(self.to_string()) }
}
impl ToVal for String {
    fn to_val(&self) -> Value { Value::Text(self.clone()) }
}
impl<T: ToVal> ToVal for Option<T> {
    fn to_val(&self) -> Value {
        match self {
            Some(v) => v.to_val(),
            None => Value::Null,
        }
    }
}
impl<T: ToVal + ?Sized> ToVal for &T {
    fn to_val(&self) -> Value { (**self).to_val() }
}

macro_rules! vals {
    ($($e:expr),* $(,)?) => { &[$(ToVal::to_val(&$e)),*][..] };
}

/// Reading one row's values by position.
fn int(row: &[Value], i: usize) -> i64 { row.get(i).and_then(Value::as_int).unwrap_or(0) }
fn opt_int(row: &[Value], i: usize) -> Option<i64> { row.get(i).and_then(Value::as_int) }
fn text(row: &[Value], i: usize) -> String { opt_text(row, i).unwrap_or_default() }
fn opt_text(row: &[Value], i: usize) -> Option<String> {
    row.get(i).and_then(|v| v.as_text()).map(str::to_string)
}

// ---------------------------------------------------------------------------
// Database wrapper
// ---------------------------------------------------------------------------

pub struct Database {
    store: Mutex<Store>,
    /// `calendars` beside the store, the list other programs read.
    list: PathBuf,
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS schema_version (
        version    INTEGER PRIMARY KEY,
        applied_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS calendars (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        name           TEXT    NOT NULL,
        source_type    TEXT    NOT NULL,
        source_config  TEXT,
        color          INTEGER DEFAULT 39,
        enabled        INTEGER DEFAULT 1,
        sync_token     TEXT,
        last_synced_at INTEGER,
        created_at     INTEGER
    );

    CREATE TABLE IF NOT EXISTS events (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        calendar_id      INTEGER NOT NULL,
        external_id      TEXT,
        title            TEXT    NOT NULL,
        description      TEXT,
        location         TEXT,
        start_time       INTEGER NOT NULL,
        end_time         INTEGER,
        all_day          INTEGER DEFAULT 0,
        timezone         TEXT,
        recurrence_rule  TEXT,
        series_master_id INTEGER,
        status           TEXT DEFAULT 'confirmed',
        organizer        TEXT,
        attendees        TEXT,
        my_status        TEXT,
        alarms           TEXT,
        metadata         TEXT,
        created_at       INTEGER NOT NULL,
        updated_at       INTEGER NOT NULL,
        FOREIGN KEY(calendar_id) REFERENCES calendars(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS settings (
        key        TEXT PRIMARY KEY,
        value      TEXT NOT NULL,
        updated_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS weather_cache (
        date       TEXT    NOT NULL,
        hour       INTEGER,
        data       TEXT,
        fetched_at INTEGER NOT NULL,
        PRIMARY KEY(date, hour)
    );

    CREATE TABLE IF NOT EXISTS astronomy_cache (
        date            TEXT PRIMARY KEY,
        moon_phase      REAL,
        moon_phase_name TEXT,
        events          TEXT,
        fetched_at      INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS notification_log (
        event_id     INTEGER NOT NULL,
        alarm_offset INTEGER NOT NULL,
        notified_at  INTEGER NOT NULL,
        PRIMARY KEY(event_id, alarm_offset)
    );

    CREATE INDEX IF NOT EXISTS idx_calendars_enabled ON calendars(enabled);
    CREATE INDEX IF NOT EXISTS idx_events_calendar ON events(calendar_id);
    CREATE INDEX IF NOT EXISTS idx_events_start ON events(start_time);
    CREATE INDEX IF NOT EXISTS idx_events_end ON events(end_time);
    CREATE INDEX IF NOT EXISTS idx_events_range ON events(start_time, end_time);
    CREATE INDEX IF NOT EXISTS idx_events_external ON events(calendar_id, external_id);
";

/// An event with its calendar's name and colour, the way every reader
/// wants it.
const EVENT_COLUMNS: &str = "
    SELECT e.id, e.calendar_id, e.external_id, e.title, e.description,
           e.location, e.start_time, e.end_time, e.all_day, e.timezone,
           e.recurrence_rule, e.series_master_id, e.status, e.organizer,
           e.attendees, e.my_status, e.alarms, e.metadata,
           c.name, c.color
    FROM events e
    JOIN calendars c ON c.id = e.calendar_id";

impl Database {
    /// Open (or create) `~/.tock/tock.ferrite`, apply the schema, and
    /// make sure a default "Personal" calendar exists.
    pub fn new() -> Result<Self> {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        Database::open(&PathBuf::from(home).join(".tock").join("tock.ferrite"))
    }

    /// Open the store in the directory `dir`, made when missing.
    pub fn open(dir: &Path) -> Result<Self> {
        let store = Store(ferrite::Db::open_with(dir, ferrite::Durability::Full)?);
        let db = Database { store: Mutex::new(store), list: dir.with_file_name("calendars") };
        db.create_schema()?;
        db.ensure_default_calendar()?;
        db.list_calendars();
        Ok(db)
    }

    fn store(&self) -> std::sync::MutexGuard<'_, Store> {
        self.store.lock().unwrap()
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    fn create_schema(&self) -> Result<()> {
        let mut s = self.store();
        s.batch(SCHEMA)?;
        // Record schema version 1 if not already present.
        let exists = s.count("SELECT COUNT(*) > 0 FROM schema_version WHERE version = 1", &[])?;
        if exists == 0 {
            s.run(
                "INSERT INTO schema_version (version, applied_at) VALUES (1, ?1)",
                vals![now_secs()],
            )?;
        }
        Ok(())
    }

    fn ensure_default_calendar(&self) -> Result<()> {
        let mut s = self.store();
        let count = s.count("SELECT COUNT(*) FROM calendars", &[])?;
        if count == 0 {
            s.run(
                "INSERT INTO calendars (name, source_type, color, enabled, created_at)
                 VALUES ('Personal', 'local', 39, 1, ?1)",
                vals![now_secs()],
            )?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event queries
    // -----------------------------------------------------------------------

    /// Return all events whose time range overlaps `[start_ts, end_ts)` and
    /// that belong to an enabled calendar.
    pub fn get_events_in_range(&self, start_ts: i64, end_ts: i64) -> Result<Vec<Event>> {
        let sql = format!(
            "{EVENT_COLUMNS}
             WHERE c.enabled = 1
               AND e.start_time < ?2
               AND (e.end_time IS NULL OR e.end_time > ?1)
             ORDER BY e.start_time"
        );
        let rows = self.store().rows(&sql, vals![start_ts, end_ts])?;
        Ok(rows.iter().map(|r| row_to_event(r)).collect())
    }

    /// Look up a single event by its local row id.
    pub fn get_event(&self, id: i64) -> Result<Option<Event>> {
        let sql = format!("{EVENT_COLUMNS} WHERE e.id = ?1");
        Ok(self.store().row(&sql, vals![id])?.map(|r| row_to_event(&r)))
    }

    /// Convenience: events for a single calendar date.
    pub fn get_events_for_date(&self, year: i32, month: u32, day: u32) -> Result<Vec<Event>> {
        let start = date_to_ts(year, month, day);
        let end = start + 86400;
        self.get_events_in_range(start, end)
    }

    // -----------------------------------------------------------------------
    // Event mutations
    // -----------------------------------------------------------------------

    /// Insert or update an event. Returns the row id.
    pub fn save_event(&self, data: &EventData) -> Result<i64> {
        let mut s = self.store();
        let now = now_secs();

        if let Some(id) = data.id {
            s.run(
                "UPDATE events SET
                    calendar_id = ?1, external_id = ?2, title = ?3,
                    description = ?4, location = ?5, start_time = ?6,
                    end_time = ?7, all_day = ?8, timezone = ?9,
                    recurrence_rule = ?10, series_master_id = ?11,
                    status = ?12, organizer = ?13, attendees = ?14,
                    my_status = ?15, alarms = ?16, metadata = ?17,
                    updated_at = ?18
                 WHERE id = ?19",
                vals![
                    data.calendar_id,
                    data.external_id,
                    data.title,
                    data.description,
                    data.location,
                    data.start_time,
                    data.end_time,
                    data.all_day as i64,
                    data.timezone,
                    data.recurrence_rule,
                    data.series_master_id,
                    data.status,
                    data.organizer,
                    json_opt_to_string(&data.attendees),
                    data.my_status,
                    json_opt_to_string(&data.alarms),
                    json_opt_to_string(&data.metadata),
                    now,
                    id,
                ],
            )?;
            Ok(id)
        } else {
            s.run(
                "INSERT INTO events
                    (calendar_id, external_id, title, description, location,
                     start_time, end_time, all_day, timezone, recurrence_rule,
                     series_master_id, status, organizer, attendees, my_status,
                     alarms, metadata, created_at, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                vals![
                    data.calendar_id,
                    data.external_id,
                    data.title,
                    data.description,
                    data.location,
                    data.start_time,
                    data.end_time,
                    data.all_day as i64,
                    data.timezone,
                    data.recurrence_rule,
                    data.series_master_id,
                    data.status,
                    data.organizer,
                    json_opt_to_string(&data.attendees),
                    data.my_status,
                    json_opt_to_string(&data.alarms),
                    json_opt_to_string(&data.metadata),
                    now,
                    now,
                ],
            )?;
            Ok(s.last_id())
        }
    }

    pub fn delete_event(&self, id: i64) -> Result<()> {
        self.store().run("DELETE FROM events WHERE id = ?1", vals![id])?;
        Ok(())
    }

    /// Delete the master row and all expanded occurrences of a series. Pass
    /// either the master's id or any occurrence's id; the method resolves to
    /// the real master via series_master_id when needed. Returns the number
    /// of rows removed.
    pub fn delete_event_series(&self, id: i64) -> Result<usize> {
        let mut s = self.store();
        // Resolve to the master id: if this row has series_master_id set,
        // that points at the master; otherwise this row IS the master.
        let master_id = s
            .row("SELECT COALESCE(series_master_id, id) FROM events WHERE id = ?1", vals![id])?
            .map(|r| int(&r, 0))
            .unwrap_or(id);
        let mut removed = 0usize;
        removed += s.run("DELETE FROM events WHERE series_master_id = ?1", vals![master_id])?;
        removed += s.run("DELETE FROM events WHERE id = ?1", vals![master_id])?;
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Calendar CRUD
    // -----------------------------------------------------------------------

    pub fn get_calendars(&self, enabled_only: bool) -> Result<Vec<Calendar>> {
        let sql = if enabled_only {
            "SELECT id, name, source_type, source_config, color, enabled
             FROM calendars WHERE enabled = 1 ORDER BY name"
        } else {
            "SELECT id, name, source_type, source_config, color, enabled
             FROM calendars ORDER BY name"
        };
        let rows = self.store().rows(sql, &[])?;
        Ok(rows
            .iter()
            .map(|r| Calendar {
                id: int(r, 0),
                name: text(r, 1),
                source_type: text(r, 2),
                source_config: opt_text(r, 3),
                color: int(r, 4),
                enabled: int(r, 5) != 0,
            })
            .collect())
    }

    /// Add a calendar and give back its id.
    pub fn add_calendar(&self, name: &str, source_type: &str, source_config: &str, color: i64, enabled: bool) -> Result<i64> {
        let mut s = self.store();
        s.run(
            "INSERT INTO calendars (name, source_type, source_config, color, enabled, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            vals![name, source_type, source_config, color, enabled as i64, now_secs()],
        )?;
        let id = s.last_id();
        drop(s);
        self.list_calendars();
        Ok(id)
    }

    pub fn update_calendar_color(&self, id: i64, color: i64) -> Result<()> {
        self.store().run("UPDATE calendars SET color = ?1 WHERE id = ?2", vals![color, id])?;
        Ok(())
    }

    pub fn toggle_calendar_enabled(&self, id: i64) -> Result<()> {
        self.store().run("UPDATE calendars SET enabled = 1 - enabled WHERE id = ?1", vals![id])?;
        self.list_calendars();
        Ok(())
    }

    pub fn delete_calendar_with_events(&self, id: i64) -> Result<()> {
        let mut s = self.store();
        s.run("DELETE FROM events WHERE calendar_id = ?1", vals![id])?;
        s.run("DELETE FROM calendars WHERE id = ?1", vals![id])?;
        drop(s);
        self.list_calendars();
        Ok(())
    }

    /// Write `calendars` beside the store: one enabled calendar a line,
    /// its id, kind and name split by tabs. Only when the list changed,
    /// and a failed write never stops tock.
    fn list_calendars(&self) {
        let Ok(mut cals) = self.get_calendars(true) else { return };
        cals.sort_by_key(|c| c.id);
        let text: String = cals.iter()
            .map(|c| format!("{}\t{}\t{}\n", c.id, c.source_type, c.name.replace(['\t', '\n'], " ")))
            .collect();
        if std::fs::read_to_string(&self.list).is_ok_and(|old| old == text) { return; }
        let tmp = self.list.with_extension("tmp");
        if std::fs::write(&tmp, &text).is_ok() { let _ = std::fs::rename(&tmp, &self.list); }
    }

    pub fn update_calendar_sync(
        &self,
        id: i64,
        last_synced_at: i64,
        source_config: Option<&str>,
    ) -> Result<()> {
        let mut s = self.store();
        if let Some(cfg) = source_config {
            s.run(
                "UPDATE calendars SET last_synced_at = ?1, source_config = ?2 WHERE id = ?3",
                vals![last_synced_at, cfg, id],
            )?;
        } else {
            s.run(
                "UPDATE calendars SET last_synced_at = ?1 WHERE id = ?2",
                vals![last_synced_at, id],
            )?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Existence / duplicate checks
    // -----------------------------------------------------------------------

    pub fn event_exists(&self, calendar_id: i64, external_id: &str) -> Result<bool> {
        let count = self.store().count(
            "SELECT COUNT(*) FROM events WHERE calendar_id = ?1 AND external_id = ?2",
            vals![calendar_id, external_id],
        )?;
        Ok(count > 0)
    }

    /// Check whether an event with the same title exists within 60 s of
    /// `start_time`.
    pub fn event_duplicate(&self, title: &str, start_time: i64) -> Result<bool> {
        let count = self.store().count(
            "SELECT COUNT(*) FROM events WHERE title = ?1 AND start_time BETWEEN ?2 AND ?3",
            vals![title, start_time - 60, start_time + 60],
        )?;
        Ok(count > 0)
    }

    pub fn find_event_by_external_id(
        &self,
        calendar_id: i64,
        external_id: &str,
    ) -> Result<Option<Event>> {
        let sql = format!("{EVENT_COLUMNS} WHERE e.calendar_id = ?1 AND e.external_id = ?2");
        Ok(self.store().row(&sql, vals![calendar_id, external_id])?.map(|r| row_to_event(&r)))
    }

    pub fn delete_event_by_external_id(&self, calendar_id: i64, external_id: &str) -> Result<usize> {
        self.store().run(
            "DELETE FROM events WHERE calendar_id = ?1 AND external_id = ?2",
            vals![calendar_id, external_id],
        )
    }

    // -----------------------------------------------------------------------
    // Sync helper
    // -----------------------------------------------------------------------

    /// Insert, update, or skip an event coming from a remote sync source.
    pub fn upsert_synced_event(&self, calendar_id: i64, data: &EventData) -> Result<SyncResult> {
        let ext_id = match &data.external_id {
            Some(id) => id.clone(),
            None => return Ok(SyncResult::Skipped),
        };

        let existing = self.find_event_by_external_id(calendar_id, &ext_id)?;

        match existing {
            Some(ev) => {
                // Only update when something actually changed.
                if ev.title == data.title
                    && ev.start_time == data.start_time
                    && ev.end_time == data.end_time
                    && ev.description == data.description
                    && ev.location == data.location
                    && ev.all_day == data.all_day
                    && ev.status == data.status
                {
                    return Ok(SyncResult::Skipped);
                }

                let update = EventData { id: Some(ev.id), calendar_id, ..data.clone() };
                self.save_event(&update)?;
                Ok(SyncResult::Updated)
            }
            None => {
                let insert = EventData { id: None, calendar_id, ..data.clone() };
                self.save_event(&insert)?;
                Ok(SyncResult::New)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Notification log
    // -----------------------------------------------------------------------

    /// Remove notification entries older than 24 hours.
    pub fn clean_old_notifications(&self) -> Result<()> {
        let cutoff = now_secs() - 86400;
        self.store().run("DELETE FROM notification_log WHERE notified_at < ?1", vals![cutoff])?;
        Ok(())
    }

    /// Check whether a notification has already been sent for (event_id, alarm_offset).
    pub fn is_notified(&self, event_id: i64, alarm_offset: i64) -> Result<bool> {
        let count = self.store().count(
            "SELECT COUNT(*) FROM notification_log WHERE event_id = ?1 AND alarm_offset = ?2",
            vals![event_id, alarm_offset],
        )?;
        Ok(count > 0)
    }

    /// Record that a notification was sent for (event_id, alarm_offset).
    pub fn log_notification(&self, event_id: i64, alarm_offset: i64) -> Result<()> {
        self.store().run(
            "INSERT OR IGNORE INTO notification_log (event_id, alarm_offset, notified_at)
             VALUES (?1, ?2, ?3)",
            vals![event_id, alarm_offset, now_secs()],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Weather cache
    // -----------------------------------------------------------------------

    /// Read the cached weather forecast JSON string and its timestamp.
    /// Returns `None` if no row exists.
    pub fn get_weather_cache(&self) -> Result<Option<(String, i64)>> {
        let row = self.store().row(
            "SELECT data, fetched_at FROM weather_cache WHERE date = 'forecast' LIMIT 1",
            &[],
        )?;
        Ok(row.map(|r| (text(&r, 0), int(&r, 1))))
    }

    /// Write (insert or replace) the cached weather forecast.
    pub fn set_weather_cache(&self, json: &str) -> Result<()> {
        // The hour column is an INTEGER; SQLite used to take "00" and
        // store it as 0, and ferrite wants the 0 said outright.
        self.store().run(
            "INSERT OR REPLACE INTO weather_cache (date, hour, data, fetched_at) \
             VALUES (?1, ?2, ?3, ?4)",
            vals!["forecast", 0i64, json, now_secs()],
        )?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Current UNIX timestamp in seconds.
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Convert a calendar date to a UNIX timestamp at midnight UTC.
fn date_to_ts(year: i32, month: u32, day: u32) -> i64 {
    // Days from civil (Howard Hinnant).
    let y = if month <= 2 { year - 1 } else { year } as i64;
    let m = if month <= 2 { month + 9 } else { month - 3 } as i64;
    let d = day as i64;

    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64;
    let doy = (153 * m + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;

    days * 86400
}

/// Map one row of `EVENT_COLUMNS` to an `Event`.
fn row_to_event(r: &[Value]) -> Event {
    Event {
        id: int(r, 0),
        calendar_id: int(r, 1),
        external_id: opt_text(r, 2),
        title: text(r, 3),
        description: opt_text(r, 4),
        location: opt_text(r, 5),
        start_time: int(r, 6),
        end_time: opt_int(r, 7).unwrap_or(0),
        all_day: int(r, 8) != 0,
        timezone: opt_text(r, 9),
        recurrence_rule: opt_text(r, 10),
        series_master_id: opt_int(r, 11),
        status: opt_text(r, 12).unwrap_or_else(|| "confirmed".into()),
        organizer: opt_text(r, 13),
        attendees: parse_json_opt(opt_text(r, 14)),
        my_status: opt_text(r, 15),
        alarms: parse_json_opt(opt_text(r, 16)),
        metadata: parse_json_opt(opt_text(r, 17)),
        calendar_name: text(r, 18),
        calendar_color: int(r, 19),
    }
}

/// Parse a JSON string into a `serde_json::Value`, returning `None` on
/// parse failure or missing input.
fn parse_json_opt(s: Option<String>) -> Option<JsonValue> {
    s.and_then(|v| serde_json::from_str(&v).ok())
}

/// Serialize an optional `serde_json::Value` to a `String` suitable for
/// storage, or `None` if absent.
fn json_opt_to_string(v: &Option<JsonValue>) -> Option<String> {
    v.as_ref().map(|j| j.to_string())
}

#[cfg(test)]
mod tests {
    //! What tock writes is what it reads back, after a reopen too.
    use super::*;

    struct Dir(PathBuf);
    impl Dir {
        fn new(what: &str) -> Dir {
            let p = std::env::temp_dir().join(format!("tock-db-{what}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
        fn store(&self) -> PathBuf { self.0.join("tock.ferrite") }
    }
    impl Drop for Dir {
        fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
    }

    fn event(cal: i64, n: i64) -> EventData {
        EventData {
            id: None,
            calendar_id: cal,
            external_id: if n % 3 == 0 { None } else { Some(format!("ext-{n}")) },
            title: format!("event {n}"),
            description: Some("a note, with 'quotes'".into()),
            location: None,
            start_time: 1_700_000_000 + n * 3600,
            end_time: 1_700_000_000 + n * 3600 + 1800,
            all_day: n % 4 == 0,
            timezone: Some("Europe/Oslo".into()),
            recurrence_rule: None,
            series_master_id: if (6..=8).contains(&n) { Some(1) } else { None },
            status: "confirmed".into(),
            organizer: None,
            attendees: Some(serde_json::json!([{"name": "Alice"}])),
            my_status: Some("accepted".into()),
            alarms: Some(serde_json::json!([15, 60])),
            metadata: None,
        }
    }

    /// Everything a reader can see, as text, so two opens can be
    /// compared in one line.
    fn picture(db: &Database) -> String {
        let cals = db.get_calendars(false).unwrap();
        let events = db.get_events_in_range(0, i64::MAX).unwrap();
        let weather = db.get_weather_cache().unwrap();
        format!("{cals:?}\n{events:?}\n{weather:?}\n{}", db.is_notified(1, 15).unwrap())
    }

    #[test]
    fn every_change_is_there_after_a_reopen() {
        let dir = Dir::new("reopen");
        let db = Database::open(&dir.store()).unwrap();
        {
            let mut s = db.store();
            s.run("INSERT INTO calendars (name, source_type, color, enabled, created_at) VALUES ('Work', 'google', 12, 1, 5)", &[]).unwrap();
            s.run("INSERT INTO calendars (name, source_type, color, enabled, created_at) VALUES ('Off', 'local', 3, 0, 5)", &[]).unwrap();
        }
        for n in 1..=8 {
            db.save_event(&event(1 + n % 2, n)).unwrap();
        }
        db.set_weather_cache("{\"t\": 17}").unwrap();
        db.log_notification(1, 15).unwrap();
        let id = db.save_event(&event(1, 9)).unwrap();
        assert_eq!(id, 9);
        let mut ev = event(1, 3);
        ev.id = Some(3);
        ev.title = "changed".into();
        db.save_event(&ev).unwrap();
        assert_eq!(db.upsert_synced_event(1, &event(1, 4)).unwrap(), SyncResult::Skipped);
        assert_eq!(db.upsert_synced_event(2, &event(2, 40)).unwrap(), SyncResult::New);
        assert!(db.event_exists(2, "ext-40").unwrap());
        assert!(!db.event_exists(2, "ext-41").unwrap());
        assert!(db.event_duplicate("event 5", 1_700_000_000 + 5 * 3600 + 30).unwrap());
        assert_eq!(db.delete_event_series(7).unwrap(), 4, "the master and its three occurrences");
        db.delete_event(2).unwrap();
        db.toggle_calendar_enabled(3).unwrap();
        db.update_calendar_color(2, 99).unwrap();
        db.update_calendar_sync(2, 77, Some("{}")).unwrap();
        assert_eq!(db.delete_event_by_external_id(2, "ext-40").unwrap(), 1);
        db.set_weather_cache("{\"t\": 18}").unwrap();
        db.clean_old_notifications().unwrap();
        assert!(db.get_event(3).unwrap().is_some_and(|e| e.title == "changed"));
        assert!(db.get_event(2).unwrap().is_none());
        db.delete_calendar_with_events(2).unwrap();
        let before = picture(&db);
        assert_eq!(before.matches("Event {").count(), 3, "events 3, 4 and 9 are left");
        drop(db);
        assert_eq!(picture(&Database::open(&dir.store()).unwrap()), before);
    }

    #[test]
    fn the_calendar_list_beside_the_store_follows_the_calendars() {
        let dir = Dir::new("list");
        let db = Database::open(&dir.store()).unwrap();
        let list = || std::fs::read_to_string(dir.0.join("calendars")).unwrap();
        assert_eq!(list(), "1\tlocal\tPersonal\n");
        let work = db.add_calendar("Work\tday", "google", "{}", 12, true).unwrap();
        assert_eq!(list(), "1\tlocal\tPersonal\n2\tgoogle\tWork day\n");
        db.toggle_calendar_enabled(1).unwrap();
        assert_eq!(list(), "2\tgoogle\tWork day\n");
        db.delete_calendar_with_events(work).unwrap();
        assert_eq!(list(), "");
    }
}

#[cfg(test)]
mod startup {
    //! How long opening takes, on a copy of this machine's own store.
    //! Run by hand with `cargo test --release startup -- --ignored --nocapture`.
    use super::*;

    #[test]
    #[ignore]
    fn how_long_an_open_takes() {
        let home = std::env::var("HOME").unwrap();
        let real = PathBuf::from(home).join(".tock").join("tock.ferrite");
        if !real.exists() { return; }
        let dir = std::env::temp_dir().join(format!("tock-open-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("tock.ferrite")).unwrap();
        for f in ["log", "snapshot"] {
            let _ = std::fs::copy(real.join(f), dir.join("tock.ferrite").join(f));
        }
        let mut times = Vec::new();
        for _ in 0..5 {
            let t0 = std::time::Instant::now();
            let db = Database::open(&dir.join("tock.ferrite")).unwrap();
            let n = db.get_events_in_range(0, i64::MAX).unwrap().len();
            times.push((t0.elapsed().as_secs_f64() * 1e3, n));
        }
        times.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        eprintln!("open and read every event, median of 5: {:.1} ms ({} events)", times[2].0, times[2].1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
