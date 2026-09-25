//! Where tock keeps its calendars and events.
//!
//! Two engines speak the same SQL here: SQLite, which is the default,
//! and ferrite, the Fe₂O₃ database that keeps its tables in memory.
//! `database: ferrite` in `~/.tock/config.yml` picks the second. The
//! first time it opens, it copies everything out of `tock.db`, which is
//! left as it was.

use rusqlite::{types::ValueRef, Connection};
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
// Errors, from either engine
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    Sqlite(rusqlite::Error),
    Ferrite(ferrite::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Sqlite(e) => write!(f, "{e}"),
            Error::Ferrite(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self { Error::Sqlite(e) }
}

impl From<ferrite::Error> for Error {
    fn from(e: ferrite::Error) -> Self { Error::Ferrite(e) }
}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// The two engines behind one door
// ---------------------------------------------------------------------------

enum Store {
    Sqlite(Connection),
    Ferrite(ferrite::Db),
}

impl Store {
    /// Run a statement that changes something. Gives how many rows.
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<usize> {
        match self {
            Store::Sqlite(conn) => {
                Ok(conn.execute(sql, rusqlite::params_from_iter(params.iter().map(to_sqlite)))?)
            }
            Store::Ferrite(db) => Ok(db.execute(sql, params)?.changed()),
        }
    }

    /// Run a statement that reads, and give every row back.
    fn rows(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        match self {
            Store::Sqlite(conn) => {
                let mut stmt = conn.prepare_cached(sql)?;
                let width = stmt.column_count();
                let mut rows = stmt.query(rusqlite::params_from_iter(params.iter().map(to_sqlite)))?;
                let mut out = Vec::new();
                while let Some(row) = rows.next()? {
                    let mut one = Vec::with_capacity(width);
                    for i in 0..width {
                        one.push(from_sqlite(row.get_ref(i)?));
                    }
                    out.push(one);
                }
                Ok(out)
            }
            Store::Ferrite(db) => Ok(db.query(sql, params)?.rows().to_vec()),
        }
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
        match self {
            Store::Sqlite(conn) => Ok(conn.execute_batch(sql)?),
            Store::Ferrite(db) => Ok(db.execute_batch(sql)?),
        }
    }

    fn last_id(&self) -> i64 {
        match self {
            Store::Sqlite(conn) => conn.last_insert_rowid(),
            Store::Ferrite(db) => db.last_insert_key(),
        }
    }
}

fn to_sqlite(v: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as S;
    match v {
        Value::Null => S::Null,
        Value::Int(i) => S::Integer(*i),
        Value::Real(r) => S::Real(*r),
        Value::Text(t) => S::Text(t.clone()),
        Value::Blob(b) => S::Blob(b.clone()),
    }
}

fn from_sqlite(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Int(i),
        ValueRef::Real(r) => Value::Real(r),
        ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
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

/// The tables, in an order that puts every calendar before the events
/// that point at it.
const TABLES: &[&str] = &[
    "schema_version",
    "calendars",
    "events",
    "settings",
    "weather_cache",
    "astronomy_cache",
    "notification_log",
];

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
    /// Open (or create) the database, apply the schema, and make sure a
    /// default "Personal" calendar exists. `db_path` is the SQLite file;
    /// ferrite keeps its files in a directory next to it.
    pub fn new(db_path: Option<&str>) -> Result<Self> {
        let path = match db_path {
            Some(p) => PathBuf::from(p),
            None => {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                let dir = PathBuf::from(home).join(".tock");
                std::fs::create_dir_all(&dir).ok();
                dir.join("tock.db")
            }
        };
        let engine = crate::config::Config::new().get_str("database", "sqlite");
        Database::open(&path, &engine)
    }

    /// Open the SQLite file at `path` with the engine named: `sqlite`,
    /// or `ferrite`, whose files go in a directory next to it.
    pub fn open(path: &Path, engine: &str) -> Result<Self> {
        let (store, fresh) = if engine == "ferrite" {
            let dir = path.with_extension("ferrite");
            let fresh = !dir.join("log").exists();
            (Store::Ferrite(ferrite::Db::open_with(&dir, ferrite::Durability::Full)?), fresh)
        } else {
            let conn = Connection::open(&path)?;
            // WAL mode + 5 s busy timeout
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "busy_timeout", 5000)?;
            conn.pragma_update(None, "foreign_keys", "ON")?;
            (Store::Sqlite(conn), false)
        };

        let db = Database { store: Mutex::new(store) };
        db.create_schema()?;
        if fresh && path.exists() {
            db.import_from_sqlite(path)?;
        }
        db.ensure_default_calendar()?;
        Ok(db)
    }

    /// Which engine this is, for the status line.
    pub fn engine(&self) -> &'static str {
        match *self.store() {
            Store::Sqlite(_) => "sqlite",
            Store::Ferrite(_) => "ferrite",
        }
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

    /// Copy every row of every table out of the SQLite file into a
    /// ferrite that has just been made. The file is only read.
    fn import_from_sqlite(&self, path: &Path) -> Result<()> {
        let src = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let mut s = self.store();
        s.run("BEGIN", &[])?;
        for table in TABLES {
            let mut stmt = src.prepare(&format!("SELECT * FROM {table}"))?;
            let names: Vec<String> = stmt.column_names().iter().map(|n| n.to_string()).collect();
            let slots: Vec<String> = (1..=names.len()).map(|i| format!("?{i}")).collect();
            let insert = format!(
                "INSERT OR REPLACE INTO {table} ({}) VALUES ({})",
                names.join(", "),
                slots.join(", ")
            );
            let kinds = column_kinds(&s, table, &names);
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let mut vals = Vec::with_capacity(names.len());
                for (i, kind) in kinds.iter().enumerate() {
                    vals.push(coerce(from_sqlite(row.get_ref(i)?), *kind));
                }
                s.run(&insert, &vals)?;
            }
        }
        s.run("COMMIT", &[])?;
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
        Ok(s.last_id())
    }

    pub fn update_calendar_color(&self, id: i64, color: i64) -> Result<()> {
        self.store().run("UPDATE calendars SET color = ?1 WHERE id = ?2", vals![color, id])?;
        Ok(())
    }

    pub fn toggle_calendar_enabled(&self, id: i64) -> Result<()> {
        self.store().run("UPDATE calendars SET enabled = 1 - enabled WHERE id = ?1", vals![id])?;
        Ok(())
    }

    pub fn delete_calendar_with_events(&self, id: i64) -> Result<()> {
        let mut s = self.store();
        s.run("DELETE FROM events WHERE calendar_id = ?1", vals![id])?;
        s.run("DELETE FROM calendars WHERE id = ?1", vals![id])?;
        Ok(())
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

/// What kind each named column of a table holds, so that a value read
/// out of SQLite can be bent to fit. None for a column ferrite does not
/// have, which is left as it is.
fn column_kinds(s: &Store, table: &str, names: &[String]) -> Vec<Option<ferrite::Kind>> {
    let Store::Ferrite(db) = s else { return vec![None; names.len()] };
    let Ok(id) = db.table_id(table) else { return vec![None; names.len()] };
    let t = db.table(id);
    names
        .iter()
        .map(|n| {
            if n == t.key_name() {
                Some(ferrite::Kind::Int)
            } else {
                t.column_of(n).map(|i| t.columns()[i].kind)
            }
        })
        .collect()
}

/// SQLite keeps whatever it was given; ferrite keeps what the column
/// says. A "0" in an INTEGER column becomes 0, a 3 in a TEXT column
/// becomes "3".
fn coerce(v: Value, kind: Option<ferrite::Kind>) -> Value {
    use ferrite::Kind;
    match (kind, v) {
        (Some(Kind::Int), Value::Text(t)) => t.trim().parse().map(Value::Int).unwrap_or(Value::Text(t)),
        (Some(Kind::Int), Value::Real(r)) if r.fract() == 0.0 => Value::Int(r as i64),
        (Some(Kind::Real), Value::Int(i)) => Value::Real(i as f64),
        (Some(Kind::Real), Value::Text(t)) => t.trim().parse().map(Value::Real).unwrap_or(Value::Text(t)),
        (Some(Kind::Text), Value::Int(i)) => Value::Text(i.to_string()),
        (Some(Kind::Text), Value::Real(r)) => Value::Text(r.to_string()),
        (_, v) => v,
    }
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
    //! The same calls on both engines have to give the same answers.
    use super::*;

    struct Dir(PathBuf);
    impl Dir {
        fn new(what: &str) -> Dir {
            let p = std::env::temp_dir().join(format!("tock-db-{what}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
        fn db(&self) -> PathBuf { self.0.join("tock.db") }
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

    /// Everything a reader can see, as text, so two engines can be
    /// compared in one line.
    fn picture(db: &Database) -> String {
        let cals = db.get_calendars(false).unwrap();
        let events = db.get_events_in_range(0, i64::MAX).unwrap();
        let weather = db.get_weather_cache().unwrap();
        format!("{cals:?}\n{events:?}\n{weather:?}\n{}", db.is_notified(1, 15).unwrap())
    }

    #[test]
    fn both_engines_tell_the_same_story() {
        let dir = Dir::new("both");
        // Build it up in SQLite first.
        let sqlite = Database::open(&dir.db(), "sqlite").unwrap();
        assert_eq!(sqlite.engine(), "sqlite");
        {
            let mut s = sqlite.store();
            s.run("INSERT INTO calendars (name, source_type, color, enabled, created_at) VALUES ('Work', 'google', 12, 1, 5)", &[]).unwrap();
            s.run("INSERT INTO calendars (name, source_type, color, enabled, created_at) VALUES ('Off', 'local', 3, 0, 5)", &[]).unwrap();
        }
        for n in 1..=8 {
            sqlite.save_event(&event(1 + n % 2, n)).unwrap();
        }
        sqlite.set_weather_cache("{\"t\": 17}").unwrap();
        sqlite.log_notification(1, 15).unwrap();

        // Then ferrite copies it in.
        let ferrite = Database::open(&dir.db(), "ferrite").unwrap();
        assert_eq!(ferrite.engine(), "ferrite");
        assert_eq!(picture(&ferrite), picture(&sqlite));

        // And the same changes on both keep them the same.
        for db in [&sqlite, &ferrite] {
            let id = db.save_event(&event(1, 9)).unwrap();
            assert_eq!(id, 9, "{}: the same key on both", db.engine());
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
            db.log_notification(1, 15).unwrap();
            db.clean_old_notifications().unwrap();
            assert!(db.get_event(3).unwrap().is_some_and(|e| e.title == "changed"));
            assert!(db.get_event(2).unwrap().is_none());
            db.delete_calendar_with_events(2).unwrap();
        }
        assert_eq!(picture(&ferrite), picture(&sqlite));
        assert_eq!(picture(&ferrite).matches("Event {").count(), 3, "events 3, 4 and 9 are left");

        // Opening ferrite again does not copy a second time.
        drop(ferrite);
        let again = Database::open(&dir.db(), "ferrite").unwrap();
        assert_eq!(picture(&again), picture(&sqlite));
    }

    /// The real thing, when it is there: this machine's own tock.db
    /// copied into ferrite and read back the same. Run by hand with
    /// `cargo test --release -- --ignored`.
    #[test]
    #[ignore]
    fn the_real_database_reads_the_same_through_both() {
        let home = std::env::var("HOME").unwrap();
        let real = PathBuf::from(home).join(".tock").join("tock.db");
        if !real.exists() { return; }
        let dir = Dir::new("real");
        std::fs::copy(&real, dir.db()).unwrap();
        let sqlite = Database::open(&dir.db(), "sqlite").unwrap();
        let t0 = std::time::Instant::now();
        let ferrite = Database::open(&dir.db(), "ferrite").unwrap();
        let took = t0.elapsed();
        // Compared field by field, and only the ids and field names are
        // printed: what is in there is the user's own.
        let by_id = |db: &Database| {
            let mut c = db.get_calendars(false).unwrap();
            c.sort_by_key(|c| c.id);
            let mut e = db.get_events_in_range(0, i64::MAX).unwrap();
            e.sort_by_key(|e| e.id);
            (c, e)
        };
        let (sc, se) = by_id(&sqlite);
        let (fc, fe) = by_id(&ferrite);
        assert_eq!(sc.len(), fc.len(), "calendars");
        for (a, b) in sc.iter().zip(&fc) {
            let mut wrong = Vec::new();
            if a.id != b.id { wrong.push("id"); }
            if a.name != b.name { wrong.push("name"); }
            if a.source_type != b.source_type { wrong.push("source_type"); }
            if a.source_config != b.source_config { wrong.push("source_config"); }
            if a.color != b.color { wrong.push("color"); }
            if a.enabled != b.enabled { wrong.push("enabled"); }
            assert!(wrong.is_empty(), "calendar {} differs in {wrong:?}", a.id);
        }
        assert_eq!(se.len(), fe.len(), "events");
        for (a, b) in se.iter().zip(&fe) {
            let mut wrong = Vec::new();
            if a.id != b.id { wrong.push("id"); }
            if a.calendar_id != b.calendar_id { wrong.push("calendar_id"); }
            if a.external_id != b.external_id { wrong.push("external_id"); }
            if a.title != b.title { wrong.push("title"); }
            if a.description != b.description { wrong.push("description"); }
            if a.location != b.location { wrong.push("location"); }
            if a.start_time != b.start_time { wrong.push("start_time"); }
            if a.end_time != b.end_time { wrong.push("end_time"); }
            if a.all_day != b.all_day { wrong.push("all_day"); }
            if a.timezone != b.timezone { wrong.push("timezone"); }
            if a.recurrence_rule != b.recurrence_rule { wrong.push("recurrence_rule"); }
            if a.series_master_id != b.series_master_id { wrong.push("series_master_id"); }
            if a.status != b.status { wrong.push("status"); }
            if a.organizer != b.organizer { wrong.push("organizer"); }
            if a.attendees != b.attendees { wrong.push("attendees"); }
            if a.my_status != b.my_status { wrong.push("my_status"); }
            if a.alarms != b.alarms { wrong.push("alarms"); }
            if a.metadata != b.metadata { wrong.push("metadata"); }
            if a.calendar_name != b.calendar_name { wrong.push("calendar_name"); }
            if a.calendar_color != b.calendar_color { wrong.push("calendar_color"); }
            assert!(wrong.is_empty(), "event {} differs in {wrong:?}", a.id);
        }
        assert_eq!(sqlite.get_weather_cache().unwrap(), ferrite.get_weather_cache().unwrap());
        eprintln!(
            "{} calendars and {} events copied in and read back the same, {:.0} ms",
            fc.len(),
            fe.len(),
            took.as_secs_f64() * 1e3
        );
    }
}

#[cfg(test)]
mod startup {
    //! How long opening takes, on copies of this machine's own files.
    //! Run by hand with `cargo test --release startup -- --ignored --nocapture`.
    use super::*;

    #[test]
    #[ignore]
    fn how_long_an_open_takes() {
        let home = std::env::var("HOME").unwrap();
        let real = PathBuf::from(home).join(".tock");
        if !real.join("tock.db").exists() || !real.join("tock.ferrite").exists() { return; }
        let dir = std::env::temp_dir().join(format!("tock-open-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("tock.ferrite")).unwrap();
        std::fs::copy(real.join("tock.db"), dir.join("tock.db")).unwrap();
        for f in ["log", "snapshot"] {
            let _ = std::fs::copy(real.join("tock.ferrite").join(f), dir.join("tock.ferrite").join(f));
        }
        for engine in ["sqlite", "ferrite"] {
            let mut times = Vec::new();
            for _ in 0..5 {
                let t0 = std::time::Instant::now();
                let db = Database::open(&dir.join("tock.db"), engine).unwrap();
                let n = db.get_events_in_range(0, i64::MAX).unwrap().len();
                times.push((t0.elapsed().as_secs_f64() * 1e3, n));
            }
            times.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            eprintln!("{engine}: open and read every event, median of 5: {:.1} ms ({} events)", times[2].0, times[2].1);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
