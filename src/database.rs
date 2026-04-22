use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

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
    pub sync_token: Option<String>,
    pub last_synced_at: Option<i64>,
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
// Database wrapper
// ---------------------------------------------------------------------------

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    /// Open (or create) the database at `db_path`, apply schema, and ensure a
    /// default "Personal" calendar exists.
    pub fn new(db_path: Option<&str>) -> rusqlite::Result<Self> {
        let path = match db_path {
            Some(p) => PathBuf::from(p),
            None => {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                let dir = PathBuf::from(home).join(".tock");
                std::fs::create_dir_all(&dir).ok();
                dir.join("tock.db")
            }
        };

        let conn = Connection::open(&path)?;

        // WAL mode + 5 s busy timeout
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let db = Database {
            conn: Mutex::new(conn),
        };
        db.create_schema()?;
        db.ensure_default_calendar()?;
        Ok(db)
    }

    // -----------------------------------------------------------------------
    // Schema
    // -----------------------------------------------------------------------

    fn create_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
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

            CREATE INDEX IF NOT EXISTS idx_calendars_enabled
                ON calendars(enabled);

            CREATE INDEX IF NOT EXISTS idx_events_calendar
                ON events(calendar_id);

            CREATE INDEX IF NOT EXISTS idx_events_start
                ON events(start_time);

            CREATE INDEX IF NOT EXISTS idx_events_end
                ON events(end_time);

            CREATE INDEX IF NOT EXISTS idx_events_range
                ON events(start_time, end_time);

            CREATE INDEX IF NOT EXISTS idx_events_external
                ON events(calendar_id, external_id);
            ",
        )?;

        // Record schema version 1 if not already present.
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM schema_version WHERE version = 1",
            [],
            |r| r.get(0),
        )?;
        if !exists {
            conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (1, ?1)",
                params![now_secs()],
            )?;
        }

        Ok(())
    }

    fn ensure_default_calendar(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM calendars", [], |r| r.get(0))?;
        if count == 0 {
            conn.execute(
                "INSERT INTO calendars (name, source_type, color, enabled, created_at)
                 VALUES ('Personal', 'local', 39, 1, ?1)",
                params![now_secs()],
            )?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Event queries
    // -----------------------------------------------------------------------

    /// Return all events whose time range overlaps `[start_ts, end_ts)` and
    /// that belong to an enabled calendar.
    pub fn get_events_in_range(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> rusqlite::Result<Vec<Event>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.calendar_id, e.external_id, e.title, e.description,
                    e.location, e.start_time, e.end_time, e.all_day, e.timezone,
                    e.recurrence_rule, e.series_master_id, e.status, e.organizer,
                    e.attendees, e.my_status, e.alarms, e.metadata,
                    c.name, c.color
             FROM events e
             JOIN calendars c ON c.id = e.calendar_id
             WHERE c.enabled = 1
               AND e.start_time < ?2
               AND (e.end_time IS NULL OR e.end_time > ?1)
             ORDER BY e.start_time",
        )?;
        let rows = stmt.query_map(params![start_ts, end_ts], row_to_event)?;
        rows.collect()
    }

    /// Convenience: events for a single calendar date.
    pub fn get_events_for_date(
        &self,
        year: i32,
        month: u32,
        day: u32,
    ) -> rusqlite::Result<Vec<Event>> {
        let start = date_to_ts(year, month, day);
        let end = start + 86400;
        self.get_events_in_range(start, end)
    }

    // -----------------------------------------------------------------------
    // Event mutations
    // -----------------------------------------------------------------------

    /// Insert or update an event. Returns the row id.
    pub fn save_event(&self, data: &EventData) -> rusqlite::Result<i64> {
        let conn = self.conn.lock().unwrap();
        let now = now_secs();

        if let Some(id) = data.id {
            conn.execute(
                "UPDATE events SET
                    calendar_id = ?1, external_id = ?2, title = ?3,
                    description = ?4, location = ?5, start_time = ?6,
                    end_time = ?7, all_day = ?8, timezone = ?9,
                    recurrence_rule = ?10, series_master_id = ?11,
                    status = ?12, organizer = ?13, attendees = ?14,
                    my_status = ?15, alarms = ?16, metadata = ?17,
                    updated_at = ?18
                 WHERE id = ?19",
                params![
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
            conn.execute(
                "INSERT INTO events
                    (calendar_id, external_id, title, description, location,
                     start_time, end_time, all_day, timezone, recurrence_rule,
                     series_master_id, status, organizer, attendees, my_status,
                     alarms, metadata, created_at, updated_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                params![
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
            Ok(conn.last_insert_rowid())
        }
    }

    pub fn delete_event(&self, id: i64) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM events WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// Delete the master row and all expanded occurrences of a series. Pass
    /// either the master's id or any occurrence's id — the method resolves to
    /// the real master via series_master_id when needed. Returns the number
    /// of rows removed.
    pub fn delete_event_series(&self, id: i64) -> rusqlite::Result<usize> {
        let conn = self.conn.lock().unwrap();
        // Resolve to the master id: if this row has series_master_id set,
        // that points at the master; otherwise this row IS the master.
        let master_id: i64 = conn
            .query_row(
                "SELECT COALESCE(series_master_id, id) FROM events WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap_or(id);
        let mut removed = 0usize;
        removed += conn.execute(
            "DELETE FROM events WHERE series_master_id = ?1",
            params![master_id],
        )?;
        removed += conn.execute(
            "DELETE FROM events WHERE id = ?1",
            params![master_id],
        )?;
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Calendar CRUD
    // -----------------------------------------------------------------------

    pub fn get_calendars(&self, enabled_only: bool) -> rusqlite::Result<Vec<Calendar>> {
        let conn = self.conn.lock().unwrap();
        let sql = if enabled_only {
            "SELECT id, name, source_type, source_config, color, enabled,
                    sync_token, last_synced_at
             FROM calendars WHERE enabled = 1 ORDER BY name"
        } else {
            "SELECT id, name, source_type, source_config, color, enabled,
                    sync_token, last_synced_at
             FROM calendars ORDER BY name"
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(Calendar {
                id: row.get(0)?,
                name: row.get(1)?,
                source_type: row.get(2)?,
                source_config: row.get(3)?,
                color: row.get(4)?,
                enabled: row.get::<_, i64>(5)? != 0,
                sync_token: row.get(6)?,
                last_synced_at: row.get(7)?,
            })
        })?;
        rows.collect()
    }

    /// Insert or update a calendar. Returns the row id.
    pub fn save_calendar(&self, cal: &Calendar) -> rusqlite::Result<i64> {
        let conn = self.conn.lock().unwrap();
        if cal.id > 0 {
            conn.execute(
                "UPDATE calendars SET
                    name = ?1, source_type = ?2, source_config = ?3,
                    color = ?4, enabled = ?5, sync_token = ?6,
                    last_synced_at = ?7
                 WHERE id = ?8",
                params![
                    cal.name,
                    cal.source_type,
                    cal.source_config,
                    cal.color,
                    cal.enabled as i64,
                    cal.sync_token,
                    cal.last_synced_at,
                    cal.id,
                ],
            )?;
            Ok(cal.id)
        } else {
            conn.execute(
                "INSERT INTO calendars
                    (name, source_type, source_config, color, enabled, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    cal.name,
                    cal.source_type,
                    cal.source_config,
                    cal.color,
                    cal.enabled as i64,
                    now_secs(),
                ],
            )?;
            Ok(conn.last_insert_rowid())
        }
    }

    pub fn update_calendar_color(&self, id: i64, color: i64) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calendars SET color = ?1 WHERE id = ?2",
            params![color, id],
        )?;
        Ok(())
    }

    pub fn toggle_calendar_enabled(&self, id: i64) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE calendars SET enabled = 1 - enabled WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn delete_calendar_with_events(&self, id: i64) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM events WHERE calendar_id = ?1", params![id])?;
        conn.execute("DELETE FROM calendars WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn update_calendar_sync(
        &self,
        id: i64,
        last_synced_at: i64,
        source_config: Option<&str>,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        if let Some(cfg) = source_config {
            conn.execute(
                "UPDATE calendars SET last_synced_at = ?1, source_config = ?2 WHERE id = ?3",
                params![last_synced_at, cfg, id],
            )?;
        } else {
            conn.execute(
                "UPDATE calendars SET last_synced_at = ?1 WHERE id = ?2",
                params![last_synced_at, id],
            )?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Settings
    // -----------------------------------------------------------------------

    pub fn get_setting(&self, key: &str) -> rusqlite::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM settings WHERE key = ?1",
            params![key],
            |r| r.get(0),
        )
        .optional()
    }

    pub fn set_setting(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO settings (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = ?3",
            params![key, value, now_secs()],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Existence / duplicate checks
    // -----------------------------------------------------------------------

    pub fn event_exists(
        &self,
        calendar_id: i64,
        external_id: &str,
    ) -> rusqlite::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events
             WHERE calendar_id = ?1 AND external_id = ?2",
            params![calendar_id, external_id],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check whether an event with the same title exists within 60 s of
    /// `start_time`.
    pub fn event_duplicate(
        &self,
        title: &str,
        start_time: i64,
    ) -> rusqlite::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events
             WHERE title = ?1
               AND start_time BETWEEN ?2 AND ?3",
            params![title, start_time - 60, start_time + 60],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn find_event_by_external_id(
        &self,
        calendar_id: i64,
        external_id: &str,
    ) -> rusqlite::Result<Option<Event>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT e.id, e.calendar_id, e.external_id, e.title, e.description,
                    e.location, e.start_time, e.end_time, e.all_day, e.timezone,
                    e.recurrence_rule, e.series_master_id, e.status, e.organizer,
                    e.attendees, e.my_status, e.alarms, e.metadata,
                    c.name, c.color
             FROM events e
             JOIN calendars c ON c.id = e.calendar_id
             WHERE e.calendar_id = ?1 AND e.external_id = ?2",
            params![calendar_id, external_id],
            row_to_event,
        )
        .optional()
    }

    pub fn delete_event_by_external_id(
        &self,
        calendar_id: i64,
        external_id: &str,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM events WHERE calendar_id = ?1 AND external_id = ?2",
            params![calendar_id, external_id],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Sync helper
    // -----------------------------------------------------------------------

    /// Insert, update, or skip an event coming from a remote sync source.
    pub fn upsert_synced_event(
        &self,
        calendar_id: i64,
        data: &EventData,
    ) -> rusqlite::Result<SyncResult> {
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

                let update = EventData {
                    id: Some(ev.id),
                    calendar_id,
                    ..data.clone()
                };
                self.save_event(&update)?;
                Ok(SyncResult::Updated)
            }
            None => {
                let insert = EventData {
                    id: None,
                    calendar_id,
                    ..data.clone()
                };
                self.save_event(&insert)?;
                Ok(SyncResult::New)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Notification log
    // -----------------------------------------------------------------------

    /// Remove notification entries older than 24 hours.
    pub fn clean_old_notifications(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        let cutoff = now_secs() - 86400;
        conn.execute(
            "DELETE FROM notification_log WHERE notified_at < ?1",
            params![cutoff],
        )?;
        Ok(())
    }

    /// Check whether a notification has already been sent for (event_id, alarm_offset).
    pub fn is_notified(&self, event_id: i64, alarm_offset: i64) -> rusqlite::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM notification_log
             WHERE event_id = ?1 AND alarm_offset = ?2",
            params![event_id, alarm_offset],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// Record that a notification was sent for (event_id, alarm_offset).
    pub fn log_notification(&self, event_id: i64, alarm_offset: i64) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO notification_log (event_id, alarm_offset, notified_at)
             VALUES (?1, ?2, ?3)",
            params![event_id, alarm_offset, now_secs()],
        )?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Weather cache
    // -----------------------------------------------------------------------

    /// Read the cached weather forecast JSON string and its timestamp.
    /// Returns `None` if no row exists.
    pub fn get_weather_cache(&self) -> rusqlite::Result<Option<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT data, fetched_at FROM weather_cache WHERE date = 'forecast' LIMIT 1",
            [],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()
    }

    /// Write (insert or replace) the cached weather forecast.
    pub fn set_weather_cache(&self, json: &str) -> rusqlite::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO weather_cache (date, hour, data, fetched_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params!["forecast", "00", json, now_secs()],
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
    //算法: days from civil (Howard Hinnant)
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

/// Map a `rusqlite::Row` to an `Event`.
fn row_to_event(row: &rusqlite::Row) -> rusqlite::Result<Event> {
    Ok(Event {
        id: row.get(0)?,
        calendar_id: row.get(1)?,
        external_id: row.get(2)?,
        title: row.get(3)?,
        description: row.get(4)?,
        location: row.get(5)?,
        start_time: row.get(6)?,
        end_time: row.get::<_, Option<i64>>(7)?.unwrap_or(0),
        all_day: row.get::<_, i64>(8)? != 0,
        timezone: row.get(9)?,
        recurrence_rule: row.get(10)?,
        series_master_id: row.get(11)?,
        status: row.get::<_, Option<String>>(12)?.unwrap_or_else(|| "confirmed".into()),
        organizer: row.get(13)?,
        attendees: parse_json_opt(row.get::<_, Option<String>>(14)?),
        my_status: row.get(15)?,
        alarms: parse_json_opt(row.get::<_, Option<String>>(16)?),
        metadata: parse_json_opt(row.get::<_, Option<String>>(17)?),
        calendar_name: row.get(18)?,
        calendar_color: row.get(19)?,
    })
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
