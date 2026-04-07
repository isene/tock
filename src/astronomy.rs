// astronomy.rs - Moon phases, sun times, and astronomical events for Tock.
// Ported from Timely's astronomy.rb.

use std::f64::consts::PI;

// ── Structs ──────────────────────────────────────────────────────────

pub struct MoonPhase {
    pub illumination: f64,
    pub phase: f64,
    pub phase_name: &'static str,
    pub symbol: &'static str,
    pub phase_index: usize,
}

pub struct NotablePhase {
    pub day: u32,
    pub phase_name: &'static str,
    pub symbol: &'static str,
}

// ── Constants ────────────────────────────────────────────────────────

const SYNODIC_MONTH: f64 = 29.530588853;
const NEW_MOON_EPOCH_JD: f64 = 2451550.1; // Known new moon Jan 6, 2000

const PHASE_NAMES: [&str; 8] = [
    "New Moon",
    "Waxing Crescent",
    "First Quarter",
    "Waxing Gibbous",
    "Full Moon",
    "Waning Gibbous",
    "Last Quarter",
    "Waning Crescent",
];

const PHASE_SYMBOLS: [&str; 8] = [
    "\u{1F311}", // 🌑
    "\u{1F312}", // 🌒
    "\u{1F313}", // 🌓
    "\u{1F314}", // 🌔
    "\u{1F315}", // 🌕
    "\u{1F316}", // 🌖
    "\u{1F317}", // 🌗
    "\u{1F318}", // 🌘
];

pub const PLANET_SYMBOLS: &[(&str, &str)] = &[
    ("mercury", "\u{263F}"),
    ("venus", "\u{2640}"),
    ("mars", "\u{2642}"),
    ("jupiter", "\u{2643}"),
    ("saturn", "\u{2644}"),
];

pub const BODY_COLORS: &[(&str, &str)] = &[
    ("sun", "FFD700"),
    ("moon", "888888"),
    ("mercury", "8F6E54"),
    ("venus", "E6B07C"),
    ("mars", "BC2732"),
    ("jupiter", "C08040"),
    ("saturn", "E8D9A0"),
];

// ── Moon phase calculation ───────────────────────────────────────────

/// Julian date from calendar date (integer arithmetic matching Ruby's Timely).
fn julian_date(y: i32, m: u32, d: u32) -> f64 {
    let y = y as f64;
    let m = m as f64;
    let d = d as f64;
    367.0 * y
        - ((7.0 * (y + ((m + 9.0) / 12.0).floor())) / 4.0).floor()
        + ((275.0 * m) / 9.0).floor()
        + d
        + 1_721_013.5
}

/// Calculate moon phase for a given date.
pub fn moon_phase(year: i32, month: u32, day: u32) -> MoonPhase {
    let jd = julian_date(year, month, day);
    let days_since = jd - NEW_MOON_EPOCH_JD;

    // Normalize to 0.0..1.0 within synodic month
    let mut phase = (days_since / SYNODIC_MONTH) % 1.0;
    if phase < 0.0 {
        phase += 1.0;
    }

    // Illumination follows a cosine curve (0 = new, 0.5 = full)
    let illumination = (1.0 - (phase * 2.0 * PI).cos()) / 2.0;

    // Phase index 0..7
    let phase_index = ((phase * 8.0).floor() as usize) % 8;

    MoonPhase {
        illumination: (illumination * 10000.0).round() / 10000.0,
        phase: (phase * 10000.0).round() / 10000.0,
        phase_name: PHASE_NAMES[phase_index],
        symbol: PHASE_SYMBOLS[phase_index],
        phase_index,
    }
}

/// Short moon symbol for calendar cells.
pub fn moon_symbol(year: i32, month: u32, day: u32) -> &'static str {
    moon_phase(year, month, day).symbol
}

// ── Notable phase detection ──────────────────────────────────────────

/// True if today is a cardinal phase (new, first quarter, full, last quarter)
/// AND yesterday had a different phase index (transition day).
pub fn notable_phase(year: i32, month: u32, day: u32) -> bool {
    let today = moon_phase(year, month, day);
    if !matches!(today.phase_index, 0 | 2 | 4 | 6) {
        return false;
    }
    // Compute yesterday's date
    let (py, pm, pd) = prev_day(year, month, day);
    let yesterday = moon_phase(py, pm, pd);
    yesterday.phase_index != today.phase_index
}

/// All notable moon phases in a given month.
pub fn notable_phases_in_month(year: i32, month: u32) -> Vec<NotablePhase> {
    let last = days_in_month(year, month);
    let mut result = Vec::new();
    for d in 1..=last {
        if notable_phase(year, month, d) {
            let p = moon_phase(year, month, d);
            result.push(NotablePhase {
                day: d,
                phase_name: p.phase_name,
                symbol: p.symbol,
            });
        }
    }
    result
}

// ── Astronomical events ──────────────────────────────────────────────

/// Returns event descriptions for a given date.
/// Includes notable moon phases, solstices, equinoxes, and meteor showers.
pub fn astro_events(month: u32, day: u32) -> Vec<String> {
    astro_events_for_year(2025, month, day)
}

/// Year-aware version (moon phase depends on year).
pub fn astro_events_for_year(year: i32, month: u32, day: u32) -> Vec<String> {
    let mut events = Vec::new();

    // Notable moon phase
    if notable_phase(year, month, day) {
        let p = moon_phase(year, month, day);
        events.push(format!("{} {}", p.symbol, p.phase_name));
    }

    // Solstices and equinoxes
    match (month, day) {
        (6, 21) => events.push("\u{2600} Summer Solstice".into()),
        (12, 21) => events.push("\u{2744} Winter Solstice".into()),
        (3, 20) => events.push("\u{2600} Vernal Equinox".into()),
        (9, 22) => events.push("\u{2600} Autumnal Equinox".into()),
        _ => {}
    }

    // Major meteor showers (peak dates)
    match (month, day) {
        (1, 3) => events.push("\u{2604} Quadrantids peak".into()),
        (4, 22) => events.push("\u{2604} Lyrids peak".into()),
        (5, 6) => events.push("\u{2604} Eta Aquariids peak".into()),
        (8, 12) => events.push("\u{2604} Perseids peak".into()),
        (10, 21) => events.push("\u{2604} Orionids peak".into()),
        (11, 17) => events.push("\u{2604} Leonids peak".into()),
        (12, 14) => events.push("\u{2604} Geminids peak".into()),
        _ => {}
    }

    events
}

// ── Sun times (simple algorithm, no external deps) ───────────────────

/// Approximate sunrise/sunset using the standard sunrise equation.
/// Returns (rise, set) as "HH:MM" strings, or None for polar day/night.
/// `lat`/`lon` in decimal degrees; `tz_offset` in hours from UTC.
pub fn sun_times(
    year: i32,
    month: u32,
    day: u32,
    lat: f64,
    lon: f64,
    tz_offset: f64,
) -> Option<(String, String)> {
    let doy = day_of_year(year, month, day) as f64;

    // Solar declination (radians)
    let declination = 23.45_f64.to_radians() * ((360.0 / 365.0 * (doy - 81.0)).to_radians()).sin();

    let lat_rad = lat.to_radians();

    // Hour angle (degrees)
    let cos_ha = -(lat_rad.tan() * declination.tan());

    // Check polar conditions
    if cos_ha < -1.0 || cos_ha > 1.0 {
        return None; // Polar day or polar night
    }

    let ha = cos_ha.acos().to_degrees();

    // Solar noon offset from longitude
    let solar_noon_offset = -lon / 15.0;

    let sunrise = 12.0 - ha / 15.0 + solar_noon_offset + tz_offset;
    let sunset = 12.0 + ha / 15.0 + solar_noon_offset + tz_offset;

    Some((format_hhmm(sunrise), format_hhmm(sunset)))
}

/// Convenience wrapper with Oslo defaults (lat 59.9139, lon 10.7522, tz +1).
pub fn sun_times_oslo(year: i32, month: u32, day: u32) -> Option<(String, String)> {
    sun_times(year, month, day, 59.9139, 10.7522, 1.0)
}

// ── Stub: visible planets ────────────────────────────────────────────
// The Ruby version uses the `ruby-ephemeris` gem which has no Rust
// equivalent. These stubs return empty/None so the info bar compiles
// but only shows moon phase (no planet positions) until a helper is
// available.

/// Stub: returns None. Needs an ephemeris library or external helper.
pub fn visible_planets(
    _year: i32,
    _month: u32,
    _day: u32,
    _lat: f64,
    _lon: f64,
    _tz: f64,
) -> Vec<(&'static str, &'static str)> {
    Vec::new()
}

// ── Helper functions ─────────────────────────────────────────────────

/// Number of days in a given month, accounting for leap years.
pub fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Previous calendar day.
fn prev_day(year: i32, month: u32, day: u32) -> (i32, u32, u32) {
    if day > 1 {
        (year, month, day - 1)
    } else if month > 1 {
        let pm = month - 1;
        (year, pm, days_in_month(year, pm))
    } else {
        (year - 1, 12, 31)
    }
}

/// Day of year (1-based).
fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let mut doy = 0;
    for m in 1..month {
        doy += days_in_month(year, m);
    }
    doy + day
}

/// Format fractional hours as "HH:MM".
fn format_hhmm(hours: f64) -> String {
    let mut h = hours % 24.0;
    if h < 0.0 {
        h += 24.0;
    }
    let hh = h.floor() as u32;
    let mm = ((h - hh as f64) * 60.0).round() as u32;
    if mm >= 60 {
        format!("{:02}:{:02}", (hh + 1) % 24, 0)
    } else {
        format!("{:02}:{:02}", hh, mm)
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_julian_date_epoch() {
        // Jan 6, 2000 at midnight = JD 2451549.5
        // Reference epoch 2451550.1 is 18:14 UTC that day
        let jd = julian_date(2000, 1, 6);
        assert!((jd - 2451549.5).abs() < 0.01, "Got JD {}", jd);
    }

    #[test]
    fn test_moon_phase_range() {
        let p = moon_phase(2025, 4, 6);
        assert!(p.illumination >= 0.0 && p.illumination <= 1.0);
        assert!(p.phase >= 0.0 && p.phase < 1.0);
        assert!(p.phase_index < 8);
    }

    #[test]
    fn test_phase_names_and_symbols() {
        // Cycle through a full synodic month starting from a known new moon
        // and verify we see all 8 phase indices.
        let mut seen = [false; 8];
        for d in 0..30 {
            let p = moon_phase(2000, 1, 6 + d);
            seen[p.phase_index] = true;
        }
        assert!(seen.iter().all(|&s| s), "Should see all 8 phases in one cycle");
    }

    #[test]
    fn test_notable_phase_detection() {
        // Over a month there should be ~4 notable phases
        let phases = notable_phases_in_month(2025, 4);
        assert!(!phases.is_empty(), "April 2025 should have notable phases");
        assert!(phases.len() <= 5, "At most 5 notable phases per month");
    }

    #[test]
    fn test_astro_events_solstice() {
        let events = astro_events(6, 21);
        assert!(events.iter().any(|e| e.contains("Solstice")));
    }

    #[test]
    fn test_astro_events_meteor() {
        let events = astro_events(8, 12);
        assert!(events.iter().any(|e| e.contains("Perseids")));
    }

    #[test]
    fn test_sun_times_oslo_summer() {
        // Oslo, summer: sunrise should be early, sunset late
        if let Some((rise, set)) = sun_times_oslo(2025, 6, 21) {
            let rh: u32 = rise[..2].parse().unwrap();
            let sh: u32 = set[..2].parse().unwrap();
            assert!(rh < 6, "Oslo summer sunrise before 06:00, got {}", rise);
            assert!(sh > 20, "Oslo summer sunset after 20:00, got {}", set);
        }
    }

    #[test]
    fn test_sun_times_oslo_winter() {
        if let Some((rise, set)) = sun_times_oslo(2025, 12, 21) {
            let rh: u32 = rise[..2].parse().unwrap();
            let sh: u32 = set[..2].parse().unwrap();
            assert!(rh >= 8, "Oslo winter sunrise after 08:00, got {}", rise);
            assert!(sh <= 16, "Oslo winter sunset before 16:00, got {}", set);
        }
    }

    #[test]
    fn test_days_in_month_leap() {
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(1900, 2), 28);
        assert_eq!(days_in_month(2000, 2), 29);
    }

    #[test]
    fn test_prev_day_boundaries() {
        assert_eq!(prev_day(2025, 3, 1), (2025, 2, 28));
        assert_eq!(prev_day(2024, 3, 1), (2024, 2, 29));
        assert_eq!(prev_day(2025, 1, 1), (2024, 12, 31));
    }

    #[test]
    fn test_format_hhmm() {
        assert_eq!(format_hhmm(6.5), "06:30");
        assert_eq!(format_hhmm(23.75), "23:45");
        assert_eq!(format_hhmm(0.0), "00:00");
        assert_eq!(format_hhmm(-1.0), "23:00");
    }

    #[test]
    fn test_visible_planets_stub() {
        let v = visible_planets(2025, 4, 6, 59.9, 10.7, 1.0);
        assert!(v.is_empty(), "Stub should return empty");
    }
}
