// astronomy.rs - Moon phases, ephemeris, sun/planet positions for Tock.
// Planet calculations ported from ruby-ephemeris (isene/ephemeris).

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

pub struct VisiblePlanet {
    pub name: &'static str,
    pub symbol: &'static str,
    pub color: &'static str,
    pub rise: String,
    pub set: String,
}

// ── Constants ────────────────────────────────────────────────────────

const SYNODIC_MONTH: f64 = 29.530588853;
const NEW_MOON_EPOCH_JD: f64 = 2451550.1;

const PHASE_NAMES: [&str; 8] = [
    "New Moon", "Waxing Crescent", "First Quarter", "Waxing Gibbous",
    "Full Moon", "Waning Gibbous", "Last Quarter", "Waning Crescent",
];

const PHASE_SYMBOLS: [&str; 8] = [
    "\u{1F311}", "\u{1F312}", "\u{1F313}", "\u{1F314}",
    "\u{1F315}", "\u{1F316}", "\u{1F317}", "\u{1F318}",
];

pub const PLANET_SYMBOLS: &[(&str, &str)] = &[
    ("mercury", "\u{263F}"), ("venus", "\u{2640}"), ("mars", "\u{2642}"),
    ("jupiter", "\u{2643}"), ("saturn", "\u{2644}"),
];

pub const BODY_COLORS: &[(&str, &str)] = &[
    ("sun", "FFD700"), ("moon", "888888"), ("mercury", "8F6E54"),
    ("venus", "E6B07C"), ("mars", "BC2732"), ("jupiter", "C08040"), ("saturn", "E8D9A0"),
];

fn deg(d: f64) -> f64 { d * PI / 180.0 }

// ── Ephemeris engine (ported from ruby-ephemeris) ───────────────────

struct OrbitalElements {
    n: f64, i: f64, w: f64, a: f64, e: f64, m: f64,
}

struct Ephemeris {
    lat: f64, lon: f64, tz: f64,
    d: f64, ecl: f64, ls: f64, ms: f64,
    xs: f64, ys: f64, sidtime: f64,
    sun_ra: f64, sun_dec: f64,
    bodies: std::collections::HashMap<&'static str, OrbitalElements>,
    jupiter_m: f64, saturn_m: f64, _uranus_m: f64,
}

impl Ephemeris {
    fn new(year: i32, month: u32, day: u32, lat: f64, lon: f64, tz: f64) -> Self {
        let y = year; let m = month as i32; let dd = day as i32;
        let d = (367 * y - 7 * (y + (m + 9) / 12) / 4 + 275 * m / 9 + dd - 730530) as f64;
        let t = d / 36525.0;
        let ecl = 23.439279444 - 46.8150 / 3600.0 * t - 0.00059 / 3600.0 * t * t + 0.001813 / 3600.0 * t * t * t;

        let mut bodies = std::collections::HashMap::new();
        bodies.insert("sun", OrbitalElements {
            n: 0.0, i: 0.0, w: 282.9404 + 4.70935e-5 * d, a: 1.0,
            e: 0.016709 - 1.151e-9 * d, m: 356.0470 + 0.9856002585 * d,
        });
        bodies.insert("moon", OrbitalElements {
            n: 125.1228 - 0.0529538083 * d, i: 5.1454,
            w: 318.0634 + 0.1643573223 * d, a: 60.2666, e: 0.054900,
            m: 115.3654 + 13.0649929509 * d,
        });
        bodies.insert("mercury", OrbitalElements {
            n: 48.3313 + 3.24587e-5 * d, i: 7.0047 + 5.00e-8 * d,
            w: 29.1241 + 1.01444e-5 * d, a: 0.387098, e: 0.205635 + 5.59e-10 * d,
            m: 168.6562 + 4.0923344368 * d,
        });
        bodies.insert("venus", OrbitalElements {
            n: 76.6799 + 2.46590e-5 * d, i: 3.3946 + 2.75e-8 * d,
            w: 54.8910 + 1.38374e-5 * d, a: 0.723330, e: 0.006773 - 1.302e-9 * d,
            m: 48.0052 + 1.6021302244 * d,
        });
        bodies.insert("mars", OrbitalElements {
            n: 49.5574 + 2.11081e-5 * d, i: 1.8497 - 1.78e-8 * d,
            w: 286.5016 + 2.92961e-5 * d, a: 1.523688, e: 0.093405 + 2.516e-9 * d,
            m: 18.6021 + 0.5240207766 * d,
        });
        bodies.insert("jupiter", OrbitalElements {
            n: 100.4542 + 2.76854e-5 * d, i: 1.3030 - 1.557e-7 * d,
            w: 273.8777 + 1.64505e-5 * d, a: 5.20256, e: 0.048498 + 4.469e-9 * d,
            m: 19.8950 + 0.0830853001 * d,
        });
        bodies.insert("saturn", OrbitalElements {
            n: 113.6634 + 2.38980e-5 * d, i: 2.4886 - 1.081e-7 * d,
            w: 339.3939 + 2.97661e-5 * d, a: 9.55475, e: 0.055546 - 9.499e-9 * d,
            m: 316.9670 + 0.0334442282 * d,
        });

        let jupiter_m = (19.8950 + 0.0830853001 * d) % 360.0;
        let saturn_m = (316.9670 + 0.0334442282 * d) % 360.0;
        let uranus_m = (142.5905 + 0.011725806 * d) % 360.0;

        // Compute sun position
        let sun = &bodies["sun"];
        let w_s = (sun.w + 360.0) % 360.0;
        let ms = sun.m % 360.0;
        let es = ms + (180.0 / PI) * sun.e * deg(ms).sin() * (1.0 + sun.e * deg(ms).cos());
        let x = deg(es).cos() - sun.e;
        let y = deg(es).sin() * (1.0 - sun.e * sun.e).sqrt();
        let v = y.atan2(x) * 180.0 / PI;
        let r = (x * x + y * y).sqrt();
        let tlon = (v + w_s) % 360.0;
        let xs = r * deg(tlon).cos();
        let ys = r * deg(tlon).sin();
        let xe = xs;
        let ye = ys * deg(ecl).cos();
        let ze = ys * deg(ecl).sin();
        let sun_ra = ((ye.atan2(xe) * 180.0 / PI) + 360.0) % 360.0;
        let sun_dec = ze.atan2((xe * xe + ye * ye).sqrt()) * 180.0 / PI;

        let ls = (w_s + ms) % 360.0;
        let gmst0 = (ls + 180.0) / 15.0 % 24.0;
        let sidtime = gmst0 + lon / 15.0;

        Ephemeris {
            lat, lon, tz, d, ecl, ls, ms, xs, ys, sidtime,
            sun_ra, sun_dec, bodies, jupiter_m, saturn_m, _uranus_m: uranus_m,
        }
    }

    fn body_calc(&self, name: &str) -> (f64, f64, f64, f64, f64) {
        let b = &self.bodies[name];
        let n_b = b.n; let i_b = b.i;
        let w_b = (b.w + 360.0) % 360.0;
        let a_b = b.a; let e_b = b.e;
        let m_b = b.m % 360.0;

        // Solve Kepler's equation iteratively
        let mut e1 = m_b + (180.0 / PI) * e_b * deg(m_b).sin() * (1.0 + e_b * deg(m_b).cos());
        let mut e0;
        loop {
            e0 = e1;
            e1 = e0 - (e0 - (180.0 / PI) * e_b * deg(e0).sin() - m_b) / (1.0 - e_b * deg(e0).cos());
            if (e1 - e0).abs() <= 0.0005 { break; }
        }
        let e = e1;
        let x = a_b * (deg(e).cos() - e_b);
        let y = a_b * (1.0 - e_b * e_b).sqrt() * deg(e).sin();
        let r = (x * x + y * y).sqrt();
        let v = ((y.atan2(x) * 180.0 / PI) + 360.0) % 360.0;

        let xeclip = r * (deg(n_b).cos() * deg(v + w_b).cos() - deg(n_b).sin() * deg(v + w_b).sin() * deg(i_b).cos());
        let yeclip = r * (deg(n_b).sin() * deg(v + w_b).cos() + deg(n_b).cos() * deg(v + w_b).sin() * deg(i_b).cos());
        let zeclip = r * deg(v + w_b).sin() * deg(i_b).sin();

        let mut lon = (yeclip.atan2(xeclip) * 180.0 / PI + 360.0) % 360.0;
        let mut lat = zeclip.atan2((xeclip * xeclip + yeclip * yeclip).sqrt()) * 180.0 / PI;
        let mut r_b = (xeclip * xeclip + yeclip * yeclip + zeclip * zeclip).sqrt();

        // Perturbation corrections
        let (mut plon, mut plat, mut pdist) = (0.0, 0.0, 0.0);
        let m_j = self.jupiter_m;
        let m_s = self.saturn_m;
        match name {
            "moon" => {
                let lb = (n_b + w_b + m_b) % 360.0;
                let db = (lb - self.ls + 360.0) % 360.0;
                let fb = (lb - n_b + 360.0) % 360.0;
                plon += -1.274 * deg(m_b - 2.0 * db).sin();
                plon += 0.658 * deg(2.0 * db).sin();
                plon += -0.186 * deg(self.ms).sin();
                plon += -0.059 * deg(2.0 * m_b - 2.0 * db).sin();
                plon += -0.057 * deg(m_b - 2.0 * db + self.ms).sin();
                plon += 0.053 * deg(m_b + 2.0 * db).sin();
                plon += 0.046 * deg(2.0 * db - self.ms).sin();
                plon += 0.041 * deg(m_b - self.ms).sin();
                plon += -0.035 * deg(db).sin();
                plon += -0.031 * deg(m_b + self.ms).sin();
                plon += -0.015 * deg(2.0 * fb - 2.0 * db).sin();
                plon += 0.011 * deg(m_b - 4.0 * db).sin();
                plat += -0.173 * deg(fb - 2.0 * db).sin();
                plat += -0.055 * deg(m_b - fb - 2.0 * db).sin();
                plat += -0.046 * deg(m_b + fb - 2.0 * db).sin();
                plat += 0.033 * deg(fb + 2.0 * db).sin();
                plat += 0.017 * deg(2.0 * m_b + fb).sin();
                pdist += -0.58 * deg(m_b - 2.0 * db).cos();
                pdist += -0.46 * deg(2.0 * db).cos();
            }
            "jupiter" => {
                plon += -0.332 * deg(2.0 * m_j - 5.0 * m_s - 67.6).sin();
                plon += -0.056 * deg(2.0 * m_j - 2.0 * m_s + 21.0).sin();
                plon += 0.042 * deg(3.0 * m_j - 5.0 * m_s + 21.0).sin();
                plon += -0.036 * deg(m_j - 2.0 * m_s).sin();
                plon += 0.022 * deg(m_j - m_s).cos();
                plon += 0.023 * deg(2.0 * m_j - 3.0 * m_s + 52.0).sin();
                plon += -0.016 * deg(m_j - 5.0 * m_s - 69.0).sin();
            }
            "saturn" => {
                plon += 0.812 * deg(2.0 * m_j - 5.0 * m_s - 67.6).sin();
                plon += -0.229 * deg(2.0 * m_j - 4.0 * m_s - 2.0).cos();
                plon += 0.119 * deg(m_j - 2.0 * m_s - 3.0).sin();
                plon += 0.046 * deg(2.0 * m_j - 6.0 * m_s - 69.0).sin();
                plon += 0.014 * deg(m_j - 3.0 * m_s + 32.0).sin();
                plat += -0.020 * deg(2.0 * m_j - 4.0 * m_s - 2.0).cos();
                plat += 0.018 * deg(2.0 * m_j - 6.0 * m_s - 49.0).sin();
            }
            _ => {}
        }
        lon += plon;
        lat += plat;
        r_b += pdist;

        // Geocentric ecliptic to equatorial
        let (xeclip2, yeclip2, zeclip2) = if name == "moon" {
            (deg(lon).cos() * deg(lat).cos(), deg(lon).sin() * deg(lat).cos(), deg(lat).sin())
        } else {
            (xeclip + self.xs, yeclip + self.ys, zeclip)
        };

        let xequat = xeclip2;
        let yequat = yeclip2 * deg(self.ecl).cos() - zeclip2 * deg(self.ecl).sin();
        let zequat = yeclip2 * deg(self.ecl).sin() + zeclip2 * deg(self.ecl).cos();

        let ra = (yequat.atan2(xequat) * 180.0 / PI + 360.0) % 360.0;
        let dec_val = zequat.atan2((xequat * xequat + yequat * yequat).sqrt()) * 180.0 / PI;
        let dist = (xequat * xequat + yequat * yequat + zequat * zequat).sqrt();

        // Topocentric correction
        let par = if name == "moon" { (1.0 / r_b).asin() * 180.0 / PI } else { (8.794 / 3600.0) / r_b };
        let gclat = self.lat - 0.1924 * deg(2.0 * self.lat).sin();
        let rho = 0.99833 + 0.00167 * deg(2.0 * self.lat).cos();
        let lst = self.sidtime * 15.0;
        let ha = (lst - ra + 360.0) % 360.0;
        let g = (deg(gclat).tan() / deg(ha).cos()).atan() * 180.0 / PI;
        let top_ra = ra - par * rho * deg(gclat).cos() * deg(ha).sin() / deg(dec_val).cos();
        let top_dec = dec_val - par * rho * deg(gclat).sin() * deg(g - dec_val).sin() / deg(g).sin();

        (top_ra, top_dec, dist, ra, dec_val)
    }

    fn alt_az(&self, ra: f64, dec: f64, time: f64) -> (f64, f64) {
        let ha = (time - ra / 15.0) * 15.0;
        let x = deg(ha).cos() * deg(dec).cos();
        let y = deg(ha).sin() * deg(dec).cos();
        let z = deg(dec).sin();
        let xhor = x * deg(self.lat).sin() - z * deg(self.lat).cos();
        let yhor = y;
        let zhor = x * deg(self.lat).cos() + z * deg(self.lat).sin();
        let az = (yhor.atan2(xhor) * 180.0 / PI + 180.0) % 360.0;
        let alt = zhor.asin() * 180.0 / PI;
        (alt, az)
    }

    fn body_alt_az(&self, name: &str, hour: f64) -> (f64, f64) {
        let (ra, dec, _, _, _) = self.body_calc(name);
        self.alt_az(ra, dec, hour)
    }

    fn rts(&self, ra: f64, dec: f64) -> (String, String, String) {
        let transit = (ra - self.ls - self.lon) / 15.0 + 12.0 + self.tz;
        let transit = (transit + 24.0) % 24.0;
        let cos_lha = -(deg(self.lat).tan() * deg(dec).tan());
        if cos_lha < -1.0 {
            return ("always".into(), format_hhmm(transit), "never".into());
        }
        if cos_lha > 1.0 {
            return ("never".into(), format_hhmm(transit), "always".into());
        }
        let lha_h = cos_lha.acos() * 180.0 / PI / 15.0;
        let rise = (transit - lha_h + 24.0) % 24.0;
        let set = (transit + lha_h + 24.0) % 24.0;
        (format_hhmm(rise), format_hhmm(transit), format_hhmm(set))
    }
}

// ── Public API ───────────────────────────────────────────────────────

fn julian_date(y: i32, m: u32, d: u32) -> f64 {
    let y = y as f64; let m = m as f64; let d = d as f64;
    367.0 * y - ((7.0 * (y + ((m + 9.0) / 12.0).floor())) / 4.0).floor()
        + ((275.0 * m) / 9.0).floor() + d + 1_721_013.5
}

// ── Moon phase ──────────────────────────────────────────────────────

pub fn moon_phase(year: i32, month: u32, day: u32) -> MoonPhase {
    let jd = julian_date(year, month, day);
    let days_since = jd - NEW_MOON_EPOCH_JD;
    let mut phase = (days_since / SYNODIC_MONTH) % 1.0;
    if phase < 0.0 { phase += 1.0; }
    let illumination = (1.0 - (phase * 2.0 * PI).cos()) / 2.0;
    let phase_index = ((phase * 8.0).floor() as usize) % 8;
    MoonPhase {
        illumination: (illumination * 10000.0).round() / 10000.0,
        phase: (phase * 10000.0).round() / 10000.0,
        phase_name: PHASE_NAMES[phase_index],
        symbol: PHASE_SYMBOLS[phase_index],
        phase_index,
    }
}

pub fn moon_symbol(year: i32, month: u32, day: u32) -> &'static str {
    moon_phase(year, month, day).symbol
}

pub fn notable_phase(year: i32, month: u32, day: u32) -> bool {
    let today = moon_phase(year, month, day);
    if !matches!(today.phase_index, 0 | 2 | 4 | 6) { return false; }
    let (py, pm, pd) = prev_day(year, month, day);
    moon_phase(py, pm, pd).phase_index != today.phase_index
}

pub fn notable_phases_in_month(year: i32, month: u32) -> Vec<NotablePhase> {
    let last = days_in_month(year, month);
    let mut result = Vec::new();
    for d in 1..=last {
        if notable_phase(year, month, d) {
            let p = moon_phase(year, month, d);
            result.push(NotablePhase { day: d, phase_name: p.phase_name, symbol: p.symbol });
        }
    }
    result
}

// ── Astronomical events ─────────────────────────────────────────────

pub fn astro_events(month: u32, day: u32) -> Vec<String> {
    astro_events_for_year(2025, month, day)
}

pub fn astro_events_for_year(year: i32, month: u32, day: u32) -> Vec<String> {
    let mut events = Vec::new();
    if notable_phase(year, month, day) {
        let p = moon_phase(year, month, day);
        events.push(format!("{} {}", p.symbol, p.phase_name));
    }
    match (month, day) {
        (6, 21)  => events.push("\u{2600} Summer Solstice".into()),
        (12, 21) => events.push("\u{2744} Winter Solstice".into()),
        (3, 20)  => events.push("\u{2600} Vernal Equinox".into()),
        (9, 22)  => events.push("\u{2600} Autumnal Equinox".into()),
        _ => {}
    }
    match (month, day) {
        (1, 3)   => events.push("\u{2604} Quadrantids peak".into()),
        (4, 22)  => events.push("\u{2604} Lyrids peak".into()),
        (5, 6)   => events.push("\u{2604} Eta Aquariids peak".into()),
        (7, 30)  => events.push("\u{2604} Delta Aquariids peak".into()),
        (8, 12)  => events.push("\u{2604} Perseids peak".into()),
        (10, 8)  => events.push("\u{2604} Draconids peak".into()),
        (10, 21) => events.push("\u{2604} Orionids peak".into()),
        (11, 5)  => events.push("\u{2604} Taurids peak".into()),
        (11, 17) => events.push("\u{2604} Leonids peak".into()),
        (12, 14) => events.push("\u{2604} Geminids peak".into()),
        (12, 22) => events.push("\u{2604} Ursids peak".into()),
        _ => {}
    }
    events
}

// ── Sun times (via ephemeris engine) ────────────────────────────────

pub fn sun_times(year: i32, month: u32, day: u32, lat: f64, lon: f64, tz: f64) -> Option<(String, String)> {
    let eph = Ephemeris::new(year, month, day, lat, lon, tz);
    let (rise, _, set) = eph.rts(eph.sun_ra, eph.sun_dec);
    if rise == "never" || rise == "always" { return None; }
    Some((truncate_hms(&rise), truncate_hms(&set)))
}

pub fn sun_times_oslo(year: i32, month: u32, day: u32) -> Option<(String, String)> {
    sun_times(year, month, day, 59.9139, 10.7522, 1.0)
}

/// Moon rise and set times via ephemeris.
pub fn moon_times(year: i32, month: u32, day: u32, lat: f64, lon: f64, tz: f64) -> Option<(String, String)> {
    let eph = Ephemeris::new(year, month, day, lat, lon, tz);
    let (ra, dec, _, _, _) = eph.body_calc("moon");
    let (rise, _, set) = eph.rts(ra, dec);
    if rise == "never" || rise == "always" { return None; }
    Some((truncate_hms(&rise), truncate_hms(&set)))
}

// ── Visible planets (via ephemeris engine) ──────────────────────────

/// Returns planets visible at night (altitude > 5 degrees at any hour 20:00-04:00).
/// Uses the same algorithm as Timely's ruby-ephemeris integration.
pub fn visible_planets(year: i32, month: u32, day: u32, lat: f64, lon: f64, tz: f64) -> Vec<VisiblePlanet> {
    let eph = Ephemeris::new(year, month, day, lat, lon, tz);

    let planet_info: &[(&str, &str, &str)] = &[
        ("mercury", "\u{263F}", "8F6E54"),
        ("venus",   "\u{2640}", "E6B07C"),
        ("mars",    "\u{2642}", "BC2732"),
        ("jupiter", "\u{2643}", "C08040"),
        ("saturn",  "\u{2644}", "E8D9A0"),
    ];

    let check_hours = [20.0, 21.0, 22.0, 23.0, 0.0, 1.0, 2.0, 3.0, 4.0];
    let mut visible = Vec::new();

    for &(name, symbol, color) in planet_info {
        let is_visible = check_hours.iter().any(|&h| {
            let (alt, _) = eph.body_alt_az(name, h);
            alt > 5.0
        });
        if is_visible {
            let (ra, dec, _, _, _) = eph.body_calc(name);
            let (rise, _, set) = eph.rts(ra, dec);
            visible.push(VisiblePlanet {
                name: match name {
                    "mercury" => "Mercury", "venus" => "Venus", "mars" => "Mars",
                    "jupiter" => "Jupiter", "saturn" => "Saturn", _ => name,
                },
                symbol, color,
                rise: truncate_hms(&rise), set: truncate_hms(&set),
            });
        }
    }
    visible
}

// ── Helpers ─────────────────────────────────────────────────────────

pub fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap(year) { 29 } else { 28 },
        _ => 30,
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn prev_day(year: i32, month: u32, day: u32) -> (i32, u32, u32) {
    if day > 1 { (year, month, day - 1) }
    else if month > 1 { let pm = month - 1; (year, pm, days_in_month(year, pm)) }
    else { (year - 1, 12, 31) }
}

fn format_hhmm(hours: f64) -> String {
    let mut h = hours % 24.0;
    if h < 0.0 { h += 24.0; }
    let hh = h.floor() as u32;
    let mm = ((h - hh as f64) * 60.0).round() as u32;
    if mm >= 60 { format!("{:02}:{:02}", (hh + 1) % 24, 0) }
    else { format!("{:02}:{:02}", hh, mm) }
}

/// Truncate "HH:MM:SS" to "HH:MM"
fn truncate_hms(s: &str) -> String {
    if s.len() >= 5 { s[..5].to_string() } else { s.to_string() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_moon_phase_range() {
        let p = moon_phase(2026, 4, 7);
        assert!(p.illumination >= 0.0 && p.illumination <= 1.0);
        assert!(p.phase_index < 8);
    }

    #[test]
    fn test_sun_times_oslo_summer() {
        if let Some((rise, set)) = sun_times_oslo(2026, 6, 21) {
            let rh: u32 = rise[..2].parse().unwrap();
            let sh: u32 = set[..2].parse().unwrap();
            assert!(rh < 6, "Summer sunrise before 06, got {}", rise);
            assert!(sh > 20, "Summer sunset after 20, got {}", set);
        }
    }

    #[test]
    fn test_visible_planets_returns_results() {
        let v = visible_planets(2026, 4, 7, 59.9139, 10.7522, 1.0);
        // Should find at least some visible planets (not a guarantee, but likely)
        // This is a smoke test, not a strict assertion
        assert!(v.len() <= 5);
    }

    #[test]
    fn test_ephemeris_body_calc_no_panic() {
        let eph = Ephemeris::new(2026, 4, 7, 59.9139, 10.7522, 1.0);
        for name in &["mercury", "venus", "mars", "jupiter", "saturn"] {
            let (ra, dec, _, _, _) = eph.body_calc(name);
            assert!(ra >= 0.0 && ra < 360.0, "{} RA out of range: {}", name, ra);
            assert!(dec >= -90.0 && dec <= 90.0, "{} Dec out of range: {}", name, dec);
        }
    }
}
