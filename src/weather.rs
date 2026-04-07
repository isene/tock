use crate::database::Database;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// DayForecast
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DayForecast {
    pub temp_high: f64,
    pub temp_low: f64,
    pub temp_mid: f64,
    pub symbol: String,
    pub wind: f64,
    pub cloud: i64,
}

// ---------------------------------------------------------------------------
// Weather symbols (matching Timely exactly)
// ---------------------------------------------------------------------------

fn weather_symbol(cloud_fraction: i64) -> &'static str {
    if cloud_fraction < 15 {
        "\u{2600}" // ☀ Clear
    } else if cloud_fraction < 40 {
        "\u{1F324}" // 🌤 Mostly clear
    } else if cloud_fraction < 70 {
        "\u{26C5}" // ⛅ Partly cloudy
    } else {
        "\u{2601}" // ☁ Cloudy
    }
}

// ---------------------------------------------------------------------------
// Fetch forecast from Met.no
// ---------------------------------------------------------------------------

/// Fetch weather forecast from api.met.no for the given coordinates.
/// Returns a map of date strings ("YYYY-MM-DD") to DayForecast.
pub fn fetch_weather(lat: f64, lon: f64) -> HashMap<String, DayForecast> {
    let url = format!(
        "https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={}&lon={}",
        lat, lon
    );

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(std::time::Duration::from_secs(5))
        .timeout_read(std::time::Duration::from_secs(10))
        .build();

    let resp = match agent
        .get(&url)
        .set("User-Agent", "tock-calendar/0.1 g@isene.com")
        .set("Accept-Encoding", "identity")
        .call()
    {
        Ok(r) => r,
        Err(_) => return HashMap::new(),
    };

    let body: JsonValue = match resp.into_json() {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };

    let timeseries = match body.pointer("/properties/timeseries") {
        Some(JsonValue::Array(arr)) => arr,
        _ => return HashMap::new(),
    };

    // Intermediate accumulator per date
    struct DayAccum {
        temps: Vec<f64>,
        midday_temp: Option<f64>,
        wind: f64,
        cloud: i64,
    }

    let mut by_date: HashMap<String, DayAccum> = HashMap::new();

    for ts in timeseries {
        let time = match ts.get("time").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => continue,
        };
        let details = match ts.pointer("/data/instant/details") {
            Some(d) => d,
            None => continue,
        };

        let date = &time[..10]; // "YYYY-MM-DD"
        let hour: i64 = time[11..13].parse().unwrap_or(-1);
        let temp = details
            .get("air_temperature")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let acc = by_date.entry(date.to_string()).or_insert(DayAccum {
            temps: Vec::new(),
            midday_temp: None,
            wind: 0.0,
            cloud: 0,
        });

        acc.temps.push(temp);

        // Capture midday (12:00) conditions for the symbol
        if hour == 12 {
            acc.midday_temp = Some(temp);
            acc.wind = details
                .get("wind_speed")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            acc.wind = (acc.wind * 10.0).round() / 10.0;
            acc.cloud = details
                .get("cloud_area_fraction")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0) as i64;
        }
    }

    // Build final forecast map
    let mut forecast = HashMap::new();
    for (date, data) in &by_date {
        let temps = &data.temps;
        if temps.is_empty() {
            continue;
        }

        let temp_high =
            (temps.iter().cloned().fold(f64::NEG_INFINITY, f64::max) * 10.0).round() / 10.0;
        let temp_low =
            (temps.iter().cloned().fold(f64::INFINITY, f64::min) * 10.0).round() / 10.0;
        let temp_mid = match data.midday_temp {
            Some(t) => (t * 10.0).round() / 10.0,
            None => {
                let mid = temps[temps.len() / 2];
                (mid * 10.0).round() / 10.0
            }
        };

        forecast.insert(
            date.clone(),
            DayForecast {
                temp_high,
                temp_low,
                temp_mid,
                symbol: weather_symbol(data.cloud).to_string(),
                wind: data.wind,
                cloud: data.cloud,
            },
        );
    }

    forecast
}

// ---------------------------------------------------------------------------
// Short format for day headers
// ---------------------------------------------------------------------------

/// Returns a short weather string like "☀ 12°" for the given date, or None
/// if no forecast data is available.
pub fn short_for_date(
    forecast: &HashMap<String, DayForecast>,
    year: i32,
    month: u32,
    day: u32,
) -> Option<String> {
    let date_str = format!("{:04}-{:02}-{:02}", year, month, day);
    let w = forecast.get(&date_str)?;
    Some(format!("{} {}°", w.symbol, w.temp_mid))
}

// ---------------------------------------------------------------------------
// DB-cached fetch
// ---------------------------------------------------------------------------

const CACHE_TTL_SECS: i64 = 21600; // 6 hours

/// Fetch forecast with DB caching. Checks weather_cache table first; if the
/// cached data is less than 6 hours old it is returned directly. Otherwise
/// a fresh fetch is performed and the result is stored.
pub fn fetch_cached(
    lat: f64,
    lon: f64,
    db: &Database,
) -> HashMap<String, DayForecast> {
    // Try cache first
    if let Some(cached) = read_cache(db) {
        return cached;
    }

    let forecast = fetch_weather(lat, lon);

    // Store in cache
    if !forecast.is_empty() {
        write_cache(db, &forecast);
    }

    forecast
}

/// Read forecast from the weather_cache table. Returns None if missing or
/// expired (older than CACHE_TTL_SECS).
fn read_cache(db: &Database) -> Option<HashMap<String, DayForecast>> {
    let (json_str, fetched_at) = db.get_weather_cache().ok()??;

    let now = crate::database::now_secs();
    if now - fetched_at >= CACHE_TTL_SECS {
        return None;
    }

    parse_forecast_json(&json_str)
}

/// Write forecast data to the weather_cache table.
fn write_cache(db: &Database, forecast: &HashMap<String, DayForecast>) {
    let json = serialize_forecast(forecast);
    let _ = db.set_weather_cache(&json);
}

// ---------------------------------------------------------------------------
// JSON serialization helpers
// ---------------------------------------------------------------------------

fn serialize_forecast(forecast: &HashMap<String, DayForecast>) -> String {
    let mut map = serde_json::Map::new();
    for (date, day) in forecast {
        let mut obj = serde_json::Map::new();
        obj.insert("temp_high".into(), serde_json::json!(day.temp_high));
        obj.insert("temp_low".into(), serde_json::json!(day.temp_low));
        obj.insert("temp_mid".into(), serde_json::json!(day.temp_mid));
        obj.insert("symbol".into(), serde_json::json!(day.symbol));
        obj.insert("wind".into(), serde_json::json!(day.wind));
        obj.insert("cloud".into(), serde_json::json!(day.cloud));
        map.insert(date.clone(), JsonValue::Object(obj));
    }
    JsonValue::Object(map).to_string()
}

fn parse_forecast_json(json_str: &str) -> Option<HashMap<String, DayForecast>> {
    let val: JsonValue = serde_json::from_str(json_str).ok()?;
    let obj = val.as_object()?;

    let mut forecast = HashMap::new();
    for (date, entry) in obj {
        let e = entry.as_object()?;
        forecast.insert(
            date.clone(),
            DayForecast {
                temp_high: e.get("temp_high")?.as_f64()?,
                temp_low: e.get("temp_low")?.as_f64()?,
                temp_mid: e.get("temp_mid")?.as_f64()?,
                symbol: e.get("symbol")?.as_str()?.to_string(),
                wind: e.get("wind")?.as_f64()?,
                cloud: e.get("cloud")?.as_i64()?,
            },
        );
    }

    Some(forecast)
}
