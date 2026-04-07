// Tock configuration module
// Loads/saves YAML config from ~/.tock/config.yml

use serde_yaml::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

/// User home directory.
pub fn home_dir() -> PathBuf {
    match std::env::var("HOME") {
        Ok(h) => PathBuf::from(h),
        Err(_) => PathBuf::from("/tmp"),
    }
}

/// ~/.tock
pub fn tock_home() -> PathBuf {
    home_dir().join(".tock")
}

/// ~/.tock/tock.db
pub fn tock_db() -> PathBuf {
    tock_home().join("tock.db")
}

/// ~/.tock/config.yml
pub fn tock_config() -> PathBuf {
    tock_home().join("config.yml")
}

/// Exported path constants (computed once, accessible as &PathBuf).
pub static TOCK_HOME: LazyLock<PathBuf> = LazyLock::new(tock_home);
pub static TOCK_DB: LazyLock<PathBuf> = LazyLock::new(tock_db);
pub static TOCK_CONFIG: LazyLock<PathBuf> = LazyLock::new(tock_config);

// Keep old names as aliases for compatibility with other modules
pub fn timely_home() -> PathBuf { tock_home() }
pub fn timely_db() -> PathBuf { tock_db() }
pub fn timely_config() -> PathBuf { tock_config() }

/// Build the default configuration as a serde_yaml::Value tree.
fn default_config() -> Value {
    let yaml_str = r#"
version: "1.0.3"
location:
  lat: 59.9139
  lon: 10.7522
timezone: Europe/Oslo
timezone_offset: 1
default_view: month
work_hours:
  start: 8
  end: 17
week_starts_on: monday
google:
  safe_dir: ~/.config/tock/credentials
  sync_interval: 300
outlook:
  client_id: ''
  tenant_id: common
notifications:
  enabled: true
  default_alarm: 15
colors:
  selected_bg_a: 235
  selected_bg_b: 234
  alt_bg_a: 233
  alt_bg_b: 0
  current_month_bg: 233
  saturday: 208
  sunday: 167
  today_fg: 232
  today_bg: 246
  slot_selected_bg: 237
  info_bg: 235
  status_bg: 235
default_calendar: 1
"#;
    serde_yaml::from_str(yaml_str).expect("default config must parse")
}

/// Merge `overlay` into `base`, preserving keys in `base` that are absent
/// from `overlay`. Both must be Mapping values at the top level.
fn deep_merge(base: &mut Value, overlay: &Value) {
    match (base, overlay) {
        (Value::Mapping(b), Value::Mapping(o)) => {
            for (k, v) in o {
                if let Some(existing) = b.get_mut(k) {
                    deep_merge(existing, v);
                } else {
                    b.insert(k.clone(), v.clone());
                }
            }
        }
        (base, overlay) => {
            *base = overlay.clone();
        }
    }
}

/// YAML-backed configuration with dot-path accessors.
pub struct Config {
    data: Value,
    path: PathBuf,
}

impl Config {
    /// Load config from ~/.tock/config.yml.
    /// Creates the directory and a default file if they do not exist.
    /// Merges defaults under any existing file (so new keys get added).
    pub fn new() -> Self {
        let dir = tock_home();
        let path = tock_config();

        // Ensure directory exists.
        if !dir.exists() {
            let _ = fs::create_dir_all(&dir);
        }

        let mut data = default_config();

        if path.exists() {
            if let Ok(contents) = fs::read_to_string(&path) {
                if let Ok(file_val) = serde_yaml::from_str::<Value>(&contents) {
                    deep_merge(&mut data, &file_val);
                }
            }
        } else {
            // Write defaults to disk.
            if let Ok(yaml) = serde_yaml::to_string(&data) {
                let _ = fs::write(&path, yaml);
            }
        }

        Config { data, path }
    }

    fn resolve(&self, key_path: &str) -> Option<&Value> {
        let mut cur = &self.data;
        for seg in key_path.split('.') {
            match cur {
                Value::Mapping(m) => {
                    cur = m.get(Value::String(seg.to_string()))?;
                }
                _ => return None,
            }
        }
        Some(cur)
    }

    fn resolve_mut(&mut self, key_path: &str) -> &mut Value {
        let segments: Vec<&str> = key_path.split('.').collect();
        let mut cur = &mut self.data;
        for seg in &segments {
            if !cur.is_mapping() {
                *cur = Value::Mapping(serde_yaml::Mapping::new());
            }
            let key = Value::String(seg.to_string());
            if cur.as_mapping().unwrap().get(&key).is_none() {
                cur.as_mapping_mut()
                    .unwrap()
                    .insert(key.clone(), Value::Null);
            }
            cur = cur.as_mapping_mut().unwrap().get_mut(&key).unwrap();
        }
        cur
    }

    pub fn get(&self, key_path: &str, default: Value) -> Value {
        self.resolve(key_path).cloned().unwrap_or(default)
    }

    pub fn get_i64(&self, key_path: &str, default: i64) -> i64 {
        match self.resolve(key_path) {
            Some(Value::Number(n)) => n.as_i64().unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_f64(&self, key_path: &str, default: f64) -> f64 {
        match self.resolve(key_path) {
            Some(Value::Number(n)) => n.as_f64().unwrap_or(default),
            _ => default,
        }
    }

    pub fn get_str(&self, key_path: &str, default: &str) -> String {
        match self.resolve(key_path) {
            Some(Value::String(s)) => s.clone(),
            _ => default.to_string(),
        }
    }

    pub fn get_bool(&self, key_path: &str, default: bool) -> bool {
        match self.resolve(key_path) {
            Some(Value::Bool(b)) => *b,
            _ => default,
        }
    }

    pub fn set(&mut self, key_path: &str, value: Value) {
        let slot = self.resolve_mut(key_path);
        *slot = value;
    }

    pub fn save(&self) -> std::io::Result<()> {
        let mut merged = if self.path.exists() {
            if let Ok(contents) = fs::read_to_string(&self.path) {
                serde_yaml::from_str::<Value>(&contents).unwrap_or_else(|_| Value::Mapping(serde_yaml::Mapping::new()))
            } else {
                Value::Mapping(serde_yaml::Mapping::new())
            }
        } else {
            Value::Mapping(serde_yaml::Mapping::new())
        };

        deep_merge(&mut merged, &self.data);

        let yaml = serde_yaml::to_string(&merged)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(&self.path, yaml)
    }

    pub fn reload(&mut self) {
        let mut data = default_config();
        if self.path.exists() {
            if let Ok(contents) = fs::read_to_string(&self.path) {
                if let Ok(file_val) = serde_yaml::from_str::<Value>(&contents) {
                    deep_merge(&mut data, &file_val);
                }
            }
        }
        self.data = data;
    }
}
