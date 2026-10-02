use std::{error::Error, fmt, time::Duration};

use redis::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo};

pub const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";
pub const DEFAULT_REDIS_TIMEOUT_SECS: u64 = 5;

/// Validated settings shared by all Redis-facing commands.
///
/// Connection information contains credentials; Debug deliberately omits it.
pub struct RedisConfig {
    pub(crate) connection_info: ConnectionInfo,
    pub(crate) timeout: Duration,
}

impl RedisConfig {
    pub fn new(url: &str, timeout_secs: u64) -> Result<Self, ConfigError> {
        if !(1..=60).contains(&timeout_secs) {
            return Err(ConfigError::InvalidTimeout);
        }
        // Never expose parser errors: they can include the original URL.
        let connection_info = url
            .into_connection_info()
            .map_err(|_| ConfigError::InvalidRedisUrl)?;
        let ConnectionAddr::Tcp(_, port) = connection_info.addr() else {
            return Err(ConfigError::InvalidRedisUrl);
        };
        if *port == 0 || connection_info.redis_settings().db() < 0 {
            return Err(ConfigError::InvalidRedisUrl);
        }
        Ok(Self {
            connection_info,
            timeout: Duration::from_secs(timeout_secs),
        })
    }
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("connection_info", &"[redacted]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConfigError {
    InvalidRedisUrl,
    InvalidTimeout,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRedisUrl => write!(
                f,
                "invalid Redis URL; use redis://host:port/database with a nonzero port and nonnegative database (TLS and Unix sockets are not supported)"
            ),
            Self::InvalidTimeout => write!(f, "Redis timeout must be between 1 and 60 seconds"),
        }
    }
}

impl Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_local_and_bounded() {
        let config = RedisConfig::new(DEFAULT_REDIS_URL, DEFAULT_REDIS_TIMEOUT_SECS).unwrap();
        assert_eq!(
            config.connection_info.addr(),
            &ConnectionAddr::Tcp("127.0.0.1".into(), 6379)
        );
        assert_eq!(config.connection_info.redis_settings().db(), 0);
        assert_eq!(config.timeout, Duration::from_secs(5));
    }

    #[test]
    fn parses_credentials_database_and_remote_address_without_logging_them() {
        let config = RedisConfig::new("redis://user:test-secret@192.0.2.1:6380/2", 60).unwrap();
        assert_eq!(config.connection_info.redis_settings().db(), 2);
        assert_eq!(
            config.connection_info.redis_settings().username(),
            Some("user")
        );
        assert_eq!(
            config.connection_info.redis_settings().password(),
            Some("test-secret")
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("test-secret"));
        assert!(!debug.contains("192.0.2.1"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn rejects_unsupported_or_invalid_urls_without_echoing_input() {
        for url in [
            "https://user:test-secret@example.org/",
            "redis://user:test-secret@localhost/not-a-database",
            "redis://localhost:0/0",
            "redis://localhost/-1",
            "redis://localhost/9223372036854775808",
            "rediss://localhost/0",
            "unix:///tmp/redis.sock",
            "",
        ] {
            let error = RedisConfig::new(url, 5).unwrap_err();
            assert_eq!(error, ConfigError::InvalidRedisUrl);
            assert!(!error.to_string().contains("test-secret"));
        }
    }

    #[test]
    fn rejects_unbounded_or_zero_timeouts() {
        for seconds in [0, 61, u64::MAX] {
            assert_eq!(
                RedisConfig::new(DEFAULT_REDIS_URL, seconds).unwrap_err(),
                ConfigError::InvalidTimeout
            );
        }
        assert!(RedisConfig::new(DEFAULT_REDIS_URL, 1).is_ok());
    }
}
