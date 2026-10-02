use std::{error::Error, fmt};

use crate::config::RedisConfig;

/// Read-only environment smoke check. It neither creates nor clears job keys.
/// The timeout bounds connection setup and PING together.
pub async fn check_connection(config: &RedisConfig) -> Result<(), CheckError> {
    let check = async {
        let client = ::redis::Client::open(config.connection_info.clone())
            .map_err(|error| CheckError::redis("initialize client", error))?;
        // One explicit deadline owns the whole check; do not let the crate's
        // shorter default connect/response timeouts override configuration.
        let connection_config = ::redis::AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(None);
        let mut connection = client
            .get_multiplexed_async_connection_with_config(&connection_config)
            .await
            .map_err(|error| CheckError::redis("connect", error))?;
        let reply: String = ::redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(|error| CheckError::redis("PING", error))?;
        if reply != "PONG" {
            return Err(CheckError::UnexpectedReply);
        }
        Ok(())
    };
    tokio::time::timeout(config.timeout, check)
        .await
        .map_err(|_| CheckError::Timeout)?
}

#[derive(Debug)]
pub enum CheckError {
    Timeout,
    Redis {
        operation: &'static str,
        kind: ::redis::ErrorKind,
    },
    UnexpectedReply,
}

impl CheckError {
    fn redis(operation: &'static str, error: ::redis::RedisError) -> Self {
        // Server messages and transport errors may contain secrets. Preserve
        // the stage and error category, not the raw message or URL.
        Self::Redis {
            operation,
            kind: error.kind(),
        }
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => write!(
                f,
                "Redis check timed out; verify the service, address and network, or increase --redis-timeout-secs"
            ),
            Self::Redis { operation, kind } => write!(
                f,
                "Redis {operation} failed ({kind:?}); verify the service, address and authentication settings"
            ),
            Self::UnexpectedReply => write!(f, "Redis PING did not return PONG"),
        }
    }
}

impl Error for CheckError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_error_details_are_not_exposed() {
        let raw =
            ::redis::RedisError::from((::redis::ErrorKind::AuthenticationFailed, "test-secret"));
        let error = CheckError::redis("connect", raw);
        assert!(error.to_string().contains("AuthenticationFailed"));
        assert!(!format!("{error:?}: {error}").contains("test-secret"));
    }

    #[tokio::test]
    async fn silent_server_cannot_make_check_wait_forever() {
        // A TCP peer that accepts but never speaks RESP tests the timeout, not
        // Redis interoperability (which has its own opt-in integration test).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let config = RedisConfig::new(&format!("redis://{address}/0"), 1).unwrap();
        let check = tokio::spawn(async move { check_connection(&config).await });
        let (_socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), check)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(CheckError::Timeout)), "{result:?}");
    }
}
