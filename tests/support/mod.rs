//! Real Redis isolation shared by storage and frontier suites.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::{AsyncConnectionConfig, FromRedisValue, aio::MultiplexedConnection};
use swarmcrawl::{
    config::RedisConfig,
    jobs::{JobId, JobStore},
};

#[derive(Clone)]
pub struct Context {
    pub config: Arc<RedisConfig>,
    pub connection: MultiplexedConnection,
    pub namespace: String,
}

impl Context {
    pub fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    pub fn job_key(&self, job: JobId, suffix: &str) -> String {
        self.key(&format!("job:{job}:{suffix}"))
    }

    pub async fn store(&self) -> JobStore {
        JobStore::connect_in_namespace(&self.config, &self.namespace)
            .await
            .unwrap()
    }

    pub async fn query<T: FromRedisValue>(&self, command: &mut redis::Cmd) -> T {
        command
            .query_async(&mut self.connection.clone())
            .await
            .unwrap()
    }

    pub async fn hash_set(&self, key: &str, field: &str, value: &str) {
        self.query::<usize>(redis::cmd("HSET").arg(key).arg(field).arg(value))
            .await;
    }

    pub async fn keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, batch): (u64, Vec<String>) = self
                .query(
                    redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(self.key("*"))
                        .arg("COUNT")
                        .arg(100),
                )
                .await;
            keys.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        keys
    }

    pub async fn cleanup(&self) {
        for key in self.keys().await {
            self.query::<usize>(redis::cmd("DEL").arg(key)).await;
        }
        assert!(self.keys().await.is_empty(), "test-owned keys remain");
    }
}

pub async fn with_redis<F, Fut>(case: F) -> Result<(), tokio::task::JoinError>
where
    F: FnOnce(Context) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let url = std::env::var("CRAWL_REDIS_URL")
        .expect("set CRAWL_REDIS_URL or run scripts/redis-smoke.sh");
    let config = Arc::new(RedisConfig::new(&url, 5).unwrap());
    let connection_config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(5)))
        .set_response_timeout(Some(Duration::from_secs(5)));
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection_with_config(&connection_config)
        .await
        .unwrap();
    let context = Context {
        config,
        connection,
        namespace: format!(
            "swarmcrawl-test:v1:{}:{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
    };
    let owned = context.clone();
    let outcome = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(30), case(owned))
            .await
            .expect("bounded Redis test case");
    })
    .await;
    tokio::time::timeout(Duration::from_secs(10), context.cleanup())
        .await
        .expect("bounded owned-key cleanup");
    outcome // Cleanup precedes both success and propagation of a case's panic.
}
