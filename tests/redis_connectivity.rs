use swarmcrawl::{
    config::{DEFAULT_REDIS_TIMEOUT_SECS, RedisConfig},
    redis::check_connection,
};

#[tokio::test]
#[ignore = "requires a real isolated Redis; run scripts/redis-smoke.sh"]
async fn real_redis_answers_ping() {
    let url = std::env::var("SWARMCRAWL_REDIS_URL")
        .expect("set SWARMCRAWL_REDIS_URL to the isolated test Redis endpoint");
    let config = RedisConfig::new(&url, DEFAULT_REDIS_TIMEOUT_SECS).unwrap();
    check_connection(&config)
        .await
        .expect("real Redis connectivity check");
}
