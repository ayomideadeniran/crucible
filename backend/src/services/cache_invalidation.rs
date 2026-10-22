//! Redis Cache Invalidation System
//!
//! This module implements a robust cache invalidation system that works with Redis
//! to ensure data consistency between the database and cache layers.

use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Cache invalidation strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvalidationStrategy {
    /// Invalidate specific keys
    Key,
    /// Invalidate all keys matching a pattern
    Pattern,
    /// Invalidate all keys in a namespace
    Namespace,
    /// Invalidate based on dependencies (cache tags)
    Tag,
}

/// Cache invalidation event
#[derive(Debug, Clone)]
pub struct CacheInvalidationEvent {
    /// Strategy to use for invalidation
    pub strategy: InvalidationStrategy,
    /// Keys or patterns to invalidate
    pub targets: Vec<String>,
    /// Optional namespace or tag information
    pub metadata: Option<HashMap<String, String>>,
    /// Timestamp when event was created
    pub timestamp: std::time::Instant,
}

/// Cache invalidation manager
#[derive(Debug, Clone)]
pub struct CacheInvalidationManager {
    /// Redis client for cache operations
    redis_client: Arc<redis::Client>,
    /// Channel for receiving invalidation events
    tx: mpsc::Sender<CacheInvalidationEvent>,
    /// Configuration for invalidation behavior
    config: CacheInvalidationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInvalidationConfig {
    /// Whether to enable background invalidation processing
    pub background_processing: bool,
    /// Maximum number of concurrent invalidation operations
    pub max_concurrent: usize,
    /// Timeout for individual invalidation operations
    pub timeout_ms: u64,
    /// Retry configuration
    pub retry_attempts: u8,
    /// Delay between retries in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for CacheInvalidationConfig {
    fn default() -> Self {
        Self {
            background_processing: true,
            max_concurrent: 10,
            timeout_ms: 5000,
            retry_attempts: 3,
            retry_delay_ms: 100,
        }
    }
}

/// Number of keys fetched per `SCAN` page when invalidating by pattern.
///
/// `SCAN`'s `COUNT` is only a hint of the work done per call: it samples the
/// keyspace and yields between iterations instead of blocking the
/// single-threaded Redis event loop for the whole keyspace like `KEYS` does.
const SCAN_PAGE_SIZE: usize = 500;

/// Fetches a single page of keys matching `pattern`, starting at `cursor`.
///
/// Returns the cursor to continue scanning with (`0` marks the end of the
/// iteration) together with the keys found in this page.
async fn scan_page(
    conn: &mut redis::aio::MultiplexedConnection,
    cursor: u64,
    pattern: &str,
) -> Result<(u64, Vec<String>), redis::RedisError> {
    redis::cmd("SCAN")
        .arg(cursor)
        .arg("MATCH")
        .arg(pattern)
        .arg("COUNT")
        .arg(SCAN_PAGE_SIZE)
        .query_async(conn)
        .await
}

/// Deletes `keys` in a single pipelined round trip.
async fn delete_keys(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<(), redis::RedisError> {
    if keys.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    for key in keys {
        pipe.del(key);
    }
    let _: () = pipe.query_async(conn).await?;
    Ok(())
}

/// Deletes every key matching `pattern`, page by page, and returns how many
/// keys were deleted.
///
/// Keys are deleted per `SCAN` page so that neither the Redis keyspace walk
/// nor the client-side key buffer grows unbounded on large caches.
async fn scan_and_delete(
    conn: &mut redis::aio::MultiplexedConnection,
    pattern: &str,
) -> Result<u64, redis::RedisError> {
    let mut cursor: u64 = 0;
    let mut deleted: u64 = 0;
    loop {
        let (next_cursor, page) = scan_page(conn, cursor, pattern).await?;
        if !page.is_empty() {
            delete_keys(conn, &page).await?;
            deleted += page.len() as u64;
        }
        if next_cursor == 0 {
            return Ok(deleted);
        }
        cursor = next_cursor;
    }
}

impl CacheInvalidationManager {
    /// Create a new cache invalidation manager
    pub fn new(redis_client: Arc<redis::Client>, config: CacheInvalidationConfig) -> Self {
        let (tx, mut rx) = mpsc::channel::<CacheInvalidationEvent>(100);

        // Start background processor if enabled
        if config.background_processing {
            let client = redis_client.clone();
            let config_clone = config.clone();

            tokio::spawn(async move {
                Self::background_processor(client, rx, config_clone).await;
            });
        }

        Self {
            redis_client,
            tx,
            config,
        }
    }

    /// Enqueue an invalidation event
    pub async fn enqueue_invalidation(
        &self,
        event: CacheInvalidationEvent,
    ) -> Result<(), anyhow::Error> {
        self.tx.send(event).await.map_err(|e| {
            error!(error = ?e, "Failed to send invalidation event");
            anyhow::anyhow!("Failed to send invalidation event: {}", e)
        })
    }

    /// Invalidate specific cache keys synchronously
    pub async fn invalidate_keys(&self, keys: Vec<String>) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        // Use pipelining for better performance
        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.del(key);
        }

        let _: () = pipe.query_async(&mut conn).await?;

        info!(count = keys.len(), "Invalidated cache keys");
        Ok(())
    }

    /// Invalidate keys matching a pattern
    ///
    /// Matching keys are discovered with cursor-based `SCAN` iteration and
    /// deleted page by page. `KEYS` is deliberately avoided: it walks the
    /// whole keyspace in one blocking call on the single-threaded Redis event
    /// loop, causing severe latency spikes for all other clients.
    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let deleted = scan_and_delete(&mut conn, pattern).await?;

        if deleted > 0 {
            info!(pattern = %pattern, count = deleted, "Invalidated keys by pattern");
        }

        Ok(())
    }

    /// Invalidate all keys in a namespace
    pub async fn invalidate_namespace(&self, namespace: &str) -> Result<(), anyhow::Error> {
        let pattern = format!("{}:*", namespace);
        self.invalidate_pattern(&pattern).await
    }

    /// Invalidate keys by tag (using Redis sets)
    pub async fn invalidate_by_tag(&self, tag: &str) -> Result<(), anyhow::Error> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        // Get all keys associated with this tag
        let keys: Vec<String> = redis::cmd("SMEMBERS")
            .arg(format!("cache:tags:{}", tag))
            .query_async(&mut conn)
            .await?;

        if !keys.is_empty() {
            self.invalidate_keys(keys.clone()).await?;
            info!(tag = %tag, count = keys.len(), "Invalidated keys by tag");
        }

        Ok(())
    }

    /// Background processor for handling invalidation events
    async fn background_processor(
        redis_client: Arc<redis::Client>,
        mut rx: mpsc::Receiver<CacheInvalidationEvent>,
        config: CacheInvalidationConfig,
    ) {
        info!("Starting cache invalidation background processor");

        while let Some(event) = rx.recv().await {
            debug!(strategy = ?event.strategy, targets_count = event.targets.len(), "Processing invalidation event");

            let result = match event.strategy {
                InvalidationStrategy::Key => {
                    Self::process_key_invalidation(&redis_client, &event.targets, &config).await
                }
                InvalidationStrategy::Pattern => {
                    Self::process_pattern_invalidation(&redis_client, &event.targets, &config).await
                }
                InvalidationStrategy::Namespace => {
                    Self::process_namespace_invalidation(&redis_client, &event.targets, &config)
                        .await
                }
                InvalidationStrategy::Tag => {
                    Self::process_tag_invalidation(&redis_client, &event.targets, &config).await
                }
            };

            if let Err(e) = result {
                error!(error = ?e, "Failed to process invalidation event");
            }
        }
    }

    async fn process_key_invalidation(
        redis_client: &Arc<redis::Client>,
        keys: &[String],
        config: &CacheInvalidationConfig,
    ) -> Result<(), anyhow::Error> {
        let mut attempts = 0;
        loop {
            match Self::invalidate_keys_impl(redis_client, keys.to_vec()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= config.retry_attempts {
                        return Err(e);
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(config.retry_delay_ms))
                        .await;
                }
            }
        }
    }

    async fn process_pattern_invalidation(
        redis_client: &Arc<redis::Client>,
        patterns: &[String],
        config: &CacheInvalidationConfig,
    ) -> Result<(), anyhow::Error> {
        let mut attempts = 0;
        loop {
            match Self::invalidate_patterns_impl(redis_client, patterns.to_vec()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= config.retry_attempts {
                        return Err(e);
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(config.retry_delay_ms))
                        .await;
                }
            }
        }
    }

    async fn process_namespace_invalidation(
        redis_client: &Arc<redis::Client>,
        namespaces: &[String],
        config: &CacheInvalidationConfig,
    ) -> Result<(), anyhow::Error> {
        let mut attempts = 0;
        loop {
            match Self::invalidate_namespaces_impl(redis_client, namespaces.to_vec()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= config.retry_attempts {
                        return Err(e);
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(config.retry_delay_ms))
                        .await;
                }
            }
        }
    }

    async fn process_tag_invalidation(
        redis_client: &Arc<redis::Client>,
        tags: &[String],
        config: &CacheInvalidationConfig,
    ) -> Result<(), anyhow::Error> {
        let mut attempts = 0;
        loop {
            match Self::invalidate_tags_impl(redis_client, tags.to_vec()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= config.retry_attempts {
                        return Err(e);
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(config.retry_delay_ms))
                        .await;
                }
            }
        }
    }

    async fn invalidate_keys_impl(
        redis_client: &Arc<redis::Client>,
        keys: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        // Use pipelining for better performance
        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.del(key);
        }

        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    async fn invalidate_patterns_impl(
        redis_client: &Arc<redis::Client>,
        patterns: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        for pattern in &patterns {
            let mut conn = redis_client.get_multiplexed_async_connection().await?;
            scan_and_delete(&mut conn, pattern).await?;
        }
        Ok(())
    }

    async fn invalidate_namespaces_impl(
        redis_client: &Arc<redis::Client>,
        namespaces: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        for namespace in &namespaces {
            let pattern = format!("{}:*", namespace);
            Self::invalidate_patterns_impl(redis_client, vec![pattern]).await?;
        }
        Ok(())
    }

    async fn invalidate_tags_impl(
        redis_client: &Arc<redis::Client>,
        tags: Vec<String>,
    ) -> Result<(), anyhow::Error> {
        for tag in &tags {
            let mut conn = redis_client.get_multiplexed_async_connection().await?;

            // Get all keys associated with this tag
            let keys: Vec<String> = redis::cmd("SMEMBERS")
                .arg(format!("cache:tags:{}", tag))
                .query_async(&mut conn)
                .await?;
            if !keys.is_empty() {
                Self::invalidate_keys_impl(redis_client, keys).await?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transaction Simulation Cache (Moka L1 + Redis L2)
// ---------------------------------------------------------------------------

use std::time::Duration;

/// Key structure for transaction simulation caching.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SimulationCacheKey {
    pub contract_id: String,
    pub function: String,
    pub args_hash: String,
    pub ledger_sequence: u64,
}

impl SimulationCacheKey {
    pub fn new(
        contract_id: impl Into<String>,
        function: impl Into<String>,
        args_hash: impl Into<String>,
        ledger_sequence: u64,
    ) -> Self {
        Self {
            contract_id: contract_id.into(),
            function: function.into(),
            args_hash: args_hash.into(),
            ledger_sequence,
        }
    }

    pub fn to_redis_key(&self) -> String {
        format!(
            "sim:{}:{}:{}:{}",
            self.contract_id, self.function, self.args_hash, self.ledger_sequence
        )
    }
}

/// Simulation execution result payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub success: bool,
    pub result_xdr: String,
    pub cpu_instructions: u64,
    pub memory_bytes: u64,
    pub footprint: Vec<String>,
}

/// Two-tier simulation cache manager (Moka in-memory + Redis cluster).
#[derive(Clone)]
pub struct SimulationCacheManager {
    moka_cache: moka::future::Cache<String, SimulationResult>,
    redis_client: Arc<redis::Client>,
    ttl: Duration,
}

impl SimulationCacheManager {
    pub fn new(redis_client: Arc<redis::Client>, ttl_secs: u64) -> Self {
        let moka_cache = moka::future::Cache::builder()
            .max_capacity(10_000)
            .time_to_live(Duration::from_secs(ttl_secs))
            .build();

        Self {
            moka_cache,
            redis_client,
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    pub async fn get(&self, key: &SimulationCacheKey) -> Option<SimulationResult> {
        let redis_key = key.to_redis_key();

        // Level 1: Moka in-memory cache
        if let Some(res) = self.moka_cache.get(&redis_key).await {
            debug!(key = %redis_key, "Simulation cache L1 hit (Moka)");
            return Some(res);
        }

        // Level 2: Redis cluster cache
        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            let res: Result<Option<String>, _> = conn.get(&redis_key).await;
            if let Ok(Some(cached_json)) = res {
                if let Ok(res) = serde_json::from_str::<SimulationResult>(&cached_json) {
                    debug!(key = %redis_key, "Simulation cache L2 hit (Redis)");
                    self.moka_cache.insert(redis_key, res.clone()).await;
                    return Some(res);
                }
            }
        }

        None
    }

    pub async fn insert(&self, key: &SimulationCacheKey, result: SimulationResult) {
        let redis_key = key.to_redis_key();
        self.moka_cache
            .insert(redis_key.clone(), result.clone())
            .await;

        if let Ok(mut conn) = self.redis_client.get_multiplexed_async_connection().await {
            if let Ok(json) = serde_json::to_string(&result) {
                let _: Result<(), _> = conn.set_ex(redis_key, json, self.ttl.as_secs()).await;
            }
        }
    }

    /// Invalidate simulation cache entries when a ledger modifies the target contract footprint.
    ///
    /// Matching keys are discovered with cursor-based `SCAN` iteration; each
    /// page is invalidated from the L1 Moka cache and deleted from Redis as
    /// it is discovered, so neither layer buffers the full keyset.
    pub async fn invalidate_contract_footprint(
        &self,
        contract_id: &str,
    ) -> Result<(), anyhow::Error> {
        info!(contract_id = %contract_id, "Invalidating simulation cache for updated contract footprint");

        let pattern = format!("sim:{}*", contract_id);
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        let mut cursor: u64 = 0;
        loop {
            let (next_cursor, page) = scan_page(&mut conn, cursor, &pattern).await?;
            if !page.is_empty() {
                for key in &page {
                    self.moka_cache.invalidate(key).await;
                }
                delete_keys(&mut conn, &page).await?;
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(())
    }
}

/// Helper trait for cache-aware services
pub trait CacheAware {
    /// Invalidate cache for this entity
    fn invalidate_cache(&self) -> Result<(), anyhow::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_cache_invalidation_manager_creation() {
        // Test that manager can be created
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let manager =
            CacheInvalidationManager::new(Arc::new(client), CacheInvalidationConfig::default());

        assert!(manager.config.background_processing);
        assert_eq!(manager.config.max_concurrent, 10);
    }

    #[tokio::test]
    async fn test_invalidation_strategy_enum() {
        assert_eq!(InvalidationStrategy::Key as i32, 0);
        assert_eq!(InvalidationStrategy::Pattern as i32, 1);
        assert_eq!(InvalidationStrategy::Namespace as i32, 2);
        assert_eq!(InvalidationStrategy::Tag as i32, 3);
    }

    #[tokio::test]
    async fn test_invalidate_pattern_returns_error_when_redis_unreachable() {
        // Point at a closed port so connection setup fails immediately. The
        // SCAN-based invalidation must surface the error instead of hanging.
        let client = Arc::new(redis::Client::open("redis://127.0.0.1:1/").unwrap());
        let manager = CacheInvalidationManager::new(
            client,
            CacheInvalidationConfig {
                background_processing: false,
                ..Default::default()
            },
        );

        let result = manager.invalidate_pattern("unreachable:*").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalidate_pattern_deletes_only_matching_keys() {
        let client = Arc::new(redis::Client::open("redis://127.0.0.1/").unwrap());
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(_) => return, // no local Redis available; nothing to verify
        };

        let mut seed = redis::pipe();
        seed.set("crucible_scan_test:one", 1);
        seed.set("crucible_scan_test:two", 2);
        seed.set("crucible_scan_test_other:three", 3);
        let _: () = seed.query_async(&mut conn).await.unwrap();

        let manager = CacheInvalidationManager::new(
            client,
            CacheInvalidationConfig {
                background_processing: false,
                ..Default::default()
            },
        );
        manager
            .invalidate_pattern("crucible_scan_test:*")
            .await
            .unwrap();

        let one: Option<i32> = conn.get("crucible_scan_test:one").await.unwrap();
        let two: Option<i32> = conn.get("crucible_scan_test:two").await.unwrap();
        let other: Option<i32> = conn.get("crucible_scan_test_other:three").await.unwrap();
        assert!(one.is_none(), "matching key must be deleted");
        assert!(two.is_none(), "matching key must be deleted");
        assert_eq!(other, Some(3), "non-matching key must survive");

        let _: () = redis::cmd("DEL")
            .arg("crucible_scan_test_other:three")
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_invalidate_contract_footprint_deletes_only_matching_keys() {
        let client = Arc::new(redis::Client::open("redis://127.0.0.1/").unwrap());
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(_) => return, // no local Redis available; nothing to verify
        };

        let sim_cache = SimulationCacheManager::new(client, 300);
        sim_cache
            .insert(
                &SimulationCacheKey::new("CTR_SCAN_TEST", "transfer", "hash_1", 1),
                SimulationResult {
                    success: true,
                    result_xdr: "AAAAAAA==".to_string(),
                    cpu_instructions: 1,
                    memory_bytes: 1,
                    footprint: vec![],
                },
            )
            .await;
        sim_cache
            .insert(
                &SimulationCacheKey::new("CTR_OTHER_TEST", "transfer", "hash_2", 2),
                SimulationResult {
                    success: true,
                    result_xdr: "BBBBBBB==".to_string(),
                    cpu_instructions: 2,
                    memory_bytes: 2,
                    footprint: vec![],
                },
            )
            .await;

        sim_cache
            .invalidate_contract_footprint("CTR_SCAN_TEST")
            .await
            .unwrap();

        let matching: Option<String> = conn
            .get("sim:CTR_SCAN_TEST:transfer:hash_1:1")
            .await
            .unwrap();
        let other: Option<String> = conn
            .get("sim:CTR_OTHER_TEST:transfer:hash_2:2")
            .await
            .unwrap();
        assert!(
            matching.is_none(),
            "matching simulation key must be deleted"
        );
        assert!(other.is_some(), "non-matching simulation key must survive");

        let _: () = redis::cmd("DEL")
            .arg("sim:CTR_OTHER_TEST:transfer:hash_2:2")
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_simulation_cache_hit_vs_miss_latency() {
        let client = Arc::new(redis::Client::open("redis://127.0.0.1/").unwrap());
        let sim_cache = SimulationCacheManager::new(client.clone(), 300);

        // A previous run may have populated this key and it would still be
        // inside its TTL, so start from a guaranteed cold cache.
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
            let _: redis::RedisResult<i64> = redis::cmd("DEL")
                .arg("sim:C123456789:transfer:hash_abc_123:100500")
                .query_async(&mut conn)
                .await;
        }

        let key = SimulationCacheKey::new("C123456789", "transfer", "hash_abc_123", 100500);
        let sim_res = SimulationResult {
            success: true,
            result_xdr: "AAAAAAA==".to_string(),
            cpu_instructions: 150_000,
            memory_bytes: 4096,
            footprint: vec!["contract_data".to_string()],
        };

        // Cache miss timing
        let t_start_miss = Instant::now();
        let miss_res = sim_cache.get(&key).await;
        let miss_duration = t_start_miss.elapsed();

        assert!(miss_res.is_none());

        // Insert into cache
        sim_cache.insert(&key, sim_res).await;

        // Cache hit timing
        let t_start_hit = Instant::now();
        let hit_res = sim_cache.get(&key).await;
        let hit_duration = t_start_hit.elapsed();

        assert!(hit_res.is_some());
        assert_eq!(hit_res.unwrap().cpu_instructions, 150_000);
        assert!(hit_duration < miss_duration + Duration::from_millis(50));
    }
}
