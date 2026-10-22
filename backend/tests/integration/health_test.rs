//! Integration tests for `GET /health/live` and `GET /health/ready`.
//!
//! Each scenario builds a minimal Axum router with a `HealthState` tuned to
//! the conditions under test (healthy DB, broken DB, no worker, etc.).

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use backend::api::handlers::health::{self, HealthState};
use redis::AsyncCommands;
use redis::Client as RedisClient;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A pool whose queries will succeed (requires a reachable PostgreSQL).
fn live_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://postgres:password@localhost:5432/crucible_test".into());
    PgPoolOptions::new()
        .max_connections(2)
        .connect_lazy(&url)
        .expect("live_pool")
}

/// A pool whose queries will always fail.
fn dead_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://invalid:5432/nonexistent")
        .expect("dead_pool")
}

/// A Redis client for a live local instance.
fn live_redis() -> RedisClient {
    let url = std::env::var("REDIS_URL")
        .or_else(|_| std::env::var("TEST_REDIS_URL"))
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    RedisClient::open(url).expect("live_redis client")
}

/// A Redis client whose connections are always refused.
fn dead_redis() -> RedisClient {
    RedisClient::open("redis://127.0.0.1:1/").expect("dead_redis client")
}

/// Register a synthetic worker heartbeat so the queue-probe sees an active
/// worker and reports "up".
async fn register_worker_hb(client: &RedisClient) {
    let payload = serde_json::json!({
        "worker_id": "health-test-worker",
        "last_heartbeat": 0,
        "last_checked": 0,
        "is_healthy": true,
        "uptime_seconds": 0,
    })
    .to_string();
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("register_worker_hb connection");
    let _: () = conn
        .set("worker:health-test-worker:health", &payload)
        .await
        .expect("register_worker_hb");
}

/// Remove the synthetic worker heartbeat.
async fn remove_worker_hb(client: &RedisClient) {
    if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
        let _: Result<(), _> = conn.del("worker:health-test-worker:health").await;
    }
}

/// Parse the response body into a JSON value.
async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("valid JSON")
}

// ---------------------------------------------------------------------------
// Liveness — never depends on external services
// ---------------------------------------------------------------------------

#[tokio::test]
async fn liveness_returns_200() {
    let app = Router::new().route("/live", axum::routing::get(health::liveness));
    let resp = app
        .oneshot(Request::builder().uri("/live").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let json = body_json(resp).await;
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

// ---------------------------------------------------------------------------
// Readiness — all dependencies healthy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readiness_all_healthy() {
    let db = live_pool();
    let redis = live_redis();

    // Register a worker heartbeat so the queue check passes
    register_worker_hb(&redis).await;

    let state = HealthState {
        db,
        redis: redis.clone(),
        soroban_rpc_url: None,
    };

    let app = Router::new().nest("/health", health::router().with_state(state));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "healthy");
    assert_eq!(json["checks"]["database"]["status"], "up");
    assert_eq!(json["checks"]["redis"]["status"], "up");
    assert_eq!(json["checks"]["queue"]["status"], "up");
    assert!(json["version"].is_string());

    // Cleanup — remove the synthetic heartbeat
    remove_worker_hb(&redis).await;
}

// ---------------------------------------------------------------------------
// Readiness — PostgreSQL unavailable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readiness_db_unavailable() {
    let state = HealthState {
        db: dead_pool(),
        redis: live_redis(),
        soroban_rpc_url: None,
    };

    let app = Router::new().nest("/health", health::router().with_state(state));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["checks"]["database"]["status"], "down");
    assert_eq!(
        json["checks"]["database"]["message"],
        "connection unavailable"
    );
}

// ---------------------------------------------------------------------------
// Readiness — queue unavailable (no active workers)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readiness_queue_unavailable() {
    let db = live_pool();
    // No worker heartbeat or Apalis consumer exists in the test namespace,
    // so the queue probe will correctly report "down".
    let state = HealthState {
        db,
        redis: live_redis(),
        soroban_rpc_url: None,
    };

    let app = Router::new().nest("/health", health::router().with_state(state));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["checks"]["queue"]["status"], "down");
    assert_eq!(json["checks"]["queue"]["message"], "workers unavailable");
}

// ---------------------------------------------------------------------------
// Readiness — Redis unavailable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn readiness_redis_unavailable() {
    let state = HealthState {
        db: live_pool(),
        redis: dead_redis(),
        soroban_rpc_url: None,
    };

    let app = Router::new().nest("/health", health::router().with_state(state));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Redis is down so both the cache and queue checks should fail.
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(resp).await;
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["checks"]["redis"]["status"], "down");
    assert_eq!(json["checks"]["redis"]["message"], "connection unavailable");
    assert_eq!(json["checks"]["queue"]["status"], "down");
}

// ---------------------------------------------------------------------------
// Liveness stays healthy when readiness would be degraded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn liveness_stays_healthy_when_deps_are_down() {
    let state = HealthState {
        db: dead_pool(),
        redis: live_redis(),
        soroban_rpc_url: None,
    };

    // Mount both liveness and readiness.
    let app = Router::new().nest("/health", health::router().with_state(state));

    // Liveness MUST return 200 even when deps are down.
    let live = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(live.status(), StatusCode::OK);
    let json = body_json(live).await;
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());

    // Readiness returns 503 with the same broken state.
    let ready = app
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(ready).await;
    assert_eq!(json["status"], "degraded");
    assert_eq!(json["checks"]["database"]["status"], "down");
}
