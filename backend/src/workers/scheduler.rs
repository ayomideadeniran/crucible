//! SCHEDULER APPROACH: Option B — Custom Tokio interval loop with cron crate
//! Rationale: We require strict control over execution guarantees such as distributed locking, timeouts, retries, and history logging. A custom loop using `tokio::time::sleep_until` and the `cron` crate allows us to wrap the execution precisely in a Redis lock and accurately handle timeouts using `tokio::time::timeout`.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::workers::error::{JobError, SchedulerError};
use crate::workers::job_history::{
    get_last_run_at, record_failure, record_start, record_success, record_timeout,
};

/// Only ticks missed within this window are eligible for catch-up after a
/// failover. Older ticks are considered stale and are not replayed.
const CATCH_UP_TOLERANCE: Duration = Duration::from_secs(60 * 60);

/// Upper bound on the number of missed ticks replayed in a single catch-up
/// pass, so a long outage cannot cause a burst of executions.
const MAX_CATCH_UP_RUNS: usize = 3;

/// Context passed to a job handler during execution.
#[derive(Clone)]
pub struct JobContext {
    pub job_name: String,
    pub scheduled_at: chrono::DateTime<Utc>,
    pub pool: PgPool,
    pub redis: redis::Client,
}

/// Trait implemented by all schedulable jobs.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn run(&self, ctx: JobContext) -> Result<(), JobError>;
}

/// Definition of a single job.
#[derive(Clone)]
pub struct JobDefinition {
    pub name: String,
    pub cron_expr: String,
    pub handler: Arc<dyn JobHandler>,
    pub timeout_secs: u64,
    pub max_retries: u8,
}

/// The main job scheduler.
pub struct Scheduler {
    jobs: Vec<JobDefinition>,
    pool: PgPool,
    redis: redis::Client,
    started: bool,
}

impl Scheduler {
    pub fn new(pool: PgPool, redis: redis::Client) -> Self {
        Self {
            jobs: Vec::new(),
            pool,
            redis,
            started: false,
        }
    }

    /// Registers a job before starting the scheduler.
    pub fn register(&mut self, job: JobDefinition) -> Result<(), SchedulerError> {
        if self.started {
            return Err(SchedulerError::AlreadyStarted);
        }
        // Verify the cron expression is valid
        Schedule::from_str(&job.cron_expr)
            .map_err(|e| SchedulerError::JobRegistrationFailed(e.to_string()))?;

        self.jobs.push(job);
        Ok(())
    }

    /// Starts the scheduler, spawning all job loops as Tokio tasks.
    /// Returns a `SchedulerHandle` to manage graceful shutdown.
    pub fn start(mut self) -> Result<SchedulerHandle, SchedulerError> {
        if self.started {
            return Err(SchedulerError::AlreadyStarted);
        }
        self.started = true;

        let cancel_token = CancellationToken::new();
        let mut task_handles = Vec::new();

        for job in self.jobs {
            let cancel_token = cancel_token.clone();
            let pool = self.pool.clone();
            let redis = self.redis.clone();

            let handle = tokio::spawn(async move {
                run_job_loop(job, pool, redis, cancel_token).await;
            });
            task_handles.push(handle);
        }

        Ok(SchedulerHandle {
            cancel_token,
            task_handles: Arc::new(Mutex::new(task_handles)),
        })
    }
}

/// A handle to the running scheduler, allowing for graceful shutdown.
#[derive(Clone)]
pub struct SchedulerHandle {
    cancel_token: CancellationToken,
    task_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl SchedulerHandle {
    /// Signals all job loops to stop and waits for them to finish cleanly.
    pub async fn shutdown(self) -> Result<(), SchedulerError> {
        info!("Initiating scheduler graceful shutdown...");
        self.cancel_token.cancel();

        let mut handles = self.task_handles.lock().await;
        for handle in handles.drain(..) {
            // Wait for the task to finish, ignoring cancel errors
            let _ = handle.await;
        }

        info!("Scheduler stopped successfully");
        Ok(())
    }
}

/// The core loop for a single registered job.
#[instrument(skip(job, pool, redis, cancel_token), fields(job_name = %job.name))]
async fn run_job_loop(
    job: JobDefinition,
    pool: PgPool,
    redis: redis::Client,
    cancel_token: CancellationToken,
) {
    let schedule =
        Schedule::from_str(&job.cron_expr).expect("cron expression must be pre-validated");

    loop {
        // Recover ticks that were missed while no instance was running this
        // job (for example during a leader failover). Without this, a newly
        // elected leader would compute its next execution from `now()` and
        // silently skip every tick that elapsed during the failover window.
        catch_up_missed_ticks(&job, &schedule, &pool, &redis, &cancel_token).await;

        // Calculate the next execution time
        let now = Utc::now();
        let next_tick = match schedule.upcoming(Utc).next() {
            Some(time) => time,
            None => {
                warn!("No upcoming execution time found for job");
                break;
            }
        };

        let duration_until_next = match (next_tick - now).to_std() {
            Ok(duration) => duration,
            Err(_) => {
                // Next tick is in the past, meaning we're falling behind. Tick immediately.
                Duration::from_millis(0)
            }
        };

        debug!("Next execution scheduled in {:?}", duration_until_next);

        // Wait until the next tick using an interval with MissedTickBehavior::Skip
        // to prevent rapid burst executions if Tokio runtime is starved
        let mut interval_ticker =
            tokio::time::interval(duration_until_next.max(Duration::from_millis(1)));
        interval_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval_ticker.tick().await; // consume initial tick

        tokio::select! {
            _ = interval_ticker.tick() => {}
            _ = cancel_token.cancelled() => {
                info!("Job loop cancelled, shutting down");
                break;
            }
        }

        try_run_tick(&job, &pool, &redis, next_tick).await;
    }
}

/// Replays cron ticks missed since the last recorded run.
///
/// Only ticks inside [`CATCH_UP_TOLERANCE`] are considered, and at most
/// [`MAX_CATCH_UP_RUNS`] of the most recent ones are executed so that a long
/// outage cannot trigger an unbounded burst. A per-tick Redis marker makes the
/// replay idempotent when several scheduler instances start simultaneously
/// after a failover.
async fn catch_up_missed_ticks(
    job: &JobDefinition,
    schedule: &Schedule,
    pool: &PgPool,
    redis: &redis::Client,
    cancel_token: &CancellationToken,
) {
    let last_run_at = match get_last_run_at(pool, &job.name).await {
        Ok(last_run_at) => last_run_at,
        Err(e) => {
            warn!(
                job_name = %job.name,
                error = %e,
                "Failed to read the last recorded run; skipping catch-up"
            );
            return;
        }
    };
    let Some(last_run_at) = last_run_at else {
        return;
    };

    let (missed_count, ticks) = missed_ticks(
        schedule,
        last_run_at,
        Utc::now(),
        chrono::Duration::from_std(CATCH_UP_TOLERANCE)
            .unwrap_or_else(|_| chrono::Duration::hours(1)),
        MAX_CATCH_UP_RUNS,
    );

    if ticks.is_empty() {
        return;
    }

    warn!(
        job_name = %job.name,
        missed_within_tolerance = missed_count,
        replaying = ticks.len(),
        last_run_at = %last_run_at,
        "Detected missed scheduled executions; replaying the most recent ticks"
    );

    let marker_ttl_ms = (CATCH_UP_TOLERANCE.as_millis() as u64).max((job.timeout_secs + 5) * 1000);

    let mut conn = match redis.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to connect to Redis for catch-up: {}", e);
            return;
        }
    };

    for tick in ticks {
        if cancel_token.is_cancelled() {
            break;
        }

        // Claim the tick first so that another instance starting at the same
        // time does not replay the same missed run.
        let marker_key = format!("scheduler:job:{}:tick:{}", job.name, tick.timestamp());
        let claimed: Option<String> = match redis::cmd("SET")
            .arg(&marker_key)
            .arg("1")
            .arg("NX")
            .arg("PX")
            .arg(marker_ttl_ms)
            .query_async(&mut conn)
            .await
        {
            Ok(claimed) => claimed,
            Err(e) => {
                warn!(
                    job_name = %job.name,
                    scheduled_at = %tick,
                    error = %e,
                    "Failed to claim missed tick; skipping it"
                );
                continue;
            }
        };

        if claimed.as_deref() != Some("OK") {
            debug!(
                job_name = %job.name,
                scheduled_at = %tick,
                "Missed tick already claimed by another instance"
            );
            continue;
        }

        try_run_tick(job, pool, redis, tick).await;
    }
}

/// Selects missed ticks between `last_run_at` and `now`.
///
/// Only ticks inside `tolerance` are eligible. The scan starts at the later of
/// `last_run_at` and the tolerance cutoff so that a job whose last run is days
/// in the past does not iterate over every historical tick.
///
/// Returns the number of eligible ticks together with the most recent ones,
/// capped at `max_runs`.
fn missed_ticks(
    schedule: &Schedule,
    last_run_at: DateTime<Utc>,
    now: DateTime<Utc>,
    tolerance: chrono::Duration,
    max_runs: usize,
) -> (usize, Vec<DateTime<Utc>>) {
    if max_runs == 0 || now <= last_run_at {
        return (0, Vec::new());
    }

    let cutoff = now - tolerance;
    let window_start = last_run_at.max(cutoff);

    // Step back one second so a tick exactly on `window_start` is included
    // without walking the ticks between an old `last_run_at` and `cutoff`.
    let ticks: Vec<DateTime<Utc>> = schedule
        .after(&(window_start - chrono::Duration::seconds(1)))
        .take_while(|tick| *tick <= now)
        .filter(|tick| *tick > last_run_at && *tick >= cutoff)
        .collect();

    let total = ticks.len();
    let start = total.saturating_sub(max_runs);
    (total, ticks[start..].to_vec())
}

/// Runs `job` for `scheduled_at` if this instance wins the distributed lock.
/// Returns `true` when the job was executed.
async fn try_run_tick(
    job: &JobDefinition,
    pool: &PgPool,
    redis: &redis::Client,
    scheduled_at: DateTime<Utc>,
) -> bool {
    // Lock TTL is timeout_secs + 5.
    // WHY: If the lock TTL exactly matched the job timeout, they could expire at the same millisecond.
    // If the lock expires automatically while the job is timing out, another instance could
    // acquire the lock and start the job before the current instance finishes logging the timeout
    // to PostgreSQL, potentially causing overlapping state or duplicate runs.
    let lock_key = format!("{}:lock", job.name);
    let lock_ttl_ms = (job.timeout_secs + 5) * 1000;

    let mut conn = match redis.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to connect to Redis for lock acquisition: {}", e);
            return false;
        }
    };

    let acquired: Option<String> = match redis::cmd("SET")
        .arg(&lock_key)
        .arg("1")
        .arg("NX")
        .arg("PX")
        .arg(lock_ttl_ms)
        .query_async::<Option<String>>(&mut conn)
        .await
    {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to acquire distributed lock: {}", e);
            return false;
        }
    };

    if acquired.as_deref() != Some("OK") {
        debug!("Another instance is running this tick, skipping");
        return false;
    }

    let ctx = JobContext {
        job_name: job.name.clone(),
        scheduled_at,
        pool: pool.clone(),
        redis: redis.clone(),
    };

    execute_job_with_retries(job, ctx).await;

    // Release the lock manually (optional, since it has a TTL, but good practice if it finishes early)
    let _: redis::RedisResult<()> = redis::cmd("DEL")
        .arg(&lock_key)
        .query_async(&mut conn)
        .await;

    true
}

async fn execute_job_with_retries(job: &JobDefinition, ctx: JobContext) {
    let mut attempt = 0;
    let max_attempts = job.max_retries + 1;
    let start_time = tokio::time::Instant::now();

    // Record start in db
    let run_id = match record_start(&ctx.pool, &job.name).await {
        Ok(id) => id,
        Err(e) => {
            error!("Failed to record job start in DB: {}", e);
            return;
        }
    };

    let handler_clone = job.handler.clone();

    loop {
        attempt += 1;
        let ctx_clone = ctx.clone();

        // Spawn a blocking task to handle panics safely.
        // Or rather, we spawn a standard tokio task to catch unwinds.
        let job_name = job.name.clone();
        let handler = handler_clone.clone();

        let task = tokio::spawn(async move { handler.run(ctx_clone).await });

        let timeout_duration = Duration::from_secs(job.timeout_secs);
        let result = tokio::time::timeout(timeout_duration, task).await;

        let duration_ms = start_time.elapsed().as_millis() as i64;

        match result {
            Ok(Ok(Ok(()))) => {
                info!("Job completed successfully on attempt {}", attempt);
                if let Err(e) = record_success(&ctx.pool, run_id, duration_ms).await {
                    error!("Failed to record job success: {}", e);
                }
                break;
            }
            Ok(Ok(Err(e))) => {
                warn!("Job failed on attempt {}/{}: {}", attempt, max_attempts, e);
                if attempt >= max_attempts {
                    error!("Job failed permanently after {} attempts", max_attempts);
                    if let Err(db_e) =
                        record_failure(&ctx.pool, run_id, &e.to_string(), duration_ms).await
                    {
                        error!("Failed to record job failure: {}", db_e);
                    }
                    break;
                }
            }
            Ok(Err(join_err)) => {
                let err_msg = if join_err.is_panic() {
                    "Job handler panicked".to_string()
                } else {
                    format!("Job task cancelled or failed: {}", join_err)
                };
                warn!(
                    "Job panicked/cancelled on attempt {}/{}: {}",
                    attempt, max_attempts, err_msg
                );

                if attempt >= max_attempts {
                    error!(
                        "Job failed permanently after panicking {} times",
                        max_attempts
                    );
                    if let Err(db_e) =
                        record_failure(&ctx.pool, run_id, &err_msg, duration_ms).await
                    {
                        error!("Failed to record job failure: {}", db_e);
                    }
                    break;
                }
            }
            Err(_) => {
                warn!("Job timed out on attempt {}/{}", attempt, max_attempts);

                if attempt >= max_attempts {
                    error!("Job timed out permanently after {} attempts", max_attempts);
                    if let Err(db_e) = record_timeout(&ctx.pool, run_id, duration_ms).await {
                        error!("Failed to record job timeout: {}", db_e);
                    }
                    break;
                }
            }
        }

        // Backoff slightly before retry (optional, but good practice)
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod catch_up_tests {
    use super::*;
    use chrono::TimeZone;

    fn every_minute() -> Schedule {
        Schedule::from_str("0 * * * * *").unwrap()
    }

    fn at(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 22, hour, minute, second)
            .unwrap()
    }

    #[test]
    fn no_catch_up_when_no_tick_was_missed() {
        let (total, ticks) = missed_ticks(
            &every_minute(),
            at(12, 0, 30),
            at(12, 0, 45),
            chrono::Duration::hours(1),
            3,
        );

        assert_eq!(total, 0);
        assert!(ticks.is_empty());
    }

    #[test]
    fn replays_all_eligible_missed_ticks() {
        let (total, ticks) = missed_ticks(
            &every_minute(),
            at(12, 0, 0),
            at(12, 5, 30),
            chrono::Duration::hours(1),
            10,
        );

        assert_eq!(total, 5);
        assert_eq!(
            ticks,
            vec![
                at(12, 1, 0),
                at(12, 2, 0),
                at(12, 3, 0),
                at(12, 4, 0),
                at(12, 5, 0),
            ]
        );
    }

    #[test]
    fn caps_catch_up_to_the_most_recent_ticks() {
        let (total, ticks) = missed_ticks(
            &every_minute(),
            at(12, 0, 0),
            at(12, 5, 30),
            chrono::Duration::hours(1),
            2,
        );

        assert_eq!(total, 5, "all five missed ticks are eligible");
        assert_eq!(ticks, vec![at(12, 4, 0), at(12, 5, 0)]);
    }

    #[test]
    fn ignores_ticks_outside_the_tolerance_window() {
        let last_run_at = at(9, 0, 0);
        let now = at(12, 0, 0);
        let tolerance = chrono::Duration::minutes(30);

        let (total, ticks) = missed_ticks(&every_minute(), last_run_at, now, tolerance, 3);

        assert_eq!(total, 31, "only ticks inside the 30 minute tolerance");
        assert_eq!(ticks.len(), 3);
        assert_eq!(ticks.last(), Some(&at(12, 0, 0)));
        assert!(ticks.iter().all(|tick| *tick >= now - tolerance));
    }

    #[test]
    fn no_catch_up_when_disabled_or_clock_moved_backwards() {
        let schedule = every_minute();

        let (total, ticks) = missed_ticks(
            &schedule,
            at(12, 0, 0),
            at(12, 5, 0),
            chrono::Duration::hours(1),
            0,
        );
        assert_eq!(total, 0);
        assert!(ticks.is_empty());

        let (total, ticks) = missed_ticks(
            &schedule,
            at(12, 5, 0),
            at(12, 0, 0),
            chrono::Duration::hours(1),
            3,
        );
        assert_eq!(total, 0);
        assert!(ticks.is_empty());
    }
}
