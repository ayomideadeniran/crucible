use axum::{
    extract::{ConnectInfo, Request},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::warn;

/// Tiered rate limit quota configuration based on caller identity/tier
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RateLimitTier {
    Anonymous,
    FreeTier,
    Standard,
    Enterprise,
}

impl RateLimitTier {
    pub fn config(&self) -> RateLimitConfig {
        match self {
            RateLimitTier::Anonymous => RateLimitConfig {
                capacity: 30,
                refill_rate_per_sec: 2,
                ttl: Duration::from_secs(3600),
            },
            RateLimitTier::FreeTier => RateLimitConfig {
                capacity: 100,
                refill_rate_per_sec: 10,
                ttl: Duration::from_secs(3600),
            },
            RateLimitTier::Standard => RateLimitConfig {
                capacity: 500,
                refill_rate_per_sec: 50,
                ttl: Duration::from_secs(3600),
            },
            RateLimitTier::Enterprise => RateLimitConfig {
                capacity: 2000,
                refill_rate_per_sec: 200,
                ttl: Duration::from_secs(3600),
            },
        }
    }
}

/// Configuration for Token Bucket Rate Limiter
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub capacity: u64,
    pub refill_rate_per_sec: u64,
    pub ttl: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        RateLimitTier::FreeTier.config()
    }
}

/// In-memory token bucket state used for fallback or direct state tracking
#[derive(Debug, Clone)]
pub struct LocalTokenBucket {
    pub tokens: u64,
    pub last_refill: Instant,
}

/// Redis-backed Token Bucket Rate Limiter with Local Memory Fallback
#[derive(Debug, Clone)]
pub struct TokenBucketRateLimiter {
    redis_client: Option<redis::Client>,
    config: RateLimitConfig,
    local_buckets: Arc<Mutex<HashMap<String, LocalTokenBucket>>>,
}

static SHARED_LIMITER: OnceLock<Arc<TokenBucketRateLimiter>> = OnceLock::new();

impl TokenBucketRateLimiter {
    pub fn new(redis_client: Option<redis::Client>, config: RateLimitConfig) -> Self {
        Self {
            redis_client,
            config,
            local_buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get global shared rate limiter instance
    pub fn global() -> Arc<Self> {
        SHARED_LIMITER
            .get_or_init(|| Arc::new(Self::new(None, RateLimitConfig::default())))
            .clone()
    }

    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    /// Check and consume a token for a given key (IP address or API key) with custom or default config
    pub async fn check_and_consume(&self, key: &str) -> Result<RateLimitResult, String> {
        self.check_and_consume_with_config(key, &self.config).await
    }

    /// Check and consume a token with specific tiered RateLimitConfig
    pub async fn check_and_consume_with_config(&self, key: &str, config: &RateLimitConfig) -> Result<RateLimitResult, String> {
        let key_name = format!("rate_limit:{}", key);

        if let Some(client) = &self.redis_client {
            if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                // Token Bucket Script in Redis Lua
                let script = redis::Script::new(
                    r#"
                    local key = KEYS[1]
                    local capacity = tonumber(ARGV[1])
                    local refill_rate = tonumber(ARGV[2])
                    local now = tonumber(ARGV[3])
                    local requested = 1

                    local data = redis.call("HMGET", key, "tokens", "last_refill")
                    local tokens = tonumber(data[1])
                    local last_refill = tonumber(data[2])

                    if not tokens or not last_refill then
                        tokens = capacity
                        last_refill = now
                    else
                        local delta = math.max(0, now - last_refill)
                        tokens = math.min(capacity, tokens + (delta * refill_rate))
                        last_refill = now
                    end

                    local allowed = false
                    local remaining = tokens
                    if tokens >= requested then
                        allowed = true
                        tokens = tokens - requested
                        remaining = tokens
                        redis.call("HMSET", key, "tokens", tokens, "last_refill", last_refill)
                        redis.call("EXPIRE", key, 3600)
                    end

                    return { allowed and 1 or 0, remaining, capacity }
                    "#,
                );

                if let Ok((allowed_int, remaining, capacity)) = script
                    .key(&key_name)
                    .arg(config.capacity)
                    .arg(config.refill_rate_per_sec)
                    .arg(now)
                    .invoke_async::<(i32, u64, u64)>(&mut conn)
                    .await
                {
                    return Ok(RateLimitResult {
                        allowed: allowed_int == 1,
                        limit: capacity,
                        remaining,
                        reset_secs: 1,
                    });
                }
            }
        }

        // Local Memory Fallback Rate Limiting
        let mut buckets = self
            .local_buckets
            .lock()
            .map_err(|e| format!("Lock error: {}", e))?;

        let now = Instant::now();
        let bucket = buckets.entry(key.to_string()).or_insert(LocalTokenBucket {
            tokens: config.capacity,
            last_refill: now,
        });

        let elapsed = now.duration_since(bucket.last_refill).as_secs();
        if elapsed > 0 {
            bucket.tokens = (bucket.tokens + elapsed * config.refill_rate_per_sec)
                .min(config.capacity);
            bucket.last_refill = now;
        }

        let allowed = if bucket.tokens >= 1 {
            bucket.tokens -= 1;
            true
        } else {
            false
        };

        Ok(RateLimitResult {
            allowed,
            limit: config.capacity,
            remaining: bucket.tokens,
            reset_secs: 1,
        })
    }
}

/// Result of Rate Limit evaluation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateLimitResult {
    pub allowed: bool,
    pub limit: u64,
    pub remaining: u64,
    pub reset_secs: u64,
}

// ---------------------------------------------------------------------------
// Client IP resolution
// ---------------------------------------------------------------------------

/// Environment variable with a comma-separated list of proxy CIDR ranges whose
/// forwarding headers may be trusted, e.g. `10.0.0.0/8,fd00::/8`.
pub const TRUSTED_PROXY_CIDRS_ENV: &str = "TRUSTED_PROXY_CIDRS";

/// Upper bound on how many `X-Forwarded-For` hops are inspected per request, so
/// an attacker-controlled header cannot make the proxy walk unbounded.
const MAX_FORWARDED_HOPS: usize = 32;

/// A parsed CIDR range. A bare address is treated as a `/32` or `/128`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl IpCidr {
    fn parse(value: &str) -> Result<Self, String> {
        let value = value.trim();
        let (addr_part, prefix_part) = value
            .split_once('/')
            .map_or((value, None), |(addr, prefix)| (addr, Some(prefix)));

        let addr = addr_part
            .parse::<IpAddr>()
            .map_err(|_| format!("invalid IP address {addr_part:?}"))?;

        match addr {
            IpAddr::V4(v4) => Ok(Self {
                network: IpAddr::V4(v4),
                prefix: parse_prefix(prefix_part, 32)?,
            }),
            IpAddr::V6(v6) => {
                let prefix = parse_prefix(prefix_part, 128)?;
                match v6.to_ipv4_mapped() {
                    // Represent IPv4-mapped ranges as IPv4 so they match
                    // clients that connect over a dual-stack listener.
                    Some(v4) if prefix >= 96 => Ok(Self {
                        network: IpAddr::V4(v4),
                        prefix: prefix - 96,
                    }),
                    _ => Ok(Self {
                        network: IpAddr::V6(v6),
                        prefix,
                    }),
                }
            }
        }
    }

    fn contains(&self, addr: IpAddr) -> bool {
        match (self.network, canonical_ip(addr)) {
            (IpAddr::V4(network), IpAddr::V4(addr)) => {
                prefix_matches(&network.octets(), &addr.octets(), self.prefix)
            }
            (IpAddr::V6(network), IpAddr::V6(addr)) => {
                prefix_matches(&network.octets(), &addr.octets(), self.prefix)
            }
            _ => false,
        }
    }
}

fn parse_prefix(prefix: Option<&str>, max: u8) -> Result<u8, String> {
    match prefix {
        None => Ok(max),
        Some(raw) => {
            let value = raw
                .trim()
                .parse::<u8>()
                .map_err(|_| format!("invalid prefix length {raw:?}"))?;
            if value > max {
                return Err(format!("prefix /{value} exceeds /{max}"));
            }
            Ok(value)
        }
    }
}

/// Collapses IPv4-mapped IPv6 addresses (`::ffff:10.0.0.1`) to IPv4 so they
/// compare equal to their IPv4 form.
fn canonical_ip(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(addr, IpAddr::V4),
        IpAddr::V4(_) => addr,
    }
}

fn prefix_matches(network: &[u8], addr: &[u8], prefix: u8) -> bool {
    let full_bytes = usize::from(prefix / 8);
    if network[..full_bytes] != addr[..full_bytes] {
        return false;
    }
    let remaining_bits = prefix % 8;
    if remaining_bits == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - remaining_bits);
    (network[full_bytes] & mask) == (addr[full_bytes] & mask)
}

/// Proxy networks whose forwarding headers are believed.
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies {
    ranges: Vec<IpCidr>,
}

impl TrustedProxies {
    /// Trust no proxy: `X-Forwarded-For` and `X-Real-IP` are ignored.
    pub fn none() -> Self {
        Self::default()
    }

    /// Loopback, private, and link-local ranges. Used when
    /// [`TRUSTED_PROXY_CIDRS_ENV`] is unset: a client on the public internet
    /// can never be the origin of these addresses, so a directly connected
    /// attacker cannot spoof its way into a trusted range.
    pub fn default_ranges() -> Self {
        Self::parse(
            "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,\
             ::1/128,fc00::/7,fe80::/10",
        )
        .expect("hard-coded default proxy ranges are valid")
    }

    /// Parses a comma-separated list of CIDR ranges or bare addresses.
    pub fn parse(list: &str) -> Result<Self, String> {
        let ranges = list
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(IpCidr::parse)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { ranges })
    }

    /// Reads [`TRUSTED_PROXY_CIDRS_ENV`], falling back to
    /// [`Self::default_ranges`] when it is unset. Invalid entries are skipped
    /// with a warning; if none remain, no proxy is trusted (secure fallback).
    pub fn from_env() -> Self {
        let Ok(value) = std::env::var(TRUSTED_PROXY_CIDRS_ENV) else {
            return Self::default_ranges();
        };

        let mut ranges = Vec::new();
        for entry in value.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
            match IpCidr::parse(entry) {
                Ok(range) => ranges.push(range),
                Err(error) => warn!(
                    "Ignoring invalid entry {entry:?} in {TRUSTED_PROXY_CIDRS_ENV}: {error}"
                ),
            }
        }

        if ranges.is_empty() {
            warn!(
                "{TRUSTED_PROXY_CIDRS_ENV} contains no valid ranges; \
                 forwarding headers will not be trusted"
            );
        }
        Self { ranges }
    }

    /// Whether `addr` belongs to a trusted proxy network.
    pub fn contains(&self, addr: IpAddr) -> bool {
        let addr = canonical_ip(addr);
        self.ranges.iter().any(|range| range.contains(addr))
    }
}

static TRUSTED_PROXIES: OnceLock<TrustedProxies> = OnceLock::new();

fn configured_trusted_proxies() -> &'static TrustedProxies {
    TRUSTED_PROXIES.get_or_init(TrustedProxies::from_env)
}

fn parse_forwarded_addr(entry: &str) -> Option<IpAddr> {
    let entry = entry.trim().trim_matches('"');
    if let Ok(ip) = entry.parse::<IpAddr>() {
        return Some(ip);
    }
    // Some proxies append a port (`203.0.113.7:4711`, `[2001:db8::1]:443`).
    entry.parse::<SocketAddr>().ok().map(|socket| socket.ip())
}

/// Parses every `X-Forwarded-For` value in header order (client first, nearest
/// proxy last), keeping only the most recent [`MAX_FORWARDED_HOPS`] entries.
fn forwarded_hops(headers: &HeaderMap) -> VecDeque<IpAddr> {
    let mut hops = VecDeque::new();
    for value in headers.get_all("x-forwarded-for") {
        let Ok(value) = value.to_str() else {
            continue;
        };
        for entry in value.split(',') {
            let Some(ip) = parse_forwarded_addr(entry) else {
                // Malformed hops cannot be attributed to anyone; skip them.
                // They can only ever appear on the client-supplied left side.
                continue;
            };
            if hops.len() == MAX_FORWARDED_HOPS {
                hops.pop_front();
            }
            hops.push_back(canonical_ip(ip));
        }
    }
    hops
}

fn x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_forwarded_addr)
        .map(canonical_ip)
}

/// Resolves the client from the forwarding chain when the socket peer is a
/// trusted proxy.
///
/// The chain is walked from right to left (nearest proxy towards the client)
/// and the first hop that is not itself a trusted proxy is returned, which is
/// the closest address the proxy chain can vouch for. Headers supplied by an
/// untrusted peer are ignored entirely.
fn forwarded_client_ip(headers: &HeaderMap, trusted_proxies: &TrustedProxies) -> Option<IpAddr> {
    let hops = forwarded_hops(headers);
    for hop in hops.iter().rev().copied() {
        if trusted_proxies.contains(hop) {
            continue;
        }
        return Some(hop);
    }

    // No untrusted X-Forwarded-For hop. Fall back to X-Real-IP, which proxies
    // such as nginx set alongside it, but only ever behind a trusted peer.
    x_real_ip(headers)
}

/// Resolves the client IP for rate limiting.
///
/// `peer` is the socket address the request arrived on. When it belongs to a
/// trusted proxy the forwarding headers are used; otherwise the peer itself is
/// the client and any forwarding headers are treated as untrusted input.
pub fn resolve_client_ip(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    trusted_proxies: &TrustedProxies,
) -> Option<IpAddr> {
    let peer = canonical_ip(peer?);
    if !trusted_proxies.contains(peer) {
        return Some(peer);
    }
    Some(forwarded_client_ip(headers, trusted_proxies).unwrap_or(peer))
}

fn peer_ip(request: &Request) -> Option<IpAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|connect_info| connect_info.0.ip())
}

/// Extract rate limiting key (API Key token or client IP) from request headers.
///
/// `peer` is the socket address the request arrived on, and `trusted_proxies`
/// defines which proxy hops may contribute forwarding headers.
pub fn extract_rate_limit_key(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    trusted_proxies: &TrustedProxies,
) -> String {
    // 1. Check API Key header
    if let Some(key) = headers.get("x-api-key").and_then(|h| h.to_str().ok()) {
        if !key.trim().is_empty() {
            return format!("token:{}", key.trim());
        }
    }

    // 2. Check Authorization header (Bearer token)
    if let Some(auth) = headers.get("authorization").and_then(|h| h.to_str().ok()) {
        if auth.to_lowercase().starts_with("bearer ") {
            let token = auth[7..].trim();
            if !token.is_empty() {
                return format!("token:{}", token);
            }
        }
    }

    // 3. Socket peer, or the rightmost untrusted forwarding hop
    match resolve_client_ip(headers, peer, trusted_proxies) {
        Some(ip) => format!("ip:{}", ip),
        None => "ip:anonymous".to_string(),
    }
}

fn client_ip_was_forwarded(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    trusted_proxies: &TrustedProxies,
) -> bool {
    let Some(peer) = peer.map(canonical_ip) else {
        return false;
    };
    trusted_proxies.contains(peer) && forwarded_client_ip(headers, trusted_proxies).is_some()
}

/// Resolves the tier from request headers (checking subscription tier, API keys, or fallback to IP/anonymous)
pub fn resolve_rate_limit_tier(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    trusted_proxies: &TrustedProxies,
) -> RateLimitTier {
    if let Some(tier_str) = headers.get("x-subscription-tier").and_then(|h| h.to_str().ok()) {
        match tier_str.to_lowercase().as_str() {
            "enterprise" => return RateLimitTier::Enterprise,
            "standard" | "pro" => return RateLimitTier::Standard,
            "free" => return RateLimitTier::FreeTier,
            _ => {}
        }
    }

    if headers.contains_key("x-api-key") || headers.contains_key("authorization") {
        RateLimitTier::Standard
    } else if client_ip_was_forwarded(headers, peer, trusted_proxies) {
        RateLimitTier::FreeTier
    } else {
        RateLimitTier::Anonymous
    }
}

/// Axum Middleware function for Token Bucket Rate Limiting with IP & API Key Tiering
pub async fn rate_limit_middleware(
    request: Request,
    next: Next,
) -> Response {
    let trusted_proxies = configured_trusted_proxies();
    let peer = peer_ip(&request);
    let key = extract_rate_limit_key(request.headers(), peer, trusted_proxies);
    let tier = resolve_rate_limit_tier(request.headers(), peer, trusted_proxies);
    let tier_config = tier.config();

    let limiter = request
        .extensions()
        .get::<Arc<TokenBucketRateLimiter>>()
        .cloned()
        .unwrap_or_else(TokenBucketRateLimiter::global);

    match limiter.check_and_consume_with_config(&key, &tier_config).await {
        Ok(result) => {
            if !result.allowed {
                let mut response = crate::api::errors::make_error_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "too_many_requests",
                    "429 Too Many Requests: Rate limit exceeded",
                );
                let response_headers = response.headers_mut();
                response_headers.insert("X-RateLimit-Limit", HeaderValue::from(result.limit));
                response_headers.insert("X-RateLimit-Remaining", HeaderValue::from(result.remaining));
                response_headers.insert("X-RateLimit-Reset", HeaderValue::from(result.reset_secs));
                response_headers.insert("Retry-After", HeaderValue::from(result.reset_secs));
                return response;
            }

            let mut response = next.run(request).await;
            let headers = response.headers_mut();
            headers.insert("X-RateLimit-Limit", HeaderValue::from(result.limit));
            headers.insert("X-RateLimit-Remaining", HeaderValue::from(result.remaining));
            headers.insert("X-RateLimit-Reset", HeaderValue::from(result.reset_secs));
            response
        }
        Err(_) => next.run(request).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use std::net::Ipv4Addr;
    use std::str::FromStr;
    use tower::ServiceExt;

    const TRUSTED_NET: &str = "10.0.0.0/8";
    const PROXY: &str = "10.0.0.2";
    const CLIENT: &str = "198.51.100.4";
    const PUBLIC: &str = "203.0.113.7";

    fn trusted() -> TrustedProxies {
        TrustedProxies::parse(TRUSTED_NET).unwrap()
    }

    fn peer(ip: &str) -> Option<IpAddr> {
        Some(ip.parse().unwrap())
    }

    fn header_map(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_static(value));
        }
        map
    }

    // -----------------------------------------------------------------------
    // Trusted proxy configuration
    // -----------------------------------------------------------------------

    #[test]
    fn parses_ipv4_cidr_boundaries() {
        let proxies = TrustedProxies::parse("10.0.0.0/8,192.168.1.0/24").unwrap();
        for addr in ["10.0.0.1", "10.255.255.255", "192.168.1.255"] {
            assert!(proxies.contains(addr.parse().unwrap()), "{addr} should match");
        }
        for addr in ["11.0.0.1", "192.168.2.1", "203.0.113.7"] {
            assert!(!proxies.contains(addr.parse().unwrap()), "{addr} should not match");
        }
    }

    #[test]
    fn bare_addresses_are_host_routes() {
        let proxies = TrustedProxies::parse("203.0.113.7,2001:db8::1").unwrap();
        assert!(proxies.contains("203.0.113.7".parse().unwrap()));
        assert!(!proxies.contains("203.0.113.8".parse().unwrap()));
        assert!(proxies.contains("2001:db8::1".parse().unwrap()));
        assert!(!proxies.contains("2001:db8::2".parse().unwrap()));
    }

    #[test]
    fn ipv4_mapped_addresses_match_ipv4_ranges() {
        let proxies = TrustedProxies::parse("10.0.0.0/8").unwrap();
        assert!(proxies.contains("::ffff:10.1.2.3".parse().unwrap()));

        let mapped = TrustedProxies::parse("::ffff:10.0.0.0/104").unwrap();
        assert!(mapped.contains("10.1.2.3".parse().unwrap()));
        assert!(!mapped.contains("11.1.2.3".parse().unwrap()));
    }

    #[test]
    fn rejects_invalid_cidr_entries() {
        assert!(TrustedProxies::parse("10.0.0.0/33").is_err());
        assert!(TrustedProxies::parse("2001:db8::/129").is_err());
        assert!(TrustedProxies::parse("not-an-ip").is_err());
        assert!(TrustedProxies::parse("10.0.0.0/8,bad").is_err());
    }

    #[test]
    fn default_ranges_cover_private_networks_only() {
        let proxies = TrustedProxies::default_ranges();
        for addr in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.1.1",
            "::1",
            "fd00::1",
            "fe80::1",
        ] {
            assert!(proxies.contains(addr.parse().unwrap()), "{addr} should be trusted");
        }
        for addr in ["8.8.8.8", PUBLIC, "172.32.0.1", "2001:db8::1"] {
            assert!(!proxies.contains(addr.parse().unwrap()), "{addr} must not be trusted");
        }
    }

    #[test]
    fn no_trusted_proxies_means_headers_are_ignored() {
        let headers = header_map(&[("x-forwarded-for", "1.2.3.4"), ("x-real-ip", "5.6.7.8")]);
        let proxies = TrustedProxies::none();
        assert_eq!(
            resolve_client_ip(&headers, peer(PUBLIC), &proxies),
            peer(PUBLIC)
        );
    }

    // -----------------------------------------------------------------------
    // Client IP resolution / spoofing resistance
    // -----------------------------------------------------------------------

    #[test]
    fn untrusted_peer_ignores_forwarding_headers() {
        let headers = header_map(&[
            ("x-forwarded-for", "1.2.3.4"),
            ("x-real-ip", "5.6.7.8"),
        ]);
        let proxies = trusted();

        assert_eq!(resolve_client_ip(&headers, peer(PUBLIC), &proxies), peer(PUBLIC));
        assert_eq!(
            extract_rate_limit_key(&headers, peer(PUBLIC), &proxies),
            format!("ip:{PUBLIC}")
        );
    }

    #[test]
    fn trusted_proxy_uses_rightmost_untrusted_hop() {
        // The leftmost value is client-controlled; the address the proxy
        // actually observed is appended on the right.
        let headers = header_map(&[("x-forwarded-for", "1.2.3.4, 198.51.100.4")]);
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(CLIENT));
    }

    #[test]
    fn skips_trusted_hops_when_walking_the_chain() {
        let headers = header_map(&[("x-forwarded-for", "1.2.3.4, 198.51.100.4, 10.0.0.3")]);
        assert_eq!(resolve_client_ip(&headers, peer("10.0.0.5"), &trusted()), peer(CLIENT));
    }

    #[test]
    fn all_trusted_hops_fall_back_to_the_peer() {
        let headers = header_map(&[("x-forwarded-for", "10.0.0.2, 10.0.0.3")]);
        assert_eq!(
            resolve_client_ip(&headers, peer("10.0.0.5"), &trusted()),
            peer("10.0.0.5")
        );
    }

    #[test]
    fn x_real_ip_is_only_used_behind_a_trusted_proxy() {
        let headers = header_map(&[("x-real-ip", "198.51.100.9")]);
        assert_eq!(
            resolve_client_ip(&headers, peer(PROXY), &trusted()),
            peer("198.51.100.9")
        );
        assert_eq!(
            resolve_client_ip(&headers, peer(PUBLIC), &trusted()),
            peer(PUBLIC)
        );
    }

    #[test]
    fn malformed_forwarded_values_are_ignored() {
        let headers = header_map(&[("x-forwarded-for", "not-an-ip, 198.51.100.4")]);
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(CLIENT));

        let headers = header_map(&[("x-forwarded-for", "definitely-not-an-ip")]);
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(PROXY));
    }

    #[test]
    fn forwarded_entries_with_ports_are_parsed() {
        let headers = header_map(&[("x-forwarded-for", "198.51.100.4:51234")]);
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(CLIENT));

        let headers = header_map(&[("x-forwarded-for", "[2001:db8::7]:443")]);
        assert_eq!(
            resolve_client_ip(&headers, peer(PROXY), &trusted()),
            peer("2001:db8::7")
        );
    }

    #[test]
    fn multiple_forwarded_headers_are_read_in_order() {
        let mut headers = HeaderMap::new();
        headers.append("x-forwarded-for", HeaderValue::from_static("1.2.3.4"));
        headers.append("x-forwarded-for", HeaderValue::from_static("198.51.100.4"));
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(CLIENT));
    }

    #[test]
    fn hop_limit_keeps_the_rightmost_entries() {
        let mut chain = String::new();
        for i in 1..=40u8 {
            chain.push_str(&format!("192.0.2.{i},"));
        }
        chain.push_str(CLIENT);
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_str(&chain).unwrap());

        assert_eq!(forwarded_hops(&headers).len(), MAX_FORWARDED_HOPS);
        assert_eq!(resolve_client_ip(&headers, peer(PROXY), &trusted()), peer(CLIENT));
    }

    #[test]
    fn no_peer_and_no_trusted_proxy_yields_anonymous_key() {
        let headers = header_map(&[("x-forwarded-for", "1.2.3.4")]);
        assert_eq!(
            extract_rate_limit_key(&headers, None, &TrustedProxies::none()),
            "ip:anonymous"
        );
    }

    // -----------------------------------------------------------------------
    // Key extraction and tiering
    // -----------------------------------------------------------------------

    #[test]
    fn extract_rate_limit_key_priority() {
        let proxies = trusted();
        let proxy = peer(PROXY);

        assert_eq!(
            extract_rate_limit_key(&HeaderMap::new(), None, &proxies),
            "ip:anonymous"
        );

        let mut headers = header_map(&[("x-real-ip", "198.51.100.9")]);
        assert_eq!(
            extract_rate_limit_key(&headers, proxy, &proxies),
            "ip:198.51.100.9"
        );

        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.4, 10.0.0.9"),
        );
        assert_eq!(
            extract_rate_limit_key(&headers, proxy, &proxies),
            "ip:198.51.100.4"
        );

        headers.insert("authorization", HeaderValue::from_static("Bearer secret-jwt"));
        assert_eq!(
            extract_rate_limit_key(&headers, proxy, &proxies),
            "token:secret-jwt"
        );

        headers.insert("x-api-key", HeaderValue::from_static("api-key-123"));
        assert_eq!(
            extract_rate_limit_key(&headers, proxy, &proxies),
            "token:api-key-123"
        );
    }

    #[test]
    fn spoofed_forwarding_headers_do_not_upgrade_tier() {
        let headers = header_map(&[("x-forwarded-for", "1.2.3.4")]);
        let proxies = trusted();

        assert_eq!(
            resolve_rate_limit_tier(&headers, peer(PUBLIC), &proxies),
            RateLimitTier::Anonymous
        );
        assert_eq!(
            resolve_rate_limit_tier(&headers, peer(PROXY), &proxies),
            RateLimitTier::FreeTier
        );
    }

    #[test]
    fn test_rate_limit_tiering_quota_enforcement() {
        let proxies = trusted();

        let headers_anon = HeaderMap::new();
        assert_eq!(
            resolve_rate_limit_tier(&headers_anon, peer(PUBLIC), &proxies),
            RateLimitTier::Anonymous
        );
        assert_eq!(RateLimitTier::Anonymous.config().capacity, 30);

        let headers_ip = header_map(&[("x-forwarded-for", "1.2.3.4")]);
        assert_eq!(
            resolve_rate_limit_tier(&headers_ip, peer(PROXY), &proxies),
            RateLimitTier::FreeTier
        );
        assert_eq!(RateLimitTier::FreeTier.config().capacity, 100);

        let headers_api = header_map(&[("x-api-key", "key-123")]);
        assert_eq!(
            resolve_rate_limit_tier(&headers_api, None, &proxies),
            RateLimitTier::Standard
        );
        assert_eq!(RateLimitTier::Standard.config().capacity, 500);

        let headers_ent = header_map(&[("x-subscription-tier", "enterprise")]);
        assert_eq!(
            resolve_rate_limit_tier(&headers_ent, None, &proxies),
            RateLimitTier::Enterprise
        );
        assert_eq!(RateLimitTier::Enterprise.config().capacity, 2000);
    }

    // -----------------------------------------------------------------------
    // Middleware
    // -----------------------------------------------------------------------

    fn limited_app() -> Router {
        let limiter = Arc::new(TokenBucketRateLimiter::new(
            None,
            RateLimitConfig::default(),
        ));
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .layer(axum::middleware::from_fn(rate_limit_middleware))
            .layer(axum::Extension(limiter))
    }

    fn request_from(ip: &str, forwarded_for: &str) -> Request {
        let mut request = Request::builder()
            .uri("/test")
            .header("x-forwarded-for", forwarded_for)
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(SocketAddr::from((
            Ipv4Addr::from_str(ip).unwrap(),
            49152,
        ))));
        request
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_429_too_many_requests() {
        // Without credentials or a forwarded IP the caller is anonymous,
        // whose tier allows 30 requests.
        let app = limited_app();
        let request = || Request::builder().uri("/test").body(Body::empty()).unwrap();

        let first = app.clone().oneshot(request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(first.headers().get("X-RateLimit-Remaining").unwrap(), "29");

        for _ in 1..30 {
            let allowed = app.clone().oneshot(request()).await.unwrap();
            assert_eq!(allowed.status(), StatusCode::OK);
        }

        let limited = app.oneshot(request()).await.unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(limited.headers().contains_key("X-RateLimit-Limit"));
        assert!(limited.headers().contains_key("Retry-After"));
    }

    #[tokio::test]
    async fn middleware_ignores_spoofed_forwarded_ips_from_untrusted_peers() {
        let app = limited_app();

        // Same untrusted peer, a different forged client IP per request:
        // every request still lands in the peer's single anonymous bucket.
        for i in 0..30 {
            let forwarded = format!("192.0.2.{i}");
            let allowed = app
                .clone()
                .oneshot(request_from(PUBLIC, &forwarded))
                .await
                .unwrap();
            assert_eq!(allowed.status(), StatusCode::OK, "request {i}");
        }

        let spoofed_again = app
            .clone()
            .oneshot(request_from(PUBLIC, "192.0.2.99"))
            .await
            .unwrap();
        assert_eq!(spoofed_again.status(), StatusCode::TOO_MANY_REQUESTS);

        // Behind a trusted proxy the forwarded client IPs are distinct keys.
        let client_a = app
            .clone()
            .oneshot(request_from(PROXY, "192.0.2.10"))
            .await
            .unwrap();
        assert_eq!(client_a.status(), StatusCode::OK);

        let client_b = app
            .oneshot(request_from(PROXY, "192.0.2.11"))
            .await
            .unwrap();
        assert_eq!(client_b.status(), StatusCode::OK);
    }
}
