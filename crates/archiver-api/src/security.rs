//! Security middleware: rate limiting, security headers.

use std::net::IpAddr;
use std::time::Instant;

use axum::http::{HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;

/// IP-based token bucket rate limiter.
pub struct RateLimiter {
    buckets: DashMap<IpAddr, TokenBucket>,
    rate: f64,
    burst: f64,
}

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(rate_per_second: u32, burst: u32) -> Self {
        Self {
            buckets: DashMap::new(),
            rate: rate_per_second as f64,
            burst: burst as f64,
        }
    }

    /// Returns true if the request is allowed.
    pub fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut entry = self.buckets.entry(ip).or_insert_with(|| TokenBucket {
            tokens: self.burst,
            last_refill: now,
        });
        let bucket = entry.value_mut();

        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst);
        bucket.last_refill = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Remove entries not seen in the last 5 minutes.
    pub fn cleanup(&self) {
        let cutoff = Instant::now() - std::time::Duration::from_secs(300);
        self.buckets.retain(|_, v| v.last_refill > cutoff);
    }
}

/// Middleware that adds security headers to all responses.
pub(crate) async fn security_headers(
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let mut resp = next.run(request).await;
    let headers = resp.headers_mut();
    headers.insert("x-content-type-options", HeaderValue::from_static("nosniff"));
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert("x-xss-protection", HeaderValue::from_static("1; mode=block"));
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'",
        ),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("strict-origin-when-cross-origin"));
    resp
}

/// Middleware that enforces per-IP rate limiting.
pub(crate) async fn rate_limit(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if let Some(ref limiter) = state.rate_limiter {
        // Extract client IP from ConnectInfo or X-Forwarded-For.
        let ip = request
            .extensions()
            .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
            .map(|ci| ci.0.ip());
        if let Some(ip) = ip {
            if !limiter.check(ip) {
                return (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response();
            }
        }
    }
    next.run(request).await
}

use axum::extract::State;
use crate::AppState;
