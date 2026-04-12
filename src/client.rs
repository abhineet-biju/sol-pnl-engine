use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, sleep_until};

use crate::model::{
    Filters, FullTransactionRecord, GetTransactionsConfig, GtfaPage, RpcFailure, RpcRequest,
    RpcSuccess, SignatureRecord, SortOrder,
};

#[derive(Debug, Error)]
pub enum HeliusClientError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc returned error {code}: {message}")]
    Rpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("failed to deserialize rpc response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unexpected rpc response: missing result and error fields")]
    InvalidResponse,
    #[error("failed to read fixture: {0}")]
    Io(#[from] std::io::Error),
    #[error("fixture address mismatch: requested {requested}, fixture contains {expected}")]
    FixtureAddressMismatch { expected: String, requested: String },
    #[error("fixture pagination token was invalid: {token}")]
    InvalidPaginationToken { token: String },
}

#[async_trait]
pub trait TransactionsSource: Send + Sync {
    async fn get_signatures(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<SignatureRecord>, HeliusClientError>;

    async fn get_full_transactions(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<FullTransactionRecord>, HeliusClientError>;
}

#[derive(Debug, Clone)]
pub struct HeliusClient {
    http: reqwest::Client,
    rpc_url: String,
    retries: usize,
    pacer: Option<Arc<RequestPacer>>,
    metrics: Arc<ClientMetrics>,
}

#[derive(Debug)]
struct RequestPacer {
    interval: Duration,
    next_slot: Mutex<Instant>,
}

#[derive(Debug, Default)]
struct ClientMetrics {
    attempts: AtomicUsize,
    retries: AtomicUsize,
    total_http_nanos: AtomicU64,
    first_completed_attempt_nanos: AtomicU64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct HeliusClientTimingStats {
    pub attempts: usize,
    pub retries: usize,
    pub total_http_time_ms: u64,
    pub avg_http_time_ms: Option<f64>,
    pub first_completed_attempt_ms: Option<u64>,
}

impl HeliusClient {
    pub fn new(rpc_url: impl Into<String>) -> Result<Self, HeliusClientError> {
        Self::new_with_rps(rpc_url, None)
    }

    pub fn new_with_rps(
        rpc_url: impl Into<String>,
        max_rps: Option<u32>,
    ) -> Result<Self, HeliusClientError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(0)
            .tcp_nodelay(true)
            .build()?;

        Ok(Self {
            http,
            rpc_url: rpc_url.into(),
            retries: 3,
            pacer: max_rps.and_then(RequestPacer::from_rps).map(Arc::new),
            metrics: Arc::new(ClientMetrics::default()),
        })
    }

    pub fn timing_stats(&self) -> HeliusClientTimingStats {
        self.metrics.snapshot()
    }
}

impl RequestPacer {
    fn from_rps(rps: u32) -> Option<Self> {
        if rps == 0 {
            return None;
        }

        Some(Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(rps)),
            next_slot: Mutex::new(Instant::now()),
        })
    }

    async fn acquire(&self) {
        let now = Instant::now();
        let mut next_slot = self.next_slot.lock().await;
        let scheduled = if *next_slot > now { *next_slot } else { now };
        *next_slot = scheduled + self.interval;
        drop(next_slot);

        if scheduled > now {
            sleep_until(scheduled).await;
        }
    }
}

#[async_trait]
impl TransactionsSource for HeliusClient {
    async fn get_signatures(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<SignatureRecord>, HeliusClientError> {
        self.call(address, config).await
    }

    async fn get_full_transactions(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<FullTransactionRecord>, HeliusClientError> {
        self.call(address, config).await
    }
}

impl HeliusClient {
    async fn call<T>(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<T>, HeliusClientError>
    where
        T: DeserializeOwned,
    {
        let request = RpcRequest {
            jsonrpc: "2.0",
            id: "1",
            method: "getTransactionsForAddress",
            params: (address, config),
        };

        let mut attempt = 0usize;
        loop {
            if let Some(pacer) = &self.pacer {
                pacer.acquire().await;
            }
            let started_at = Instant::now();
            let response = self.http.post(&self.rpc_url).json(&request).send().await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    if should_retry_status(status) && attempt < self.retries {
                        self.metrics.note_attempt(started_at.elapsed(), true);
                        sleep(backoff_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }

                    let body = response.text().await?;
                    self.metrics.note_attempt(started_at.elapsed(), false);
                    return parse_rpc_response(&body);
                }
                Err(error) => {
                    if attempt < self.retries && (error.is_timeout() || error.is_connect()) {
                        self.metrics.note_attempt(started_at.elapsed(), true);
                        sleep(backoff_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    self.metrics.note_attempt(started_at.elapsed(), false);
                    return Err(HeliusClientError::Http(error));
                }
            }
        }
    }
}

impl ClientMetrics {
    fn note_attempt(&self, elapsed: Duration, was_retry: bool) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if was_retry {
            self.retries.fetch_add(1, Ordering::Relaxed);
        }

        let nanos = duration_to_nanos(elapsed);
        self.total_http_nanos.fetch_add(nanos, Ordering::Relaxed);

        let _ = self.first_completed_attempt_nanos.compare_exchange(
            0,
            nanos,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> HeliusClientTimingStats {
        let attempts = self.attempts.load(Ordering::Relaxed);
        let total_http_nanos = self.total_http_nanos.load(Ordering::Relaxed);
        let first_completed_attempt_nanos =
            self.first_completed_attempt_nanos.load(Ordering::Relaxed);

        HeliusClientTimingStats {
            attempts,
            retries: self.retries.load(Ordering::Relaxed),
            total_http_time_ms: nanos_to_millis(total_http_nanos),
            avg_http_time_ms: if attempts == 0 {
                None
            } else {
                Some(total_http_nanos as f64 / 1_000_000.0 / attempts as f64)
            },
            first_completed_attempt_ms: if first_completed_attempt_nanos == 0 {
                None
            } else {
                Some(nanos_to_millis(first_completed_attempt_nanos))
            },
        }
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn nanos_to_millis(nanos: u64) -> u64 {
    nanos / 1_000_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureData {
    #[serde(default)]
    pub address: Option<String>,
    pub transactions: Vec<FullTransactionRecord>,
}

#[derive(Debug, Clone)]
pub struct FixtureClient {
    fixture: FixtureData,
    signatures: Vec<SignatureRecord>,
}

impl FixtureClient {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, HeliusClientError> {
        let fixture = fs::read_to_string(path)?;
        let data: FixtureData = serde_json::from_str(&fixture)?;
        Self::from_fixture(data)
    }

    pub fn from_fixture(mut fixture: FixtureData) -> Result<Self, HeliusClientError> {
        fixture.transactions.sort_by(full_record_order_asc);
        fixture.transactions.dedup_by(|left, right| {
            left.slot == right.slot
                && left.transaction_index == right.transaction_index
                && left.primary_signature() == right.primary_signature()
        });

        let signatures = fixture
            .transactions
            .iter()
            .map(signature_from_full)
            .collect::<Vec<_>>();

        Ok(Self {
            fixture,
            signatures,
        })
    }

    pub fn address(&self) -> Option<&str> {
        self.fixture.address.as_deref()
    }

    fn validate_address(&self, address: &str) -> Result<(), HeliusClientError> {
        if let Some(expected) = self.fixture.address.as_deref()
            && expected != address
        {
            return Err(HeliusClientError::FixtureAddressMismatch {
                expected: expected.to_owned(),
                requested: address.to_owned(),
            });
        }
        Ok(())
    }

    fn page_signatures(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<SignatureRecord>, HeliusClientError> {
        self.validate_address(address)?;

        let mut records = self
            .signatures
            .iter()
            .filter(|record| matches_slot_filter(record.slot, config.filters.as_ref()))
            .cloned()
            .collect::<Vec<_>>();
        sort_signatures(&mut records, config.sort_order);

        build_page(
            records,
            config.sort_order,
            config.limit,
            config.pagination_token,
            signature_token,
            signature_key,
        )
    }

    fn page_full_transactions(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<FullTransactionRecord>, HeliusClientError> {
        self.validate_address(address)?;

        let mut records = self
            .fixture
            .transactions
            .iter()
            .filter(|record| matches_slot_filter(record.slot, config.filters.as_ref()))
            .cloned()
            .collect::<Vec<_>>();
        sort_full_transactions(&mut records, config.sort_order);

        build_page(
            records,
            config.sort_order,
            config.limit,
            config.pagination_token,
            full_transaction_token,
            full_transaction_key,
        )
    }
}

#[async_trait]
impl TransactionsSource for FixtureClient {
    async fn get_signatures(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<SignatureRecord>, HeliusClientError> {
        self.page_signatures(address, config)
    }

    async fn get_full_transactions(
        &self,
        address: &str,
        config: GetTransactionsConfig,
    ) -> Result<GtfaPage<FullTransactionRecord>, HeliusClientError> {
        self.page_full_transactions(address, config)
    }
}

fn parse_rpc_response<T>(body: &str) -> Result<GtfaPage<T>, HeliusClientError>
where
    T: DeserializeOwned,
{
    let value: Value = serde_json::from_str(body)?;
    if value.get("error").is_some() {
        let failure: RpcFailure = serde_json::from_value(value)?;
        return Err(HeliusClientError::Rpc {
            code: failure.error.code,
            message: failure.error.message,
            data: failure.error.data,
        });
    }

    if value.get("result").is_some() {
        let success: RpcSuccess<T> = serde_json::from_value(value)?;
        return Ok(success.result);
    }

    Err(HeliusClientError::InvalidResponse)
}

fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn backoff_delay(attempt: usize) -> Duration {
    let millis = 150u64.saturating_mul(1u64 << attempt.min(4) as u32);
    Duration::from_millis(millis)
}

fn signature_from_full(transaction: &FullTransactionRecord) -> SignatureRecord {
    SignatureRecord {
        signature: transaction
            .primary_signature()
            .unwrap_or_default()
            .to_owned(),
        slot: transaction.slot,
        transaction_index: transaction.transaction_index,
        err: transaction.meta.as_ref().and_then(|meta| meta.err.clone()),
        memo: None,
        block_time: transaction.block_time,
        confirmation_status: Some("finalized".to_owned()),
    }
}

fn signature_token(record: &SignatureRecord) -> String {
    format!("{}:{}", record.slot, record.transaction_index)
}

fn signature_key(record: &SignatureRecord) -> (u64, u32) {
    (record.slot, record.transaction_index)
}

fn full_transaction_token(record: &FullTransactionRecord) -> String {
    format!("{}:{}", record.slot, record.transaction_index)
}

fn full_transaction_key(record: &FullTransactionRecord) -> (u64, u32) {
    (record.slot, record.transaction_index)
}

fn matches_slot_filter(slot: u64, filters: Option<&Filters>) -> bool {
    let Some(range) = filters.and_then(|filters| filters.slot.as_ref()) else {
        return true;
    };

    if let Some(gte) = range.gte
        && slot < gte
    {
        return false;
    }
    if let Some(lte) = range.lte
        && slot > lte
    {
        return false;
    }

    true
}

fn sort_signatures(records: &mut [SignatureRecord], sort_order: SortOrder) {
    records.sort_by(signature_record_order_asc);
    if matches!(sort_order, SortOrder::Desc) {
        records.reverse();
    }
}

fn sort_full_transactions(records: &mut [FullTransactionRecord], sort_order: SortOrder) {
    records.sort_by(full_record_order_asc);
    if matches!(sort_order, SortOrder::Desc) {
        records.reverse();
    }
}

fn signature_record_order_asc(
    left: &SignatureRecord,
    right: &SignatureRecord,
) -> std::cmp::Ordering {
    match left.slot.cmp(&right.slot) {
        std::cmp::Ordering::Equal => match left.transaction_index.cmp(&right.transaction_index) {
            std::cmp::Ordering::Equal => left.signature.cmp(&right.signature),
            other => other,
        },
        other => other,
    }
}

fn full_record_order_asc(
    left: &FullTransactionRecord,
    right: &FullTransactionRecord,
) -> std::cmp::Ordering {
    match left.slot.cmp(&right.slot) {
        std::cmp::Ordering::Equal => match left.transaction_index.cmp(&right.transaction_index) {
            std::cmp::Ordering::Equal => left
                .primary_signature()
                .unwrap_or_default()
                .cmp(right.primary_signature().unwrap_or_default()),
            other => other,
        },
        other => other,
    }
}

fn build_page<T, F>(
    records: Vec<T>,
    sort_order: SortOrder,
    limit: usize,
    pagination_token: Option<String>,
    token_for: F,
    key_for: fn(&T) -> (u64, u32),
) -> Result<GtfaPage<T>, HeliusClientError>
where
    T: Clone,
    F: Fn(&T) -> String,
{
    let start = match pagination_token {
        Some(token) => {
            if let Some(index) = records.iter().position(|record| token_for(record) == token) {
                index + 1
            } else {
                let token_key = parse_token_key(&token)?;
                records
                    .iter()
                    .position(|record| match sort_order {
                        SortOrder::Asc => key_for(record) > token_key,
                        SortOrder::Desc => key_for(record) < token_key,
                    })
                    .unwrap_or(records.len())
            }
        }
        None => 0,
    };

    let page_limit = limit.max(1);
    let end = start.saturating_add(page_limit).min(records.len());
    let data = if start < records.len() {
        records[start..end].to_vec()
    } else {
        Vec::new()
    };

    let pagination_token = if end < records.len() && end > start {
        Some(token_for(&records[end - 1]))
    } else {
        None
    };

    Ok(GtfaPage {
        data,
        pagination_token,
    })
}

fn parse_token_key(token: &str) -> Result<(u64, u32), HeliusClientError> {
    let Some((slot, transaction_index)) = token.split_once(':') else {
        return Err(HeliusClientError::InvalidPaginationToken {
            token: token.to_owned(),
        });
    };

    let slot = slot
        .parse()
        .map_err(|_| HeliusClientError::InvalidPaginationToken {
            token: token.to_owned(),
        })?;
    let transaction_index =
        transaction_index
            .parse()
            .map_err(|_| HeliusClientError::InvalidPaginationToken {
                token: token.to_owned(),
            })?;

    Ok((slot, transaction_index))
}
