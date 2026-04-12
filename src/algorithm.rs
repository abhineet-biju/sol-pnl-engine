use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use async_recursion::async_recursion;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::client::{HeliusClientError, TransactionsSource};
use crate::model::{
    Filters, FullTransactionRecord, GetTransactionsConfig, GtfaPage, SignatureRecord, SortOrder,
};

#[derive(Debug, Clone, Copy)]
struct SlotRange {
    start: u64,
    end: u64,
}

impl SlotRange {
    fn midpoint(self) -> u64 {
        self.start + (self.end - self.start) / 2
    }

    fn span(self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }
}

const DENSITY_SAFETY_FACTOR: f64 = 1.5;
const MAX_ADAPTIVE_WINDOWS: u64 = 16;
const SPARSE_GAP_MULTIPLIER: u64 = 8;
const PARALLEL_DENSE_MIN_CONCURRENCY: usize = 32;
const PARALLEL_DENSE_MIN_ESTIMATED_SIGNATURES_MULTIPLIER: usize = 8;
const PARALLEL_DENSE_MIN_RPS: u32 = 200;
const PARALLEL_DENSE_MIN_WAVE_ADVANTAGE: usize = 4;
const SEEDED_FANOUT_MIN_CONCURRENCY: usize = 32;
const SEEDED_FANOUT_MIN_ESTIMATED_SIGNATURES_MULTIPLIER: usize = 16;
const SEEDED_FANOUT_MIN_RPS: u32 = 200;
const SEEDED_FANOUT_MIN_SLOT_SPAN_MULTIPLIER: u64 = 8;
const SEEDED_FANOUT_MAX_SEGMENTS: usize = 32;
const SEEDED_FANOUT_TARGET_PAGES_PER_SEGMENT: usize = 6;
const SEEDED_FANOUT_VALIDATION_TOLERANCE_DIVISOR: u64 = 8;
const SPARSE_CONFIRMATION_MIN_SAMPLE_SIZE: usize = 256;
const SPARSE_CONFIRMATION_SHRINK_FACTOR: u64 = 4;

#[derive(Clone)]
struct ScanContext {
    semaphore: Arc<Semaphore>,
    stats: Arc<StatsTracker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FullFetchJob {
    start: u64,
    end: u64,
    expected_count: usize,
    pagination_token: Option<String>,
    sort_order: SortOrder,
}

struct DensePipelineResult {
    signatures: Vec<SignatureRecord>,
    full_transactions: Vec<FullTransactionRecord>,
    fetch_jobs: usize,
}

struct SeededFanoutResult {
    signatures: Vec<SignatureRecord>,
    full_transactions: Vec<FullTransactionRecord>,
    fetch_jobs: usize,
}

struct DenseEdgePagination {
    range: SlotRange,
    left_page: GtfaPage<SignatureRecord>,
    right_page: GtfaPage<SignatureRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub block_time: Option<i64>,
    pub succeeded: bool,
    pub pre_balance_lamports: u64,
    pub post_balance_lamports: u64,
    pub delta_lamports: i64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ScanStats {
    pub signature_requests: usize,
    pub full_requests: usize,
    pub discovered_signatures: usize,
    pub fetched_transactions: usize,
    pub fetch_jobs: usize,
    pub max_discovery_depth: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScanStrategy {
    Empty,
    BoundaryOnly,
    SeededFullFanout,
    SparseRecursive,
    DenseParallelRange,
    DensePincer,
}

impl ScanStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::BoundaryOnly => "boundary_only",
            Self::SeededFullFanout => "seeded_full_fanout",
            Self::SparseRecursive => "sparse_recursive",
            Self::DenseParallelRange => "dense_parallel_range",
            Self::DensePincer => "dense_pincer",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineOutput {
    pub address: String,
    pub strategy: ScanStrategy,
    pub initial_balance_lamports: Option<u64>,
    pub final_balance_lamports: Option<u64>,
    pub entries: Vec<TimelineEntry>,
    pub stats: ScanStats,
}

#[derive(Debug, Clone)]
pub struct RuntimeScanConfig {
    pub concurrency: usize,
    pub scout_limit: usize,
    pub full_limit: usize,
    pub rpc_rps: Option<u32>,
}

impl Default for RuntimeScanConfig {
    fn default() -> Self {
        Self {
            concurrency: 32,
            scout_limit: 1000,
            full_limit: 100,
            rpc_rps: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ScanError {
    #[error(transparent)]
    Client(#[from] HeliusClientError),
    #[error("internal semaphore was unexpectedly closed")]
    SemaphoreClosed,
    #[error("transaction {signature} is missing metadata")]
    MissingMeta { signature: String },
    #[error("target address {address} was not present in transaction {signature}")]
    MissingAddress { address: String, signature: String },
    #[error(
        "balance arrays were too short for target address {address} in transaction {signature}"
    )]
    MissingBalanceIndex { address: String, signature: String },
}

#[derive(Default)]
struct StatsTracker {
    signature_requests: AtomicUsize,
    full_requests: AtomicUsize,
    max_discovery_depth: AtomicUsize,
}

impl StatsTracker {
    fn snapshot(&self) -> ScanStats {
        ScanStats {
            signature_requests: self.signature_requests.load(AtomicOrdering::Relaxed),
            full_requests: self.full_requests.load(AtomicOrdering::Relaxed),
            discovered_signatures: 0,
            fetched_transactions: 0,
            fetch_jobs: 0,
            max_discovery_depth: self.max_discovery_depth.load(AtomicOrdering::Relaxed),
        }
    }

    fn note_depth(&self, depth: usize) {
        let mut current = self.max_discovery_depth.load(AtomicOrdering::Relaxed);
        while depth > current {
            match self.max_discovery_depth.compare_exchange(
                current,
                depth,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
    }
}

pub async fn reconstruct_sol_balance_timeline(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    config: RuntimeScanConfig,
) -> Result<TimelineOutput, ScanError> {
    let concurrency = config.concurrency.max(1);
    let scout_limit = config.scout_limit.clamp(1, 1000);
    let full_limit = config.full_limit.clamp(1, 100);

    let context = ScanContext {
        semaphore: Arc::new(Semaphore::new(concurrency)),
        stats: Arc::new(StatsTracker::default()),
    };

    let (oldest_page, newest_page) = tokio::join!(
        get_signatures_page(
            client,
            address,
            context.clone(),
            SlotRange {
                start: 0,
                end: u64::MAX,
            },
            SortOrder::Asc,
            scout_limit,
            None,
        ),
        get_signatures_page(
            client,
            address,
            context.clone(),
            SlotRange {
                start: 0,
                end: u64::MAX,
            },
            SortOrder::Desc,
            scout_limit,
            None,
        )
    );

    let oldest_page = oldest_page?;
    let newest_page = newest_page?;

    let Some(oldest) = oldest_page.data.first() else {
        let mut stats_snapshot = context.stats.snapshot();
        stats_snapshot.discovered_signatures = 0;
        stats_snapshot.fetched_transactions = 0;
        stats_snapshot.fetch_jobs = 0;

        return Ok(TimelineOutput {
            address: address.to_owned(),
            strategy: ScanStrategy::Empty,
            initial_balance_lamports: None,
            final_balance_lamports: None,
            entries: Vec::new(),
            stats: stats_snapshot,
        });
    };

    let Some(newest) = newest_page.data.first() else {
        let mut stats_snapshot = context.stats.snapshot();
        stats_snapshot.discovered_signatures = 0;
        stats_snapshot.fetched_transactions = 0;
        stats_snapshot.fetch_jobs = 0;

        return Ok(TimelineOutput {
            address: address.to_owned(),
            strategy: ScanStrategy::Empty,
            initial_balance_lamports: None,
            final_balance_lamports: None,
            entries: Vec::new(),
            stats: stats_snapshot,
        });
    };

    let signature_range = SlotRange {
        start: oldest.slot,
        end: newest.slot,
    };

    let history_fits_boundary_pages = pages_cover_complete_history(&oldest_page, &newest_page);
    let mut prefetched_full_transactions = None;
    let mut prefetched_fetch_jobs = None;
    let mut prefetched_signatures = None;

    let mut strategy = if history_fits_boundary_pages {
        ScanStrategy::BoundaryOnly
    } else {
        ScanStrategy::DensePincer
    };

    let mut signatures = if history_fits_boundary_pages {
        let mut signatures = oldest_page.data.clone();
        signatures.extend(newest_page.data.clone());
        signatures
    } else {
        let left_frontier = oldest_page
            .data
            .last()
            .cloned()
            .expect("non-empty boundary page");
        let right_frontier = newest_page
            .data
            .last()
            .cloned()
            .expect("non-empty boundary page");

        let should_try_seeded_fanout = should_use_seeded_dense_full_fanout(
            &config,
            &oldest_page,
            &newest_page,
            signature_range,
        );

        if should_try_seeded_fanout {
            match try_seeded_dense_full_fanout(
                client,
                address,
                context.clone(),
                &config,
                full_limit,
                &oldest_page,
                &newest_page,
                signature_range,
            )
            .await?
            {
                Some(seeded_result) => {
                    strategy = ScanStrategy::SeededFullFanout;
                    prefetched_fetch_jobs = Some(seeded_result.fetch_jobs);
                    prefetched_full_transactions = Some(seeded_result.full_transactions);
                    prefetched_signatures = Some(seeded_result.signatures);
                }
                None => {}
            }
        }

        let should_use_sparse_search = if prefetched_full_transactions.is_some() {
            false
        } else if should_use_sparse_gap_search(&oldest_page, &newest_page) {
            if should_confirm_sparse_gap_classification(
                &config,
                &oldest_page,
                &newest_page,
                signature_range,
            ) {
                confirm_sparse_gap_search(
                    client,
                    address,
                    context.clone(),
                    left_frontier.slot,
                    right_frontier.slot,
                )
                .await?
            } else {
                true
            }
        } else {
            false
        };

        if prefetched_full_transactions.is_some() {
            prefetched_signatures.take().unwrap_or_default()
        } else if should_use_sparse_search {
            strategy = ScanStrategy::SparseRecursive;
            let mut signatures = oldest_page.data.clone();
            signatures.extend(newest_page.data.clone());
            discover_signatures_in_range(
                client,
                address,
                SlotRange {
                    start: left_frontier.slot,
                    end: right_frontier.slot,
                },
                context.clone(),
                scout_limit,
                0,
            )
            .await
            .map(|middle| {
                let mut merged = signatures;
                merged.extend(middle);
                merged
            })?
        } else if should_use_parallel_dense_range_search(
            &config,
            &oldest_page,
            &newest_page,
            signature_range,
        ) {
            strategy = ScanStrategy::DenseParallelRange;
            let mut signatures = oldest_page.data.clone();
            signatures.extend(newest_page.data.clone());
            discover_signatures_in_range(
                client,
                address,
                SlotRange {
                    start: left_frontier.slot,
                    end: right_frontier.slot,
                },
                context.clone(),
                scout_limit,
                0,
            )
            .await
            .map(|middle| {
                let mut merged = signatures;
                merged.extend(middle);
                merged
            })?
        } else {
            strategy = ScanStrategy::DensePincer;
            let dense_result = discover_and_fetch_dense_history(
                client,
                address,
                context.clone(),
                scout_limit,
                full_limit,
                DenseEdgePagination {
                    range: signature_range,
                    left_page: oldest_page.clone(),
                    right_page: newest_page.clone(),
                },
            )
            .await?;
            prefetched_fetch_jobs = Some(dense_result.fetch_jobs);
            prefetched_full_transactions = Some(dense_result.full_transactions);
            dense_result.signatures
        }
    };

    sort_and_dedup_signatures(&mut signatures);

    let (fetch_jobs_len, mut full_transactions) =
        if let Some(prefetched_full_transactions) = prefetched_full_transactions {
            (
                prefetched_fetch_jobs.expect("dense pipeline should report fetch jobs"),
                prefetched_full_transactions,
            )
        } else {
            let fetch_jobs = build_full_fetch_jobs(&signatures, full_limit);
            let mut full_futures = FuturesUnordered::new();
            for job in fetch_jobs.clone() {
                full_futures.push(fetch_full_job(
                    client,
                    address,
                    job,
                    context.clone(),
                    full_limit,
                ));
            }

            let mut full_transactions = Vec::new();
            while let Some(job_result) = full_futures.next().await {
                full_transactions.extend(job_result?);
            }
            (fetch_jobs.len(), full_transactions)
        };
    sort_and_dedup_full_transactions(&mut full_transactions);

    let entries = build_timeline_entries(address, full_transactions)?;

    let mut stats_snapshot = context.stats.snapshot();
    stats_snapshot.discovered_signatures = signatures.len();
    stats_snapshot.fetched_transactions = entries.len();
    stats_snapshot.fetch_jobs = fetch_jobs_len;

    Ok(TimelineOutput {
        address: address.to_owned(),
        strategy,
        initial_balance_lamports: entries.first().map(|entry| entry.pre_balance_lamports),
        final_balance_lamports: entries.last().map(|entry| entry.post_balance_lamports),
        entries,
        stats: stats_snapshot,
    })
}

fn pages_cover_complete_history(
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
) -> bool {
    if asc_page.pagination_token.is_none() || desc_page.pagination_token.is_none() {
        return true;
    }

    let Some(left_frontier) = asc_page.data.last() else {
        return true;
    };
    let Some(right_frontier) = desc_page.data.last() else {
        return true;
    };

    signature_order(left_frontier, right_frontier) != Ordering::Less
}

fn should_use_sparse_gap_search(
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
) -> bool {
    let Some(left_first) = asc_page.data.first() else {
        return false;
    };
    let Some(left_last) = asc_page.data.last() else {
        return false;
    };
    let Some(right_first) = desc_page.data.first() else {
        return false;
    };
    let Some(right_last) = desc_page.data.last() else {
        return false;
    };

    if left_last.slot >= right_last.slot {
        return false;
    }

    let gap_span = right_last.slot.saturating_sub(left_last.slot);
    let left_span = left_last
        .slot
        .saturating_sub(left_first.slot)
        .saturating_add(1);
    let right_span = right_first
        .slot
        .saturating_sub(right_last.slot)
        .saturating_add(1);
    let covered_span = left_span.saturating_add(right_span);

    gap_span > covered_span.saturating_mul(SPARSE_GAP_MULTIPLIER)
}

fn should_confirm_sparse_gap_classification(
    config: &RuntimeScanConfig,
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
    range: SlotRange,
) -> bool {
    let scout_limit = config.scout_limit.max(1);
    if scout_limit < SPARSE_CONFIRMATION_MIN_SAMPLE_SIZE {
        return false;
    }

    if asc_page.data.len() < scout_limit || desc_page.data.len() < scout_limit {
        return false;
    }

    estimate_dense_signature_count(asc_page, desc_page, range).is_some_and(|estimated| {
        estimated >= scout_limit.saturating_mul(PARALLEL_DENSE_MIN_ESTIMATED_SIGNATURES_MULTIPLIER)
    })
}

async fn confirm_sparse_gap_search(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    left_slot: u64,
    right_slot: u64,
) -> Result<bool, ScanError> {
    if left_slot >= right_slot {
        return Ok(false);
    }

    let range = SlotRange {
        start: left_slot,
        end: right_slot,
    };
    let mid = range.midpoint();
    if mid >= range.end {
        return Ok(false);
    }

    let (left_probe, right_probe) = tokio::join!(
        get_signatures_page(
            client,
            address,
            context.clone(),
            SlotRange {
                start: range.start,
                end: mid,
            },
            SortOrder::Desc,
            1,
            None,
        ),
        get_signatures_page(
            client,
            address,
            context,
            SlotRange {
                start: mid + 1,
                end: range.end,
            },
            SortOrder::Asc,
            1,
            None,
        )
    );

    let left_probe = left_probe?;
    let right_probe = right_probe?;

    let Some(left_midpoint_slot) = left_probe.data.first().map(|record| record.slot) else {
        return Ok(true);
    };
    let Some(right_midpoint_slot) = right_probe.data.first().map(|record| record.slot) else {
        return Ok(true);
    };

    let original_gap = right_slot.saturating_sub(left_slot);
    let narrowed_gap = right_midpoint_slot.saturating_sub(left_midpoint_slot);

    Ok(narrowed_gap > original_gap / SPARSE_CONFIRMATION_SHRINK_FACTOR)
}

fn should_use_seeded_dense_full_fanout(
    config: &RuntimeScanConfig,
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
    range: SlotRange,
) -> bool {
    if config.concurrency < SEEDED_FANOUT_MIN_CONCURRENCY {
        return false;
    }

    if let Some(rpc_rps) = config.rpc_rps
        && rpc_rps < SEEDED_FANOUT_MIN_RPS
    {
        return false;
    }

    let scout_limit = config.scout_limit.max(1);
    if asc_page.data.len() < scout_limit || desc_page.data.len() < scout_limit {
        return false;
    }

    if range.span() < (scout_limit as u64).saturating_mul(SEEDED_FANOUT_MIN_SLOT_SPAN_MULTIPLIER) {
        return false;
    }

    let Some(estimated_signatures) = estimate_dense_signature_count(asc_page, desc_page, range)
    else {
        return false;
    };

    if estimated_signatures
        < scout_limit.saturating_mul(SEEDED_FANOUT_MIN_ESTIMATED_SIGNATURES_MULTIPLIER)
    {
        return false;
    }

    let pincer_rounds = estimated_signatures.div_ceil(scout_limit.saturating_mul(2));
    pincer_rounds >= 8
}

async fn try_seeded_dense_full_fanout(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    config: &RuntimeScanConfig,
    full_limit: usize,
    oldest_page: &GtfaPage<SignatureRecord>,
    newest_page: &GtfaPage<SignatureRecord>,
    signature_range: SlotRange,
) -> Result<Option<SeededFanoutResult>, ScanError> {
    let Some(estimated_signatures) =
        estimate_dense_signature_count(oldest_page, newest_page, signature_range)
    else {
        return Ok(None);
    };

    let left_signature_frontier = oldest_page
        .data
        .last()
        .cloned()
        .expect("seeded dense fanout requires a left frontier");
    let right_signature_frontier = newest_page
        .data
        .last()
        .cloned()
        .expect("seeded dense fanout requires a right frontier");

    let validation_ok = match validate_seeded_full_pagination(
        client,
        address,
        context.clone(),
        full_limit,
        &left_signature_frontier,
        &right_signature_frontier,
    )
    .await
    {
        Ok(valid) => valid,
        Err(ScanError::Client(_)) => false,
        Err(error) => return Err(error),
    };

    if !validation_ok {
        return Ok(None);
    }

    let (left_page, right_page) = tokio::join!(
        get_full_page_unfiltered(
            client,
            address,
            context.clone(),
            SortOrder::Asc,
            full_limit,
            None,
        ),
        get_full_page_unfiltered(
            client,
            address,
            context.clone(),
            SortOrder::Desc,
            full_limit,
            None,
        )
    );

    let left_page = match left_page {
        Ok(page) => page,
        Err(ScanError::Client(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let right_page = match right_page {
        Ok(page) => page,
        Err(ScanError::Client(_)) => return Ok(None),
        Err(error) => return Err(error),
    };

    let Some(left_frontier) = left_page.data.last().cloned() else {
        return Ok(None);
    };
    let Some(right_frontier) = right_page.data.last().cloned() else {
        return Ok(None);
    };

    let mut signatures = left_page
        .data
        .iter()
        .map(signature_record_from_full_transaction)
        .collect::<Vec<_>>();
    signatures.extend(
        right_page
            .data
            .iter()
            .map(signature_record_from_full_transaction),
    );

    let mut full_transactions = left_page.data.clone();
    full_transactions.extend(right_page.data.clone());

    if full_record_order(&left_frontier, &right_frontier) != Ordering::Less {
        sort_and_dedup_signatures(&mut signatures);
        sort_and_dedup_full_transactions(&mut full_transactions);
        return Ok(Some(SeededFanoutResult {
            signatures,
            full_transactions,
            fetch_jobs: 2,
        }));
    }

    let segments = plan_seeded_fanout_segments(
        config,
        full_limit,
        estimated_signatures,
        &left_frontier,
        &right_frontier,
    );

    let mut segment_futures = FuturesUnordered::new();
    for segment in segments.clone() {
        segment_futures.push(fetch_seeded_segment_transactions(
            client,
            address,
            context.clone(),
            full_limit,
            segment,
        ));
    }

    while let Some(result) = segment_futures.next().await {
        match result {
            Ok(segment_records) => {
                signatures.extend(
                    segment_records
                        .iter()
                        .map(signature_record_from_full_transaction),
                );
                full_transactions.extend(segment_records);
            }
            Err(ScanError::Client(_)) => return Ok(None),
            Err(error) => return Err(error),
        }
    }

    sort_and_dedup_signatures(&mut signatures);
    sort_and_dedup_full_transactions(&mut full_transactions);

    Ok(Some(SeededFanoutResult {
        signatures,
        full_transactions,
        fetch_jobs: 2 + segments.len(),
    }))
}

async fn validate_seeded_full_pagination(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    full_limit: usize,
    left_frontier: &SignatureRecord,
    right_frontier: &SignatureRecord,
) -> Result<bool, ScanError> {
    if left_frontier.slot >= right_frontier.slot {
        return Ok(false);
    }

    let midpoint = left_frontier.slot + (right_frontier.slot - left_frontier.slot) / 2;
    let page = get_full_page_with_filters(
        client,
        address,
        context,
        SortOrder::Asc,
        full_limit,
        Some(Filters::slot_range(midpoint, right_frontier.slot)),
        Some(seeded_fanout_token(midpoint)),
    )
    .await?;

    let Some(first_record) = page.data.first() else {
        return Ok(false);
    };

    let span = right_frontier
        .slot
        .saturating_sub(left_frontier.slot)
        .saturating_add(1);
    let tolerance = (span / SEEDED_FANOUT_VALIDATION_TOLERANCE_DIVISOR).max(1);
    let minimum_expected_slot = midpoint.saturating_sub(tolerance);

    Ok(first_record.slot >= minimum_expected_slot && first_record.slot <= right_frontier.slot)
}

#[derive(Debug, Clone)]
struct SeededFanoutSegment {
    start_slot: u64,
    end_slot_exclusive: Option<u64>,
    lower_exclusive: Option<SignatureRecord>,
    upper_exclusive: Option<SignatureRecord>,
}

fn plan_seeded_fanout_segments(
    config: &RuntimeScanConfig,
    full_limit: usize,
    estimated_signatures: usize,
    left_frontier: &FullTransactionRecord,
    right_frontier: &FullTransactionRecord,
) -> Vec<SeededFanoutSegment> {
    let max_segments = config.concurrency.min(SEEDED_FANOUT_MAX_SEGMENTS).max(4);
    let estimated_pages = estimated_signatures.div_ceil(full_limit.max(1));
    let segment_count = estimated_pages
        .div_ceil(SEEDED_FANOUT_TARGET_PAGES_PER_SEGMENT)
        .clamp(4, max_segments);

    let start = left_frontier.slot;
    let end = right_frontier.slot;
    let slot_span = end.saturating_sub(start).saturating_add(1);

    let mut start_slots = Vec::new();
    for index in 0..segment_count {
        let offset = ((u128::from(slot_span) * index as u128) / segment_count as u128)
            .min(u128::from(u64::MAX)) as u64;
        let slot = start.saturating_add(offset).min(end);
        if start_slots.last().copied() != Some(slot) {
            start_slots.push(slot);
        }
    }

    if start_slots.is_empty() {
        start_slots.push(start);
    }

    start_slots
        .iter()
        .enumerate()
        .map(|(index, slot)| SeededFanoutSegment {
            start_slot: *slot,
            end_slot_exclusive: start_slots.get(index + 1).copied(),
            lower_exclusive: (index == 0)
                .then(|| signature_record_from_full_transaction(left_frontier)),
            upper_exclusive: (index + 1 == start_slots.len())
                .then(|| signature_record_from_full_transaction(right_frontier)),
        })
        .collect()
}

async fn fetch_seeded_segment_transactions(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    full_limit: usize,
    segment: SeededFanoutSegment,
) -> Result<Vec<FullTransactionRecord>, ScanError> {
    let initial_token = seeded_fanout_token(segment.start_slot);
    let mut pagination_token = Some(initial_token.clone());
    let mut records = Vec::new();
    let mut first_page = true;
    let filters = Some(Filters::slot_range(
        segment.start_slot,
        seeded_segment_filter_end(&segment),
    ));

    loop {
        let page = get_full_page_with_filters(
            client,
            address,
            context.clone(),
            SortOrder::Asc,
            full_limit,
            filters.clone(),
            pagination_token.take(),
        )
        .await?;

        if page.data.is_empty() {
            break;
        }

        if first_page {
            first_page = false;
            let first_slot = page
                .data
                .first()
                .map(|record| record.slot)
                .unwrap_or_default();
            if first_slot < segment.start_slot.saturating_sub(1) {
                return Err(ScanError::Client(
                    HeliusClientError::InvalidPaginationToken {
                        token: initial_token,
                    },
                ));
            }
        }

        let reached_upper_boundary = page
            .data
            .last()
            .is_some_and(|record| seeded_segment_reached_upper_boundary(record, &segment));

        records.extend(
            page.data
                .into_iter()
                .filter(|record| full_record_in_seeded_segment(record, &segment)),
        );

        let Some(next_token) = page.pagination_token else {
            break;
        };
        if reached_upper_boundary {
            break;
        }
        pagination_token = Some(next_token);
    }

    Ok(records)
}

fn seeded_fanout_token(start_slot: u64) -> String {
    format!("{}:0", start_slot.saturating_sub(1))
}

fn full_record_in_seeded_segment(
    record: &FullTransactionRecord,
    segment: &SeededFanoutSegment,
) -> bool {
    if record.slot < segment.start_slot {
        return false;
    }

    if let Some(lower_exclusive) = segment.lower_exclusive.as_ref()
        && compare_full_record_to_signature_record(record, lower_exclusive) != Ordering::Greater
    {
        return false;
    }

    if let Some(end_slot) = segment.end_slot_exclusive
        && record.slot >= end_slot
    {
        return false;
    }

    if let Some(upper_exclusive) = segment.upper_exclusive.as_ref()
        && compare_full_record_to_signature_record(record, upper_exclusive) != Ordering::Less
    {
        return false;
    }

    true
}

fn seeded_segment_reached_upper_boundary(
    record: &FullTransactionRecord,
    segment: &SeededFanoutSegment,
) -> bool {
    if let Some(end_slot) = segment.end_slot_exclusive
        && record.slot >= end_slot
    {
        return true;
    }

    if let Some(upper_exclusive) = segment.upper_exclusive.as_ref() {
        return compare_full_record_to_signature_record(record, upper_exclusive) != Ordering::Less;
    }

    false
}

fn seeded_segment_filter_end(segment: &SeededFanoutSegment) -> u64 {
    if let Some(end_slot) = segment.end_slot_exclusive {
        return end_slot.saturating_sub(1).max(segment.start_slot);
    }

    segment
        .upper_exclusive
        .as_ref()
        .map_or(u64::MAX, |record| record.slot)
}

fn should_use_parallel_dense_range_search(
    config: &RuntimeScanConfig,
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
    range: SlotRange,
) -> bool {
    if config.concurrency < PARALLEL_DENSE_MIN_CONCURRENCY {
        return false;
    }

    if let Some(rpc_rps) = config.rpc_rps {
        if rpc_rps < PARALLEL_DENSE_MIN_RPS {
            return false;
        }
    }

    let scout_limit = config.scout_limit.max(1);
    let full_limit = config.full_limit.max(1);

    if asc_page.data.len() < scout_limit || desc_page.data.len() < scout_limit {
        return false;
    }

    let Some(estimated_signatures) = estimate_dense_signature_count(asc_page, desc_page, range)
    else {
        return false;
    };

    if estimated_signatures
        < scout_limit.saturating_mul(PARALLEL_DENSE_MIN_ESTIMATED_SIGNATURES_MULTIPLIER)
    {
        return false;
    }

    let pincer_rounds = estimated_signatures.div_ceil(scout_limit.saturating_mul(2));
    let full_fetch_waves =
        estimated_signatures.div_ceil(full_limit.saturating_mul(config.concurrency));

    pincer_rounds > full_fetch_waves.saturating_add(PARALLEL_DENSE_MIN_WAVE_ADVANTAGE)
}

fn estimate_dense_signature_count(
    asc_page: &GtfaPage<SignatureRecord>,
    desc_page: &GtfaPage<SignatureRecord>,
    range: SlotRange,
) -> Option<usize> {
    let left_first = asc_page.data.first()?;
    let left_last = asc_page.data.last()?;
    let right_first = desc_page.data.first()?;
    let right_last = desc_page.data.last()?;

    let left_span = left_last
        .slot
        .saturating_sub(left_first.slot)
        .saturating_add(1);
    let right_span = right_first
        .slot
        .saturating_sub(right_last.slot)
        .saturating_add(1);

    if left_span == 0 || right_span == 0 {
        return None;
    }

    let left_density = asc_page.data.len() as f64 / left_span as f64;
    let right_density = desc_page.data.len() as f64 / right_span as f64;
    let estimated_from_density = left_density.min(right_density) * range.span() as f64;
    let observed_boundary_count = asc_page.data.len().saturating_add(desc_page.data.len());

    Some(estimated_from_density.ceil() as usize)
        .map(|estimate| estimate.max(observed_boundary_count))
}

async fn discover_and_fetch_dense_history(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    scout_limit: usize,
    full_limit: usize,
    pagination: DenseEdgePagination,
) -> Result<DensePipelineResult, ScanError> {
    let mut signatures = Vec::new();
    let mut full_futures = FuturesUnordered::new();
    let mut fetch_jobs = 0usize;

    let mut left_token = pagination.left_page.pagination_token.clone();
    let mut right_token = pagination.right_page.pagination_token.clone();
    let mut left_frontier = pagination
        .left_page
        .data
        .last()
        .cloned()
        .expect("dense edge pagination requires a non-empty left page");
    let mut right_frontier = pagination
        .right_page
        .data
        .last()
        .cloned()
        .expect("dense edge pagination requires a non-empty right page");

    let (mut left_boundary, left_jobs) = prepare_full_jobs_for_signature_page(
        full_limit,
        SortOrder::Asc,
        None,
        take_unique_signature_prefix(
            pagination.left_page.data.clone(),
            &right_frontier,
            SortOrder::Asc,
        ),
        &mut signatures,
    );
    fetch_jobs += left_jobs.len();
    for job in left_jobs {
        full_futures.push(fetch_full_job(
            client,
            address,
            job,
            context.clone(),
            full_limit,
        ));
    }

    let (mut right_boundary, right_jobs) = prepare_full_jobs_for_signature_page(
        full_limit,
        SortOrder::Desc,
        None,
        take_unique_signature_prefix(
            pagination.right_page.data.clone(),
            &left_frontier,
            SortOrder::Desc,
        ),
        &mut signatures,
    );
    fetch_jobs += right_jobs.len();
    for job in right_jobs {
        full_futures.push(fetch_full_job(
            client,
            address,
            job,
            context.clone(),
            full_limit,
        ));
    }

    while signature_order(&left_frontier, &right_frontier) == Ordering::Less {
        let previous_right_frontier = right_frontier.clone();
        let next_left_token = left_token.take();
        let next_right_token = right_token.take();

        if next_left_token.is_none() && next_right_token.is_none() {
            break;
        }

        let (left_page, right_page) = tokio::join!(
            async {
                match next_left_token {
                    Some(token) => Some(
                        get_signatures_page(
                            client,
                            address,
                            context.clone(),
                            pagination.range,
                            SortOrder::Asc,
                            scout_limit,
                            Some(token),
                        )
                        .await,
                    ),
                    None => None,
                }
            },
            async {
                match next_right_token {
                    Some(token) => Some(
                        get_signatures_page(
                            client,
                            address,
                            context.clone(),
                            pagination.range,
                            SortOrder::Desc,
                            scout_limit,
                            Some(token),
                        )
                        .await,
                    ),
                    None => None,
                }
            }
        );

        let mut left_page_data = None;
        if let Some(page_result) = left_page {
            let page = page_result?;
            if let Some(frontier) = page.data.last().cloned() {
                left_frontier = frontier;
            }
            left_token = page.pagination_token.clone();
            left_page_data = Some(page.data);
        }

        let mut right_page_data = None;
        if let Some(page_result) = right_page {
            let page = page_result?;
            if let Some(frontier) = page.data.last().cloned() {
                right_frontier = frontier;
            }
            right_token = page.pagination_token.clone();
            right_page_data = Some(page.data);
        }

        if let Some(page_data) = left_page_data {
            let (next_boundary, jobs) = prepare_full_jobs_for_signature_page(
                full_limit,
                SortOrder::Asc,
                left_boundary.as_ref(),
                take_unique_signature_prefix(page_data, &previous_right_frontier, SortOrder::Asc),
                &mut signatures,
            );
            fetch_jobs += jobs.len();
            for job in jobs {
                full_futures.push(fetch_full_job(
                    client,
                    address,
                    job,
                    context.clone(),
                    full_limit,
                ));
            }
            left_boundary = next_boundary;
        }

        if let Some(page_data) = right_page_data {
            let (next_boundary, jobs) = prepare_full_jobs_for_signature_page(
                full_limit,
                SortOrder::Desc,
                right_boundary.as_ref(),
                take_unique_signature_prefix(page_data, &left_frontier, SortOrder::Desc),
                &mut signatures,
            );
            fetch_jobs += jobs.len();
            for job in jobs {
                full_futures.push(fetch_full_job(
                    client,
                    address,
                    job,
                    context.clone(),
                    full_limit,
                ));
            }
            right_boundary = next_boundary;
        }
    }

    let mut full_transactions = Vec::new();
    while let Some(job_result) = full_futures.next().await {
        full_transactions.extend(job_result?);
    }

    Ok(DensePipelineResult {
        signatures,
        full_transactions,
        fetch_jobs,
    })
}

fn prepare_full_jobs_for_signature_page(
    full_limit: usize,
    sort_order: SortOrder,
    previous_boundary: Option<&SignatureRecord>,
    page_records: Vec<SignatureRecord>,
    signatures: &mut Vec<SignatureRecord>,
) -> (Option<SignatureRecord>, Vec<FullFetchJob>) {
    if page_records.is_empty() {
        return (previous_boundary.cloned(), Vec::new());
    }

    let jobs =
        build_full_fetch_jobs_for_order(&page_records, full_limit, sort_order, previous_boundary);
    let next_boundary = page_records.last().cloned();
    signatures.extend(page_records);
    (next_boundary, jobs)
}

fn take_unique_signature_prefix(
    records: Vec<SignatureRecord>,
    opposite_frontier: &SignatureRecord,
    sort_order: SortOrder,
) -> Vec<SignatureRecord> {
    records
        .into_iter()
        .take_while(|record| match sort_order {
            SortOrder::Asc => signature_order(record, opposite_frontier) == Ordering::Less,
            SortOrder::Desc => signature_order(record, opposite_frontier) == Ordering::Greater,
        })
        .collect()
}

#[async_recursion]
async fn discover_signatures_in_range(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    range: SlotRange,
    context: ScanContext,
    scout_limit: usize,
    depth: usize,
) -> Result<Vec<SignatureRecord>, ScanError> {
    context.stats.note_depth(depth);

    let first_page = get_signatures_page(
        client,
        address,
        context.clone(),
        range,
        SortOrder::Asc,
        scout_limit,
        None,
    )
    .await?;

    if first_page.data.is_empty() {
        return Ok(Vec::new());
    }

    if first_page.pagination_token.is_none() {
        return Ok(first_page.data);
    }

    if range.start == range.end {
        return paginate_dense_signature_slot(
            client,
            address,
            range.start,
            context,
            scout_limit,
            first_page,
        )
        .await;
    }

    if let Some(subranges) = plan_adaptive_signature_subranges(range, &first_page, scout_limit) {
        let mut futures = FuturesUnordered::new();
        for subrange in subranges {
            futures.push(discover_signatures_in_range(
                client,
                address,
                subrange,
                context.clone(),
                scout_limit,
                depth + 1,
            ));
        }

        let mut merged = first_page.data;
        while let Some(result) = futures.next().await {
            merged.extend(result?);
        }
        return Ok(merged);
    }

    let mid = range.midpoint();
    if mid >= range.end {
        return paginate_signatures(client, address, range, context, scout_limit, first_page).await;
    }

    let (left_probe, right_probe) = tokio::join!(
        get_signatures_page(
            client,
            address,
            context.clone(),
            SlotRange {
                start: range.start,
                end: mid,
            },
            SortOrder::Desc,
            1,
            None,
        ),
        get_signatures_page(
            client,
            address,
            context.clone(),
            SlotRange {
                start: mid + 1,
                end: range.end,
            },
            SortOrder::Asc,
            1,
            None,
        )
    );

    let left_slot = left_probe?.data.first().map(|record| record.slot);
    let right_slot = right_probe?.data.first().map(|record| record.slot);

    match (left_slot, right_slot) {
        (Some(left_end), Some(right_start)) => {
            let left_range = SlotRange {
                start: range.start,
                end: left_end,
            };
            let right_range = SlotRange {
                start: right_start,
                end: range.end,
            };

            if left_range.start == range.start && left_range.end == range.end
                || right_range.start == range.start && right_range.end == range.end
            {
                return paginate_signatures(
                    client,
                    address,
                    range,
                    context,
                    scout_limit,
                    first_page,
                )
                .await;
            }

            let (left, right) = tokio::join!(
                discover_signatures_in_range(
                    client,
                    address,
                    left_range,
                    context.clone(),
                    scout_limit,
                    depth + 1,
                ),
                discover_signatures_in_range(
                    client,
                    address,
                    right_range,
                    context.clone(),
                    scout_limit,
                    depth + 1,
                )
            );

            let mut merged = left?;
            merged.extend(right?);
            Ok(merged)
        }
        (Some(left_end), None) => {
            let narrowed = SlotRange {
                start: range.start,
                end: left_end,
            };
            if narrowed.start == range.start && narrowed.end == range.end {
                paginate_signatures(client, address, range, context, scout_limit, first_page).await
            } else {
                discover_signatures_in_range(
                    client,
                    address,
                    narrowed,
                    context,
                    scout_limit,
                    depth + 1,
                )
                .await
            }
        }
        (None, Some(right_start)) => {
            let narrowed = SlotRange {
                start: right_start,
                end: range.end,
            };
            if narrowed.start == range.start && narrowed.end == range.end {
                paginate_signatures(client, address, range, context, scout_limit, first_page).await
            } else {
                discover_signatures_in_range(
                    client,
                    address,
                    narrowed,
                    context,
                    scout_limit,
                    depth + 1,
                )
                .await
            }
        }
        (None, None) => Ok(Vec::new()),
    }
}

async fn paginate_signatures(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    range: SlotRange,
    context: ScanContext,
    scout_limit: usize,
    first_page: GtfaPage<SignatureRecord>,
) -> Result<Vec<SignatureRecord>, ScanError> {
    let mut records = first_page.data;
    let mut pagination_token = first_page.pagination_token;

    while let Some(token) = pagination_token {
        let next_page = get_signatures_page(
            client,
            address,
            context.clone(),
            range,
            SortOrder::Asc,
            scout_limit,
            Some(token),
        )
        .await?;
        pagination_token = next_page.pagination_token.clone();
        records.extend(next_page.data);
    }

    Ok(records)
}

async fn paginate_dense_signature_slot(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    slot: u64,
    context: ScanContext,
    scout_limit: usize,
    first_page: GtfaPage<SignatureRecord>,
) -> Result<Vec<SignatureRecord>, ScanError> {
    let range = SlotRange {
        start: slot,
        end: slot,
    };
    let Some(mut left_frontier) = first_page.data.last().cloned() else {
        return Ok(Vec::new());
    };

    let mut records = first_page.data;
    let mut left_token = first_page.pagination_token;
    let desc_page = get_signatures_page(
        client,
        address,
        context.clone(),
        range,
        SortOrder::Desc,
        scout_limit,
        None,
    )
    .await?;

    let Some(mut right_frontier) = desc_page.data.last().cloned() else {
        return Ok(records);
    };
    let mut right_token = desc_page.pagination_token.clone();
    records.extend(desc_page.data);

    while signature_order(&left_frontier, &right_frontier) == Ordering::Less {
        let next_left_token = left_token.take();
        let next_right_token = right_token.take();

        if next_left_token.is_none() && next_right_token.is_none() {
            break;
        }

        let (left_page, right_page) = tokio::join!(
            async {
                match next_left_token {
                    Some(token) => Some(
                        get_signatures_page(
                            client,
                            address,
                            context.clone(),
                            range,
                            SortOrder::Asc,
                            scout_limit,
                            Some(token),
                        )
                        .await,
                    ),
                    None => None,
                }
            },
            async {
                match next_right_token {
                    Some(token) => Some(
                        get_signatures_page(
                            client,
                            address,
                            context.clone(),
                            range,
                            SortOrder::Desc,
                            scout_limit,
                            Some(token),
                        )
                        .await,
                    ),
                    None => None,
                }
            }
        );

        if let Some(page_result) = left_page {
            let page = page_result?;
            if let Some(frontier) = page.data.last().cloned() {
                left_frontier = frontier;
            }
            left_token = page.pagination_token.clone();
            records.extend(page.data);
        }

        if let Some(page_result) = right_page {
            let page = page_result?;
            if let Some(frontier) = page.data.last().cloned() {
                right_frontier = frontier;
            }
            right_token = page.pagination_token.clone();
            records.extend(page.data);
        }
    }

    Ok(records)
}

fn plan_adaptive_signature_subranges(
    range: SlotRange,
    first_page: &GtfaPage<SignatureRecord>,
    scout_limit: usize,
) -> Option<Vec<SlotRange>> {
    let first = first_page.data.first()?;
    let last = first_page.data.last()?;
    if last.slot >= range.end {
        return None;
    }

    let observed_span = last.slot.saturating_sub(first.slot).saturating_add(1);
    let remaining_range = SlotRange {
        start: last.slot,
        end: range.end,
    };
    let remaining_span = remaining_range.span();

    if observed_span == 0 || remaining_span <= 1 {
        return None;
    }

    if observed_span < 8 && remaining_span > observed_span.saturating_mul(64) {
        return None;
    }

    let density = first_page.data.len() as f64 / observed_span as f64;
    if density <= 0.0 {
        return None;
    }

    let mut target_span = ((scout_limit as f64 / density) / DENSITY_SAFETY_FACTOR).ceil() as u64;
    target_span = target_span.max(1);

    let estimated_windows = remaining_span.div_ceil(target_span);
    if estimated_windows < 2 {
        return None;
    }

    if estimated_windows > MAX_ADAPTIVE_WINDOWS {
        target_span = remaining_span.div_ceil(MAX_ADAPTIVE_WINDOWS);
    }

    let mut subranges = Vec::new();
    let mut start = remaining_range.start;
    while start <= remaining_range.end {
        let end = start
            .saturating_add(target_span.saturating_sub(1))
            .min(remaining_range.end);
        subranges.push(SlotRange { start, end });
        if end == u64::MAX {
            break;
        }
        start = end.saturating_add(1);
    }

    if subranges.len() < 2 {
        None
    } else {
        Some(subranges)
    }
}

async fn get_signatures_page(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    range: SlotRange,
    sort_order: SortOrder,
    limit: usize,
    pagination_token: Option<String>,
) -> Result<GtfaPage<SignatureRecord>, ScanError> {
    let _permit = context
        .semaphore
        .acquire_owned()
        .await
        .map_err(|_| ScanError::SemaphoreClosed)?;
    context
        .stats
        .signature_requests
        .fetch_add(1, AtomicOrdering::Relaxed);

    let config = GetTransactionsConfig::signatures(
        sort_order,
        limit,
        Some(Filters::slot_range(range.start, range.end)),
    )
    .with_pagination_token(pagination_token);
    Ok(client.get_signatures(address, config).await?)
}

async fn get_full_page(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    range: SlotRange,
    sort_order: SortOrder,
    limit: usize,
    pagination_token: Option<String>,
) -> Result<GtfaPage<FullTransactionRecord>, ScanError> {
    get_full_page_with_filters(
        client,
        address,
        context,
        sort_order,
        limit,
        Some(Filters::slot_range(range.start, range.end)),
        pagination_token,
    )
    .await
}

async fn get_full_page_unfiltered(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    sort_order: SortOrder,
    limit: usize,
    pagination_token: Option<String>,
) -> Result<GtfaPage<FullTransactionRecord>, ScanError> {
    get_full_page_with_filters(
        client,
        address,
        context,
        sort_order,
        limit,
        None,
        pagination_token,
    )
    .await
}

async fn get_full_page_with_filters(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    context: ScanContext,
    sort_order: SortOrder,
    limit: usize,
    filters: Option<Filters>,
    pagination_token: Option<String>,
) -> Result<GtfaPage<FullTransactionRecord>, ScanError> {
    let _permit = context
        .semaphore
        .acquire_owned()
        .await
        .map_err(|_| ScanError::SemaphoreClosed)?;
    context
        .stats
        .full_requests
        .fetch_add(1, AtomicOrdering::Relaxed);

    let config = GetTransactionsConfig::full(sort_order, limit, filters)
        .with_pagination_token(pagination_token);
    Ok(client.get_full_transactions(address, config).await?)
}

async fn fetch_full_job(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    job: FullFetchJob,
    context: ScanContext,
    full_limit: usize,
) -> Result<Vec<FullTransactionRecord>, ScanError> {
    let range = SlotRange {
        start: job.start,
        end: job.end,
    };
    let first_page = get_full_page(
        client,
        address,
        context.clone(),
        range,
        job.sort_order,
        full_limit,
        job.pagination_token.clone(),
    )
    .await?;

    if first_page.data.len() >= job.expected_count || first_page.pagination_token.is_none() {
        return Ok(trim_full_records(first_page.data, job.expected_count));
    }

    paginate_full_range(client, address, &job, context, full_limit, first_page).await
}

async fn paginate_full_range(
    client: &(impl TransactionsSource + ?Sized),
    address: &str,
    job: &FullFetchJob,
    context: ScanContext,
    full_limit: usize,
    first_page: GtfaPage<FullTransactionRecord>,
) -> Result<Vec<FullTransactionRecord>, ScanError> {
    let range = SlotRange {
        start: job.start,
        end: job.end,
    };
    let mut records = first_page.data;
    let mut pagination_token = first_page.pagination_token;

    while records.len() < job.expected_count {
        let Some(token) = pagination_token else {
            break;
        };
        let next_page = get_full_page(
            client,
            address,
            context.clone(),
            range,
            job.sort_order,
            full_limit,
            Some(token),
        )
        .await?;
        pagination_token = next_page.pagination_token.clone();
        records.extend(next_page.data);
    }

    Ok(trim_full_records(records, job.expected_count))
}

fn sort_and_dedup_signatures(signatures: &mut Vec<SignatureRecord>) {
    signatures.sort_by(signature_order);
    signatures.dedup_by(|left, right| same_signature_record(left, right));
}

fn sort_and_dedup_full_transactions(transactions: &mut Vec<FullTransactionRecord>) {
    transactions.sort_by(full_record_order);
    transactions.dedup_by(|left, right| {
        left.primary_signature() == right.primary_signature()
            && left.slot == right.slot
            && left.transaction_index == right.transaction_index
    });
}

fn signature_order(left: &SignatureRecord, right: &SignatureRecord) -> Ordering {
    match left.slot.cmp(&right.slot) {
        Ordering::Equal => match left.transaction_index.cmp(&right.transaction_index) {
            Ordering::Equal => left.signature.cmp(&right.signature),
            other => other,
        },
        other => other,
    }
}

fn full_record_order(left: &FullTransactionRecord, right: &FullTransactionRecord) -> Ordering {
    match left.slot.cmp(&right.slot) {
        Ordering::Equal => match left.transaction_index.cmp(&right.transaction_index) {
            Ordering::Equal => left
                .primary_signature()
                .unwrap_or_default()
                .cmp(right.primary_signature().unwrap_or_default()),
            other => other,
        },
        other => other,
    }
}

fn compare_full_record_to_signature_record(
    record: &FullTransactionRecord,
    boundary: &SignatureRecord,
) -> Ordering {
    match record.slot.cmp(&boundary.slot) {
        Ordering::Equal => match record.transaction_index.cmp(&boundary.transaction_index) {
            Ordering::Equal => record
                .primary_signature()
                .unwrap_or_default()
                .cmp(boundary.signature.as_str()),
            other => other,
        },
        other => other,
    }
}

fn same_signature_record(left: &SignatureRecord, right: &SignatureRecord) -> bool {
    if !left.signature.is_empty() && !right.signature.is_empty() {
        return left.signature == right.signature;
    }

    left.slot == right.slot && left.transaction_index == right.transaction_index
}

fn build_full_fetch_jobs(signatures: &[SignatureRecord], full_limit: usize) -> Vec<FullFetchJob> {
    build_full_fetch_jobs_for_order(signatures, full_limit, SortOrder::Asc, None)
}

fn build_full_fetch_jobs_for_order(
    signatures: &[SignatureRecord],
    full_limit: usize,
    sort_order: SortOrder,
    previous_boundary: Option<&SignatureRecord>,
) -> Vec<FullFetchJob> {
    let page_limit = full_limit.max(1);
    let mut jobs = Vec::new();
    let mut boundary = previous_boundary.cloned();

    for chunk in signatures.chunks(page_limit) {
        let first = &chunk[0];
        let last = chunk.last().expect("chunk is non-empty");
        let (start, end) = match sort_order {
            SortOrder::Asc => (
                boundary.as_ref().map_or(first.slot, |record| record.slot),
                last.slot,
            ),
            SortOrder::Desc => (
                last.slot,
                boundary.as_ref().map_or(first.slot, |record| record.slot),
            ),
        };
        jobs.push(FullFetchJob {
            start,
            end,
            expected_count: chunk.len(),
            pagination_token: boundary.as_ref().map(signature_pagination_token),
            sort_order,
        });
        boundary = Some(last.clone());
    }

    jobs
}

fn trim_full_records(
    mut records: Vec<FullTransactionRecord>,
    expected_count: usize,
) -> Vec<FullTransactionRecord> {
    if records.len() > expected_count {
        records.truncate(expected_count);
    }
    records
}

fn signature_pagination_token(record: &SignatureRecord) -> String {
    format!("{}:{}", record.slot, record.transaction_index)
}

fn signature_record_from_full_transaction(record: &FullTransactionRecord) -> SignatureRecord {
    SignatureRecord {
        signature: record.primary_signature().unwrap_or_default().to_owned(),
        slot: record.slot,
        transaction_index: record.transaction_index,
        err: record.meta.as_ref().and_then(|meta| meta.err.clone()),
        memo: None,
        block_time: record.block_time,
        confirmation_status: Some("finalized".to_owned()),
    }
}

fn build_timeline_entries(
    address: &str,
    transactions: Vec<FullTransactionRecord>,
) -> Result<Vec<TimelineEntry>, ScanError> {
    transactions
        .into_iter()
        .map(|transaction| timeline_entry_for_transaction(address, transaction))
        .collect()
}

fn timeline_entry_for_transaction(
    address: &str,
    transaction: FullTransactionRecord,
) -> Result<TimelineEntry, ScanError> {
    let signature = transaction
        .primary_signature()
        .unwrap_or_default()
        .to_owned();
    let meta = transaction.meta.ok_or_else(|| ScanError::MissingMeta {
        signature: signature.clone(),
    })?;

    let mut account_keys: Vec<&str> = transaction
        .transaction
        .message
        .account_keys
        .iter()
        .map(String::as_str)
        .collect();

    if let Some(loaded_addresses) = meta.loaded_addresses.as_ref() {
        account_keys.extend(loaded_addresses.writable.iter().map(String::as_str));
        account_keys.extend(loaded_addresses.readonly.iter().map(String::as_str));
    }

    let Some(target_index) = account_keys.iter().position(|account| *account == address) else {
        return Err(ScanError::MissingAddress {
            address: address.to_owned(),
            signature,
        });
    };

    let Some(pre_balance) = meta.pre_balances.get(target_index).copied() else {
        return Err(ScanError::MissingBalanceIndex {
            address: address.to_owned(),
            signature: signature.clone(),
        });
    };
    let Some(post_balance) = meta.post_balances.get(target_index).copied() else {
        return Err(ScanError::MissingBalanceIndex {
            address: address.to_owned(),
            signature: signature.clone(),
        });
    };

    Ok(TimelineEntry {
        signature,
        slot: transaction.slot,
        transaction_index: transaction.transaction_index,
        block_time: transaction.block_time,
        succeeded: meta.err.is_none(),
        pre_balance_lamports: pre_balance,
        post_balance_lamports: post_balance,
        delta_lamports: post_balance as i64 - pre_balance as i64,
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{
        FullFetchJob, RuntimeScanConfig, ScanStrategy, build_full_fetch_jobs,
        build_timeline_entries, reconstruct_sol_balance_timeline, sort_and_dedup_signatures,
    };
    use crate::client::{FixtureClient, FixtureData};
    use crate::model::{
        EncodedTransaction, FullTransactionRecord, LoadedAddresses, SignatureRecord, SortOrder,
        TransactionMessage, TransactionMeta,
    };

    fn signature(slot: u64, transaction_index: u32, signature: &str) -> SignatureRecord {
        SignatureRecord {
            signature: signature.to_owned(),
            slot,
            transaction_index,
            err: None,
            memo: None,
            block_time: Some(slot as i64),
            confirmation_status: Some("finalized".to_owned()),
        }
    }

    #[test]
    fn build_jobs_groups_complete_slots_under_the_limit() {
        let signatures = vec![
            signature(10, 0, "a"),
            signature(10, 1, "b"),
            signature(11, 0, "c"),
            signature(20, 0, "d"),
        ];

        let jobs = build_full_fetch_jobs(&signatures, 3);
        assert_eq!(
            jobs,
            vec![
                FullFetchJob {
                    start: 10,
                    end: 11,
                    expected_count: 3,
                    pagination_token: None,
                    sort_order: SortOrder::Asc,
                },
                FullFetchJob {
                    start: 11,
                    end: 20,
                    expected_count: 1,
                    pagination_token: Some("11:0".to_owned()),
                    sort_order: SortOrder::Asc,
                },
            ]
        );
    }

    #[test]
    fn build_jobs_chunk_by_exact_signature_windows() {
        let mut signatures = Vec::new();
        for index in 0..101 {
            signatures.push(signature(42, index, &format!("dense-{index}")));
        }
        signatures.push(signature(100, 0, "tail"));

        let jobs = build_full_fetch_jobs(&signatures, 100);
        assert_eq!(
            jobs,
            vec![
                FullFetchJob {
                    start: 42,
                    end: 42,
                    expected_count: 100,
                    pagination_token: None,
                    sort_order: SortOrder::Asc,
                },
                FullFetchJob {
                    start: 42,
                    end: 100,
                    expected_count: 2,
                    pagination_token: Some("42:99".to_owned()),
                    sort_order: SortOrder::Asc,
                },
            ]
        );
    }

    #[test]
    fn sort_and_dedup_signatures_orders_by_slot_then_index() {
        let mut signatures = vec![
            signature(11, 0, "c"),
            signature(10, 1, "b"),
            signature(10, 0, "a"),
            signature(10, 0, "a"),
        ];

        sort_and_dedup_signatures(&mut signatures);

        assert_eq!(
            signatures,
            vec![
                signature(10, 0, "a"),
                signature(10, 1, "b"),
                signature(11, 0, "c"),
            ]
        );
    }

    #[test]
    fn build_timeline_uses_static_account_keys() {
        let transactions = vec![FullTransactionRecord {
            slot: 1,
            transaction_index: 0,
            block_time: Some(1),
            transaction: EncodedTransaction {
                signatures: vec!["sig-1".to_owned()],
                message: TransactionMessage {
                    account_keys: vec!["target".to_owned(), "other".to_owned()],
                },
            },
            meta: Some(TransactionMeta {
                err: None,
                fee: 5000,
                pre_balances: vec![10, 5],
                post_balances: vec![7, 8],
                loaded_addresses: None,
            }),
        }];

        let timeline = build_timeline_entries("target", transactions).expect("timeline");
        assert_eq!(timeline[0].delta_lamports, -3);
        assert_eq!(timeline[0].pre_balance_lamports, 10);
        assert_eq!(timeline[0].post_balance_lamports, 7);
    }

    #[test]
    fn build_timeline_uses_loaded_addresses_for_versioned_transactions() {
        let transactions = vec![FullTransactionRecord {
            slot: 2,
            transaction_index: 9,
            block_time: Some(2),
            transaction: EncodedTransaction {
                signatures: vec!["sig-2".to_owned()],
                message: TransactionMessage {
                    account_keys: vec!["payer".to_owned(), "program".to_owned()],
                },
            },
            meta: Some(TransactionMeta {
                err: None,
                fee: 5000,
                pre_balances: vec![1, 0, 40],
                post_balances: vec![1, 0, 55],
                loaded_addresses: Some(LoadedAddresses {
                    writable: vec!["target".to_owned()],
                    readonly: vec![],
                }),
            }),
        }];

        let timeline = build_timeline_entries("target", transactions).expect("timeline");
        assert_eq!(timeline[0].delta_lamports, 15);
        assert_eq!(timeline[0].transaction_index, 9);
    }

    #[tokio::test]
    async fn fixture_client_replays_the_full_scan_offline() {
        let fixture = FixtureData {
            address: Some("target".to_owned()),
            transactions: vec![
                FullTransactionRecord {
                    slot: 10,
                    transaction_index: 0,
                    block_time: Some(10),
                    transaction: EncodedTransaction {
                        signatures: vec!["sig-10-0".to_owned()],
                        message: TransactionMessage {
                            account_keys: vec!["target".to_owned(), "funder".to_owned()],
                        },
                    },
                    meta: Some(TransactionMeta {
                        err: None,
                        fee: 5000,
                        pre_balances: vec![0, 10_000_000_000],
                        post_balances: vec![1_000_000_000, 8_999_995_000],
                        loaded_addresses: None,
                    }),
                },
                FullTransactionRecord {
                    slot: 50,
                    transaction_index: 2,
                    block_time: Some(50),
                    transaction: EncodedTransaction {
                        signatures: vec!["sig-50-2".to_owned()],
                        message: TransactionMessage {
                            account_keys: vec!["target".to_owned(), "program".to_owned()],
                        },
                    },
                    meta: Some(TransactionMeta {
                        err: Some(serde_json::json!({"InstructionError":[0,"Custom"]})),
                        fee: 5000,
                        pre_balances: vec![1_000_000_000, 1],
                        post_balances: vec![999_995_000, 1],
                        loaded_addresses: None,
                    }),
                },
                FullTransactionRecord {
                    slot: 50,
                    transaction_index: 3,
                    block_time: Some(50),
                    transaction: EncodedTransaction {
                        signatures: vec!["sig-50-3".to_owned()],
                        message: TransactionMessage {
                            account_keys: vec!["target".to_owned(), "recipient".to_owned()],
                        },
                    },
                    meta: Some(TransactionMeta {
                        err: None,
                        fee: 5000,
                        pre_balances: vec![999_995_000, 0],
                        post_balances: vec![799_990_000, 200_000_000],
                        loaded_addresses: None,
                    }),
                },
                FullTransactionRecord {
                    slot: 120,
                    transaction_index: 7,
                    block_time: Some(120),
                    transaction: EncodedTransaction {
                        signatures: vec!["sig-120-7".to_owned()],
                        message: TransactionMessage {
                            account_keys: vec!["payer".to_owned(), "program".to_owned()],
                        },
                    },
                    meta: Some(TransactionMeta {
                        err: None,
                        fee: 5000,
                        pre_balances: vec![1, 1, 799_990_000],
                        post_balances: vec![1, 1, 899_990_000],
                        loaded_addresses: Some(LoadedAddresses {
                            writable: vec!["target".to_owned()],
                            readonly: vec![],
                        }),
                    }),
                },
            ],
        };

        let client = FixtureClient::from_fixture(fixture).expect("fixture client");
        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 4,
                scout_limit: 2,
                full_limit: 2,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        let signatures = output
            .entries
            .iter()
            .map(|entry| entry.signature.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            signatures,
            vec!["sig-10-0", "sig-50-2", "sig-50-3", "sig-120-7"]
        );
        assert_eq!(output.initial_balance_lamports, Some(0));
        assert_eq!(output.final_balance_lamports, Some(899_990_000));
        assert_eq!(output.entries[1].succeeded, false);
        assert!(output.stats.signature_requests >= 2);
        assert!(output.stats.full_requests >= 2);
    }

    #[tokio::test]
    async fn small_wallet_finishes_with_boundary_signature_probes() {
        let fixture = FixtureData {
            address: Some("target".to_owned()),
            transactions: vec![
                full_tx("target", "sig-10", 10, 0, 0, 10),
                full_tx("target", "sig-11", 11, 0, 10, 20),
                full_tx("target", "sig-12", 12, 0, 20, 30),
            ],
        };

        let client = FixtureClient::from_fixture(fixture).expect("fixture client");
        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 4,
                scout_limit: 1000,
                full_limit: 2,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 3);
        assert_eq!(output.stats.signature_requests, 2);
        assert_eq!(output.stats.max_discovery_depth, 0);
        assert_eq!(output.strategy, ScanStrategy::BoundaryOnly);
    }

    #[tokio::test]
    async fn recursive_discovery_increases_depth_for_sparse_histories() {
        let fixture = FixtureData {
            address: Some("target".to_owned()),
            transactions: vec![
                full_tx("target", "sig-10", 10, 0, 0, 100),
                full_tx("target", "sig-100", 100, 0, 100, 200),
                full_tx("target", "sig-200", 200, 0, 200, 400),
            ],
        };

        let client = FixtureClient::from_fixture(fixture).expect("fixture client");
        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 4,
                scout_limit: 1,
                full_limit: 2,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 3);
        assert!(output.stats.max_discovery_depth > 0);
        assert_eq!(output.strategy, ScanStrategy::SparseRecursive);
    }

    #[tokio::test]
    async fn dense_wallet_prefers_bidirectional_boundary_sweep() {
        let transactions = (0..2_500u32)
            .map(|index| {
                full_tx(
                    "target",
                    &format!("sig-{index}"),
                    u64::from(index) + 1,
                    0,
                    u64::from(index),
                    u64::from(index) + 1,
                )
            })
            .collect::<Vec<_>>();

        let client = FixtureClient::from_fixture(FixtureData {
            address: Some("target".to_owned()),
            transactions,
        })
        .expect("fixture client");

        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 16,
                scout_limit: 1000,
                full_limit: 100,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 2_500);
        assert_eq!(output.stats.signature_requests, 4);
        assert_eq!(output.stats.max_discovery_depth, 0);
        assert_eq!(output.strategy, ScanStrategy::DensePincer);
    }

    #[tokio::test]
    async fn large_dense_wallet_prefers_seeded_full_fanout_with_headroom() {
        let transactions = (0..20_000u32)
            .map(|index| {
                full_tx(
                    "target",
                    &format!("sig-{index}"),
                    u64::from(index) + 1,
                    0,
                    u64::from(index),
                    u64::from(index) + 1,
                )
            })
            .collect::<Vec<_>>();

        let client = FixtureClient::from_fixture(FixtureData {
            address: Some("target".to_owned()),
            transactions,
        })
        .expect("fixture client");

        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 64,
                scout_limit: 1000,
                full_limit: 100,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 20_000);
        assert_eq!(output.stats.signature_requests, 2);
        assert_eq!(output.stats.max_discovery_depth, 0);
        assert!(output.stats.full_requests > 200);
        assert_eq!(output.strategy, ScanStrategy::SeededFullFanout);
    }

    #[tokio::test]
    async fn seeded_full_fanout_preserves_multi_transaction_slot_boundaries() {
        let mut transactions = Vec::new();
        let mut balance = 0u64;
        for slot in 0..10_000u64 {
            for transaction_index in 0..2u32 {
                let next_balance = balance + 1;
                transactions.push(full_tx(
                    "target",
                    &format!("sig-{slot}-{transaction_index}"),
                    slot + 1,
                    transaction_index,
                    balance,
                    next_balance,
                ));
                balance = next_balance;
            }
        }

        let client = FixtureClient::from_fixture(FixtureData {
            address: Some("target".to_owned()),
            transactions,
        })
        .expect("fixture client");

        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 64,
                scout_limit: 1000,
                full_limit: 100,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 20_000);
        assert_eq!(output.initial_balance_lamports, Some(0));
        assert_eq!(output.final_balance_lamports, Some(20_000));
        assert_eq!(output.stats.signature_requests, 2);
        assert_eq!(output.stats.max_discovery_depth, 0);
        assert_eq!(output.strategy, ScanStrategy::SeededFullFanout);
    }

    #[tokio::test]
    async fn large_dense_wallet_keeps_pincer_when_rpc_cap_is_low() {
        let transactions = (0..20_000u32)
            .map(|index| {
                full_tx(
                    "target",
                    &format!("sig-{index}"),
                    u64::from(index) + 1,
                    0,
                    u64::from(index),
                    u64::from(index) + 1,
                )
            })
            .collect::<Vec<_>>();

        let client = FixtureClient::from_fixture(FixtureData {
            address: Some("target".to_owned()),
            transactions,
        })
        .expect("fixture client");

        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 64,
                scout_limit: 1000,
                full_limit: 100,
                rpc_rps: Some(100),
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 20_000);
        assert_eq!(output.stats.max_discovery_depth, 0);
        assert_eq!(output.stats.signature_requests, 24);
        assert_eq!(output.strategy, ScanStrategy::DensePincer);
    }

    #[tokio::test]
    async fn dense_single_slot_history_paginates_full_fetches() {
        let transactions = (0..5)
            .map(|index| {
                full_tx(
                    "target",
                    &format!("sig-77-{index}"),
                    77,
                    index,
                    index as u64,
                    index as u64 + 1,
                )
            })
            .collect::<Vec<_>>();

        let client = FixtureClient::from_fixture(FixtureData {
            address: Some("target".to_owned()),
            transactions,
        })
        .expect("fixture client");

        let output = reconstruct_sol_balance_timeline(
            &client,
            "target",
            RuntimeScanConfig {
                concurrency: 4,
                scout_limit: 2,
                full_limit: 2,
                rpc_rps: None,
            },
        )
        .await
        .expect("scan output");

        assert_eq!(output.entries.len(), 5);
        assert!(output.stats.full_requests >= 3);
        assert_eq!(
            output.entries.first().map(|entry| entry.transaction_index),
            Some(0)
        );
        assert_eq!(
            output.entries.last().map(|entry| entry.transaction_index),
            Some(4)
        );
    }

    fn full_tx(
        address: &str,
        signature: &str,
        slot: u64,
        transaction_index: u32,
        pre_balance: u64,
        post_balance: u64,
    ) -> FullTransactionRecord {
        FullTransactionRecord {
            slot,
            transaction_index,
            block_time: Some(slot as i64),
            transaction: EncodedTransaction {
                signatures: vec![signature.to_owned()],
                message: TransactionMessage {
                    account_keys: vec![address.to_owned(), "counterparty".to_owned()],
                },
            },
            meta: Some(TransactionMeta {
                err: None,
                fee: 5000,
                pre_balances: vec![pre_balance, 0],
                post_balances: vec![post_balance, 0],
                loaded_addresses: None,
            }),
        }
    }
}
