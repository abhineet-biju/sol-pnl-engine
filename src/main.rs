use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use sol_balance_runtime::{
    FixtureClient, HeliusClient, HeliusClientTimingStats, RuntimeScanConfig, ScanError,
    StrategyPreference, TransactionsSource, reconstruct_sol_balance_timeline,
};

#[derive(Debug, Parser)]
#[command(
    name = "sol-balance-runtime",
    about = "Reconstructs a wallet's native SOL balance-over-time using Helius getTransactionsForAddress."
)]
struct Cli {
    #[arg(
        long,
        env = "HELIUS_RPC_URL",
        conflicts_with_all = ["fixture", "api_key"],
        help = "Live Helius RPC URL, for example https://mainnet.helius-rpc.com/?api-key=..."
    )]
    rpc_url: Option<String>,
    #[arg(
        long,
        env = "HELIUS_API_KEY",
        conflicts_with = "fixture",
        help = "Helius API key. Uses the default beta RPC URL automatically, with mainnet fallback if beta fails."
    )]
    api_key: Option<String>,
    #[arg(
        long,
        conflicts_with_all = ["rpc_url", "api_key"],
        help = "Path to a local fixture JSON file for offline replay"
    )]
    fixture: Option<PathBuf>,
    #[arg(long, help = "Target Solana address to scan")]
    address: Option<String>,
    #[arg(
        long,
        help = "Write the JSON result to this file. Defaults to outputs/<address>.json"
    )]
    output: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        help = "Optional Helius plan preset for client-side request pacing"
    )]
    plan: Option<HeliusPlan>,
    #[arg(
        long,
        help = "Optional client-side requests-per-second cap. Overrides --plan if both are set."
    )]
    rpc_rps: Option<u32>,
    #[arg(
        long,
        help = "Maximum number of concurrent requests/jobs. Defaults to 32, or the configured request cap when using --plan/--rpc-rps."
    )]
    concurrency: Option<usize>,
    #[arg(
        long,
        default_value_t = 1000,
        help = "Page size for signature discovery requests (1..=1000)"
    )]
    scout_limit: usize,
    #[arg(
        long,
        default_value_t = 100,
        help = "Page size for full transaction requests (1..=100)"
    )]
    full_limit: usize,
    #[arg(
        long,
        value_enum,
        default_value_t = CliStrategyPreference::Auto,
        help = "Optional strategy preference for benchmarking and tuning. Auto keeps the normal classifier."
    )]
    strategy_preference: CliStrategyPreference,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum HeliusPlan {
    Developer,
    Business,
    Professional,
    Enterprise,
}

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum CliStrategyPreference {
    #[default]
    Auto,
    SeededFullFanout,
    DensePincer,
    DenseParallelRange,
    SparseRecursive,
}

const DEFAULT_CONCURRENCY: usize = 32;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Cli {
        rpc_url,
        api_key,
        fixture,
        address,
        output,
        plan,
        rpc_rps,
        concurrency,
        scout_limit,
        full_limit,
        strategy_preference,
    } = Cli::parse();

    let effective_rpc_rps = resolve_rpc_rps(plan, rpc_rps);

    let config = RuntimeScanConfig {
        concurrency: resolve_concurrency(concurrency, effective_rpc_rps),
        scout_limit: scout_limit.clamp(1, 1000),
        full_limit: full_limit.clamp(1, 100),
        rpc_rps: effective_rpc_rps,
        strategy_preference: map_strategy_preference(strategy_preference),
    };

    let scan_started_at = Instant::now();
    let (scan_output, live_timing_stats, source_kind, endpoint_kind) =
        match (rpc_url, api_key, fixture) {
            (Some(rpc_url), None, None) => {
                let address = address.ok_or("`--address` is required with `--rpc-url`")?;
                emit_helius_address_warning(&address);
                let client = HeliusClient::new_with_rps(rpc_url, effective_rpc_rps)?;
                let output = run_source(&client, &address, config.clone()).await?;
                (
                    output,
                    Some(client.timing_stats()),
                    "live",
                    Some("custom rpc url"),
                )
            }
            (None, Some(api_key), None) => {
                let address = address.ok_or("`--address` is required with `--api-key`")?;
                emit_helius_address_warning(&address);
                let beta_client = HeliusClient::new_with_rps(
                    build_default_helius_rpc_url(&api_key),
                    effective_rpc_rps,
                )?;

                match run_source(&beta_client, &address, config.clone()).await {
                    Ok(output) => (
                        output,
                        Some(beta_client.timing_stats()),
                        "live",
                        Some("helius beta"),
                    ),
                    Err(ScanError::Client(error)) => {
                        eprintln!(
                            "warning: beta endpoint failed ({error}); retrying on Helius mainnet"
                        );

                        let mainnet_client = HeliusClient::new_with_rps(
                            build_mainnet_helius_rpc_url(&api_key),
                            effective_rpc_rps,
                        )?;
                        let output = run_source(&mainnet_client, &address, config.clone()).await?;
                        (
                            output,
                            Some(mainnet_client.timing_stats()),
                            "live",
                            Some("helius mainnet fallback"),
                        )
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            (None, None, Some(fixture_path)) => {
                let client = FixtureClient::from_path(fixture_path)?;
                let address = match address {
                    Some(address) => address,
                    None => client
                        .address()
                        .map(str::to_owned)
                        .ok_or("`--address` is required unless the fixture file contains one")?,
                };
                emit_helius_address_warning(&address);
                let output = run_source(&client, &address, config.clone()).await?;
                (output, None, "fixture", None)
            }
            _ => {
                return Err(
                    "choose exactly one input source: `--rpc-url`, `--api-key`, or `--fixture`"
                        .into(),
                );
            }
        };
    let scan_elapsed = scan_started_at.elapsed();

    let rendered = serde_json::to_string_pretty(&scan_output)?;

    let output_path = output.unwrap_or_else(|| default_output_path(&scan_output.address));
    write_output_file(&output_path, &rendered)?;
    emit_scan_summary(
        &scan_output,
        &output_path,
        scan_elapsed,
        source_kind,
        endpoint_kind,
        config,
        effective_rpc_rps,
        live_timing_stats,
    );

    Ok(())
}

async fn run_source(
    source: &(impl TransactionsSource + ?Sized),
    address: &str,
    config: RuntimeScanConfig,
) -> Result<sol_balance_runtime::TimelineOutput, sol_balance_runtime::ScanError> {
    reconstruct_sol_balance_timeline(source, address, config).await
}

fn emit_scan_summary(
    output: &sol_balance_runtime::TimelineOutput,
    output_path: &Path,
    elapsed: Duration,
    source_kind: &str,
    endpoint_kind: Option<&str>,
    config: RuntimeScanConfig,
    effective_rpc_rps: Option<u32>,
    live_timing_stats: Option<HeliusClientTimingStats>,
) {
    eprintln!("scan complete");
    eprintln!("source: {source_kind}");
    if let Some(endpoint_kind) = endpoint_kind {
        eprintln!("endpoint: {endpoint_kind}");
    }
    eprintln!("strategy: {}", output.strategy.as_str());
    eprintln!("address: {}", output.address);
    eprintln!("output: {}", output_path.display());
    eprintln!("wall time: {}", format_duration(elapsed));
    eprintln!(
        "entries: {} | signature requests: {} | full requests: {}",
        output.entries.len(),
        output.stats.signature_requests,
        output.stats.full_requests
    );
    eprintln!(
        "concurrency: {} | rpc pacing: {}",
        config.concurrency,
        effective_rpc_rps
            .map(|value| format!("{value} req/s"))
            .unwrap_or_else(|| "unbounded".to_owned())
    );

    if let Some(timing) = live_timing_stats {
        eprintln!(
            "http attempts: {} | retries: {} | cumulative http time: {} | avg per attempt: {}",
            timing.attempts,
            timing.retries,
            format_duration(Duration::from_millis(timing.total_http_time_ms)),
            timing
                .avg_http_time_ms
                .map(format_millis)
                .unwrap_or_else(|| "n/a".to_owned())
        );
        eprintln!(
            "first completed rpc attempt: {} (includes initial connection/TLS setup on a cold client)",
            timing
                .first_completed_attempt_ms
                .map(|millis| format_millis(millis as f64))
                .unwrap_or_else(|| "n/a".to_owned())
        );
    }
}

fn map_strategy_preference(preference: CliStrategyPreference) -> StrategyPreference {
    match preference {
        CliStrategyPreference::Auto => StrategyPreference::Auto,
        CliStrategyPreference::SeededFullFanout => StrategyPreference::SeededFullFanout,
        CliStrategyPreference::DensePincer => StrategyPreference::DensePincer,
        CliStrategyPreference::DenseParallelRange => StrategyPreference::DenseParallelRange,
        CliStrategyPreference::SparseRecursive => StrategyPreference::SparseRecursive,
    }
}

fn format_duration(duration: Duration) -> String {
    format_millis(duration.as_secs_f64() * 1_000.0)
}

fn format_millis(millis: f64) -> String {
    if millis >= 1_000.0 {
        format!("{:.2}s", millis / 1_000.0)
    } else {
        format!("{millis:.0}ms")
    }
}

fn build_default_helius_rpc_url(api_key: &str) -> String {
    build_beta_helius_rpc_url(api_key)
}

fn build_beta_helius_rpc_url(api_key: &str) -> String {
    format!("https://beta.helius-rpc.com/?api-key={api_key}")
}

fn build_mainnet_helius_rpc_url(api_key: &str) -> String {
    format!("https://mainnet.helius-rpc.com/?api-key={api_key}")
}

fn resolve_rpc_rps(plan: Option<HeliusPlan>, rpc_rps: Option<u32>) -> Option<u32> {
    rpc_rps.or_else(|| plan.and_then(HeliusPlan::default_rpc_rps))
}

fn resolve_concurrency(concurrency: Option<usize>, rpc_rps: Option<u32>) -> usize {
    match concurrency {
        Some(value) => value.max(1),
        None => rpc_rps
            .map(|value| DEFAULT_CONCURRENCY.max(value as usize))
            .unwrap_or(DEFAULT_CONCURRENCY),
    }
}

impl HeliusPlan {
    fn default_rpc_rps(self) -> Option<u32> {
        match self {
            Self::Developer => Some(50),
            Self::Business => Some(200),
            Self::Professional => Some(500),
            Self::Enterprise => None,
        }
    }
}

fn default_output_path(address: &str) -> PathBuf {
    PathBuf::from("outputs").join(format!("{}.json", sanitize_output_component(address)))
}

fn sanitize_output_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

fn write_output_file(path: &Path, contents: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents)
}

fn emit_helius_address_warning(address: &str) {
    if let Some(message) = helius_address_warning(address) {
        eprintln!("warning: {message}");
    }
}

fn helius_address_warning(address: &str) -> Option<&'static str> {
    if ROUTED_TO_OLD_ARCHIVAL.contains(&address) {
        return Some(
            "this address is documented by Helius as routed to old archival, so latency and behavior may differ from normal indexed wallets",
        );
    }

    if SLOT_SCAN_FALLBACK.contains(&address) {
        return Some(
            "this address is documented by Helius as slot-scan fallback and not indexed, so performance may be much worse than normal wallets",
        );
    }

    if RESERVED_EMPTY.contains(&address) {
        return Some(
            "this address is documented by Helius as reserved/unindexed and may return empty results",
        );
    }

    None
}

const ROUTED_TO_OLD_ARCHIVAL: &[&str] = &[
    "Stake11111111111111111111111111111111111111",
    "StakeConfig11111111111111111111111111111111",
    "Sysvar1111111111111111111111111111111111111",
    "AddressLookupTab1e1111111111111111111111111",
    "BPFLoaderUpgradeab1e11111111111111111111111",
];

const SLOT_SCAN_FALLBACK: &[&str] = &[
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
    "Vote111111111111111111111111111111111111111",
];

const RESERVED_EMPTY: &[&str] = &[
    "BPFLoader1111111111111111111111111111111111",
    "BPFLoader2111111111111111111111111111111111",
    "Config1111111111111111111111111111111111111",
    "Ed25519SigVerify111111111111111111111111111",
    "Feature111111111111111111111111111111111111",
    "KeccakSecp256k11111111111111111111111111111",
    "LoaderV411111111111111111111111111111111111",
    "NativeLoader1111111111111111111111111111111",
    "SysvarC1ock11111111111111111111111111111111",
    "SysvarEpochSchedu1e111111111111111111111111",
    "SysvarFees111111111111111111111111111111111",
    "Sysvar1nstructions1111111111111111111111111",
    "SysvarRecentB1ockHashes11111111111111111111",
    "SysvarRent111111111111111111111111111111111",
    "SysvarRewards111111111111111111111111111111",
    "SysvarS1otHashes111111111111111111111111111",
    "SysvarS1otHistory11111111111111111111111111",
    "SysvarStakeHistory1111111111111111111111111",
    "SysvarEpochRewards11111111111111111111111111",
    "SysvarLastRestartS1ot1111111111111111111111",
];

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        DEFAULT_CONCURRENCY, HeliusPlan, build_beta_helius_rpc_url, build_default_helius_rpc_url,
        build_mainnet_helius_rpc_url, default_output_path, resolve_concurrency, resolve_rpc_rps,
    };

    #[test]
    fn build_default_helius_rpc_url_prefers_beta_endpoint() {
        assert_eq!(
            build_default_helius_rpc_url("test-key"),
            "https://beta.helius-rpc.com/?api-key=test-key"
        );
    }

    #[test]
    fn build_beta_helius_rpc_url_uses_beta_endpoint() {
        assert_eq!(
            build_beta_helius_rpc_url("test-key"),
            "https://beta.helius-rpc.com/?api-key=test-key"
        );
    }

    #[test]
    fn build_mainnet_helius_rpc_url_uses_mainnet_endpoint() {
        assert_eq!(
            build_mainnet_helius_rpc_url("test-key"),
            "https://mainnet.helius-rpc.com/?api-key=test-key"
        );
    }

    #[test]
    fn default_output_path_uses_outputs_directory() {
        assert_eq!(
            default_output_path("wallet123"),
            PathBuf::from("outputs").join("wallet123.json")
        );
    }

    #[test]
    fn resolve_rpc_rps_uses_plan_defaults() {
        assert_eq!(resolve_rpc_rps(Some(HeliusPlan::Developer), None), Some(50));
        assert_eq!(resolve_rpc_rps(Some(HeliusPlan::Business), None), Some(200));
        assert_eq!(
            resolve_rpc_rps(Some(HeliusPlan::Professional), None),
            Some(500)
        );
        assert_eq!(resolve_rpc_rps(Some(HeliusPlan::Enterprise), None), None);
    }

    #[test]
    fn resolve_rpc_rps_prefers_explicit_override() {
        assert_eq!(
            resolve_rpc_rps(Some(HeliusPlan::Developer), Some(123)),
            Some(123)
        );
    }

    #[test]
    fn resolve_concurrency_defaults_to_base_without_rate_cap() {
        assert_eq!(resolve_concurrency(None, None), DEFAULT_CONCURRENCY);
    }

    #[test]
    fn resolve_concurrency_tracks_request_cap_when_higher() {
        assert_eq!(resolve_concurrency(None, Some(50)), 50);
        assert_eq!(resolve_concurrency(None, Some(200)), 200);
        assert_eq!(resolve_concurrency(None, Some(500)), 500);
    }

    #[test]
    fn resolve_concurrency_prefers_explicit_override() {
        assert_eq!(resolve_concurrency(Some(8), Some(500)), 8);
        assert_eq!(resolve_concurrency(Some(0), Some(500)), 1);
    }
}
