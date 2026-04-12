# sol-balance-runtime

Reconstructs a Solana wallet's **native SOL balance over time** using only Helius [`getTransactionsForAddress`](https://docs.helius.dev/solana-rpc-nodes/enhanced-methods/getTransactionsForAddress). No indexing, no database, just RPC.

Built for [Mert's latency challenge](https://x.com/mert/status/2042941421297050109?s=20): lowest average latency for SOL balance reconstruction across busy, sparse, and periodic wallets.

> **Note:** If you're on a rate-limited Helius plan (Developer, Business, Professional), see [With rate limiting](#with-rate-limiting) before running.

> **Latency default:** `--api-key` runs against Helius beta / Gatekeeper-style infrastructure by default for lower latency, and automatically falls back to the standard mainnet endpoint if beta is unavailable. Use `--rpc-url` if you want to force a specific endpoint.

```
cargo run -- --api-key YOUR_KEY --address 2W2sRxN4ioj5ZJkicCqgr97kzugAHrcYGRWbbbxkqQds
```

```
scan complete
source: live
endpoint: helius beta
strategy: dense_pincer
address: 2W2sRxN4ioj5ZJkicCqgr97kzugAHrcYGRWbbbxkqQds
wall time: 14.56s
entries: 6093 | signature requests: 8 | full requests: 61
concurrency: 32 | rpc pacing: unbounded
```

6,093 transactions discovered in **8 signature requests**, full history reconstructed in **14.56s**.

## Why it's fast

The algorithm exploits `getTransactionsForAddress`'s bidirectional sorting and slot-range filtering to parallelize everything it can.

| Wallet type | What happens | Signature requests |
|---|---|---|
| Small (<=1000 txs) | Two parallel boundary probes cover the entire history | **2** |
| Dense (very large, high-headroom) | Boundary probes plus validated seeded full-page fanout from multiple entry points | **2** |
| Dense (thousands of txs) | Bidirectional pincer sweep by default, with fallback dense fanout when seeded mode is not a fit | **~2 per 1000 txs** |
| Sparse (gaps between activity) | Recursive binary search over slot ranges with adaptive subrange splitting | **varies, but parallelized** |

**Key optimizations:**

- **Pipelined fetching**: for dense wallets, full-transaction fetches fire as soon as each signature page arrives, overlapping discovery and data retrieval. Phases 2 and 3 run concurrently, not sequentially.
- **Validated seeded full fanout**: for very large dense wallets with enough RPC headroom, the scanner validates whether synthetic `slot:txIndex` pagination behaves correctly on the current endpoint. If it does, the scanner fans out full-page requests from multiple seeded entry points and collapses discovery down to the two boundary signature probes.
- **Dense fanout when it helps**: for very large, clearly dense wallets with enough concurrency and RPC headroom, the scanner switches from a stepwise pincer walk to parallel range discovery. Smaller or rate-limited runs keep the lower-call-count pincer path.
- **Sparse-gap confirmation**: when a very large wallet initially looks sparse from the boundary pages alone, the scanner sends a midpoint confirmation probe before committing to recursive search. This prevents uniform high-volume histories from being misclassified as sparse.
- **Signatures-first discovery**: uses `transactionDetails: signatures` (lightweight) to map the history before requesting full transactions (heavy). This minimizes bandwidth and response times during the search phase.
- **Adaptive subrange planning**: for sparse wallets, the first page's density is used to estimate optimal slot-range windows. Up to 16 subranges are searched in parallel instead of walking the ledger sequentially.
- **Probe-and-narrow**: when splitting ranges, the algorithm sends single-record probes (`limit=1`) to find the tightest bounding slots on each side before recursing, avoiding wasted requests on empty ranges.
- **Concurrency-controlled**: all RPC calls go through a shared semaphore (default 32 permits), bounding in-flight requests without serializing independent work.

A naive sequential approach for the 6,093-transaction example above would take ~7 signature pages + ~61 full pages = 68 requests done one at a time. At 1.64s average HTTP time per request, that's **~112s sequential vs 14.56s actual, roughly 8x faster**.

## Quick start

### Run tests (no API key needed)

```bash
cargo test
```

### Run in offline replay mode (no API key needed)

```bash
cargo run -- --fixture fixtures/sample_wallet.json
```

Reads a local fixture file, simulates RPC paging and slot filtering, runs the full algorithm, and writes results to `outputs/<address>.json`.

### Run against live Helius RPC

```bash
cargo run -- --api-key YOUR_KEY --address YOUR_WALLET
```

This uses `https://beta.helius-rpc.com/?api-key=...` by default and retries on the standard mainnet endpoint if beta fails.

Or with a full RPC URL:

```bash
cargo run -- --rpc-url "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY" --address YOUR_WALLET
```

Both `--api-key` and `--rpc-url` can also be set via environment variables (`HELIUS_API_KEY` and `HELIUS_RPC_URL` respectively).

### With rate limiting

If your Helius plan has a request cap, use `--plan` to set automatic pacing:

```bash
cargo run -- --api-key YOUR_KEY --address YOUR_WALLET --plan developer
```

| Plan | Requests/sec |
|---|---|
| `developer` | 50 |
| `business` | 200 |
| `professional` | 500 |
| `enterprise` | unbounded |

Or set an explicit cap with `--rpc-rps`:

```bash
cargo run -- --api-key YOUR_KEY --address YOUR_WALLET --rpc-rps 100
```

`--rpc-rps` overrides `--plan` if both are provided.

## How the algorithm works

### Phase 1: classify the history (2 parallel requests)

Two requests fire simultaneously:

- oldest page: `sortOrder = asc, limit = scoutLimit` (default 1000)
- newest page: `sortOrder = desc, limit = scoutLimit`

If the two pages overlap, the entire history is already known. **The small-wallet fast path completes signature discovery in exactly 2 requests.**

If they don't overlap, the gap between the two frontiers determines whether the history looks dense or sparse. For very large ambiguous gaps, the scanner can either validate a seeded full-page fast path or send a midpoint confirmation probe before it commits to the sparse path.

### Phase 2: discover all signatures

**Dense path**: the two boundary pages didn't overlap, but the gap is compact relative to the covered span (gap < 8x the already-covered slot range). For the biggest dense histories, the scanner first validates a seeded full-page fast path using synthetic `slot:txIndex` pagination. When the endpoint accepts that pattern, the scanner fans out directly into full transaction pages from multiple seeded entry points and reconstructs the timeline with only the two boundary signature probes.

If seeded pagination is not accepted, or if the run is rate-limited, the scanner falls back to the lower-risk dense strategies already in the engine. The default fallback continues paginating from both ends simultaneously, letting the two cursors meet in the middle, and a secondary dense fanout mode can switch to parallel range discovery when signature-round latency is the bottleneck. As each dense signature page arrives, its corresponding full-transaction fetch jobs are **immediately dispatched**.

**Sparse path**: the gap is large relative to the covered span. The scanner recursively splits the remaining slot range and searches each subrange in parallel. Before recursing, it sends 1-record probes to find the tightest bounding slots on each side, avoiding wasted requests on empty gaps. An adaptive subrange planner uses observed density to estimate optimal window sizes, capped at 16 parallel subranges.

**Dense single-slot**: when a single slot contains more signatures than the page size, the scanner paginates that slot bidirectionally from both ends instead of walking one side linearly.

### Phase 3: fetch full transactions

Discovered signatures are grouped into bounded full-fetch jobs (max `fullLimit` transactions per page, default 100). Each job specifies a slot range and expected count. Jobs run concurrently, controlled by the concurrency semaphore.

For the dense pincer fallback, this phase is pipelined with Phase 2. Jobs are already in-flight by the time discovery finishes. For the seeded fast path, discovery and retrieval are the same full-page requests.

### Phase 4: reconstruct SOL balance over time

For each full transaction:

1. Find the target address in `accountKeys` (including `loadedAddresses` for versioned transactions)
2. Read `preBalances[i]` and `postBalances[i]` at the target's index
3. Compute `delta = post - pre`

The final timeline is sorted by `(slot, transactionIndex, signature)`. Failed transactions are included because fees still reduce SOL balance.

### Algorithm tuning constants

| Constant | Value | Purpose |
|---|---|---|
| `DENSITY_SAFETY_FACTOR` | 1.5 | Inflates estimated slot windows to avoid underfetching |
| `MAX_ADAPTIVE_WINDOWS` | 16 | Maximum parallel subranges in the adaptive planner |
| `SPARSE_GAP_MULTIPLIER` | 8 | Gap must exceed 8x covered span to trigger sparse-path recursion |

## CLI reference

```
sol-balance-runtime [OPTIONS]
```

### Input source (pick exactly one)

| Flag | Env var | Description |
|---|---|---|
| `--rpc-url <URL>` | `HELIUS_RPC_URL` | Live Helius RPC endpoint |
| `--api-key <KEY>` | `HELIUS_API_KEY` | Helius API key (uses the beta endpoint by default, with mainnet fallback) |
| `--fixture <PATH>` | - | Local fixture file for offline replay |

### Required with live sources

| Flag | Description |
|---|---|
| `--address <ADDRESS>` | Target Solana address. Optional with `--fixture` if the fixture contains an `address` field. |

### Tuning

| Flag | Default | Description |
|---|---|---|
| `--plan <PLAN>` | - | Helius plan preset for automatic request pacing |
| `--rpc-rps <N>` | unbounded | Explicit requests-per-second cap (overrides `--plan`) |
| `--concurrency <N>` | 32 (or `rpc-rps` value if higher) | Maximum concurrent in-flight requests |
| `--scout-limit <N>` | 1000 | Page size for signature discovery (1–1000) |
| `--full-limit <N>` | 100 | Page size for full transaction fetches (1–100) |
| `--output <PATH>` | `outputs/<address>.json` | Write JSON result to this file |

## Output format

The JSON output written to file:

```json
{
  "address": "2W2sRxN4ioj5ZJkicCqgr97kzugAHrcYGRWbbbxkqQds",
  "strategy": "dense_pincer",
  "initial_balance_lamports": 0,
  "final_balance_lamports": 899990000,
  "entries": [
    {
      "signature": "5abc...",
      "slot": 28429130,
      "transaction_index": 0,
      "block_time": 1700000000,
      "succeeded": true,
      "pre_balance_lamports": 0,
      "post_balance_lamports": 1000000000,
      "delta_lamports": 1000000000
    }
  ],
  "stats": {
    "signature_requests": 8,
    "full_requests": 61,
    "discovered_signatures": 6093,
    "fetched_transactions": 6093,
    "fetch_jobs": 61,
    "max_discovery_depth": 0
  }
}
```

### Top-level fields

| Field | Description |
|---|---|
| `address` | Target wallet address |
| `strategy` | Which runtime strategy won the classifier (`boundary_only`, `seeded_full_fanout`, `sparse_recursive`, `dense_parallel_range`, `dense_pincer`, or `empty`) |
| `initial_balance_lamports` | Native SOL balance before the first included transaction |
| `final_balance_lamports` | Native SOL balance after the last included transaction |
| `entries` | Per-transaction balance timeline |
| `stats` | Request and discovery counters for the run |

### Entry fields

| Field | Description |
|---|---|
| `signature` | Transaction signature |
| `slot` | Solana slot number |
| `transaction_index` | Position within the block (zero-indexed) |
| `block_time` | Unix timestamp (optional) |
| `succeeded` | Whether the transaction succeeded |
| `pre_balance_lamports` | Target address balance before the transaction |
| `post_balance_lamports` | Target address balance after the transaction |
| `delta_lamports` | `post - pre` |

### Stats fields

| Field | Description |
|---|---|
| `signature_requests` | Total signature-mode RPC requests made |
| `full_requests` | Total full-transaction RPC requests made |
| `discovered_signatures` | Unique signatures discovered during search |
| `fetched_transactions` | Transactions in the final timeline |
| `fetch_jobs` | Full-fetch job batches created |
| `max_discovery_depth` | Deepest recursion level reached during sparse search (0 = no recursion needed) |

## HTTP resilience

The live client includes automatic retry and backoff:

- **3 retries** on 429 (Too Many Requests), 5xx server errors, timeouts, and connection failures
- **Exponential backoff**: 150ms, 300ms, 600ms (150ms * 2^attempt)
- **HTTP timeouts**: 10s connect, 30s request, 90s connection pool idle

Live runs also report timing stats in the terminal:

```
http attempts: 69 | retries: 0 | total http time: 113.42s | avg per attempt: 1.64s
first completed rpc attempt: 287ms (includes initial connection/TLS setup on a cold client)
```

## Fixture format

For offline replay, provide a JSON file with full transactions:

```json
{
  "address": "target",
  "transactions": [
    {
      "slot": 10,
      "transactionIndex": 0,
      "blockTime": 10,
      "transaction": {
        "signatures": ["sig-10-0"],
        "message": {
          "accountKeys": ["target", "funder"]
        }
      },
      "meta": {
        "err": null,
        "fee": 5000,
        "preBalances": [0, 10000000000],
        "postBalances": [1000000000, 8999995000]
      }
    }
  ]
}
```

- `address` is optional (can be passed via `--address` instead)
- Transactions do not need to be pre-sorted; the fixture client sorts and deduplicates on load
- Include `loadedAddresses` when replaying versioned transactions that reference the target via lookup tables
- Signature-mode pages are derived automatically from the full transactions

## Special address handling

Some Solana addresses are documented by Helius as having degraded performance or limited indexing. The CLI warns when scanning these:

| Category | Example | Behavior |
|---|---|---|
| Routed to old archival | `Stake11111111111111111111111111111111111111` | Higher latency, different behavior |
| Slot-scan fallback (not indexed) | `11111111111111111111111111111111` | Much worse performance |
| Reserved/unindexed | `SysvarC1ock11111111111111111111111111111111` | May return empty results |

## Project structure

```
src/
  algorithm.rs    Adaptive scan logic and SOL balance reconstruction
  client.rs       Live Helius client + fixture replay client
  model.rs        RPC request/response types
  main.rs         CLI entrypoint
  lib.rs          Public library exports
fixtures/
  sample_wallet.json   Small offline example
```

## Development

```bash
cargo test                                              # run tests
cargo clippy --all-targets --all-features -- -D warnings # lints
cargo fmt                                                # format
cargo run -- --fixture fixtures/sample_wallet.json       # offline replay
```

## Requirements

- Rust toolchain with `cargo`
- For live mode: a Helius plan with access to `getTransactionsForAddress`
- Offline replay requires no network access or API key
