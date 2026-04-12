# sol-balance-runtime

`sol-balance-runtime` is a Rust CLI and library for reconstructing a Solana address's native SOL balance over time using Helius `getTransactionsForAddress`.

This project was built around a latency-focused challenge:

- compute SOL balance over time at runtime
- do not rely on a pre-indexed database
- use RPC only
- optimize for low average latency across different wallet shapes

In this repo, "SOL PnL" is interpreted the way mert clarified it: native `SOL balance over time`, not USD value, not token PnL, and not portfolio valuation.

## What this project does

Given a Solana address, the scanner:

1. classifies the address with parallel ascending and descending `signatures` boundary queries
2. chooses between a bidirectional pincer sweep for dense histories and recursive sparse-gap search
3. fetches the required full transactions in bounded batches
4. sorts the final history by `slot` and `transactionIndex`
5. reconstructs the address's native SOL balance changes from `preBalances` and `postBalances`

The output is a transaction-level timeline that can be used for benchmarking, correctness checks, or downstream analytics.

## Why there is an offline replay mode

Helius `getTransactionsForAddress` is not available on the free plan, so burning live requests while iterating on the algorithm is wasteful.

To make development practical, this repo supports two interchangeable sources:

- live Helius RPC
- local fixture replay

Both go through the same scanning algorithm. That means you can tune search heuristics, test paging behavior, and validate timeline reconstruction locally before touching a paid Helius endpoint.

## Current status

This repo currently contains:

- a working Rust CLI
- a reusable library API
- a live Helius client
- a fixture-backed offline client
- a hybrid latency strategy that combines boundary sweeps, recursive sparse search, and dense-slot pincer pagination
- warnings for Helius-documented special-case addresses
- unit tests for ordering, job packing, small-wallet fast paths, sparse recursion, dense-wallet boundary sweeps, dense-slot pagination, loaded address handling, and offline end-to-end replay

This is now a strong submission candidate. The remaining external step is live benchmarking across representative wallets once paid Helius access is available.

## Repository layout

```text
.
|-- Cargo.toml
|-- README.md
|-- fixtures/
|   `-- sample_wallet.json
`-- src/
    |-- algorithm.rs
    |-- client.rs
    |-- lib.rs
    |-- main.rs
    `-- model.rs
```

What each file is for:

- `src/main.rs`: CLI entrypoint
- `src/algorithm.rs`: adaptive scan logic and SOL balance reconstruction
- `src/client.rs`: live Helius client plus fixture replay client
- `src/model.rs`: request and response models
- `src/lib.rs`: public library exports
- `fixtures/sample_wallet.json`: small offline example fixture

## Requirements

- Rust toolchain with `cargo`
- for live mode only: a Helius RPC URL with access to `getTransactionsForAddress`

The offline replay path does not require network access or a Helius key.

## Quick start

### 1. Clone and test

```bash
cargo test
```

### 2. Run in offline replay mode

This is the recommended first run.

```bash
cargo run -- --fixture fixtures/sample_wallet.json
```

Expected behavior:

- reads the fixture file
- simulates `getTransactionsForAddress` paging and slot filtering locally
- runs the full scanner
- writes the result to `outputs/<address>.json`
- prints a short scan summary in the terminal with wall time and output location

### 3. Run against live Helius RPC

If you have a paid Helius plan that supports `getTransactionsForAddress`:

The simplest option is to pass the API key directly:

```bash
cargo run -- \
  --api-key "YOUR_API_KEY" \
  --address "YOUR_WALLET_ADDRESS"
```

You can also pass the full RPC URL:

```bash
cargo run -- \
  --rpc-url "https://mainnet.helius-rpc.com/?api-key=YOUR_API_KEY" \
  --address "YOUR_WALLET_ADDRESS"
```

You can also set the RPC URL through an environment variable:

```bash
export HELIUS_RPC_URL="https://mainnet.helius-rpc.com/?api-key=YOUR_API_KEY"

cargo run -- \
  --address "YOUR_WALLET_ADDRESS"
```

Or set just the API key:

```bash
export HELIUS_API_KEY="YOUR_API_KEY"

cargo run -- \
  --address "YOUR_WALLET_ADDRESS"
```

## CLI reference

```text
sol-balance-runtime
  --rpc-url <URL>          Live Helius RPC endpoint
  --api-key <KEY>          Helius API key; builds the default mainnet RPC URL
  --fixture <PATH>         Local fixture file for offline replay
  --address <ADDRESS>      Target wallet/address
  --output <PATH>          Write the JSON result to this file
  --plan <PLAN>            Optional Helius plan preset for pacing
  --rpc-rps <N>            Optional request-per-second cap
  --concurrency <N>        Max in-flight requests/jobs
  --scout-limit <N>        Page size for signature discovery, default 1000
  --full-limit <N>         Page size for full transactions, default 100
```

Notes:

- choose exactly one of `--rpc-url`, `--api-key`, or `--fixture`
- if the fixture file contains an `address`, `--address` can be omitted
- if `--output` is omitted, the CLI writes to `outputs/<address>.json`
- the JSON output is always written to a file and is always pretty-printed
- the terminal only shows a short summary so large scans do not flood your screen
- live runs also print request timing stats, including the first completed RPC attempt as a cold-start proxy for initial connection/TLS setup
- `--rpc-rps` overrides `--plan` if both are provided
- current built-in plan presets are `developer=50 rps`, `business=200 rps`, `professional=500 rps`, and `enterprise=uncapped/custom`
- if `--concurrency` is omitted, the CLI defaults to `32`, or to the configured request cap when using `--plan` / `--rpc-rps`
- `--scout-limit` is clamped to `1..=1000`
- `--full-limit` is clamped to `1..=100`

Example terminal summary:

```text
scan complete
source: live
address: 2W2sRxN4ioj5ZJkicCqgr97kzugAHrcYGRWbbbxkqQds
output: outputs/2W2sRxN4ioj5ZJkicCqgr97kzugAHrcYGRWbbbxkqQds.json
wall time: 14.56s
entries: 6093 | signature requests: 8 | full requests: 61
concurrency: 32 | rpc pacing: unbounded
http attempts: 69 | retries: 0 | total http time: 113.42s | avg per attempt: 1.64s
first completed rpc attempt: 287ms (includes initial connection/TLS setup on a cold client)
```

## Example output

The JSON file written by the CLI has this high-level shape:

```json
{
  "address": "target",
  "initial_balance_lamports": 0,
  "final_balance_lamports": 899990000,
  "entries": [
    {
      "signature": "sig-10-0",
      "slot": 10,
      "transaction_index": 0,
      "block_time": 10,
      "succeeded": true,
      "pre_balance_lamports": 0,
      "post_balance_lamports": 1000000000,
      "delta_lamports": 1000000000
    }
  ],
  "stats": {
    "signature_requests": 2,
    "full_requests": 1,
    "discovered_signatures": 4,
    "fetched_transactions": 4,
    "fetch_jobs": 1,
    "max_discovery_depth": 0
  }
}
```

### Output fields

- `address`: the target address scanned
- `initial_balance_lamports`: the earliest observed balance before the first transaction in the timeline
- `final_balance_lamports`: the last observed balance after the final transaction in the timeline
- `entries`: ordered transaction-level balance events
- `stats`: a small summary of how much scanning work was done

Each timeline entry includes:

- `signature`: the transaction signature
- `slot`: Solana slot
- `transaction_index`: zero-based position inside the block
- `block_time`: optional Unix timestamp
- `succeeded`: whether the transaction succeeded
- `pre_balance_lamports`: target address balance before the transaction
- `post_balance_lamports`: target address balance after the transaction
- `delta_lamports`: `post - pre`

## How the algorithm works

The scanner is designed around the key advantage of Helius `getTransactionsForAddress`: it can sort both ascending and descending, and it supports slot-range filters.

### Phase 1: classify the history quickly

The scanner asks for:

- the oldest page with `sortOrder = asc, limit = scoutLimit`
- the newest page with `sortOrder = desc, limit = scoutLimit`

That gives:

- the oldest and newest observed transactions
- an immediate fast path for small wallets when the two pages already cover the full history
- a cheap signal for whether the remaining work looks dense or sparse

### Phase 2: discover signatures cheaply

The scanner uses `transactionDetails = signatures` first because it is cheaper and lighter than fetching full transactions immediately.

For dense histories, the scanner continues from both ends in parallel and lets the two sides meet in the middle.

For sparse histories, the scanner recursively searches slot ranges and uses adaptive subrange planning so it does not behave like a purely sequential ledger walk.

For dense single-slot histories, the scanner paginates that slot from both directions instead of walking only one side.

This is the core latency-oriented search step.

### Phase 3: convert signatures into full-fetch jobs

Once the signature set is known, the scanner groups signatures by slot and creates bounded jobs for full transaction fetches.

The job packing rules are:

- pack adjacent slots together while total expected transaction count stays within the full fetch limit
- isolate dense single slots when one slot alone exceeds the limit

### Phase 4: reconstruct SOL balance over time

For each full transaction, the scanner:

- finds the target address in the transaction account list
- includes loaded lookup-table addresses when present
- reads the target index from `preBalances` and `postBalances`
- computes `delta_lamports = post - pre`

The final timeline is sorted by:

1. `slot`
2. `transactionIndex`
3. signature tie-breaker

That ordering matters. `blockTime` alone is not precise enough.

## Important assumptions

This project currently assumes:

- the target output is a transaction-level native SOL balance timeline
- failed transactions are still relevant because fees can reduce SOL balance
- versioned transactions must be included, so full fetches request `maxSupportedTransactionVersion = 0`
- requests should explicitly use `status = any` and `tokenAccounts = none`
- the target is usually a wallet-like address, not a special program account

These assumptions match the challenge framing reasonably well, but they are still assumptions. If your judging environment differs, adjust accordingly.

## Fixture format

Offline replay uses a JSON file with this shape:

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

### Fixture notes

- `address` is optional but recommended
- `transactions` should already represent the full transactions relevant to the address
- `transactionIndex` and `slot` are important for correct ordering
- `loadedAddresses` should be included when replaying versioned transactions that reference the target via lookup tables
- the fixture client derives signature-mode pages from the full transactions automatically

## Development workflow

### Run tests

```bash
cargo test
```

### Run lints

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Format code

```bash
cargo fmt
```

### Run the sample fixture

```bash
cargo run -- --fixture fixtures/sample_wallet.json --pretty
```

## Library usage

The library API is exposed from `src/lib.rs`.

The main entrypoint is:

```rust
reconstruct_sol_balance_timeline(source, address, config).await
```

Where:

- `source` implements `TransactionsSource`
- `address` is the target address
- `config` is `RuntimeScanConfig`

This makes it straightforward to:

- plug in live Helius RPC
- plug in offline fixtures
- add benchmarking or custom replay sources later

## Limitations

This baseline intentionally does not solve everything yet.

Known limitations:

- no automatic fixture capture from live RPC yet
- no benchmark harness for comparing heuristics across many wallets yet
- no custom heuristics for highly pathological account types
- no persistent memoization or warm cache layer
- no scoring framework for challenge submissions yet

There are also external limitations inherited from the upstream method:

- `getTransactionsForAddress` is Helius-specific
- it is not available on Helius free plan
- pricing and plan availability can change, so check current Helius docs before relying on a specific cost model
- some addresses are explicitly documented by Helius as routed to old archival, slot-scan fallback, or reserved/unindexed; the CLI now warns when the target address matches one of those known cases

## Why Rust

Rust was chosen because this problem is mostly about request strategy and concurrency discipline.

Rust gives this project:

- low runtime overhead
- strong async/concurrency control
- predictable performance
- clear separation between algorithm, transport, and replay

Bun or Node would also be viable, but Rust is a better fit for a latency-sensitive benchmark tool where we want the client overhead to stay out of the way.

## Roadmap

The most useful next steps are:

1. add live-to-fixture capture so a single paid run can be replayed locally forever
2. add a benchmark harness for busy, sparse, and periodic wallet archetypes
3. experiment with smarter range-splitting and stopping heuristics
4. compare cold-start latency distributions, not just mean latency
5. add correctness fixtures based on real wallet histories

## References

- Helius docs: `getTransactionsForAddress`
- Helius API reference: `getTransactionsForAddress`
- Solana RPC docs: `getTransaction`
- Solana RPC JSON structures

If you are landing here for the first time, start with fixture mode, confirm you understand the output shape, and only then move to live Helius runs.
