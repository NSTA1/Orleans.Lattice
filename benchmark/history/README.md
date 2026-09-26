# Benchmark History Stack

Long-lived companion to the per-scenario stack. Stores **summary scalars** (one
data point per metric per run) so cross-run trends are visible after the per-run
stack has been torn down. Independent of, and orthogonal to, the per-run flow.

## What's in here

| Container         | Image                                        | Port | Purpose                                                  |
|-------------------|----------------------------------------------|------|----------------------------------------------------------|
| `vfs-history-vm`  | `victoriametrics/victoria-metrics:v1.103.0`  | 8428 | PromQL-compatible long-term store with 12-month retention (`-retentionPeriod=12`); accepts pushes via `/api/v1/import/prometheus`. |
| `vfs-history-grafana` | `grafana/grafana:11.3.0`                  | 3001 | Dedicated Grafana hosting the benchmark-history persona dashboards, provisioned into its `Orleans.Lattice` folder (one per lattice-usage profile, see [Dashboards](#dashboards)). |

`docker-compose.history.yml` declares two named volumes (`victoriametrics-data`,
`grafana-history-data`) so data survives `down`. Use `down -v` to reset.

## Lifecycle

```powershell
# Stand it up (once; stays up across many scenario runs).
./benchmark.ps1 -OpenHistory

# Run scenarios as normal - they push their results.json scalars into VM
# automatically if the history stack is reachable.
./benchmark.ps1 current-state-no-replication
./benchmark.ps1 current-state-single-peer
./benchmark.ps1 read-heavy-random

# Backfill any prior runs that ran with the history stack offline.
./benchmark.ps1 -ImportHistory

# View trends.
#   http://localhost:3001  (anonymous viewer; the dashboards are provisioned automatically)

# Stop (named volumes preserved).
./benchmark.ps1 -CloseHistory
```

`./start-history.ps1` is a stand-alone equivalent of `-OpenHistory` that also
waits for Grafana to answer (up to `-TimeoutSeconds`, default 60) and then opens
it in the browser; `-NoBrowser` skips the browser. `benchmark.ps1` reaches the
stack at `BENCH_HISTORY_VM_URL` (default `http://localhost:8428`) and
`BENCH_HISTORY_GRAFANA_URL` (default `http://localhost:3001`).

## Data model

Each `.run/<scenario>/<run_id>/results.json` contributes one scalar sample to VM
per non-null key in its `metrics` block - the curated `$ScalarPanelExtra` entries
plus every auto-discovered key. Every sample is tagged:

| Label      | Example                | Source                                         |
|------------|------------------------|------------------------------------------------|
| `scenario` | `current-state-no-replication`                 | scenario id (the script argument)              |
| `run_id`   | `2026-04-30T14-08-41Z` | UTC ISO8601 timestamp taken when the script starts the run, with `:` written as `-` (script-generated) |
| `git_sha`  | `abc1234`              | `git rev-parse --short HEAD` at run time       |

Sample timestamps are the run's `ended` time, so the trend chart's x-axis is
**wall-clock when the benchmark ran**, not the artificial within-run time
window. This makes the dashboard's natural reading "how has current-state-no-replication's p99 evolved
across commits, week-over-week", which is the regression-detection question the
benchmark plan calls for.

## Metric vocabulary

The push helper in `benchmark.ps1` translates every key in `results.json`'s
`metrics` block into a Prometheus gauge named `bench_<key>`. Two ingest paths
feed it:

1. **Explicit `$ScalarPanelExtra` entries** in `benchmark.ps1` - one row per headline
   metric with its source PromQL.
2. **Auto-discovered prefixes** (`vehicle_fleet_simulator_*` and `orleans_lattice_*`,
   plus a curated `dotnet_*` allow-list) - every instrument emitted under those
   OpenTelemetry meters is synthesised, by instrument type, into
   `bench_<sanitised>_per_second` / `_increase` / `_max` / `_avg` / `_p50` /
   `_p95` / `_p99` keys without harness edits. Adding a new
   instrumentation site (e.g. the read-driver in `Bench.Sink`) just needs the
   meter registered with `WithMetrics(b => b.AddMeter(...))` in the silo and a
   matching `__name__=~"bench_<prefix>_.*"` regex in a dashboard family, as long
   as its instrument names start with one of those prefixes; one that starts
   with neither also needs its prefix added to `$AutoDiscoverPrefixes` in
   `benchmark.ps1`.

## Dashboards

The history Grafana hosts an **Overview dashboard** plus **seven generated persona
dashboards**, and one hand-maintained atomic-writes dashboard. The Overview is a
single-page roll-up showing every persona's
headline KPIs in one view (one row per persona, scoped to that persona's
scenarios) - use it as the landing page to spot the workload class that has
regressed, then click into the matching persona dashboard for trend strips
and per-run barcharts.

| Persona dashboard (`uid`)            | Scenarios it aggregates                                                                                                | What it asks                                                                  |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `lat-hist-overview`                  | every persona below, one row each                                                                                      | Single-page roll-up: is anything red right now?                                |
| `lat-hist-replication`               | `current-state-single-peer`, `bidirectional-replication`, `observer-no-peer`, `replication-key-filter`, `replication-backpressure`, `receiver-crash` | Replication ship/apply latency and commit-path overhead under replication.    |
| `lat-hist-write-heavy-random`        | `current-state-no-replication`, `skewed-key-shard-splits`                                                              | Per-vehicle current-state overwrites - steady-state and hot-key variants.     |
| `lat-hist-write-heavy-ordered`       | `event-log-with-ttl`                                                                                                    | Event-log keyspace with TTL: each tick a new key, TTL drives compaction.      |
| `lat-hist-read-heavy`                | `read-heavy-random`, `read-heavy-ordered`                                                                               | GetAsync-dominant load (95:5 read:write) across random and sequential keys.   |
| `lat-hist-read-write-mix`            | `read-write-mix-random`, `read-write-mix-ordered`                                                                       | Balanced 50:50 read/write (YCSB-A shape) across random and sequential keys.   |
| `lat-hist-microbench`                | `microbench`                                                                                                            | BenchmarkDotNet ILattice micro-suite (in-process, no Orleans cluster).        |
| `lat-hist-wal-performance`          | `current-state-single-peer`, `replication-backpressure`, `receiver-crash`, `bidirectional-replication`, `replication-key-filter` | Foreground commit path: WAL-append + in-memory Apply percentiles. The legacy shadow-write tile is retained for backwards comparison, but the commit step it reads was removed in v3.4.0, so no run since pushes a value for it: it can show only pre-v3.4.0 history, never a fresh zero. |
| `lat-hist-atomic-writes`             | `microbench` (the `SetManyAtomic` benchmarks) plus cluster-side saga health | `SetManyAtomicAsync` saga cost and saga health. Hand-maintained; `Generate-Dashboards.ps1` does not produce it. Its saga-health panels query the raw `orleans_lattice_atomic_write_completed_total` series, which the history push never writes (it imports only `bench_*` scalars), so they render empty on this stack. |

### Per-persona-dashboard layout (3 bands, top-to-bottom)

The seven persona dashboards share this 3-band layout. The Overview dashboard
is single-band (one row of stat tiles per persona, KPIs scoped to the
persona's scenarios).

| Band | Purpose                              | Panel type                                    | Reads                                                                                  |
|------|--------------------------------------|-----------------------------------------------|----------------------------------------------------------------------------------------|
| 0    | Headline KPIs                        | `stat` × {3..4} with threshold-coloured bg    | Per-persona last-known KPI values (e.g. commit p99, ship p95, reads/sec).              |
| 1    | Trends across runs                   | `timeseries` × {family count}, `points` mode  | One line per `{__name__, scenario, git_sha}` in the persona's metric families.         |
| 2    | Per-run history (commit comparator)  | `barchart` × {KPI count}, vertical            | One bar per run, hover shows `{{scenario}} {{run_id}} @ {{git_sha}}`.                  |

The dashboards are regenerated from `benchmark/history/Generate-Dashboards.ps1`.
The script wipes `BenchmarkHistory*.json` first so deleted personas don't leak,
then emits one JSON per persona under `grafana/dashboards/`, plus the Overview. The
wipe also removes the hand-maintained `BenchmarkHistory.atomic-writes.json`, and
the regenerated Overview drops the hand-added `Atomic Writes` row (the script's
`$Personas` table has no atomic-writes entry); restore both from git after
regenerating. Adding or moving a
scenario between personas is a one-line edit to the `$Personas` table at the
top of the script - re-run, wait ~30 s for Grafana's file-provider rescan,
done.

## Querying directly

VictoriaMetrics speaks PromQL. From the host:

```powershell
Invoke-RestMethod 'http://localhost:8428/api/v1/query?query=bench_lattice_commit_p99_ms{scenario="current-state-no-replication"}'
```

Or via the VM UI at <http://localhost:8428/vmui> for ad-hoc exploration.

## Why a separate stack?

- The per-run compose stays lean; tearing it down with `docker compose down -v`
  doesn't disturb the history archive.
- VictoriaMetrics' storage model is built for long retention with low overhead;
  Prometheus's TSDB is tuned for hot scrape windows and rotates aggressively.
- Running on different ports (`:3001` vs. `:3000`, `:8428` vs. `:9090`) lets the
  per-run live dashboards and the history dashboard coexist on the same machine.

## Why VictoriaMetrics rather than long-lived Prometheus?

VM accepts plain Prometheus exposition format over a single HTTP POST, so the
PowerShell push helper is ~20 lines. Prometheus is pull-based by design; using
it for this would require enabling `--web.enable-remote-write-receiver` plus a
proper remote-write client. Same query language, simpler ingest path.
