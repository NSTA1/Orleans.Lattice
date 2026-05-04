# Benchmark History Stack

Long-lived companion to the per-scenario stack. Stores **summary scalars** (one
data point per metric per run) so cross-run trends are visible after the per-run
stack has been torn down. Independent of, and orthogonal to, the per-run flow.

## What's in here

| Container         | Image                                        | Port | Purpose                                                  |
|-------------------|----------------------------------------------|------|----------------------------------------------------------|
| `vfs-history-vm`  | `victoriametrics/victoria-metrics:v1.103.0`  | 8428 | PromQL-compatible long-term store; accepts pushes via `/api/v1/import/prometheus`. |
| `vfs-history-grafana` | `grafana/grafana:11.3.0`                  | 3001 | Dedicated Grafana hosting the **Orleans.Lattice — Benchmark History** persona dashboards (one per lattice-usage profile, see [Dashboards](#dashboards)). |

`docker-compose.history.yml` declares two named volumes (`victoriametrics-data`,
`grafana-history-data`) so data survives `down`. Use `down -v` to reset.

## Lifecycle

```powershell
# Stand it up (once; stays up across many scenario runs).
./benchmark.ps1 -OpenHistory

# Run scenarios as normal — they push their results.json scalars into VM
# automatically if the history stack is reachable.
./benchmark.ps1 current-state-no-replication
./benchmark.ps1 current-state-single-peer
./benchmark.ps1 read-heavy-random

# Backfill any prior runs that ran with the history stack offline.
./benchmark.ps1 -ImportHistory

# View trends.
#   http://localhost:3001  (anonymous viewer, dashboard auto-loads)

# Stop (named volumes preserved).
./benchmark.ps1 -CloseHistory
```

## Data model

Each `.run/<scenario>/<run_id>/results.json` contributes ~18 scalar samples to
VM. Every sample is tagged:

| Label      | Example                | Source                                         |
|------------|------------------------|------------------------------------------------|
| `scenario` | `current-state-no-replication`                 | scenario id (the script argument)              |
| `run_id`   | `2026-04-30T14-08-41Z` | UTC ISO8601 timestamp of run end (script-generated) |
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

1. **Explicit `$ScalarPanel` entries** in `benchmark.ps1` — one row per headline
   metric with its source PromQL.
2. **Auto-discovered prefixes** (`vehicle_fleet_simulator_*`, `orleans_lattice_*`,
   `dotnet_*`) — every counter / histogram emitted under those OpenTelemetry
   meters is synthesised into `bench_<sanitised>_per_second` /
   `_p50` / `_p95` / `_p99` keys without harness edits. Adding a new
   instrumentation site (e.g. the read-driver in `Bench.Sink`) just needs the
   meter registered with `WithMetrics(b => b.AddMeter(...))` in the silo and a
   matching `__name__=~"bench_<prefix>_.*"` regex in a dashboard family.

## Dashboards

The history Grafana hosts an **Overview dashboard** plus **seven persona
dashboards**. The Overview is a single-page roll-up showing every persona's
headline KPIs in one view (one row per persona, scoped to that persona's
scenarios) — use it as the landing page to spot the workload class that has
regressed, then click into the matching persona dashboard for trend strips
and per-run barcharts.

| Persona dashboard (`uid`)            | Scenarios it aggregates                                                                                                | What it asks                                                                  |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `lat-hist-overview`                  | every persona below, one row each                                                                                      | Single-page roll-up: is anything red right now?                                |
| `lat-hist-replication`               | `current-state-single-peer`, `bidirectional-replication`, `observer-no-peer`, `replication-key-filter`, `replication-backpressure`, `receiver-crash` | Replication ship/apply latency and commit-path overhead under replication.    |
| `lat-hist-write-heavy-random`        | `current-state-no-replication`, `skewed-key-shard-splits`                                                              | Per-vehicle current-state overwrites — steady-state and hot-key variants.     |
| `lat-hist-write-heavy-ordered`       | `event-log-with-ttl`                                                                                                    | Event-log keyspace with TTL: each tick a new key, TTL drives compaction.      |
| `lat-hist-read-heavy`                | `read-heavy-random`, `read-heavy-ordered`                                                                               | GetAsync-dominant load (95:5 read:write) across random and sequential keys.   |
| `lat-hist-read-write-mix`            | `read-write-mix-random`, `read-write-mix-ordered`                                                                       | Balanced 50:50 read/write (YCSB-A shape) across random and sequential keys.   |
| `lat-hist-microbench`                | `microbench`                                                                                                            | BenchmarkDotNet ILattice micro-suite (in-process, no Orleans cluster).        |
| `lat-hist-wal-performance`          | `current-state-single-peer`, `replication-backpressure`, `receiver-crash`, `bidirectional-replication`, `replication-key-filter` | Foreground commit path: WAL-append + in-memory Apply percentiles. The legacy shadow-write tile is retained for backwards comparison and reads zero on every recent run. |

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
then emits one JSON per persona under `grafana/dashboards/`. Adding or moving a
scenario between personas is a one-line edit to the `$Personas` table at the
top of the script — re-run, wait ~30 s for Grafana's file-provider rescan,
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
