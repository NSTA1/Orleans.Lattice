# Benchmark History Stack

Long-lived companion to the per-scenario stack. Stores **summary scalars** (one
data point per metric per run) so cross-run trends are visible after the per-run
stack has been torn down. Independent of, and orthogonal to, the per-run flow.

## What's in here

| Container         | Image                                        | Port | Purpose                                                  |
|-------------------|----------------------------------------------|------|----------------------------------------------------------|
| `vfs-history-vm`  | `victoriametrics/victoria-metrics:v1.103.0`  | 8428 | PromQL-compatible long-term store; accepts pushes via `/api/v1/import/prometheus`. |
| `vfs-history-grafana` | `grafana/grafana:11.3.0`                  | 3001 | Dedicated Grafana with the **Orleans.Lattice — Benchmark History** dashboard. |

`docker-compose.history.yml` declares two named volumes (`victoriametrics-data`,
`grafana-history-data`) so data survives `down`. Use `down -v` to reset.

## Lifecycle

```powershell
# Stand it up (once; stays up across many scenario runs).
./benchmark.ps1 -OpenHistory

# Run scenarios as normal — they push their results.json scalars into VM
# automatically if the history stack is reachable.
./benchmark.ps1 B-01
./benchmark.ps1 B-03
./benchmark.ps1 B-04

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
| `scenario` | `B-03`                 | scenario id (the script argument)              |
| `run_id`   | `2026-04-30T14-08-41Z` | UTC ISO8601 timestamp of run end (script-generated) |
| `git_sha`  | `abc1234`              | `git rev-parse --short HEAD` at run time       |

Sample timestamps are the run's `ended` time, so the trend chart's x-axis is
**wall-clock when the benchmark ran**, not the artificial within-run time
window. This makes the dashboard's natural reading "how has B-03's p99 evolved
across commits, week-over-week", which is the regression-detection question the
benchmark plan calls for.

## Metric vocabulary (fixed at the script)

The push helper in `benchmark.ps1` translates every key in `results.json`'s
`metrics` block into a Prometheus gauge named `bench_<key>`. The vocabulary is
the contract — adding a new metric is a two-line edit:

1. Add a new entry to `$ScalarPanel` in `benchmark.ps1` (the PromQL).
2. (Optional) Update the `BenchmarkHistory.json` `$metric` template default if
   the new key should be the landing page.

The dashboard auto-discovers metric names via `label_values({__name__=~"bench_.+"}, __name__)`,
so newly-pushed metrics appear in the `Metric` dropdown without a dashboard edit.

## Querying directly

VictoriaMetrics speaks PromQL. From the host:

```powershell
Invoke-RestMethod 'http://localhost:8428/api/v1/query?query=bench_lattice_commit_p99_ms{scenario="B-03"}'
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
