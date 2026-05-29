# Phase A diagnostic attribution report

Generated: 2026-05-24T07:09:36.7298600Z
Matrix wall-clock: 2.2 min
Cells executed: 2 / 2 (resume-from 0)

Source plan: `scaling.md` (Phase A). Reports under `benchmark/diagnostic-reports/` are tracked in git as the per-run evidence trail.

## Legend

| Column | Source metric | Meaning |
|---|---|---|
| `OpsPerSec` | `lattice_commits_per_second` (loaded) / `microbench_point_write_per_second` (BDN) | End-to-end throughput at the silo commit point. |
| `P50Ms` / `P99Ms` | `orleans_lattice_leaf_commit_duration_milliseconds_pNN` (loaded) / `microbench_point_write_pNN_ns` (BDN) | End-to-end commit latency quantiles. Microbench ns values are converted to ms. |
| `CpuPct` | `process_cpu_percent_(max|avg)` derived from `dotnet_process_cpu_time_seconds_total / dotnet_process_cpu_count` | Silo container CPU% (0-100% of host cores; `max` is the peak 30s slice in the window, `avg` is the window-mean). |
| `AzureSrvP99` | `orleans_lattice_provider_commit_duration_milliseconds_p99` | Azure Tables provider commit duration p99 (azuretable scenarios only). |
| `WalProvP99` | `orleans_lattice_wal_append_provider_duration_milliseconds_p99` | WAL grain provider call duration p99. |
| `WalTurnP99` | `orleans_lattice_wal_append_turn_wait_milliseconds_p99` | WAL grain turn-wait p99 (grain-scheduling backpressure signal). |
| `SagaFanP99` | `orleans_lattice_saga_fanout_size_p99` | Atomic-write saga fan-out p99 (saga scenarios only). |

## current-state-no-replication

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 1 | 1 | 4 | - | 17,154 | 0.07 | 1.14 | 7.9 | - | 0.10 | 0.10 | - | yes |
| 2 | 16 | 4 | - | 17,149 | 0.05 | 0.39 | 7.4 | - | 0.10 | 0.10 | - | yes |

## Attribution heuristics (from scaling.md Phase A)

| Symptom | Primary suspect | Phase that fixes it |
|---|---|---|
| Microbench >> current-state-no-replication, low CPU | Orleans grain scheduling / single WalShardGrain activation | Phase B |
| current-state-no-replication flat as WalMaxPendingBatches rises | Per-partition serialisation | Phase B |
| current-state-no-replication-azuretable << current-state-no-replication, low AzureSrv P99 | Provider client-side cost (phase-2 sync, payload, retry/backoff) | Phase C |
| AzureSrv P99 ~= wall time, p99 spikes correlate with ServerBusy | Real partition-server saturation | Phase B + C |
| atomic-write << current-state at same key rate | Saga-internal serialisation | Phase D |

## Raw artefacts

| # | Scenario | Results path |
|---|---|---|
| 1 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-07-24Z\results.json` |
| 2 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-08-32Z\results.json` |
