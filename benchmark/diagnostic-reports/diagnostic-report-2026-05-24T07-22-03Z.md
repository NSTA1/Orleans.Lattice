# Phase A diagnostic attribution report

Generated: 2026-05-24T08:27:51.1345294Z
Matrix wall-clock: 65.7 min
Cells executed: 46 / 46 (resume-from 0)

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

## atomic-write

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 29 | 1 | 1 | - | 2,391 | 0.06 | 0.93 | 3.3 | - | 0.10 | 0.10 | 24.9 | yes |
| 30 | 1 | 4 | - | 4 | - | 0.11 | 5.2 | - | 0.10 | 0.10 | - | yes |
| 31 | 1 | 16 | - | 544 | - | 0.89 | 6.1 | - | 0.10 | 0.10 | 24.9 | yes |
| 32 | 4 | 1 | - | 16,123 | 0.06 | 0.49 | 7.6 | - | 0.10 | 0.10 | 24.8 | yes |
| 33 | 4 | 4 | - | 2,711 | 0.06 | 0.49 | 4.7 | - | 0.10 | 0.10 | 24.9 | yes |
| 34 | 4 | 16 | - | 31,960 | 0.06 | 0.76 | 27.4 | - | 0.10 | 0.10 | 24.9 | yes |
| 35 | 16 | 1 | - | 9,851 | 0.05 | 0.26 | 5.2 | - | 0.10 | 0.10 | 24.9 | yes |
| 36 | 16 | 4 | - | 0 | - | - | 5.4 | - | - | - | - | yes |
| 37 | 16 | 16 | - | 1,688 | 0.05 | 0.14 | 11.2 | - | 0.10 | 0.10 | 24.9 | yes |

## atomic-write-replication

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 38 | 1 | 1 | - | 7,155 | 0.07 | 5.32 | 13.5 | - | 0.10 | 0.10 | 24.9 | yes |
| 39 | 1 | 4 | - | 3,698 | 0.08 | 0.25 | 18.2 | - | 0.10 | 0.10 | - | yes |
| 40 | 1 | 16 | - | 3,156 | 0.09 | 6.32 | 9.0 | - | 0.10 | 0.10 | 24.9 | yes |
| 41 | 4 | 1 | - | 2,886 | 0.07 | 0.40 | 13.9 | - | 0.10 | 0.10 | 24.9 | yes |
| 42 | 4 | 4 | - | 6,336 | - | 4.97 | 14.0 | - | 0.10 | 0.10 | - | yes |
| 43 | 4 | 16 | - | 671 | 0.06 | 4.49 | 17.5 | - | 0.10 | 0.10 | 24.9 | yes |
| 44 | 16 | 1 | - | 16,206 | 0.08 | 4.37 | 21.0 | - | 0.10 | 0.10 | 24.9 | yes |
| 45 | 16 | 4 | - | 1,748 | 0.07 | 4.70 | 12.7 | - | 0.10 | 0.10 | 24.9 | yes |
| 46 | 16 | 16 | - | 4,310 | 0.05 | 0.22 | 21.0 | - | 0.10 | 0.10 | 24.9 | yes |

## current-state-no-replication

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 2 | 1 | 1 | - | 17,106 | 0.06 | 0.93 | 6.0 | - | 0.10 | 0.10 | - | yes |
| 3 | 1 | 4 | - | 17,119 | 0.06 | 0.97 | 6.2 | - | 0.10 | 0.10 | - | yes |
| 4 | 1 | 16 | - | 17,097 | 0.07 | 0.96 | 6.3 | - | 0.10 | 0.10 | - | yes |
| 5 | 4 | 1 | - | 17,150 | 0.06 | 0.49 | 6.3 | - | 0.10 | 0.10 | - | yes |
| 6 | 4 | 4 | - | 17,093 | 0.06 | 0.49 | 6.4 | - | 0.10 | 0.10 | - | yes |
| 7 | 4 | 16 | - | 17,496 | 0.06 | 0.49 | 6.4 | - | 0.10 | 0.10 | - | yes |
| 8 | 16 | 1 | - | 17,123 | 0.05 | 0.33 | 7.9 | - | 0.10 | 0.10 | - | yes |
| 9 | 16 | 4 | - | 17,175 | 0.05 | 0.33 | 8.2 | - | 0.10 | 0.10 | - | yes |
| 10 | 16 | 16 | - | 17,193 | 0.05 | 0.30 | 6.6 | - | 0.10 | 0.10 | - | yes |

## current-state-no-replication-azuretable

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 11 | 1 | 1 | false | 457 | 0.07 | 793.94 | 3.7 | 11.97 | 23.74 | 14.81 | - | yes |
| 12 | 1 | 1 | true | 495 | 0.07 | 687.27 | 4.7 | 19.23 | 21.33 | 22.08 | - | yes |
| 13 | 1 | 4 | false | 436 | 0.07 | 749.17 | 5.3 | 10.99 | 22.76 | 22.20 | - | yes |
| 14 | 1 | 4 | true | 487 | 0.07 | 711.37 | 5.2 | 22.62 | 24.56 | 24.96 | - | yes |
| 15 | 1 | 16 | false | 427 | 0.07 | 856.00 | 4.2 | 14.32 | 27.40 | 32.42 | - | yes |
| 16 | 1 | 16 | true | 430 | 0.07 | 862.79 | 4.4 | 24.88 | 31.77 | 42.67 | - | yes |
| 17 | 4 | 1 | false | 308 | 0.07 | 1,638.96 | 5.4 | 34.24 | 62.15 | 49.70 | - | yes |
| 18 | 4 | 1 | true | 311 | 0.07 | 1,173.86 | 5.1 | 49.78 | 50.32 | 145.75 | - | yes |
| 19 | 4 | 4 | false | 285 | 0.07 | 1,787.20 | 5.1 | 35.67 | 61.60 | 48.91 | - | yes |
| 20 | 4 | 4 | true | 321 | 0.07 | 988.56 | 4.6 | 49.66 | 49.89 | 48.60 | - | yes |
| 21 | 4 | 16 | false | 284 | 0.07 | 996.75 | 6.8 | 29.85 | 75.50 | 49.75 | - | yes |
| 22 | 4 | 16 | true | 296 | 0.07 | 993.46 | 6.3 | 49.82 | 69.24 | 49.65 | - | yes |
| 23 | 16 | 1 | false | 336 | 0.07 | 969.95 | 6.2 | 78.85 | 144.21 | 188.88 | - | yes |
| 24 | 16 | 1 | true | 345 | 0.07 | 956.70 | 6.0 | 129.20 | 133.37 | 99.39 | - | yes |
| 25 | 16 | 4 | false | 346 | 0.07 | 967.59 | 5.1 | 77.36 | 154.05 | 188.13 | - | yes |
| 26 | 16 | 4 | true | 353 | 0.07 | 953.97 | 5.1 | 110.06 | 111.72 | 128.50 | - | yes |
| 27 | 16 | 16 | false | 344 | 0.07 | 956.41 | 6.2 | 73.28 | 130.87 | 216.79 | - | yes |
| 28 | 16 | 16 | true | 364 | 0.07 | 954.85 | 6.2 | 121.09 | 131.08 | 198.10 | - | yes |

## microbench

| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |
|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 1 | 0 | 0 | - | 161,840 | 0.00 | 0.03 | - | - | - | - | - | yes |

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
| 1 | microbench | `C:\dev\lattice\benchmark\.run\microbench\2026-05-24T07-22-07Z\results.json` |
| 2 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-36-29Z\results.json` |
| 3 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-37-36Z\results.json` |
| 4 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-38-43Z\results.json` |
| 5 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-39-52Z\results.json` |
| 6 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-41-01Z\results.json` |
| 7 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-42-08Z\results.json` |
| 8 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-43-16Z\results.json` |
| 9 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-44-25Z\results.json` |
| 10 | current-state-no-replication | `C:\dev\lattice\benchmark\.run\current-state-no-replication\2026-05-24T07-45-32Z\results.json` |
| 11 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-46-40Z\results.json` |
| 12 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-47-50Z\results.json` |
| 13 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-48-58Z\results.json` |
| 14 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-50-08Z\results.json` |
| 15 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-51-17Z\results.json` |
| 16 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-52-25Z\results.json` |
| 17 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-53-34Z\results.json` |
| 18 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-54-42Z\results.json` |
| 19 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-55-52Z\results.json` |
| 20 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-57-01Z\results.json` |
| 21 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-58-10Z\results.json` |
| 22 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T07-59-17Z\results.json` |
| 23 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-00-26Z\results.json` |
| 24 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-01-35Z\results.json` |
| 25 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-02-42Z\results.json` |
| 26 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-03-48Z\results.json` |
| 27 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-04-56Z\results.json` |
| 28 | current-state-no-replication-azuretable | `C:\dev\lattice\benchmark\.run\current-state-no-replication-azuretable\2026-05-24T08-06-05Z\results.json` |
| 29 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-07-14Z\results.json` |
| 30 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-08-21Z\results.json` |
| 31 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-09-27Z\results.json` |
| 32 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-10-34Z\results.json` |
| 33 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-11-43Z\results.json` |
| 34 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-12-51Z\results.json` |
| 35 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-13-58Z\results.json` |
| 36 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-15-08Z\results.json` |
| 37 | atomic-write | `C:\dev\lattice\benchmark\.run\atomic-write\2026-05-24T08-16-15Z\results.json` |
| 38 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-17-22Z\results.json` |
| 39 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-18-31Z\results.json` |
| 40 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-19-43Z\results.json` |
| 41 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-20-53Z\results.json` |
| 42 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-22-03Z\results.json` |
| 43 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-23-12Z\results.json` |
| 44 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-24-23Z\results.json` |
| 45 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-25-34Z\results.json` |
| 46 | atomic-write-replication | `C:\dev\lattice\benchmark\.run\atomic-write-replication\2026-05-24T08-26-44Z\results.json` |
