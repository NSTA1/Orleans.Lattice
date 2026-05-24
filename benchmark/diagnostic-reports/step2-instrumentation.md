# Phase A - Step 2: instrumentation landed

Build: `dotnet build -c Release --nologo /clp:ErrorsOnly` -> `0 Warning(s), 0 Error(s)`.

## New `LatticeMetrics` instruments

All on the existing `Meter("orleans.lattice")`.

### WAL grain (per-(treeId, shardIndex) tags)

| Instrument | Kind | Unit | Where it fires |
|---|---|---|---|
| `orleans.lattice.wal.append.queue_depth` | Histogram\<int\> | `{entry}` | `WalShardGrain.AppendAsync`, just after a new segment lands in `_pendingSegments`. |
| `orleans.lattice.wal.append.batch_entries` | Histogram\<int\> | `{entry}` | `WalShardGrain.StartFlush`, captured atomically with the in-flight chain mutation. |
| `orleans.lattice.wal.append.batch_bytes` | Histogram\<long\> | `By` | Same site as `batch_entries`. |
| `orleans.lattice.wal.append.in_flight` | Histogram\<int\> | `{flush}` | Same site - in-flight count *before* adding this flush. |
| `orleans.lattice.wal.append.provider.duration` | Histogram\<double\> | `ms` | `WalShardGrain.FlushAsync`, wrapping the `AppendEncodedBatchAsync` call (success + fault). |
| `orleans.lattice.wal.append.turn_wait` | Histogram\<double\> | `ms` | `WalShardGrain.AppendAsync` `finally`, measured from entry timestamp to TCS completion (covers queue + cutover + provider + dispatch). |

### Storage provider (per-(treeId, shardIndex, phase) tags; phase = `phase1` or `phase2`)

| Instrument | Kind | Unit | Where it fires |
|---|---|---|---|
| `orleans.lattice.provider.commit.duration` | Histogram\<double\> | `ms` | Phase 1: `AzureTableWalStorageProvider.SubmitPhaseOneAsync`. Phase 2: `PhaseTwoWorker.CommitBatchAsync` around `_submit`. |
| `orleans.lattice.provider.phase2.batch_size` | Histogram\<int\> | `{commit}` | `PhaseTwoWorker.CommitBatchAsync`, recorded once per coalesced manifest transaction. |
| `orleans.lattice.provider.retry.exhausted` | Counter\<long\> | `{call}` | Phase 1 catch in `SubmitPhaseOneAsync`; phase 2 catch in `PhaseTwoWorker.CommitBatchAsync`. Tagged with `status` = HTTP status from `RequestFailedException`, or `unknown`. |

### Atomic-write saga (per-(treeId) tag)

| Instrument | Kind | Unit | Where it fires |
|---|---|---|---|
| `orleans.lattice.saga.fanout.size` | Histogram\<int\> | `{entry}` | `AtomicWriteGrain.ExecutePhaseAsync`, once per execute-phase entry. |
| `orleans.lattice.saga.perkey.duration` | Histogram\<double\> | `ms` | Wraps the per-key `await lattice.SetAsync(...)` inside the execute loop, on every successful or failing iteration (try/finally). |
| `orleans.lattice.saga.wait.serial_gap` | Histogram\<double\> | `ms` | Same site, measured from the previous successful iteration's `WriteStateAsync` completion to the next iteration's per-key entry. First iteration skipped (no predecessor). |

## New tag constants

- `TagPhase = "phase"` (phase 1 / phase 2)
- `TagStatus = "status"` (HTTP status string for provider retry exhaustion)
- Cached `KeyValuePair` singletons `PhasePhase1Tag` / `PhasePhase2Tag` to keep hot-path emission allocation-free.

## Constructor signature change

`PhaseTwoWorker` production constructor now requires `(treeId, shardIndex)` so its emitted instruments carry the correct shard tags. The internal test-only `(submit, manifestPartitionKey)` constructor is preserved as-is (forwards `treeId = ""`, `shardIndex = 0`) so the existing white-box tests in `PhaseTwoWorkerTests.cs` compile unchanged. Production callsite at `AzureTableWalStorageProvider.GetOrCreatePhaseTwoWorker` updated.

## Allocation profile (Phase A hot-path)

| Site | New allocations per call | Rationale |
|---|---|---|
| `WalShardGrain.AppendAsync` | None on hot path: `_treeTag` / `_shardTag` cached on activation; `Stopwatch.GetTimestamp` returns a `long`; `Record(int, KV, KV)` overload allocates nothing. | Cached tag fields per activation. |
| `WalShardGrain.StartFlush` | None: same two cached tags, `int` and `long` values. | Cached tag fields per activation. |
| `WalShardGrain.FlushAsync` provider call wrapper | None: `long` ticks + `double` ms via `GetElapsedTime`. | - |
| `AzureTableWalStorageProvider.SubmitPhaseOneAsync` | One `KeyValuePair` for `treeTag`, one for `shardTag`, one (only on fault) for the status tag. | Provider is shared across trees, so per-call tag construction is unavoidable here. The fault-only status tag costs nothing on the success path. |
| `PhaseTwoWorker.CommitBatchAsync` | None on success; one status `KeyValuePair` on fault. | Cached `_treeTag` / `_shardTag` per worker. |
| `AtomicWriteGrain.ExecutePhaseAsync` | One `KeyValuePair` for `sagaTreeTag` per execute-phase entry (not per-key). | The saga grain's lifetime spans a single saga activation; constructing once and reusing across the loop is allocation-free per key. |

No ⚠️ allocations on any per-entry / per-batch / per-call hot path. The phase-1 provider tag pair is created per-batch (not per-entry), which is the documented seam (provider is shared, not per-shard) and is amortised across the batch.

## Step 2 conclusion

Phase A instrumentation is complete and the build is clean. Phase A step 3 (benchmark matrix run + attribution) is unblocked; step 4 (defaults flip recommendation) will follow once attribution data is in.
