# Phase A - Step 1: architectural confirmation

Read end-to-end (or every section relevant to the commit hot path):

- `src/lattice/BPlusTree/Grains/WalShardGrain.cs` (1178 lines)
- `src/lattice/BPlusTree/Grains/AtomicWriteGrain.cs` (1763 lines; saga loop @ 1390-1480)
- `src/lattice.storage.azuretable/AzureTableWalStorageProvider.cs` (1490 lines; AppendBatchAsync hot path @ 270-365)
- `src/lattice.storage.azuretable/AzureTableWalStorageProvider.PhaseTwoWorker.cs` (377 lines, whole file)
- `src/lattice/BPlusTree/LatticeOptions.cs` (defaults @ 802-869)
- `src/lattice.storage.azuretable/AzureTableWalStorageOptions.cs` (whole file)
- `src/lattice/LatticeMetrics.cs` (1-160, plus tag scan for WAL/atomic_write/provider instruments)

## Confirmed choke points

| Choke point | Confirmed setting | File:line | Diagnosis impact |
|---|---|---|---|
| Single WAL partition per tree | `DefaultWalPartitions = 1` | `LatticeOptions.cs:809` | Every commit in a default tree funnels through one `WalShardGrain` activation (per-`{treeId}/0`). |
| Single in-flight flush per partition | `DefaultWalMaxPendingBatches = 1` | `LatticeOptions.cs:869` | Per-partition steady-state ceiling = `1 / provider_RTT`. Azure Tables 10-30 ms RTT => ~33-100 ops/s before batching gain. |
| Synchronous phase-2 commit | `PipelinePhaseTwoCommits = false` (default) | `AzureTableWalStorageOptions.cs:163` | Caller pays `max(phase0, phase1) + phase2` round-trips. Worker coalescing window (up to 49) never sees > 1 pending. |
| Phase-0 candidate row contends with worker | `EliminateCandidateRowOnHotPath = false` (default) | `AzureTableWalStorageOptions.cs:170` | Phase-0 `UpsertEntityAsync` against the same manifest partition the per-shard worker writes to. Azure Tables serialises writes within a partition server-side; phase-0 contends with the draining worker on every batch. v5.1.0 single-transaction equivalent ran ~180x faster on the same Azurite per the option's own XML doc. |
| Serial per-key saga fan-out | `while (state.State.NextIndex < state.State.Entries.Count) { await lattice.SetAsync(...); state.State.NextIndex++; }` | `AtomicWriteGrain.cs:1390-1425` | One round-trip per key inside a saga; even when keys land on distinct leaves / WAL partitions the saga does not exploit it. |

## Already-validated parallelism (not a choke point)

| Element | Evidence |
|---|---|
| Provider phase-2 workers per `(treeId, shardIndex)` | `AzureTableWalStorageProviderTests.Parallelism.cs` (read in prior session) pins one-worker-per-shard. |
| Per-batch partition keys for phase 1 | `BatchPartitionPrefix = "_b_"` + `S{startOffset:D19}` row keys; each batch hits a distinct Azure Tables partition server. `AzureTableWalStorageProvider.cs:113-126` (XML doc). |
| Worker phase-2 coalescing window | Up to 49 phase-2 commits collapsed into one 100-action transaction; sorted-set min-heap drains in strict ascending `startOffset` order. `PhaseTwoWorker.cs:154-172`. |
| WAL grain in-flight protocol | `LinkedList<InFlightFlush>` chain, sticky-failure resync, per-grain buffer-list pools. Already supports `WalMaxPendingBatches > 1` by construction. `WalShardGrain.cs:67-128`. |

## Deviations from plan assumptions

None. Every choke point listed in the plan's *Architectural context* section is real and configurable.

## Existing metrics surface (where step 2 will hook)

Already present on `Meter("orleans.lattice")` (`LatticeMetrics.cs`):

- `orleans.lattice.leaf.commit.duration` (tagged with `TagStep` = `wal|apply|observer`) - existing per-step histogram on the leaf commit path.
- `orleans.lattice.leaf.write.duration`, `orleans.lattice.leaf.scan.duration` - leaf-level histograms.
- `orleans.lattice.atomic_write.completed`, `orleans.lattice.atomic_write.duration`, `orleans.lattice.atomic_write.batch_size` - saga terminal-state instruments.
- `orleans.lattice.wal.entries_trimmed` - sole existing WAL-grain counter.

Tag conventions in use: `TagTree`, `TagShard`, `TagOperation`, `TagOutcome`, `TagKind`, `TagReason`, `TagConfig`, `TagStep`, `TagTrigger`, `TagPath`, `TagLeaf`.

**Gap for Phase A**: no histograms on the WAL grain hot path (`queue_depth`, `batch_size`, `batch_bytes`, `in_flight`, `provider_duration`, `turn_wait`), no provider-side phase-1 / phase-2 duration histograms, no `retry.exhausted` counter, no per-key saga timing (`saga.fanout.size`, `saga.perkey.duration`, `saga.wait.serial_gap`). The plan calls for adding these.

## Step 1 conclusion

Proceed to step 2 (Phase A instrumentation) with no changes to plan assumptions. Defaults will be flipped only after step 4 (Phase A matrix run) attributes the dominant cost.
