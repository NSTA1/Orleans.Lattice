# Authorization enforcement cost: microbenchmark report

- Generated: 2026-07-04
- Point single-key ops: fixed 16384-invocation in-process matched pair
  (disabled `2026-07-04T04-35-29Z` / enabled `2026-07-04T05-21-46Z`).
- Batch / range / atomic + control ops: out-of-process ShortRun matched pair
  (disabled `2026-07-04T06-43-04Z` / enabled `2026-07-04T07-03-42Z`).
- In-process pilot baseline (representative unloaded latencies): run
  `2026-07-04T03-29-18Z` (git `?`).
- Coverage: 53 matched cases (40 gate-enforced, 13 gate-independent control).

## Headline result

- Median per-operation allocation added by enforcement: **+1640 B** across 40 gated operations (range -2 to +2247290 B).
- Median enabled/disabled latency ratio on gated operations: **x1.11** (range x0.65 to x2.29).
- Gate-independent control group: latency delta +6.0% (+/-68.5% at ~p95), allocation delta within 0 B of zero, confirming the gate adds nothing to paths it does not guard.

The allocation delta splits into two clean patterns:

- **Flat per-call cost (~+1.5 KB/op).** Single-key operations and range/scan
  reads evaluate the gate once, adding roughly one decision + subject-resolution
  state-machine allocation regardless of how many keys the operation touches or
  returns. Range scans (key/entry/predicate) sit here: a 4-shard scan adds the
  same ~+1.6 KB whether it returns a handful of keys or a full page.
- **Per-entry cost (~+1.4 KB/key) on batch writes.** Bulk load, multi-key
  SetMany and atomic set-many evaluate the gate for each entry, so the added
  allocation scales with the batch size (bulk load adds ~+1.2 MB, a 4-shard
  SetMany ~+1.3 MB, a 64-key atomic set-many ~+93 KB). This is the dominant
  enforcement cost for large batch writes and is the one to size capacity
  against; single operations pay only the flat cost.

The allocation delta is the robust signal: it is measured identically in both
regimes and is dominated by the per-call decision and subject-resolution state
machines the enforcement path allocates. Latency deltas are reported against the
empirical noise band below; on this shared machine per-operation latency is noisy
and a few gated operations land inside the band or nominally faster, which is
noise, not a real speed-up.

## Why two measurement regimes

BenchmarkDotNet's in-process toolchain refuses to run a benchmark whose
estimated run time is too long ('takes too long to run'). Enabling the gate adds
a cold first-call cost to the multi-shard fan-out operations (SetMany across
several shards, atomic and cross-tree writes, multi-shard scans) that is large
enough to trip that guard. Those operations are therefore measured with the
out-of-process forking toolchain, which has no such guard. The single-key point
operations are measured in-process with a fixed 16384-invocation count so the
guard's invocation-count auto-scaling never runs. Every reported delta compares
disabled vs enabled *within the same regime*, so it is a fair comparison; only
compare absolute means across rows of the same `regime`.

## What each config measures

- **Disabled** - Membership + Auth not registered. `LatticeGrain` resolves a
  null `ILatticeAccessGate`; the enforcement path short-circuits with no subject
  resolution and no auth code on the hot path. Byte-for-byte the pre-feature
  baseline.
- **Enabled** - a real default-deny `PolicyAccessGate` plus a fixed-subject
  membership context wired into every `LatticeGrain`, with a representative
  tree/key/prefix allow ruleset for the benchmarked subject. Every gated
  operation pays subject resolution, compiled-snapshot lookup and rule eval.

## Noise band (empirical)

The gate-independent control benchmarks (pure CRDT merges, version-vector ops)
never run the gate, so their enabled-vs-disabled delta is pure run-to-run noise
and defines the band:

- control group: 13 benchmarks
- control mean latency delta: +6.0%
- control population std dev: 25.0%
- control ~p95 absolute latency delta (noise band): +/-68.5%
- control max absolute allocation delta: 0 B (expected ~0)

## Headline gate-enforced operations

`regime`: `fixed` = fixed-invocation in-process; `oop` = out-of-process ShortRun.
Read the ratio and allocation delta, not absolute nanoseconds across regimes.

| Operation | regime | Disabled | Enabled | Ratio | Alloc disabled | Alloc enabled | Alloc delta |
|---|:--:|---:|---:|---:|---:|---:|---:|
| Single-key write | fixed | 10.954 us | 11.290 us | x1.03 | 1416 B | 2888 B | +1472 B |
| Single-key read | fixed | 322.343 us | 332.634 us | x1.03 | 216 B | 1856 B | +1640 B |
| Single-key read (with version) | fixed | 9.739 us | 11.501 us | x1.18 | 264 B | 1904 B | +1640 B |
| Single-key exists | fixed | 98.399 us | 63.709 us | x0.65 | 0 B | 1640 B | +1640 B |
| Single-key delete | fixed | 64.996 us | 80.079 us | x1.23 | 2688 B | 4160 B | +1472 B |
| Get-or-set | fixed | 36.020 us | 32.458 us | x0.90 | 1472 B | 3000 B | +1528 B |
| Set-if-version (CAS) | fixed | 9.437 us | 11.265 us | x1.19 | 1168 B | 2696 B | +1528 B |
| Apply CRDT delta (via grain) | fixed | 89.418 us | 97.723 us | x1.09 | 3088 B | 4592 B | +1504 B |
| Point get-many (4-key batch) | fixed | 146.069 us | 161.627 us | x1.11 | 6632 B | 8224 B | +1592 B |
| Bulk load | oop | 1.925 ms | 4.274 ms | x2.22 | 102528 B | 1294872 B | +1192344 B |
| Mixed 70r/30w | oop | 1.284 ms | 1.258 ms | x0.98 | 753 B | 2393 B | +1640 B |
| SetMany (4 shards) | oop | 1.784 ms | 4.082 ms | x2.29 | 114880 B | 1438470 B | +1323590 B |
| Key scan page (range, 4 shards) | oop | 1.409 ms | 1.644 ms | x1.17 | 76400 B | 78040 B | +1640 B |
| Entry scan page (range, 4 shards) | oop | 1.951 ms | 1.444 ms | x0.74 | 125632 B | 127272 B | +1640 B |
| Predicate key scan (filtered range) | oop | 2.722 ms | 2.805 ms | x1.03 | 26536 B | 28464 B | +1928 B |
| Atomic set-many (single shard) | oop | 2.472 ms | 2.689 ms | x1.09 | 66740 B | 90076 B | +23336 B |
| Atomic set-many (64 keys) | oop | 3.009 ms | 2.821 ms | x0.94 | 108012 B | 201098 B | +93086 B |
| Cross-tree atomic (2 keys) | oop | 3.607 ms | 4.678 ms | x1.30 | 135745 B | 135743 B | -2 B |
| Cross-tree atomic (64 keys) | oop | 4.698 ms | 3.434 ms | x0.73 | 201103 B | 201105 B | +2 B |

## Representative unloaded latencies (in-process pilot baseline)

Absolute single-operation latency of the disabled (pre-feature) build, measured
in-process with BenchmarkDotNet's normal invocation-count auto-scaling. Use these
for 'what does the operation cost', not the fixed/oop absolute means above.

| Operation | Disabled mean | Alloc |
|---|---:|---:|
| Single-key write | 5.945 us | 1416 B |
| Single-key read | 6.152 us | 216 B |
| Single-key read (with version) | 2.380 us | 264 B |
| Single-key exists | 2.345 us | 0 B |
| Single-key delete | 11.347 us | 2520 B |
| Get-or-set | 5.258 us | 1472 B |
| Set-if-version (CAS) | 3.991 us | 1168 B |
| Apply CRDT delta (via grain) | 10.775 us | 3120 B |
| Point get-many (4-key batch) | 16.982 us | 6408 B |
| Delete range (absent) | 405.810 us | 2714 B |
| Bulk load | 1.316 ms | 102534 B |
| Mixed 70r/30w | 19.794 us | 803 B |
| SetMany (4 shards) | 860.685 us | 115053 B |
| Key scan page (range, 4 shards) | 293.274 us | 76457 B |
| Entry scan page (range, 4 shards) | 317.733 us | 125689 B |
| Predicate key scan (filtered range) | 708.563 us | 26597 B |
| Atomic set-many (single shard) | 320.113 us | 67871 B |
| Atomic set-many (64 keys) | 239.519 us | 109143 B |
| Cross-tree atomic (2 keys) | 352.632 us | 138375 B |
| Cross-tree atomic (64 keys) | 443.040 us | 201166 B |

## Gate-independent operations (no enforcement cost by construction)

These paths never invoke the gate (pure CRDT/merge math, WAL serialization,
gRPC framing, and the system-origin replication apply paths that bypass the gate)
so registering authorization does not change them. Latencies shown are the
in-process pilot baseline for context.

| Benchmark | Disabled mean | Alloc |
|---|---:|---:|
| leaf_queue_serialized_appends_32 | 491.967 ms | 191816 B |
| leaf_queue_serialized_appends_8 | 123.305 ms | 27350 B |
| ship_typed_envelope_entry_count1024_payload_bytes16384 | 32.576 ms | 67547864 B |
| leaf_queue_pipelined_appends_32 | 31.024 ms | 74160 B |
| crdt_receiver_apply_per_entry_256 | 21.880 ms | 32525666 B |
| crdt_receiver_apply_batched_256 | 21.003 ms | 13027631 B |
| ship_framing_only_entry_count1024_payload_bytes16384 | 18.074 ms | 34284984 B |
| leaf_queue_pipelined_appends_8 | 15.558 ms | 26861 B |
| leaf_queue_batched_append_8 | 15.522 ms | 8990 B |
| leaf_queue_batched_append_32 | 15.471 ms | 27134 B |
| crdt_receiver_apply_batched_64 | 10.335 ms | 6251255 B |
| crdt_receiver_apply_per_entry_64 | 9.371 ms | 15872359 B |
| ship_typed_envelope_entry_count256_payload_bytes16384 | 7.921 ms | 16868442 B |
| crdt_receiver_apply_batched_16 | 4.956 ms | 3059958 B |

## All matched benchmarks

`ctrl` marks the gate-independent control group. Delta % and alloc delta are
within-regime (disabled vs enabled at the same toolchain/invocation settings).

| Benchmark | ctrl | regime | Disabled | Enabled | Latency delta % | Alloc delta (B) |
|---|:--:|:--:|---:|---:|---:|---:|
| point_apply_crdt_delta |  | fixed | 89.418 us | 97.723 us | +9.3% | +1504 |
| point_delete |  | fixed | 64.996 us | 80.079 us | +23.2% | +1472 |
| point_exists |  | fixed | 98.399 us | 63.709 us | -35.3% | +1640 |
| point_get_many |  | fixed | 146.069 us | 161.627 us | +10.7% | +1592 |
| point_get_many_batch_size_1 |  | fixed | 70.645 us | 82.934 us | +17.4% | +1568 |
| point_get_many_batch_size_16 |  | fixed | 58.968 us | 63.759 us | +8.1% | +1688 |
| point_get_many_batch_size_2 |  | fixed | 46.697 us | 56.256 us | +20.5% | +2600 |
| point_get_many_batch_size_32 |  | fixed | 52.256 us | 93.850 us | +79.6% | +1816 |
| point_get_many_batch_size_4 |  | fixed | 42.315 us | 62.551 us | +47.8% | +1592 |
| point_get_many_batch_size_64 |  | fixed | 60.196 us | 85.342 us | +41.8% | +2072 |
| point_get_many_batch_size_8 |  | fixed | 50.739 us | 61.009 us | +20.2% | +1624 |
| point_get_or_set |  | fixed | 36.020 us | 32.458 us | -9.9% | +1528 |
| point_read |  | fixed | 322.343 us | 332.634 us | +3.2% | +1640 |
| point_read_atomic_tree_idle |  | fixed | 16.982 us | 15.336 us | -9.7% | +1640 |
| point_read_atomic_tree_with_active_saga |  | fixed | 14.037 us | 15.201 us | +8.3% | +1640 |
| point_read_deeper_tree |  | fixed | 20.654 us | 24.935 us | +20.7% | +1585 |
| point_read_with_version |  | fixed | 9.739 us | 11.501 us | +18.1% | +1640 |
| point_set_if_version |  | fixed | 9.437 us | 11.265 us | +19.4% | +1528 |
| point_set_with_ttl |  | fixed | 38.454 us | 49.406 us | +28.5% | +1472 |
| point_write |  | fixed | 10.954 us | 11.290 us | +3.1% | +1472 |
| point_write_deep_tree |  | fixed | 14.319 us | 16.021 us | +11.9% | +1304 |
| point_write_deeper_tree |  | fixed | 39.224 us | 45.407 us | +15.8% | +744 |
| crdt_mv_register_merge | x | oop | 89.870 us | 81.343 us | -9.5% | +0 |
| crdt_mv_register_values | x | oop | 26.039 us | 34.647 us | +33.1% | +0 |
| crdt_mv_register_values_multi | x | oop | 84.528 us | 80.268 us | -5.0% | +0 |
| crdt_pn_counter_merge | x | oop | 90.979 us | 72.851 us | -19.9% | +0 |
| noop | x | oop | 14.699 us | 13.693 us | -6.8% | +0 |
| or_map_clone | x | oop | 43.543 us | 50.912 us | +16.9% | +0 |
| or_map_merge | x | oop | 81.890 us | 109.462 us | +33.7% | +0 |
| or_map_merge_from | x | oop | 120.424 us | 86.096 us | -28.5% | +0 |
| or_set_merge | x | oop | 87.440 us | 89.043 us | +1.8% | +0 |
| version_vector_clone | x | oop | 54.099 us | 53.532 us | -1.0% | +0 |
| version_vector_merge | x | oop | 101.986 us | 97.084 us | -4.8% | +0 |
| version_vector_merge_from | x | oop | 84.404 us | 142.222 us | +68.5% | +0 |
| version_vector_tick | x | oop | 19.603 us | 19.622 us | +0.1% | +0 |
| bulk_load |  | oop | 1.925 ms | 4.274 ms | +122.1% | +1192344 |
| bulk_load_deep_tree |  | oop | 939.249 us | 922.679 us | -1.8% | +25408 |
| bulk_load_deeper_tree |  | oop | 1.659 ms | 1.474 ms | -11.1% | +37873 |
| cross_tree_atomic_2_keys |  | oop | 3.607 ms | 4.678 ms | +29.7% | -2 |
| cross_tree_atomic_64_keys |  | oop | 4.698 ms | 3.434 ms | -26.9% | +2 |
| entry_scan_page_over4_shards |  | oop | 1.951 ms | 1.444 ms | -26.0% | +1640 |
| key_scan_page_over4_shards |  | oop | 1.409 ms | 1.644 ms | +16.7% | +1640 |
| mixed_70_r_30_w |  | oop | 1.284 ms | 1.258 ms | -2.0% | +1640 |
| predicate_key_scan |  | oop | 2.722 ms | 2.805 ms | +3.1% | +1928 |
| set_many_4_shards |  | oop | 1.784 ms | 4.082 ms | +128.8% | +1323590 |
| set_many_atomic |  | oop | 2.472 ms | 2.689 ms | +8.8% | +23336 |
| set_many_atomic_2_keys |  | oop | 2.521 ms | 2.438 ms | -3.3% | +4944 |
| set_many_atomic_4_shards |  | oop | 3.010 ms | 2.636 ms | -12.4% | +19584 |
| set_many_atomic_64_keys |  | oop | 3.009 ms | 2.821 ms | -6.2% | +93086 |
| set_many_atomic_concurrent_1 |  | oop | 2.641 ms | 3.542 ms | +34.1% | +19585 |
| set_many_atomic_concurrent_16 |  | oop | 6.194 ms | 7.861 ms | +26.9% | +370689 |
| set_many_atomic_concurrent_4 |  | oop | 3.331 ms | 4.293 ms | +28.9% | +77664 |
| set_many_atomic_concurrent_64 |  | oop | 19.232 ms | 20.233 ms | +5.2% | +2247290 |

