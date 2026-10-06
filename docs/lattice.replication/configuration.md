# Configuration

This document covers the public configuration surface for `Orleans.Lattice.Replication`. The gRPC transport and the Azure Table WAL backend are separate packages and are configured in their own docs, cross-linked at the end of this page. Compression knobs that are shared with the core package are cross-referenced to [core compression](../lattice/compression.md).

## Registering replication

Register replication after registering the core lattice services. `AddLatticeReplication` installs the replication pipeline and accepts the initial `LatticeReplicationOptions` callback:

```csharp verify
using Orleans.Lattice.Replication;

siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };
    opts.ReplicationPeers = new[] { "site-b" };
});
```

Replication uses the standard .NET named-options pattern. Each tree resolves `LatticeReplicationOptions` by tree id. Use `ConfigureLatticeReplication` without a tree name to set defaults for all trees, and the overload with `treeName` to override one tree. Per-tree overrides layer on top of the global defaults.

```csharp verify
siloBuilder.ConfigureLatticeReplication(o =>
{
    o.ClusterId = "site-a";
    o.ShipBatchSize = 256;
});

siloBuilder.ConfigureLatticeReplication("orders", o =>
{
    o.ShipBatchSize = 512;
    o.PreShipCoalescingEnabled = true;
});
```

A few options are read only from the cluster-wide (unnamed) instance, so a per-tree override of them has no effect: `ReplicationPeers`, `ShipPhaseTimerPeriod`, `ShipDoorbellEnabled`, and `MaxInboundDecompressedBytes`. The [WAL-retention startup guard](#walretention) likewise reads `DigestProbeEnabled` and `AllowWalRetentionWithoutAntiEntropy` from the cluster-wide instance.

Startup options validation rejects empty cluster ids, invalid replicated-tree declarations, non-positive sizes, invalid intervals, invalid jitter and factor ranges, and incompatible wire-version, compression, adaptive-batch, and remediation bounds.

## Options Reference - `LatticeReplicationOptions`

### Identity and opt-in

| Option | Type | Default |
|---|---|---|
| [`ClusterId`](#clusterid) | `string` | `""` |
| [`ReplicatedTrees`](#replicatedtrees) | `IReadOnlyDictionary<string, LatticeMergeMode>?` | `null` |
| [`KeyFilter`](#keyfilter) | `Func<string, bool>?` | `null` |
| [`KeyPrefixes`](#keyprefixes) | `IReadOnlyCollection<string>?` | `null` |

### WAL and replog

| Option | Type | Default |
|---|---|---|
| [`ReplogPartitions`](#replogpartitions) | `int` | 8 |
| [`WalStorageProvider`](#walstorageprovider) | `Func<string, IWalStorageProvider>?` | `null` |
| [`WalMaxBatchEntries`](#walmaxbatchentries) | `int` | 100 |
| [`WalMaxBatchBytes`](#walmaxbatchbytes) | `long` | 4 MiB |
| [`WalMaxPendingBatches`](#walmaxpendingbatches) | `int` | 4 |
| [`WalRetention`](#walretention) | `TimeSpan?` | `null` |
| [`AllowWalRetentionWithoutAntiEntropy`](#allowwalretentionwithoutantientropy) | `bool` | `false` |
| [`MaintenanceGcInterval`](#maintenancegcinterval) | `TimeSpan` | 5 seconds |

The core WAL - its partition grains, commit-log writer, and garbage collector - reads the tree's core `LatticeOptions`, not these fields. `AddLatticeReplication` mirrors `ReplogPartitions` (onto `LatticeOptions.WalPartitions`), `WalMaxBatchEntries`, `WalMaxBatchBytes`, `WalMaxPendingBatches`, `WalStorageProvider`, and `WalRetention` onto the same tree's `LatticeOptions`. The mirror is one-way and writes a field only when the replication-side value differs from its default here (is non-`null`, for the two nullable fields) and the core field is still at its own default, so a direct `LatticeOptions` override always wins. `WalMaxPendingBatches` is the one field whose defaults differ - `4` here, `16` on `LatticeOptions` - so leaving it at (or setting it to) `4` leaves the WAL at the core `16`.

### Apply and causal buffer

| Option | Type | Default |
|---|---|---|
| [`MaxApplyRetries`](#maxapplyretries) | `int` | 5 |
| [`SagaDeferralTimeout`](#sagadeferraltimeout) | `TimeSpan` | 15 minutes |
| [`DeadLetterQueueCapacity`](#deadletterqueuecapacity) | `int` | 1000 |
| [`CausalBufferMaxEntries`](#causalbuffermaxentries) | `int` | 1024 |
| [`CausalAppliedIdentityCapacity`](#causalappliedidentitycapacity) | `int` | 16,384 |
| [`CausalBufferMaxBytes`](#causalbuffermaxbytes) | `long` | 16 MiB |
| [`ShadowForwardDedupeCacheSize`](#shadowforwarddedupecachesize) | `int` | 4096 |
| [`ApplyMaxParallelRuns`](#applymaxparallelruns) | `int` | 1 |

### Shipping cadence and backoff

| Option | Type | Default |
|---|---|---|
| [`ReplicationPeers`](#replicationpeers) | `IReadOnlyCollection<string>?` | `null` |
| [`ShipBatchSize`](#shipbatchsize) | `int` | 256 |
| [`ShipPartitionPageSize`](#shippartitionpagesize) | `int` | 256 |
| [`ShipCursorWriteInterval`](#shipcursorwriteinterval) | `int` | 16 |
| [`ShipCursorWriteMaxDelay`](#shipcursorwritemaxdelay) | `TimeSpan` | 2 seconds |
| [`ShipMaxInFlight`](#shipmaxinflight) | `int` | 1 |
| [`ShipBackoffInitial`](#shipbackoffinitial) | `TimeSpan` | 100 ms |
| [`ShipPhaseTimerPeriod`](#shipphasetimerperiod) | `TimeSpan` | 100 ms |
| [`ShipSourceIdentityBackstopInterval`](#shipsourceidentitybackstopinterval) | `TimeSpan` | 30 seconds |
| [`LivenessProbeInterval`](#livenessprobeinterval) | `TimeSpan` | 30 seconds |
| [`ShipBackoffMax`](#shipbackoffmax) | `TimeSpan` | 30 seconds |
| [`ShipBackoffJitter`](#shipbackoffjitter) | `double` | 0.2 |
| [`ShipDoorbellEnabled`](#shipdoorbellenabled) | `bool` | `true` |

### Efficiency bundle, dedup, and compression

| Option | Type | Default |
|---|---|---|
| [`ContentHashDedupEnabled`](#contenthashdedupenabled) | `bool` | `true` |
| [`ContentHashDedupCacheSize`](#contenthashdedupcachesize) | `int` | 4096 |
| [`ContentHashDedupElisionEnabled`](#contenthashdedupelisionenabled) | `bool` | `false` |
| [`PreShipCoalescingEnabled`](#preshipcoalescingenabled) | `bool` | `true` |
| [`FramingCompression`](#framingcompression) | `LatticeCompression` | `Zstd` |
| [`FramingCompressionLevel`](#framingcompressionlevel) | `int` | 3 |
| [`MaxInboundDecompressedBytes`](#maxinbounddecompressedbytes) | `long` | 64 MiB (`16 * DefaultWalMaxBatchBytes`) |
| [`FramingCompressionMinBatchBytes`](#framingcompressionminbatchbytes) | `int` | 512 |
| [`FramingCompressionDictionaryId`](#framingcompressiondictionaryid) | `uint` | 0 |
| [`DictionaryNegotiationEnabled`](#dictionarynegotiationenabled) | `bool` | `false` |
| [`AutoSharedDictionaryEnabled`](#autoshareddictionaryenabled) | `bool` | `false` |

### Bootstrap and auto-bootstrap

| Option | Type | Default |
|---|---|---|
| [`AutoBootstrapOnFallOffLog`](#autobootstraponfallofflog) | `bool` | `true` |
| [`OperatorReseedMinInterval`](#operatorreseedmininterval) | `TimeSpan` | 1 minute |
| [`BootstrapTransientRetry`](#bootstraptransientretry) | `BoundedExponentialRetryPolicyOptions?` | `null` uses the built-in policy |
| [`MaintenanceFallOffCheckInterval`](#maintenancefalloffcheckinterval) | `TimeSpan` | 30 seconds |

### Anti-entropy and remediation

| Option | Type | Default |
|---|---|---|
| [`DigestProbeEnabled`](#digestprobeenabled) | `bool` | `false` |
| [`DigestProbeInterval`](#digestprobeinterval) | `TimeSpan` | 5 minutes |
| [`DigestProbeJitter`](#digestprobejitter) | `double` | 0.2 |
| [`MerkleWalkEnabled`](#merklewalkenabled) | `bool` | `false` |
| [`MerkleWalkMaxDepth`](#merklewalkmaxdepth) | `int` | 16 |
| [`MerkleWalkMaxBytes`](#merklewalkmaxbytes) | `long` | 1 MiB |
| [`LeafReReplayEnabled`](#leafrereplayenabled) | `bool` | `false` |
| [`LeafReReplayMaxEntries`](#leafrereplaymaxentries) | `int` | 4096 |
| [`LeafReReplayMaxBytes`](#leafrereplaymaxbytes) | `long` | 1 MiB |
| [`BootstrapFallbackEnabled`](#bootstrapfallbackenabled) | `bool` | `false` |
| [`BootstrapFallbackMaxEntries`](#bootstrapfallbackmaxentries) | `int` | 4096 |
| [`BootstrapFallbackMaxBytes`](#bootstrapfallbackmaxbytes) | `long` | 1 MiB |
| [`AutoRemediateOnDigestMismatch`](#autoremediateondigestmismatch) | `bool` | `false` |
| [`RemediationTrafficBudgetFraction`](#remediationtrafficbudgetfraction) | `double` | 0.01 |
| [`RemediationTrafficWindow`](#remediationtrafficwindow) | `TimeSpan` | 1 minute |
| [`RemediationFailureThreshold`](#remediationfailurethreshold) | `int` | 3 |
| [`RemediationCircuitResetInterval`](#remediationcircuitresetinterval) | `TimeSpan` | 5 minutes |

### Wire-version and adaptive batch sizing

| Option | Type | Default |
|---|---|---|
| [`WireVersionNegotiationEnabled`](#wireversionnegotiationenabled) | `bool` | `false` |
| [`MinimumSupportedWireVersion`](#minimumsupportedwireversion) | `int` | 1 |
| [`UnknownPeerWireVersionFloor`](#unknownpeerwireversionfloor) | `int` | `EncodedBatchHeader.CurrentWireVersion` |
| [`AdaptiveBatchSizingEnabled`](#adaptivebatchsizingenabled) | `bool` | `true` |
| [`AdaptiveBatchIncrement`](#adaptivebatchincrement) | `int` | 8 |
| [`AdaptiveBatchDecreaseFactor`](#adaptivebatchdecreasefactor) | `double` | 0.5 |
| [`AdaptiveBatchLatencyThreshold`](#adaptivebatchlatencythreshold) | `TimeSpan` | 1 s |
| [`AdaptiveBatchWindowLength`](#adaptivebatchwindowlength) | `int` | 16 |

## Option guidance

### `ClusterId`

The local cluster id stamped onto authored mutations and used for cycle-breaking. Set a stable, non-empty value that is unique within the replication topology.

`ClusterId` is a **per-tree named option**, but a cross-tree atomic write carries a guard verdict reached on one participating tree across the tree boundary to another, and that step is sound only when the trees resolve the *same* origin cluster id. No options validator can check that relation, because it is handed one tree's options at a time; the check therefore runs where a participant set first exists - at admission of a cross-tree write, and at the replicated cross-tree barrier's wait-set freeze on the receiver - and throws `InvalidOperationException` naming both trees and their resolved ids before anything is staged or dispatched. The check is on *agreement*, not on any particular value, so a host that never configured replication (uniform empty cluster id) always passes. Configure `ClusterId` cluster-wide - `AddLatticeReplication` registers it for every named options instance - and avoid per-tree overrides on trees you span in a single cross-tree write. See [Atomic Writes](../lattice/atomic-writes.md#guarantees-and-non-guarantees).

`ClusterId` also has to match what the gRPC transport stamps on its calls. Every push, peer high-water-mark probe, and content-manifest exchange request names the tree's own `ClusterId` as its origin, while the transport's `x-lattice-replication-origin` header carries `LatticeReplicationGrpcOptions.LocalClusterId` or, when that is unset, the cluster-wide `ClusterId`, fixed when a peer's channel is built and so the same for every tree that talks to that peer; a receiver refuses those calls when the header is absent or the two differ. A per-tree `ClusterId` override that differs from the cluster-wide value therefore gets that tree's pushes and probes refused. The stamped header value is also the id a receiver looks up when [`BindCredentialToOriginCluster`](#transport-security---latticereplicationsecurityoptions) is on (the default). See [Transport Security](transport-security.md#grpc-transport-behavior).

### `ReplicatedTrees`

Per-tree opt-in map from tree id to merge mode. A tree absent from the map does not replicate unless runtime replication config is enabled (`AddLatticeReplication(..., enableRuntimeConfig: true)`), in which case a tree enabled at runtime replicates too - see [Runtime Replication Config](runtime-config.md). See [Replication Modes](replication-modes.md) for mode selection.

### `KeyFilter`

Optional producer-side predicate. Use it when the replicated subset cannot be described by prefixes. The shipper applies it (and `KeyPrefixes`) before shipping, so a filtered key is never shipped incrementally. Snapshot exports and the opt-in anti-entropy repair paths do not apply it, so a peer that bootstraps from this cluster still receives filtered keys - do not treat it as a data-residency boundary.

### `KeyPrefixes`

Optional prefix allowlist. Prefer prefixes over `KeyFilter` when possible because they are simpler to audit and explain operationally.

### `ReplogPartitions`

Number of WAL partitions per replicated tree. Increase to spread write and ship load; keep consistent with storage-provider capacity. Existing retained WAL and consumers are sensitive to partitioning, so plan changes carefully.

The shipper and the change feed iterate `[0, ReplogPartitions)` directly, so the value must equal the tree's WAL partition count: a value below the core `LatticeOptions.WalPartitions` skips every write routed to a higher partition. Configure the count here - the [mirror](#wal-and-replog) copies it onto `WalPartitions` - rather than on `LatticeOptions.WalPartitions` alone, because nothing copies it back.

### `WalStorageProvider`

Optional per-tree WAL backend resolver. Leave `null` to use the registered default. Use a resolver when different trees need different WAL durability or placement.

### `WalMaxBatchEntries`

Maximum entries coalesced into one WAL append batch. Lower values reduce tail latency; higher values improve throughput until storage or message-size limits bind.

### `WalMaxBatchBytes`

Maximum byte budget for a WAL batch. Keep below provider transaction and message limits.

### `WalMaxPendingBatches`

Maximum pending WAL batches per partition. Raising it increases pipeline depth and memory; lowering it applies back-pressure earlier. It reaches the WAL only through the [mirror](#wal-and-replog), and an unconfigured tree runs at the core `LatticeOptions.WalMaxPendingBatches` default of `16`, not at this option's `4`.

### `MaxApplyRetries`

Retry budget before a poison inbound entry is moved to the dead-letter queue. Raise only when failures are usually transient.

### `SagaDeferralTimeout`

Wall-clock bound for a receiver-side deferred saga record. When a prepare has exhausted `MaxApplyRetries` and remains deferred for this long, the receiver poisons that saga, parks the deferred prepare with `reason=poisoned_saga`, withholds the saga's terminals until the re-seed retires the poison, and starts or records an owed full re-seed from the origin. A deferred `TxCommit` or `TxAbort` terminal gets the same bound (#4692): its saga is poisoned and re-seeded the same way, and the terminal itself is withheld, never parked.

### `DeadLetterQueueCapacity`

Maximum retained dead-letter entries per tree. Size for the largest operator triage window you need. A full queue never evicts: it refuses further parks and holds the affected replication link back (the link reports Stalled) until parked entries are replayed or discarded, because every parked entry was acknowledged and evicting it would lose the write. See [Capacity and backpressure](dead-letter-queue.md#capacity-and-backpressure).

### `CausalAppliedIdentityCapacity`

How many applied write identities `(origin, HLC)` a receiver remembers per tree and origin ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)). An entry whose causal dependency names a remembered write is released at once. A dependency whose identity was forgotten - evicted past this capacity, lost to a reactivation, or applied in another tree - is decided by the origin's low watermark instead (see [Causal-dependency gate](replication-apply.md#6-causal-dependency-gate)). The record is in memory only, so the capacity trades memory for latency, never correctness. It must be between 1 and 1,048,576.

### `CausalBufferMaxEntries`

Maximum entries parked while waiting for causal dependencies. Increase for highly concurrent, cross-cluster workloads with expected reordering.

### `CausalBufferMaxBytes`

Byte cap for the causal buffer. This bounds receiver memory when dependencies lag. Must be at least 64 KiB (`65536`).

### `ShadowForwardDedupeCacheSize`

Capacity of the per-tree recent-apply cache that suppresses a repeated `(origin, hlc, key, op)` point write on the receiver - a structural shadow-forward duplicate or any other re-delivery. A repeat that has aged out of the cache still applies idempotently at the leaf, so raising it trades memory for fewer leaf round trips on duplicates. Must be `>= 64`.

### `ApplyMaxParallelRuns`

Maximum concurrent receiver apply runs. The default serializes apply for strongest ordering simplicity. Raise only after validating receiver storage and causal-buffer behaviour.

### `ContentHashDedupEnabled`

Enables measurement of redundant payloads by content hash. It is observability-only unless elision is also enabled.

### `ContentHashDedupCacheSize`

Number of content hashes retained for dedup measurement and optional elision. Must be `>= 64`.

### `PreShipCoalescingEnabled`

Collapses redundant per-key versions before shipping. Keep enabled for normal deployments; disable for debugging exact WAL-to-wire shape.

### `ContentHashDedupElisionEnabled`

Enables actual payload elision for repeated content. The receiver elides only a write it already merged exactly - the same content hash, origin, and source HLC - while its leaf still holds the key at that version or newer; the same bytes at another version always ship. It is off by default because it changes what is carried on the wire, even though decoding remains part of the public protocol. It requires `ContentHashDedupEnabled`: the options validator rejects elision with the master switch off.

### `WalRetention`

Optional wall-clock hard ceiling on retained WAL. `null` means consumers and cursors drive retention. If set too low, lagging peers may fall off the log and require bootstrap.

For a local consumer, a fall-off is self-healing: the next read surfaces the trimmed prefix to the auto-bootstrap trigger. A **cross-cluster shipper is different** - the receiver-side fall-off detector only compares against the receiver's own local WAL, so it never sees entries it never received from the sender. The source shipper therefore detects sender WAL trims itself: if a shipping read returns a first sequence greater than the requested cursor, it records a re-seed epoch, withholds saga records, and asks the receiver to re-seed through `ReplicationBatch.ReseedAfterEpoch`. A custom transport that drops that field fails closed: saga records remain withheld and the link stays stalled until an operator re-seeds or the transport carries the request.

To prevent that footgun, the silo **refuses to start** when a replicated tree (declared in [`ReplicatedTrees`](#replicatedtrees)) has an effective `WalRetention` set while the anti-entropy detection backstop [`DigestProbeEnabled`](#digestprobeenabled) is off. Resolve it by one of: enable `DigestProbeEnabled` (and, for automatic repair, the remaining [anti-entropy stages](automatic-drift-remediation.md)) so the divergence is detected and healed out-of-band; remove `WalRetention` from the tree so a lagging shipper pins the WAL until it catches up; or set [`AllowWalRetentionWithoutAntiEntropy`](#allowwalretentionwithoutantientropy) to acknowledge the risk explicitly. The effective retention is read from the per-tree core `LatticeOptions.WalRetention`, which already reflects any value mirrored from this replication-side `WalRetention`, so the rule catches retention configured on either surface.

A peer that is unreachable for good holds more than the log. It also holds every tombstone of the trees it replicates: the [tombstone reap gate](replication-drivers.md#tombstone-reap-gate) reaps nothing a peer may still lack, so those tombstones accumulate until the peer is removed from `ReplicationPeers` and its shipper detaches from the log ([#4615](https://github.com/NSTA1/Orleans.Lattice/issues/4615)). `WalRetention` does not bound that growth.

### `AllowWalRetentionWithoutAntiEntropy`

Escape hatch (default `false`) that permits `WalRetention` on a replicated tree while `DigestProbeEnabled` is off, suppressing the startup guard described above. Set it to `true` only when the silent-divergence risk is knowingly acceptable - for example a strictly unidirectional deployment where the retention-trimming cluster is never a receiver, or where drift is reconciled out of band. It is a deliberate, audited acknowledgement, not a convenience default.

### `AutoBootstrapOnFallOffLog`

When enabled, a receiver-side local fall-off detection makes this cluster re-seed the tree automatically from a snapshot of the source cluster it fell behind; when disabled the detection is still counted on `peer.fell_off_log`. Source-side WAL trim gaps use the shipper's sequence-gap request path described in [Auto-Bootstrap](auto-bootstrap.md).

### `OperatorReseedMinInterval`

Rate limit for routine operator snapshot requests per tree and source cluster. Use `ForceRequestSnapshotAsync` for intentional bypasses.

### `BootstrapTransientRetry`

Optional retry policy for transient bootstrap failures. `null` installs the built-in bounded exponential policy: `DefaultBootstrapMaxAttempts` (4) attempts, a `DefaultBootstrapInitialRetryDelay` (500 ms) initial delay doubling up to `DefaultBootstrapMaxRetryDelay` (30 seconds), classified by `LatticeBootstrapTransientFaultClassifier.IsTransient`. A classified-transient fault re-opens the full snapshot with no upper bound (entries the failed attempt already applied re-apply as LWW no-ops); any other fault fails the bootstrap on the first occurrence. Supplying an instance replaces the whole policy - its unset fields take `BoundedExponentialRetryPolicyOptions`' own defaults (4 attempts, 50 ms, 2 seconds), not the bootstrap built-ins - while a `null` classifier still falls back to `LatticeBootstrapTransientFaultClassifier.IsTransient`. See [Snapshot Bootstrap](snapshot-bootstrap.md).

### `ReplicationPeers`

Static peer cluster ids. For dynamic membership, register `IReplicationTopology` instead. See [Replication Drivers](replication-drivers.md#peer-configuration-topology-vs-replicationpeers).

### `ShipBatchSize`

Target number of entries per outbound batch. Larger values improve throughput and compression; smaller values reduce latency and retry cost.

### `ShipPartitionPageSize`

Number of entries read from each WAL partition page during shipping. Tune with `ShipBatchSize` to balance page fan-out and batch fill.

### `ShipCursorWriteInterval`

Number of successful batches between persisted cursor writes. Lower values reduce replay after sender restart; higher values reduce cursor-write overhead.

### `ShipCursorWriteMaxDelay`

Wall-clock maximum delay before persisting ship cursor progress even if the interval count has not been reached. Set `Timeout.InfiniteTimeSpan` to coalesce purely by `ShipCursorWriteInterval`; any other value must be greater than zero.

### `ShipMaxInFlight`

Maximum concurrent sends per tree and peer. Keep at 1 unless the transport and receiver can tolerate pipelined acks.

### `ShipBackoffInitial`

Initial retry delay after a failed send.

### `ShipPhaseTimerPeriod`

Cadence for the shipping phase timer. Shorter periods reduce idle latency and increase timer churn.

### `ShipSourceIdentityBackstopInterval`

Safety-net cadence for re-resolving the source tree's physical identity from the registry. The shipper binds to the logical source tree's current physical WAL at activation and normally rebinds **reactively** - the tree registry pushes an alias-change notification whenever the tree's alias changes - a shadow-cutover restore or its revert, a resize or its undo, a schema remediation, or an operator alias change (see [Source-identity rebind](replication-drivers.md#source-identity-rebind)), so the steady-state pump performs **no** per-tick registry read on an idle tree. This interval bounds how long a *missed* notification (a transient observer fault, or a shipper that was deactivated across the swap) can leave the shipper bound to a retired physical identity before a coarse backstop resolve heals it. It is deliberately coarse: lowering it trades idle registry-read load for a tighter worst-case heal time, and it is never the primary detection path. Must be greater than zero.

### `LivenessProbeInterval`

Cadence for peer liveness contact when no normal traffic is flowing: an idle pump tick that finds nothing to drain ships an empty batch once this long has passed since the last successful contact. Set `Timeout.InfiniteTimeSpan` to disable the probe; any other value must be greater than zero.

### `ShipBackoffMax`

Cap on the doubled retry delay after repeated send failures. Jitter is applied after the cap, so a jittered delay can exceed it by up to the `ShipBackoffJitter` fraction (36 seconds at the defaults).

### `ShipBackoffJitter`

Randomization fraction applied to backoff to avoid synchronized retries. Must lie in `[0.0, 1.0]`.

### `MaintenanceGcInterval`

Cadence for WAL GC maintenance. The per-tree maintenance driver checks it on a fixed 5-second tick, so a value below 5 seconds runs on every tick.

### `MaintenanceFallOffCheckInterval`

Cadence for fall-off-log checks, checked on the same fixed 5-second maintenance tick.

### `DigestProbeEnabled`

Enables scheduled digest probes. Leave off unless you are operating the anti-entropy stack.

### `DigestProbeInterval`

Cadence for digest probes when enabled.

### `DigestProbeJitter`

Randomization fraction applied to digest probe scheduling. Must lie in `[0.0, 1.0]`.

### `MerkleWalkEnabled`

Enables the Merkle-walk localisation pass after a digest mismatch (it also requires `DigestProbeEnabled`). The walk is read-only: it narrows the divergence to a leaf or a few leaves and never repairs anything itself - repair is the job of the stages below. See [Merkle walks](anti-entropy-merkle-walk.md).

### `MerkleWalkMaxDepth`

Maximum depth a localisation pass descends into a shard's internal-node tree (the shard root is depth `0`) before it aborts.

### `MerkleWalkMaxBytes`

Budget of digest hash bytes - local and remote, summed - a localisation pass may compare before it aborts.

### `LeafReReplayEnabled`

Enables targeted leaf re-replay repair. See [leaf re-replay](anti-entropy-leaf-rereplay.md).

### `LeafReReplayMaxEntries`

Entry budget for targeted replay.

### `LeafReReplayMaxBytes`

Byte budget for targeted replay.

### `BootstrapFallbackEnabled`

Enables snapshot fallback when localized repair cannot use retained WAL. See [bootstrap fallback](anti-entropy-bootstrap-fallback.md).

### `BootstrapFallbackMaxEntries`

Entry budget for fallback snapshot repair.

### `BootstrapFallbackMaxBytes`

Byte budget for fallback snapshot repair.

### `ShipDoorbellEnabled`

When enabled, the commit-time nudge rings the log-tailing shipper's doorbell so a new local write wakes the shipper if it had been deactivated, instead of waiting up to the keepalive reminder for it to re-activate. There is no separate inline ship path - the shipper that tails the WAL is the only producer, and its phase timer (armed on every activation) is the sole drain-and-ship driver. The doorbell is a cheap, edge-triggered wake: it does **not** run the ship pump inline; its only effect is to (re)activate an idle shipper, whose timer then drains on its next tick. A doorbell to an already-active shipper is a no-op, and a missed or coalesced doorbell only delays the next ship by one timer tick.

### `FramingCompression`

Algorithm tag stamped into the framing header. Default `Zstd` (dict-less Zstandard); set `None` to send uncompressed frames. See [core compression](../lattice/compression.md) for the seam, the tag-space partitioning, and the shared-dictionary opt-in.

### `FramingCompressionLevel`

Zstd compression level. Validated to `[1, 22]` when the algorithm is `Zstd` or `ZstdDictionary`; default `3`. The compressors do not read it: the dict-less Zstd compressor `AddLatticeReplication` registers is built at the fixed default level `3`, and the dictionary compressor takes its level from `AddLatticeZstdDictionaryCompressor`, so changing this option alone does not change the compression level. To use a different level, register your own `ILatticeCompressor` built with that level before calling `AddLatticeReplication`.

### `MaxInboundDecompressedBytes`

Hard ceiling on the **decompressed** size of an inbound compressed framing batch. The framing decoder rejects (with `ArgumentException`) any frame whose declared uncompressed length exceeds this *before* it allocates the inflate buffer, bounding the decompression-bomb amplification a hostile or corrupt sender can drive from a tiny request. This is reachable pre-auth on the gRPC transport - framing is decoded before the shared-secret interceptor body runs. Defaults to 64 MiB - 16x the default 4 MiB `WalMaxBatchBytes` ceiling. It is a fixed value that does not follow a configured `WalMaxBatchBytes`, so raise it in step if you legitimately ship larger batches. Must be `>= 1`.

### `FramingCompressionMinBatchBytes`

Uncompressed-tail threshold below which the shipper stamps `Compression = None` for the batch, so heartbeats and small-bursty traffic skip the per-batch fixed overhead. Default `512`; `0` disables the threshold.

### `FramingCompressionDictionaryId`

Stable id of the shared dictionary the shipper requests when `FramingCompression` is `ZstdDictionary`. `0` means "no dictionary" and is the default; required to be non-zero when the algorithm is `ZstdDictionary`.

### `DictionaryNegotiationEnabled`

Enables peer negotiation for shared compression dictionaries.

### `AutoSharedDictionaryEnabled`

Enables automatic shared-dictionary training and distribution when the related registration helper is used. `AddLatticeAutoSharedDictionary` registers the auto-training dictionary provider and turns this option on for every tree; setting the flag without that provider registered does not train anything.

### `WireVersionNegotiationEnabled`

Enables negotiation of effective wire version with peers.

### `MinimumSupportedWireVersion`

Oldest peer wire version the local shipper will interoperate with while `WireVersionNegotiationEnabled` is set. It is a sender-side floor, not a receive-side acceptance check: a peer whose acks advertise a lower `ReplicationAck.SupportedWireVersion` is not shipped to - the shipper logs an error and backs off until the peer upgrades. Must lie in `[1, EncodedBatchHeader.CurrentWireVersion]`. A value below `WireVersionDownEncoder.MinimumDownEncodableWireVersion` (`4`) does not widen what the shipper can serve: a peer that advertises a version below `4`, or below the current version for a tree in a CRDT merge mode, is not shipped to either, because the shipper cannot down-stamp a batch that far.

### `UnknownPeerWireVersionFloor`

Wire version the shipper encodes at for a peer that has not yet advertised a `SupportedWireVersion` on an ack, while `WireVersionNegotiationEnabled` is set. Must lie in `[MinimumSupportedWireVersion, EncodedBatchHeader.CurrentWireVersion]`; lower it below the current version to make un-acked first batches conservative during a rolling upgrade. Only a last-writer-wins tree can be down-stamped, and only to `4` or above (any framing compression is dropped for those batches). A floor the tree cannot be down-stamped to - below `4`, or below the current version on a CRDT-mode tree - blocks every batch and liveness probe to a peer whose capability is still unknown, so no ack arrives to advertise it and replication to that peer stays paused. The advertised capability is held in memory, so each shipper activation starts from this floor again.

### `AdaptiveBatchSizingEnabled`

Enables sender-side adaptive batch size changes based on receiver latency and hints. Enabled by default: the controller's multiplicative decrease shrinks the batch on a repeated send/apply failure (such as a receiver phase-2 commit timeout under burst load) so the stream recovers automatically instead of re-shipping the identical oversized batch, and the additive increase rebuilds toward `ShipBatchSize` once the link is healthy. Set to `false` to restore static sizing.

### `AdaptiveBatchIncrement`

Step used when increasing an adaptive batch cap.

### `AdaptiveBatchDecreaseFactor`

Multiplicative factor used when decreasing an adaptive batch cap after slow or pressured sends. Must lie in the open interval `(0.0, 1.0)`.

### `AdaptiveBatchLatencyThreshold`

Latency threshold that marks a send as slow for adaptive sizing. Defaults to 1 second - above the per-batch ack round-trip a realistic cross-cluster or durable-storage-backed link sustains under load, so the controller only backs off on a genuine sustained climb. Lower it for a fast in-cluster link.

### `AdaptiveBatchWindowLength`

Number of recent observations used by adaptive sizing.

### `AutoRemediateOnDigestMismatch`

Enables automatic repair after digest mismatch. Leave disabled until your transport, budgets, and alerting are in place.

### `RemediationTrafficBudgetFraction`

Fraction of `ShipBatchSize` that one `(tree, peer)` may spend on automatic remediation re-ship per `RemediationTrafficWindow`: the per-window entry budget is `max(1, ceil(RemediationTrafficBudgetFraction x ShipBatchSize))`. It is derived from the configured batch size, not from observed traffic. Must be in `(0.0, 1.0]`.

### `RemediationTrafficWindow`

Window over which remediation traffic budget is measured.

### `RemediationFailureThreshold`

Failure count that opens the remediation circuit.

### `RemediationCircuitResetInterval`

Time before the remediation circuit may reset after failures.

## Health-check thresholds - `LatticeReplicationHealthCheckOptions`

The replication back-pressure health check has its own named options type, `LatticeReplicationHealthCheckOptions`, bound under the health check's registered name (default `"orleans.lattice.replication"`). Its tiered thresholds (`EntriesBehind`, `LastContactSeconds`, `ConsecutiveErrors`), the `UnhealthyAfter` sustained-degraded escalation window, and the opt-in `InboundDegradedAfter` / `InboundCriticalAfter` inbound-silence signals - with every type and default - are documented in [Back-pressure health check](health-check.md).

## Receiver flow-control tuning - `WalSaturationReceiverFlowControlOptions`

The default receiver-side flow-control policy is tuned through `WalSaturationReceiverFlowControlOptions` (`ThrottledBatchRatio` default `0.5`, `ThrottledPauseMs` default `50`, `SaturatedBatchSize` default `1`, `SaturatedPauseMs` default `500`), bound per tree and force-installed via `AddWalSaturationReceiverFlowControl`. Every knob, its type, and its default is documented in [Receiver-side flow control](receiver-flow-control.md).

## Transport security - `LatticeReplicationSecurityOptions`

Shared-secret authentication policy has its own options type, `LatticeReplicationSecurityOptions`, set with `ConfigureLatticeReplicationSecurity`. It carries four knobs: `RequireAuthentication` (`bool`, default `true`), `BindCredentialToOriginCluster` (`bool`, default `true`), `SecretRefreshInterval` (`TimeSpan`, default 30 seconds), and `ScanConfigurationForSecrets` (`bool`, default `true`). Secret material is not an option: it flows through `ILatticeReplicationSecretSource`, whose default implementation reads the `LATTICE_REPLICATION_SECRET` and `LATTICE_REPLICATION_ACCEPTED_SECRETS` environment variables plus per-peer `LATTICE_REPLICATION_PEER_SECRET__<CLUSTERID>` overrides; replace it with `AddLatticeReplicationSecrets` or `AddLatticeReplicationSecretsFromConfiguration`. Rotation, the startup configuration scan, and every knob are documented in [Transport Security](transport-security.md).

`BindCredentialToOriginCluster` binds the presented secret to the cluster the call claims to come from. The accepted-secret set carries no peer attribution, so matching it proves only that the caller holds some accepted secret. With the option on, and only while `RequireAuthentication` is on, the gRPC receiver's interceptor also reads the call's `x-lattice-replication-origin` header, resolves the secret this cluster would itself send to that cluster (`ILatticeReplicationSecretSource.GetOutboundSecretAsync`, through the caching provider), and refuses the call with `PermissionDenied` unless the presented secret equals it, compared in constant time. A call with no origin header, an origin for which no secret resolves, and a mismatched secret all get the same refusal. The check covers every RPC of the push, snapshot, and saga control services. Under a single cluster-wide secret every peer resolves the same value, so the check passes for any stamped origin; under a symmetric per-peer scheme it ties each origin to its own secret. An asymmetric per-peer scheme, where a peer deliberately presents a secret other than the one this cluster sends it, must set the option to `false` and then has no origin binding.

The receiver resolves that secret by the id the sender stamps - the sender's `LatticeReplicationGrpcOptions.LocalClusterId`, or its cluster-wide `ClusterId` when that is unset - not by the key its own `Peers` map uses for the sender, so a per-peer secret has to be configured on the receiver under the stamped id (for the default source, `LATTICE_REPLICATION_PEER_SECRET__<CLUSTERID>` for that id). Because the comparison uses the receiver's own outbound secret rather than its accepted set, the accepted-set rotation window does not cover it: while one cluster sends a new `LATTICE_REPLICATION_SECRET` and its peer still sends the old one, the calls between them are refused in both directions until both have switched and their cached outbound secrets have refreshed (`SecretRefreshInterval`).

## gRPC replication transport options

The gRPC transport ships as the separate `Orleans.Lattice.Replication.Grpc` package. Register it with `AddLatticeReplicationGrpc`, map endpoints with `MapLatticeReplicationGrpc`, and configure peer endpoints and channels through `LatticeReplicationGrpcOptions`. Every option and operational note lives in [Orleans.Lattice.Replication.Grpc configuration](../lattice.replication.grpc/configuration.md); see also [Transport Security](transport-security.md).

## Azure Table WAL storage options

The durable Azure Table WAL backend ships as the separate `Orleans.Lattice.Storage.AzureTable` package. Register it with `AddAzureTableWalStorage` and configure it through `AzureTableWalStorageOptions` (authentication, table, retry, pipelining, saturation, and compression). Every option and tuning note lives in [Orleans.Lattice.Storage.AzureTable configuration](../lattice.storage.azuretable/configuration.md). For the core WAL provider model, see [WAL Storage Providers](../lattice/wal-storage-providers.md); for replication WAL behaviour, see [WAL](wal.md).
