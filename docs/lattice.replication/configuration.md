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

`LatticeReplicationOptionsValidator` validates startup options. It rejects empty cluster ids, invalid replicated-tree declarations, non-positive sizes, invalid intervals, invalid jitter and factor ranges, and incompatible wire-version, compression, adaptive-batch, and remediation bounds.

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
| [`MaintenanceGcInterval`](#maintenancegcinterval) | `TimeSpan` | 5 seconds |

### Apply and causal buffer

| Option | Type | Default |
|---|---|---|
| [`MaxApplyRetries`](#maxapplyretries) | `int` | 5 |
| [`DeadLetterQueueCapacity`](#deadletterqueuecapacity) | `int` | 1000 |
| [`CausalBufferMaxEntries`](#causalbuffermaxentries) | `int` | 1024 |
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
| [`MaxInboundDecompressedBytes`](#maxinbounddecompressedbytes) | `long` | `16 * WalMaxBatchBytes` |
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

### `ReplicatedTrees`

Per-tree opt-in map from tree id to merge mode. A tree absent from the map does not replicate. See [Replication Modes](replication-modes.md) for mode selection.

### `KeyFilter`

Optional producer-side predicate. Use it when the replicated subset cannot be described by prefixes. It runs before shipping, so filtered keys never leave the source cluster.

### `KeyPrefixes`

Optional prefix allowlist. Prefer prefixes over `KeyFilter` when possible because they are simpler to audit and explain operationally.

### `ReplogPartitions`

Number of WAL partitions per replicated tree. Increase to spread write and ship load; keep consistent with storage-provider capacity. Existing retained WAL and consumers are sensitive to partitioning, so plan changes carefully.

### `WalStorageProvider`

Optional per-tree WAL backend resolver. Leave `null` to use the registered default. Use a resolver when different trees need different WAL durability or placement.

### `WalMaxBatchEntries`

Maximum entries coalesced into one WAL append batch. Lower values reduce tail latency; higher values improve throughput until storage or message-size limits bind.

### `WalMaxBatchBytes`

Maximum byte budget for a WAL batch. Keep below provider transaction and message limits.

### `WalMaxPendingBatches`

Maximum pending WAL batches per partition. Raising it increases pipeline depth and memory; lowering it applies back-pressure earlier.

### `MaxApplyRetries`

Retry budget before a poison inbound entry is moved to the dead-letter queue. Raise only when failures are usually transient.

### `DeadLetterQueueCapacity`

Maximum retained dead-letter entries per tree. Size for the largest operator triage window you need. See [Dead-Letter Queue](dead-letter-queue.md).

### `CausalBufferMaxEntries`

Maximum entries parked while waiting for causal dependencies. Increase for highly concurrent, cross-cluster workloads with expected reordering.

### `CausalBufferMaxBytes`

Byte cap for the causal buffer. This bounds receiver memory when dependencies lag.

### `ShadowForwardDedupeCacheSize`

Dedup cache for structural shadow-forward entries. Increase if topology maintenance creates many recent duplicate forwards.

### `ApplyMaxParallelRuns`

Maximum concurrent receiver apply runs. The default serializes apply for strongest ordering simplicity. Raise only after validating receiver storage and causal-buffer behaviour.

### `ContentHashDedupEnabled`

Enables measurement of redundant payloads by content hash. It is observability-only unless elision is also enabled.

### `ContentHashDedupCacheSize`

Number of content hashes retained for dedup measurement and optional elision.

### `PreShipCoalescingEnabled`

Collapses redundant per-key versions before shipping. Keep enabled for normal deployments; disable for debugging exact WAL-to-wire shape.

### `ContentHashDedupElisionEnabled`

Enables actual payload elision for repeated content. It is off by default because it changes what is carried on the wire, even though decoding remains part of the public protocol.

### `WalRetention`

Optional wall-clock hard ceiling on retained WAL. `null` means consumers and cursors drive retention. If set too low, lagging peers may fall off the log and require bootstrap.

### `AutoBootstrapOnFallOffLog`

When enabled, a peer that falls behind retained WAL is re-seeded automatically from a snapshot. See [Auto-Bootstrap](auto-bootstrap.md).

### `OperatorReseedMinInterval`

Rate limit for routine operator snapshot requests per tree and source cluster. Use `ForceRequestSnapshotAsync` for intentional bypasses.

### `BootstrapTransientRetry`

Optional retry policy for transient bootstrap failures. `null` installs the built-in bounded exponential policy.

### `ReplicationPeers`

Static peer cluster ids. For dynamic membership, register `IReplicationTopology` instead. See [Replication Drivers](replication-drivers.md#peer-configuration-topology-vs-replicationpeers).

### `ShipBatchSize`

Target number of entries per outbound batch. Larger values improve throughput and compression; smaller values reduce latency and retry cost.

### `ShipPartitionPageSize`

Number of entries read from each WAL partition page during shipping. Tune with `ShipBatchSize` to balance page fan-out and batch fill.

### `ShipCursorWriteInterval`

Number of successful batches between persisted cursor writes. Lower values reduce replay after sender restart; higher values reduce cursor-write overhead.

### `ShipCursorWriteMaxDelay`

Wall-clock maximum delay before persisting ship cursor progress even if the interval count has not been reached.

### `ShipMaxInFlight`

Maximum concurrent sends per tree and peer. Keep at 1 unless the transport and receiver can tolerate pipelined acks.

### `ShipBackoffInitial`

Initial retry delay after a failed send.

### `ShipPhaseTimerPeriod`

Cadence for the shipping phase timer. Shorter periods reduce idle latency and increase timer churn.

### `LivenessProbeInterval`

Cadence for peer liveness contact when no normal traffic is flowing.

### `ShipBackoffMax`

Maximum retry delay after repeated send failures.

### `ShipBackoffJitter`

Randomization fraction applied to backoff to avoid synchronized retries. Must be within validator bounds.

### `MaintenanceGcInterval`

Cadence for WAL GC maintenance.

### `MaintenanceFallOffCheckInterval`

Cadence for fall-off-log checks.

### `DigestProbeEnabled`

Enables scheduled digest probes. Leave off unless you are operating the anti-entropy stack.

### `DigestProbeInterval`

Cadence for digest probes when enabled.

### `DigestProbeJitter`

Randomization fraction applied to digest probe scheduling.

### `MerkleWalkEnabled`

Enables Merkle walk repair after a digest mismatch. See [Merkle walks](anti-entropy-merkle-walk.md).

### `MerkleWalkMaxDepth`

Maximum Merkle descent depth per repair attempt.

### `MerkleWalkMaxBytes`

Byte budget for Merkle walk probe traffic.

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

Compression algorithm for the replication framing tail. Default `Zstd`; set `None` to send uncompressed frames. See [core compression](../lattice/compression.md).

### `FramingCompressionLevel`

Compression level for framing compression.

### `MaxInboundDecompressedBytes`

Receiver safety cap for decompressed inbound frames.

### `FramingCompressionMinBatchBytes`

Minimum batch size before compression is attempted.

### `FramingCompressionDictionaryId`

Operator-supplied dictionary id for dictionary compression modes. `0` means no shared dictionary id.

### `DictionaryNegotiationEnabled`

Enables peer negotiation for shared compression dictionaries.

### `AutoSharedDictionaryEnabled`

Enables automatic shared-dictionary training and distribution when the related registration helper is used.

### `WireVersionNegotiationEnabled`

Enables negotiation of effective wire version with peers.

### `MinimumSupportedWireVersion`

Lowest wire version this node will accept.

### `UnknownPeerWireVersionFloor`

Effective version floor assumed before a peer reports capabilities.

### `AdaptiveBatchSizingEnabled`

Enables sender-side adaptive batch size changes based on receiver latency and hints. Enabled by default: the controller's multiplicative decrease shrinks the batch on a repeated send/apply failure (such as a receiver phase-2 commit timeout under burst load) so the stream recovers automatically instead of re-shipping the identical oversized batch, and the additive increase rebuilds toward `ShipBatchSize` once the link is healthy. Set to `false` to restore static sizing.

### `AdaptiveBatchIncrement`

Step used when increasing an adaptive batch cap.

### `AdaptiveBatchDecreaseFactor`

Multiplicative factor used when decreasing an adaptive batch cap after slow or pressured sends.

### `AdaptiveBatchLatencyThreshold`

Latency threshold that marks a send as slow for adaptive sizing. Defaults to 1 second - above the per-batch ack round-trip a realistic cross-cluster or durable-storage-backed link sustains under load, so the controller only backs off on a genuine sustained climb. Lower it for a fast in-cluster link.

### `AdaptiveBatchWindowLength`

Number of recent observations used by adaptive sizing.

### `AutoRemediateOnDigestMismatch`

Enables automatic repair after digest mismatch. Leave disabled until your transport, budgets, and alerting are in place.

### `RemediationTrafficBudgetFraction`

Fraction of normal replication traffic budget that remediation may consume.

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

## gRPC replication transport options

The gRPC transport ships as the separate `Orleans.Lattice.Replication.Grpc` package. Register it with `AddLatticeReplicationGrpc`, map endpoints with `MapLatticeReplicationGrpc`, and configure peer endpoints and channels through `LatticeReplicationGrpcOptions`. Every option and operational note lives in [Orleans.Lattice.Replication.Grpc configuration](../lattice.replication.grpc/configuration.md); see also [Transport Security](transport-security.md).

## Azure Table WAL storage options

The durable Azure Table WAL backend ships as the separate `Orleans.Lattice.Storage.AzureTable` package. Register it with `AddAzureTableWalStorage` and configure it through `AzureTableWalStorageOptions` (authentication, table, retry, pipelining, saturation, and compression). Every option and tuning note lives in [Orleans.Lattice.Storage.AzureTable configuration](../lattice.storage.azuretable/configuration.md). For the core WAL provider model, see [WAL Storage Providers](../lattice/wal-storage-providers.md); for replication WAL behaviour, see [WAL](wal.md).
