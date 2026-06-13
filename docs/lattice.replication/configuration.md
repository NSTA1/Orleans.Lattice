# Configuration

This document covers the public configuration surface for `Orleans.Lattice.Replication`, the gRPC replication transport, and the Azure Table WAL backend. Compression knobs that are shared with the core package are cross-referenced to [core compression](../lattice/compression.md).

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
| [`AdaptiveBatchSizingEnabled`](#adaptivebatchsizingenabled) | `bool` | `false` |
| [`AdaptiveBatchIncrement`](#adaptivebatchincrement) | `int` | 8 |
| [`AdaptiveBatchDecreaseFactor`](#adaptivebatchdecreasefactor) | `double` | 0.5 |
| [`AdaptiveBatchLatencyThreshold`](#adaptivebatchlatencythreshold) | `TimeSpan` | 50 ms |
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

When enabled, new local writes can wake shipping promptly instead of waiting for the next cadence tick.

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

Enables sender-side adaptive batch size changes based on receiver latency and hints.

### `AdaptiveBatchIncrement`

Step used when increasing an adaptive batch cap.

### `AdaptiveBatchDecreaseFactor`

Multiplicative factor used when decreasing an adaptive batch cap after slow or pressured sends.

### `AdaptiveBatchLatencyThreshold`

Latency threshold that marks a send as slow for adaptive sizing.

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

## gRPC replication transport options

Register the gRPC binding with `AddLatticeReplicationGrpc` and map endpoints with `MapLatticeReplicationGrpc`. See [gRPC Push Transport](grpc-push-transport.md) and [Transport Security](transport-security.md).

| Option | Type | Default |
|---|---|---|
| `Peers` | `IDictionary<string, Uri>` | empty ordinal dictionary |
| `AllowPlaintextEndpoints` | `bool` | `false` |
| `ConfigureChannel` | `Action<string, GrpcChannelOptions>?` | `null` |
| `LocalClusterId` | `string?` | `null` |

`Peers` maps peer cluster id to endpoint. `AllowPlaintextEndpoints` permits `http://` endpoints for development and tests; leave it `false` in production. `ConfigureChannel` customizes each peer's `GrpcChannelOptions`. `LocalClusterId` overrides the outbound origin header; when `null`, the binding uses `LatticeReplicationOptions.ClusterId`.

## Azure Table WAL storage options

Register with `AddAzureTableWalStorage`. This backend is the durable WAL option for production replication deployments. For the core WAL provider model, see [WAL Storage Providers](../lattice/wal-storage-providers.md); for replication WAL behaviour, see [WAL](wal.md).

| Option | Type | Default |
|---|---|---|
| `ConnectionString` | `string?` | `null` |
| `ServiceUri` | `Uri?` | `null` |
| `TokenCredential` | `TokenCredential?` | `null` |
| `SharedKeyCredential` | `TableSharedKeyCredential?` | `null` |
| `ServiceClient` | `TableServiceClient?` | `null` |
| `TableName` | `string` | `"OrleansLatticeWal"` |
| `ConfigureClientOptions` | `Action<TableClientOptions>?` | `null` |
| `RetryMaxAttempts` | `int?` | `null` |
| `RetryDelay` | `TimeSpan?` | `null` |
| `RetryMaxDelay` | `TimeSpan?` | `null` |
| `RetryNetworkTimeout` | `TimeSpan?` | `null` |
| `RetryMode` | `RetryMode?` | `null` |
| `PipelinePhaseTwoCommits` | `bool` | `true` |
| `EliminateCandidateRowOnHotPath` | `bool` | `true` |
| `PipelinedPhaseTwoFaultHandler` | `Action<Exception>?` | `null` |
| `PhaseTwoCoalescingWindow` | `TimeSpan` | 5 ms |
| `PhaseTwoCommitTimeout` | `TimeSpan?` | 3 seconds |
| `HonorSaturationSignal` | `bool` | `true` |
| `SaturationShortCircuitCooldown` | `TimeSpan` | 2 seconds |
| `Compression` | `LatticeCompression` | `Zstd` |
| `CompressionMinPayloadBytes` | `int` | 256 |

Exactly one authentication mode must be configured: connection string, service URI plus token credential, service URI plus shared key credential, or a pre-built `TableServiceClient`. `TableName` must be non-empty. Retry options are passed into Azure Tables client options when set; `ConfigureClientOptions` can apply any additional client customization.

`PipelinePhaseTwoCommits` and `PhaseTwoCoalescingWindow` control the provider's two-phase commit completion path. `EliminateCandidateRowOnHotPath` removes extra candidate-row work from the hot path where supported. `PipelinedPhaseTwoFaultHandler` observes asynchronous phase-two faults. `PhaseTwoCommitTimeout` bounds phase-two work.

`HonorSaturationSignal` lets the provider short-circuit Azure SDK retries while the local WAL saturation signal says the tree is saturated. `SaturationShortCircuitCooldown` controls how long that short-circuit remains active after a saturation observation.

`Compression` and `CompressionMinPayloadBytes` compress stored WAL payloads. Stored rows are self-describing, so changing compression affects newly written rows while existing rows continue to decode with their recorded tags.
