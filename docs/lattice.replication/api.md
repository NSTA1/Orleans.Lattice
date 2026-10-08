---
agent_spec: "docs/agents/api/replication.json"
---

# Replication Public API Reference

This document is the **contract** for the public `Orleans.Lattice.Replication` surface. It describes behaviour in caller-visible terms: what each public type is for, which members matter to callers, and where to find the operational detail. It does not name internal grains or implementation classes that are not public. For the how, follow the topic cross-references in each section.

## Setup

Install the packages you need:

```shell
dotnet add package Orleans.Lattice.Replication
dotnet add package Orleans.Lattice.Replication.Grpc
dotnet add package Orleans.Lattice.Storage.AzureTable
```

Import the replication namespace:

```csharp verify
using Orleans.Lattice.Replication;
```

Register replication on an Orleans silo, then bind a transport. The gRPC package is the canonical live-push and remote-snapshot binding. See [Configuration](configuration.md) for every option and named-options behaviour.

```csharp verify
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
    {
        ["orders"] = LatticeMergeMode.LwwRegister,
    };
    opts.ReplicationPeers = new[] { "site-b" };
});

siloBuilder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.Peers["site-b"] = new Uri("https://site-b.example:5001");
});
```

On the receiving HTTP pipeline, map the gRPC endpoints with `MapLatticeReplicationGrpc`. The gRPC binding is a separate package with its own docs - see [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/README.md) and [Transport Security](transport-security.md).

## Registration and DI

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeReplicationServiceCollectionExtensions` | static class | Registers replication services on an Orleans silo. | `AddLatticeReplication`, `ConfigureLatticeReplication`, `ReplicateLatticeSystemTrees`, `AddLatticeAutoSharedDictionary`, `AddLatticeReplicationHealthCheck`, `AddWalSaturationReceiverFlowControl`, `AddLatticeSagaParticipant` |
| `LatticeReplicationSecurityServiceCollectionExtensions` | static class | Registers shared-secret sources and security options. | `AddLatticeReplicationSecrets`, `AddLatticeReplicationSecretsFromConfiguration`, `ConfigureLatticeReplicationSecurity` |

`AddLatticeReplication` wires the replication pipeline and default no-op transport. A production deployment replaces the transport by adding the gRPC binding or a custom `IReplicationTransport`. `ConfigureLatticeReplication` follows .NET named options: the overload without a tree name sets global defaults; the `treeName` overload overrides a single tree.

The gRPC transport and the Azure Table WAL backend ship as separate packages with their own API references - see [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/api.md) (`AddLatticeReplicationGrpc`, `MapLatticeReplicationGrpc`, `LatticeReplicationGrpcOptions`) and [Orleans.Lattice.Storage.AzureTable](../lattice.storage.azuretable/api.md) (`AddAzureTableWalStorage`, `AzureTableWalStorageProvider`, `AzureTableWalStorageOptions`).

## Replication modes and configuration types

See [Replication Modes](replication-modes.md) and [Replication Drivers](replication-drivers.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeReplicationOptions` | class | Main replication options. | Identity, tree opt-in, WAL, apply, ship, bootstrap, compression, wire-version, and remediation properties. See [Configuration](configuration.md). |
| `FallOffLogDecision` | readonly record struct | Result of a receiver-side local fall-off check: whether the receiver's per-origin high-water mark is older than the local oldest retained WAL entry for that origin, and whether a bootstrap was triggered or absorbed. | `FellOffLog`, `LocalHighWaterMark`, `BootstrapTriggered`, `Suppressed` |
| `OperatorReseedDecision` | readonly record struct | Result of an operator snapshot request. | `Triggered`, `LastRequestedAt`, `RetryAfter` |

`ReplicatedTrees` is the static opt-in map from tree id to `LatticeMergeMode`. Trees not in the map do not ship unless runtime replication config is enabled (`AddLatticeReplication(..., enableRuntimeConfig: true)`), in which case a tree enabled at runtime through `ILatticeReplicationConfigAuthority` replicates too - see [Runtime Replication Config](runtime-config.md). `KeyFilter` and `KeyPrefixes` narrow which keys the shipper emits from opted-in trees; snapshot exports do not apply them.

## Change feed

See [Change Feed](change-feed.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IChangeFeed` | interface | Cursor-driven, pull-based read of locally-authored WAL entries for a tree. | `Subscribe(string, HybridLogicalClock, bool, CancellationToken)`, `Subscribe(string, ChangeFeedCursor, bool, CancellationToken)`, `GetCurrentCursorAsync` |
| `ChangeFeedCursor` | readonly struct | Per-partition offset cursor for lossless WAL consumption. | `Initial`, constructor from offsets, `GetOffsetForPartition`, `PartitionOffsets` |

The feed is for locally-authored writes. Entries installed by inbound apply are visible in local state but are not re-emitted through this feed; consumers that need to observe receiver-side installs should decorate `IReplicationApplier`. The `HybridLogicalClock` overload is a source-compatible shim: the default implementation ignores its cursor and reads every partition from the start, so resume with the `ChangeFeedCursor` overload.

## Transport and wire envelope

See [Transport](transport.md), [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/README.md), and [Wire Format](wire-format.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationTransport` | interface | Sends a batch to a peer cluster and returns the receiver ack. | `SendAsync(ReplicationBatch, CancellationToken)` |
| `ReplicationBatch` | readonly record struct | Logical outbound batch routing metadata plus payload. | `TargetClusterId`, `TreeName`, `OriginClusterId`, `Payload`, `Envelope`, `EncodedEnvelope` |
| `ReplicationBatchEnvelope` | readonly record struct | Decoded transport envelope. | `WireVersion`, `TreeName`, `OriginClusterId`, `Entries`; the `CurrentVersion` and `CurrentMinorVersion` constants |
| `ReplicationBatchEncodedEnvelope` | readonly record struct | Pre-encoded envelope used by transport implementations. | `Header` (an `EncodedBatchHeader`), `EncodedEntries` (the pre-encoded entry segments) |
| `ReplicationAck` | readonly record struct | Receiver acknowledgement and hints. | `Accepted`, `HighestAppliedHlc`, `BlockedAtHlc`, the flow-control hints `SuggestedBatchSize` / `PauseForMs`, and the capability hints `SupportedWireVersion`, `AdvertisedDictionaryIds`, `AdvertisedDictionaries` |
| `EncodedBatchHeader` | readonly record struct | Fixed 32-byte wire framing header. | `Magic`, `WireVersion`, `OriginClusterIdHash`, `EntryCount`, `BatchSequence`, `AtomicBatchSpanCount`, `Mode`, `Compression`, and the in-process `DictionaryId` (carried in the compressed tail, not the fixed header) |

A transport must be idempotent at the batch boundary: sender retries can redeliver a batch, and the receiver deduplicates by exact `(origin, hlc, key, op)` record identity. The gRPC binding that implements this seam (`LatticeReplicationGrpcOptions` and the registration helpers) is documented in [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/api.md).

## Replication apply

See [Replication Apply](replication-apply.md) and [Deltas](deltas.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationApplier` | interface | Applies inbound WAL records to the local tree. | `ApplyAsync`, `ApplyBatchAsync` |
| `ApplyResult` | readonly record struct | Apply outcome and high-water-mark visibility. | `Applied`, `HighWaterMark`, `Deferred` (the batch was held back by an inbound receive fence and must be retried), `SourceLineageRefused` (the sender read it under a source lineage this tree no longer holds; the sender re-resolves its binding, and a refused dead-letter replay stays parked) |
| `IReplicationLocalVcSeeder` | interface | Rebuilds a tree's local vector clock from the vector-clock slots on its values after an intra-cluster snapshot restore, so inbound dependency checks do not run against a zeroed vector. A no-op for a non-replicated tree. | `SeedFromTreeAsync(string, CancellationToken)` returning `LocalVcSeedReport` |
| `LocalVcSeedReport` | readonly record struct | Observable result of local version-vector seeding. | `TreeName`, `Frontier`, `EntriesScanned`, `SeedApplied` |

`ApplyAsync` preserves the source cluster HLC and origin id. `ApplyBatchAsync` is the preferred batch seam because implementations can collapse high-water-mark updates and drain causal buffers once per batch. Its default interface implementation applies each entry through `ApplyAsync` in order and aggregates the results - `Applied` when any entry applied, the pointwise-maximum `HighWaterMark`, and `Deferred` when any entry was deferred - so a custom applier that implements only `ApplyAsync` still has a receive-fence deferral re-shipped by the sender rather than acknowledged past.

## Bootstrap and snapshots

See [Snapshot Bootstrap](snapshot-bootstrap.md), [Auto-Bootstrap](auto-bootstrap.md), and [Automatic Drift Remediation](automatic-drift-remediation.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ISnapshotProvider` | interface | Exports a streaming as-of-HLC view of a tree. | `ExportAsync(string, HybridLogicalClock, CancellationToken)`, source-cluster overload, range-scoped overload |
| `IBootstrapSnapshotSource` | interface | Marker and specialization for bootstrap snapshot sources. | Inherits `ISnapshotProvider` |
| `ILatticeBootstrapCoordinator` | interface | Drives receiver-side snapshot bootstrap. | `GetStateAsync`, `GetStatusAsync`, `BootstrapAsync` |
| `LatticeBootstrapState` | enum | Bootstrap state-machine state. | `Idle`, `RequestingSnapshot`, `ApplyingSnapshot`, `IncrementalHandoff`, `LiveIncremental`, `Failed` |
| `BootstrapCoordinatorStatus` | readonly record struct | Observable bootstrap phase and source cluster. | `Phase`, `SourceClusterId` |
| `SnapshotEntry` | readonly record struct | One row in a snapshot stream. | Key, value, timestamp, prepared/tombstone flags, transaction id, source-shard index, atomic-batch size/index, TTL expiry, delta, and merge-mode slots |
| `SnapshotStream` | sealed class | Async snapshot stream wrapper. | `TreeName`, `AsOfHlc`, `CausalStableFrontier`, `Entries` |
| `IRemoteSnapshotTransport` | interface | Fetches snapshot metadata and stream items from a remote cluster. | `GetMetadataAsync`, `RequestSnapshotAsync` |
| `LatticeRemoteSnapshotService` | sealed class | Public remote snapshot transport service. | Implements `IRemoteSnapshotTransport` |
| `RemoteSnapshotProvider` | sealed class | Snapshot provider backed by a remote transport. | Implements `IBootstrapSnapshotSource` |
| `RemoteSnapshotMetadata` | readonly record struct | Remote snapshot metadata response. | `TreeName`, `SourceClusterId`, `AsOfHlc`, `CausalStableFrontier` |
| `RemoteSnapshotMetadataRequest` | readonly record struct | Remote metadata request. | `TreeName`, `SourceClusterId`, `FromAsOfHlc` (a strict upper-bound filter; `HybridLogicalClock.Zero` disables it) |
| `RemoteSnapshotStreamItem` | readonly record struct | One item returned by remote snapshot streaming. | `Entry` (one `SnapshotEntry` per streamed message) |
| `LatticeBootstrapTransientFaultClassifier` | static class | Default transient-fault classifier for the receiver-side bootstrap drain: timeouts, HTTP / socket / IO faults, retryable gRPC statuses (`Unavailable`, `DeadlineExceeded`, `Aborted`), and an expired Orleans enumeration session retry with bounded backoff; anything else pivots the bootstrap to `Failed`. | `IsTransient(Exception)` |

Bootstrap applies snapshot entries through the same public apply seam as incremental replication, then hands off to live shipping at the snapshot frontier.

## Dead-letter queue

See [Dead-Letter Queue](dead-letter-queue.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationDeadLetters` | interface | Lists, discards, and replays quarantined apply failures; host-trusted receiver saga poison and quarantine release. | `ListAsync`, `CountAsync`, `DiscardAsync`, `ReplayAsync`, `PoisonSagaAsync`, `ReleaseQuarantinedSagaAsync` |
| `DeadLetterEntry` | readonly record struct | Retained failed apply entry. | `EntryId`, `Entry`, `FailureReason`, `RetryCount`, `EnqueuedAtTicks`, `SourceLineageClusterId`, `SourceLineage` (the source lineage the entry's sender stamped, which a replay is checked against; `null` when unstamped) |

Replay runs the parked entry through the canonical applier and removes it on any non-throwing, non-deferred return, whether or not the entry applied. A replay that an in-flight coordinated restore's receive fence defers leaves the entry parked, as does a thrown exception (see [Replay semantics](dead-letter-queue.md#replay-semantics)).

## Operator, admin, and WAL introspection

See [Auto-Bootstrap](auto-bootstrap.md), [WAL](wal.md), and [Observability](observability.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationAdmin` | interface | Operator-driven snapshot re-seed controls, and two alarmed overrides: lifting the read fence a failed snapshot bootstrap left up ([Snapshot bootstrap](snapshot-bootstrap.md)), and resolving a prepared coordinated-restore participant whose coordinator is lost ([Coordinated restore](coordinated-restore.md#resolving-a-participant-whose-coordinator-is-lost)). Both overrides are default members that throw `NotSupportedException` on an implementation that does not support them. | `RequestSnapshotAsync`, `ForceRequestSnapshotAsync`, `ForceLiftBootstrapReadFenceAsync`, `ResolveCrossClusterSagaParticipantAsync` |
| `ILatticeWalIntrospection` | interface | Sender-side view of retained WAL availability. | `GetOldestAvailableHlcAsync`, `GetOldestAvailableHlcByOriginAsync` |
| `ILatticeFallOffLogDetector` | interface | Receiver-side check of the local per-origin high-water mark against this receiver's local retained WAL for that origin; on local fall-off it records the metric and, when `AutoBootstrapOnFallOffLog` is on, starts a bootstrap. Source WAL trims are detected by the sender shipper and carried as `ReplicationBatch.ReseedAfterEpoch`. | `CheckAndTriggerAsync` returning `FallOffLogDecision` |

The admin surface rate-limits routine re-seeds through `OperatorReseedMinInterval`; the force method bypasses that rate limit for disaster-recovery and scheduled re-seed scenarios.

## Cursor and GC surface

Public cursor state is represented by `ChangeFeedCursor` (per-partition offsets for change-feed consumers) and by the core `IWalCursorRegistry`, into which each per-peer shipper reports its durable ship cursor and the blocked floor the receiver stamps on each ack; `ILatticeWalIntrospection` reports how far back the retained WAL reaches. WAL retention is controlled through `LatticeReplicationOptions.WalRetention` and `MaintenanceGcInterval`; the public contract is that consumers advance cursors and GC trims only what policy permits. See [WAL](wal.md).

## Encoder and wire-version surface

See [Wire Format](wire-format.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationBatchEncoder` | interface | Encodes and decodes replication batches. | `ContentType`, `CurrentWireVersion`, `Encode`, `Decode`, and the default-implemented `EncodeFraming` / `TryDecodeFraming` |
| `EncodedBatchHeader` | readonly record struct | Fixed frame header. | `WriteTo`, `ReadFrom`, `HashClusterId`, and the `WireSize` (32), `MagicValue`, and `CurrentWireVersion` (currently `5`) constants |
| `WireVersionNegotiation` | static class | Computes an effective wire version from local and peer capabilities. | `Negotiate` |
| `WireVersionNegotiationResult` | readonly record struct | Negotiation result. | `EffectiveWireVersion`, `DowngradeActive`, `PeerCapabilityKnown` |
| `WireVersionDownEncoder` | static class | Down-stamps frames for older receivers. | `MinimumDownEncodableWireVersion`, `EnsureDownEncodable`, `PrepareHeader` |
| `ReplicationTypeAliases` | static class | Centralises the stable `olr.`-prefixed Orleans serialization aliases of the replication wire types. | None: every alias constant is `internal`, so the class exposes no public members |

Use this surface when writing a transport or compatibility shim. Most application hosts only configure the related options in [Configuration](configuration.md#wire-version-and-adaptive-batch-sizing).

## Metrics types

See [Observability](observability.md) and [Health Check](health-check.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeReplicationMetrics` | static class | Meter, counter, histogram, and tag names for replication telemetry. | Public constants and instrument names |
| `ReplicationPeerStats` | class | Per-peer metrics accumulator. | `RecordBacklog`, `RecordInFlight`, `RecordSuccess`, `RecordError`, `RecordInboundSuccess`, `RecordInboundError`, `Snapshot` |
| `ReplicationPeerSnapshot` | readonly record struct | Point-in-time per-peer telemetry. | `Tree`, `Peer`, `EntriesBehind`, `BytesBehind`, `ConsecutiveErrors`, `LastContactSeconds`, `Direction`, `InFlight` |
| `ReplicationContactDirection` | enum | Direction tag for peer contact. | `Outbound`, `Inbound` |
| `WireVersionNegotiationState` | class | Runtime wire-version telemetry state. | `Record`, `Snapshot` |
| `WireVersionNegotiationSnapshot` | readonly record struct | Wire-version telemetry snapshot. | `Tree`, `Peer`, `NegotiatedVersion`, `DowngradeActive`, `PeerCapabilityKnown` |
| `LatticeReplicationHealthCheckOptions` | sealed class | Health-check thresholds. | `EntriesBehind`, `LastContactSeconds`, `ConsecutiveErrors`, `UnhealthyAfter`, `InboundDegradedAfter`, `InboundCriticalAfter`; the nested `LongTier` / `DoubleTier` threshold records; the matching `Default*` values (`DefaultEntriesBehind`, `DefaultLastContactSeconds`, `DefaultConsecutiveErrors`, `DefaultUnhealthyAfter`, `DefaultInboundDegradedAfter`, `DefaultInboundCriticalAfter`); `DefaultName` |

## Flow control

See [Receiver Flow Control](receiver-flow-control.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReceiverFlowControlPolicy` | interface | Maps receiver state to ack hints. | `EvaluateAsync` |
| `ReceiverFlowControlContext` | readonly record struct | Input to a flow-control policy. | `TreeName`, `OriginClusterId`, `EntryCount`, `ApplyDurationMs` |
| `ReceiverFlowControlHint` | readonly record struct | Suggested sender limits. | `SuggestedBatchSize`, `PauseForMs`, `None` |
| `NoOpReceiverFlowControlPolicy` | sealed class | Policy that returns no hints. | `Instance`, `EvaluateAsync` |
| `WalSaturationReceiverFlowControlOptions` | sealed class | Tunes how the throttled and saturated WAL states map to hints (a healthy WAL returns no hint). | `ThrottledBatchRatio`, `ThrottledPauseMs`, `SaturatedBatchSize`, `SaturatedPauseMs`, and the matching `DefaultThrottledBatchRatio`, `DefaultThrottledPauseMs`, `DefaultSaturatedBatchSize`, `DefaultSaturatedPauseMs` constants |
| `WalSaturationReceiverFlowControlPolicy` | sealed class | Built-in WAL-saturation-aware policy. | `EvaluateAsync` |

Flow-control hints are advisory. The sender clamps its next batch size and pause to the ack it receives, but a receiver must still tolerate redelivery and retries.

## Anti-entropy and remediation

See [Automatic Drift Remediation](automatic-drift-remediation.md), [digest probes](anti-entropy-digest-probe.md), [Merkle walks](anti-entropy-merkle-walk.md), [leaf re-replay](anti-entropy-leaf-rereplay.md), [bootstrap fallback](anti-entropy-bootstrap-fallback.md), and [remediation guards](anti-entropy-remediation-guards.md).

| Type | Kind | Purpose |
|---|---|---|
| `IReplicationDigestProbeTransport` | interface | Transport seam for the read-only peer RPCs: `ProbeDigestAsync`, `ProbeMerkleWalkAsync`, `GetPeerHighWaterMarkAsync`, plus the shipping-side `ExchangeContentManifestAsync` (payload elision) and `PullCompressionDictionaryAsync`. Every member except `ProbeDigestAsync` has a default implementation (an unavailable or not-supported response, or `HybridLogicalClock.Zero` for the watermark read), so a custom transport implements only what it can answer. |
| `DigestProbeComparer` | static class | Compares a local digest with a peer's digest probe response (`Compare`). |
| `DigestProbeOutcome` | enum | Digest comparison outcome: `Match`, `Mismatch`, `VersionSkew`, `RemoteUnavailable`. |
| `DigestProbeRequest`, `DigestProbeResponse` | readonly record structs | Digest probe request and response. |
| `ContentManifestRequest`, `ContentManifestResponse`, `ContentManifestEntry` | readonly record structs | Content-hash manifest exchange shapes used by payload elision. |
| `MerkleWalkProbeRequest`, `MerkleWalkProbeResponse`, `MerkleWalkOutcome` | readonly record structs | Merkle walk request, response, and outcome. |
| `MerkleWalkAbortReason` | enum | Reason a Merkle walk stopped before repair: `None`, `DepthCapExceeded`, `ByteBudgetExceeded`, `RemoteUnavailable`, `VersionSkew`. |
| `PeerHighWaterMarkRequest`, `PeerHighWaterMarkResponse` | readonly record structs | High-water-mark probe shapes. |
| `LeafReReplayRange`, `LeafReReplayOutcome` | readonly record structs | Targeted leaf replay range and result. |
| `LeafReReplaySkipReason` | enum | Reason targeted replay was skipped: `None`, `Disabled`, `RangeEmpty`, `WalTrimmed`. |
| `BootstrapFallbackOutcome` | readonly record struct | Result of snapshot fallback repair. |
| `BootstrapFallbackSkipReason` | enum | Reason snapshot fallback was skipped: `None`, `Disabled`, `RangeEmpty`, `Empty`. |
| `RemediationGuard` | sealed class | Budget and circuit-breaker guard for automatic repair. |
| `RemediationDisabledReason` | enum | Reason automatic remediation is disabled: `OptOut`, `BudgetExhausted`, `CircuitOpen`. |

These types are public so custom transports and operators can integrate with the opt-in anti-entropy stack without depending on implementation details.

## Compression-dictionary negotiation

See [Compression](../lattice/compression.md) and [Wire Format](wire-format.md).

| Type | Kind | Purpose |
|---|---|---|
| `SharedDictionaryNegotiation` | static class | Computes dictionary negotiation decisions. |
| `SharedDictionaryNegotiationResult` | readonly record struct | One negotiation decision. |
| `SharedDictionaryNegotiationState` | sealed class | Tracks peer dictionary state. |
| `SharedDictionaryNegotiationSnapshot` | readonly record struct | Observable dictionary negotiation state. |
| `AdvertisedCompressionDictionary` | readonly record struct | Dictionary advertised by a peer. |
| `CompressionDictionaryAdvertisement` | static class | Builds the `(id, fingerprint)` set a receiver advertises on its acks. |
| `CompressionDictionaryFingerprint` | static class | Computes dictionary fingerprints. |
| `CompressionDictionaryConvergence` | static class | Pulls and installs the peer-advertised dictionaries the local provider does not hold (`ConvergeAsync`). |
| `CompressionDictionaryPullRequest`, `CompressionDictionaryPullResponse` | readonly record structs | Pull protocol for missing dictionaries. |

Dictionary negotiation is opt-in and separate from the default dict-less Zstandard framing compression.

## Topology and peer membership

See [Replication Drivers](replication-drivers.md#peer-configuration-topology-vs-replicationpeers).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationTopology` | interface | Runtime source of peer cluster membership. | `CurrentPeers`, `Subscribe` |
| `PeerChanged` | readonly record struct | Membership change notification. | `PeerClusterId`, `Kind` |
| `PeerChangeKind` | enum | Kind of membership change. | `Added`, `Removed` |

The default topology projects `LatticeReplicationOptions.ReplicationPeers`. Register a custom singleton `IReplicationTopology` before replication setup to source membership from a service registry or configuration provider.

## Security

See [Transport Security](transport-security.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationSecretSource` | interface | Supplies shared secrets for peer authentication. | `GetOutboundSecretAsync`, `GetAcceptedSecretsAsync` |
| `ConfigurationBindingSecretSource` | sealed class | Secret source backed by configuration. | Constructor taking the bound `IConfiguration` section; `GetOutboundSecretAsync`, `GetAcceptedSecretsAsync` |
| `LatticeReplicationAcceptedSecrets` | sealed class | Accepted shared-secret set. | `Secrets`, `Version`, `Empty` |
| `LatticeReplicationSecurityOptions` | sealed class | Shared-secret authentication options. | `RequireAuthentication`, `BindCredentialToOriginCluster`, `SecretRefreshInterval`, `ScanConfigurationForSecrets` |
| `LatticeReplicationEnvironmentVariables` | static class | Environment-variable names for replication secrets. | `Prefix`, `Secret`, `AcceptedSecrets`, `PeerSecretPrefix`, `AllowSourceTreeSecrets` |
| `LatticeReplicationSharedSecret` | static class | Shared-secret generation and validation helpers. | `MinimumLength`, `Generate`, `IsWellFormed`, `FixedTimeEquals` |

The gRPC binding requires HTTPS endpoints unless `AllowPlaintextEndpoints` is enabled. Shared-secret authentication is configured through the security extension methods above; origin binding (`BindCredentialToOriginCluster`) is on by default - see [Configuration](configuration.md#transport-security---latticereplicationsecurityoptions).

## Cross-cluster saga participation

The saga service-provider interfaces let a host join the coordinated cross-cluster commit protocol that backs a fleet-wide restore. Register an implementation with `AddLatticeSagaParticipant`. See [Coordinated restore](coordinated-restore.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ISagaParticipant` | interface | Service-provider interface a host implements to take part in a cross-cluster saga: it votes on prepare, then applies or discards its staged work. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` |
| `ISagaControlChannel` | interface | Outbound control channel the coordinator uses to drive a named peer cluster through the saga phases, and a prepared participant uses to ask the coordinator cluster for the saga's decision once its cutover fence expires ([#4637](https://github.com/NSTA1/Orleans.Lattice/issues/4637)). `GetDecisionAsync` is a default member that throws `NotSupportedException`, which the participant treats as an unreachable coordinator. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` (each taking the target `clusterId`), `GetDecisionAsync(coordinatorClusterId, ...)` |
| `ISagaPeerAuthorizer` | interface | Fail-closed gate deciding whether an inbound saga control request from a claimed origin cluster is accepted. | `IsAuthorizedAsync(string? originClusterId, CancellationToken)` |
| `ILatticeSagaControlHandler` | interface | Server-side delegation seam for the inbound saga control channel. The gRPC saga service validates the request and enforces peer authorization, then delegates each imperative RPC to this handler. `GetDecisionAsync` answers a participant's decision query on the coordinator cluster; it is a default member that throws `NotSupportedException`. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync`, `GetDecisionAsync` (each taking a `SagaControlRequest`) |
| `NoParticipantSagaControlHandler` | sealed class | The transport-only fallback handler the gRPC binding registers with `TryAddSingleton`. `AddLatticeReplication` registers its own durable handler - which routes each inbound saga call to the per-saga participant - with `TryAddSingleton` too, so on a silo that calls it before the gRPC binding (as the setup above does) the durable handler is the effective one and this class is never used. Holds no participant state: it reports `SagaPhase.None` for every saga and votes `SagaVote.Abort` on prepare, because a participant that cannot durably prepare must not let the coordinator commit. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` |
| `SagaPhase` | enum | The durable phase a participant reports for a saga. | `None = 0`, `Prepared = 1`, `Committed = 2`, `Aborted = 3` |
| `SagaVote` | enum | A participant's prepare-phase vote. | `None = 0`, `Commit = 1`, `Abort = 2` |
| `SagaControlRequest` | readonly record struct | The request every saga control call carries. `RequesterClusterId` names the participant asking a `GetDecision` query; the receiving transport overwrites it with the authenticated origin, and the coordinator checks it against the saga's recorded participants. | `SagaId`, `TargetTree`, `ManifestId`, `CoordinatorClusterId`, `SetId`, `RequesterClusterId` |
| `SagaControlResponse` | readonly record struct | A participant cluster's answer to a saga control call. | `SagaId`, `Phase`, `Vote`, `Detail` |
| `SagaParticipantPrepareResult` | readonly record struct | An `ISagaParticipant` prepare vote. | `Vote`, `Detail` |
| `LatticeSystemTreeNames` | static class | The reserved system-tree names the replication package owns. | `MembershipGroups`, `MembershipEdges`, `AuthPolicy`, `AuthAudit`, `ReplicationConfig`, `ReplicationConfigMapKey`, `BuildEnrolmentMap`, `BuildReplicationConfigEnrolmentMap` |

## Tenant isolation and runtime configuration authority

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationTenantIsolationGate` | interface | Evaluates whether an inbound replicated entry is admissible for the tenant its tree id names - the tenant must exist, be resident in this region, and be active - so a peer can never widen tenant or region scope. The replication package's default gate is inactive; the tenancy add-on supplies a real gate. | `IsActive`, `EvaluateAsync(string, CancellationToken)` returning `ReplicationTenantIsolationDecision` |
| `ReplicationTenantIsolationDecision` | enum | The gate's verdict. | `Admit = 0`, `RejectUnknownTenant = 1`, `RejectOutOfRegion = 2`, `RejectSuspendedTenant = 3` |
| `ILatticeReplicationConfigAuthority` | interface | The engine-level authoring seam for runtime per-tree replication config: authors enable / disable onto the `sys-replication-config` tree and reports the reconciled per-tree status (the runtime tree unioned with the static `ReplicatedTrees` map). It performs no authorization - the control facade authorizes first. Registered by `AddLatticeReplication(..., enableRuntimeConfig: true)`. See [Runtime configuration](runtime-config.md). | `EnableReplicationAsync`, `DisableReplicationAsync`, `GetTreeStatusAsync`, `GetAllTreeStatusesAsync` |
| `LatticeReplicationEnableResult`, `LatticeReplicationDisableResult` | readonly record structs | Authority outcomes. | `TreeId`, `Mode`, `AlreadyEnabled`, `BootstrapRequested` / `TreeId`, `AlreadyDisabled` |
| `LatticeReplicationTreeStatus` | readonly record struct | One tree's reconciled replication status. | `TreeId`, `Enabled`, `Mode`, `Ambiguous`, `Source` |
| `LatticeReplicationEnrollmentSource` | enum | Which enrollment source puts a tree's status in force. | `Runtime = 0`, `Static = 1`, `RuntimeAndStatic = 2` |
| `LatticeReplicationConfigEntry` | sealed class | One tree's CRDT config entry in the `sys-replication-config` OR-Map: a disable-wins enablement flag plus a multi-value merge-mode register, so concurrent divergent modes stay detectable. | `Enabled` (`RwFlag`), `Mode` (`MvRegister`), `IsEnabled`, `HasAmbiguousMode`, `Modes`, `IsBottom`, `TryGetMode`, `Enable`, `Disable`, `SetMode`, `MergeFrom`, `Clone`, and the static `EncodeMode` / `DecodeMode` |
| `ILatticeReplicationPreconditionValidator` | interface | Validates the runtime preconditions a tree must meet to replicate under a merge mode (today: a flag mode requires a configured local replica id). Shared by the boot-time validation of statically declared trees and the runtime enable path. | `Validate(string, LatticeMergeMode)` returning `LatticeReplicationPreconditionResult` |
| `LatticeReplicationPreconditionResult` | readonly record struct | A precondition verdict. | `IsSatisfied`, `FailureReason`, `Satisfied`, `Rejected(string)` |
| `LatticeReplicationModeChangeRejectedException` | exception | Thrown when an enable would change the merge mode of an already-enabled tree, or the tree's mode is currently ambiguous. | `TreeId`, `RequestedMode`, `CurrentMode`, `CurrentModeAmbiguous` |
| `LatticeReplicationPreconditionFailedException` | exception | Thrown when a replication precondition fails. | `TreeId`, `RequestedMode` |

## Azure Table WAL durability

The durable Azure Table WAL backend ships as the separate `Orleans.Lattice.Storage.AzureTable` package. Use `AddAzureTableWalStorage` when the replication WAL must survive silo restarts and support production retention, bootstrap, and replay windows. Its public surface (`AzureTableWalStorageProvider`, `AzureTableWalStorageOptions`, and the retry policies) and configuration are documented in [Orleans.Lattice.Storage.AzureTable](../lattice.storage.azuretable/README.md) - see its [API Reference](../lattice.storage.azuretable/api.md) and [Configuration](../lattice.storage.azuretable/configuration.md). For the core WAL provider seam, see [WAL](wal.md) and [core WAL Storage Providers](../lattice/wal-storage-providers.md).

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Replication.AdvertisedCompressionDictionary`

[Source](../../src/lattice.replication/SharedDictionaryNegotiation.cs) (line 203).

`public readonly record struct AdvertisedCompressionDictionary( uint Id, ulong Fingerprint)`

- `Primary constructor / positional members: ( [property: Id(0)] uint Id, [property: Id(1)] ulong Fingerprint)`

### `Orleans.Lattice.Replication.ApplyResult`

[Source](../../src/lattice.replication/ApplyResult.cs) (line 13).

`public readonly record struct ApplyResult`

- `public bool Applied { get; init; }`
- `public HybridLogicalClock HighWaterMark { get; init; }`
- `public bool Deferred { get; init; }`
- `public bool SourceLineageRefused { get; init; }`

### `Orleans.Lattice.Replication.BootstrapCoordinatorStatus`

[Source](../../src/lattice.replication/BootstrapCoordinatorStatus.cs) (line 30).

`public readonly record struct BootstrapCoordinatorStatus( LatticeBootstrapState Phase, string? SourceClusterId)`

- `Primary constructor / positional members: ( [property: Id(0)] LatticeBootstrapState Phase, [property: Id(1)] string? SourceClusterId)`
- `public bool ReadFenced { get; init; }`
- `public long EntriesApplied { get; init; }`
- `public int RedriveAttempts { get; init; }`
- `public string? CompletedSourceClusterId { get; init; }`

### `Orleans.Lattice.Replication.BootstrapFallbackOutcome`

[Source](../../src/lattice.replication/BootstrapFallbackOutcome.cs) (line 9).

`public readonly record struct BootstrapFallbackOutcome`

- `public bool Attempted { get; init; }`
- `public int RangesProcessed { get; init; }`
- `public int EntriesShipped { get; init; }`
- `public BootstrapFallbackSkipReason SkipReason { get; init; }`
- `public static BootstrapFallbackOutcome NotAttempted`

### `Orleans.Lattice.Replication.BootstrapFallbackSkipReason`

[Source](../../src/lattice.replication/BootstrapFallbackSkipReason.cs) (line 10).

`public enum BootstrapFallbackSkipReason`

- `None = 0`
- `Disabled = 1`
- `RangeEmpty = 2`
- `Empty = 3`

### `Orleans.Lattice.Replication.ChangeFeedCursor`

[Source](../../src/lattice.replication/ChangeFeedCursor.cs) (line 35).

`public readonly struct ChangeFeedCursor : IEquatable<ChangeFeedCursor>`

- `public static ChangeFeedCursor Initial { get; }`
- `public ChangeFeedCursor(IReadOnlyDictionary<int, long>? partitionOffsets)`
- `public long GetOffsetForPartition(int partition)`
- `public IReadOnlyDictionary<int, long> PartitionOffsets`
- `public bool Equals(ChangeFeedCursor other)`
- `public override bool Equals(object? obj)`
- `public override int GetHashCode()`
- `public static bool operator ==(ChangeFeedCursor left, ChangeFeedCursor right)`
- `public static bool operator !=(ChangeFeedCursor left, ChangeFeedCursor right)`

### `Orleans.Lattice.Replication.CompressionDictionaryAdvertisement`

[Source](../../src/lattice.replication/SharedDictionaryNegotiation.cs) (line 218).

`public static class CompressionDictionaryAdvertisement`

- `public static AdvertisedCompressionDictionary[]? Build( ILatticeCompressionDictionaryProvider? provider)`

### `Orleans.Lattice.Replication.CompressionDictionaryConvergence`

[Source](../../src/lattice.replication/CompressionDictionaryConvergence.cs) (line 27).

`public static class CompressionDictionaryConvergence`

- `public static async Task<int> ConvergeAsync( IReplicationDigestProbeTransport transport, ILatticeCompressionDictionaryProvider provider, string targetClusterId, IReadOnlyCollection<AdvertisedCompressionDictionary>? peerAdvertised, string treeId, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.CompressionDictionaryFingerprint`

[Source](../../src/lattice.replication/SharedDictionaryNegotiation.cs) (line 305).

`public static class CompressionDictionaryFingerprint`

- `public static ulong Compute(ReadOnlySpan<byte> dictionaryBytes)`

### `Orleans.Lattice.Replication.CompressionDictionaryPullRequest`

[Source](../../src/lattice.replication/CompressionDictionaryPullRequest.cs) (line 14).

`public readonly record struct CompressionDictionaryPullRequest`

- `public uint DictionaryId { get; init; }`

### `Orleans.Lattice.Replication.CompressionDictionaryPullResponse`

[Source](../../src/lattice.replication/CompressionDictionaryPullResponse.cs) (line 15).

`public readonly record struct CompressionDictionaryPullResponse`

- `public bool ExchangeSupported { get; init; }`
- `public bool Found { get; init; }`
- `public uint DictionaryId { get; init; }`
- `public ulong Fingerprint { get; init; }`
- `public ReadOnlyMemory<byte> Dictionary { get; init; }`
- `public static CompressionDictionaryPullResponse NotSupported`
- `public static CompressionDictionaryPullResponse NotHeld`

### `Orleans.Lattice.Replication.ConfigurationBindingSecretSource`

[Source](../../src/lattice.replication/Security/ConfigurationBindingSecretSource.cs) (line 31).

`public sealed class ConfigurationBindingSecretSource : ILatticeReplicationSecretSource`

- `public ConfigurationBindingSecretSource(IConfiguration section)`
- `public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)`
- `public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.ContentManifestEntry`

[Source](../../src/lattice.replication/ContentManifestEntry.cs) (line 20).

`public readonly record struct ContentManifestEntry`

- `public int EntryIndex { get; init; }`
- `public string Key { get; init; }`
- `public ulong ContentHash { get; init; }`
- `public HybridLogicalClock Hlc { get; init; }`

### `Orleans.Lattice.Replication.ContentManifestRequest`

[Source](../../src/lattice.replication/ContentManifestRequest.cs) (line 10).

`public readonly record struct ContentManifestRequest`

- `public string TreeName { get; init; }`
- `public string OriginClusterId { get; init; }`
- `public IReadOnlyList<ContentManifestEntry> Entries { get; init; }`

### `Orleans.Lattice.Replication.ContentManifestResponse`

[Source](../../src/lattice.replication/ContentManifestResponse.cs) (line 11).

`public readonly record struct ContentManifestResponse`

- `public bool ExchangeSupported { get; init; }`
- `public IReadOnlyList<int> MissingEntryIndices { get; init; }`
- `public HybridLogicalClock AdvancedHlc { get; init; }`
- `public static ContentManifestResponse NotSupported`

### `Orleans.Lattice.Replication.DeadLetterEntry`

[Source](../../src/lattice.replication/DeadLetterEntry.cs) (line 11).

`public readonly record struct DeadLetterEntry`

- `public long EntryId { get; init; }`
- `public WalRecord Entry { get; init; }`
- `public string FailureReason { get; init; }`
- `public int RetryCount { get; init; }`
- `public long EnqueuedAtTicks { get; init; }`
- `public string? SourceLineageClusterId { get; init; }`
- `public Guid? SourceLineage { get; init; }`
- `public string? ReasonTag { get; init; }`

### `Orleans.Lattice.Replication.DigestProbeComparer`

[Source](../../src/lattice.replication/DigestProbeComparer.cs) (line 9).

`public static class DigestProbeComparer`

- `public static DigestProbeOutcome Compare(LeafProjectionDigest local, DigestProbeResponse remote)`

### `Orleans.Lattice.Replication.DigestProbeOutcome`

[Source](../../src/lattice.replication/DigestProbeOutcome.cs) (line 9).

`public enum DigestProbeOutcome`

- `Match`
- `Mismatch`
- `VersionSkew`
- `RemoteUnavailable`

### `Orleans.Lattice.Replication.DigestProbeRequest`

[Source](../../src/lattice.replication/DigestProbeRequest.cs) (line 15).

`public readonly record struct DigestProbeRequest`

- `public string TreeName { get; init; }`
- `public int ShardIndex { get; init; }`

### `Orleans.Lattice.Replication.DigestProbeResponse`

[Source](../../src/lattice.replication/DigestProbeResponse.cs) (line 18).

`public readonly record struct DigestProbeResponse`

- `public bool DigestAvailable { get; init; }`
- `public LeafProjectionDigest Digest { get; init; }`

### `Orleans.Lattice.Replication.DoubleTier`

[Source](../../src/lattice.replication/LatticeReplicationHealthCheckOptions.cs) (line 174).

`public readonly record struct DoubleTier(double Degraded, double Unhealthy)`

- `Primary constructor / positional members: (double Degraded, double Unhealthy)`

### `Orleans.Lattice.Replication.EncodedBatchHeader`

[Source](../../src/lattice.replication/EncodedBatchHeader.cs) (line 35).

`public readonly record struct EncodedBatchHeader`

- `public const int WireSize`
- `public const uint MagicValue`
- `public const int CurrentWireVersion`
- `public uint Magic { get; init; }`
- `public int WireVersion { get; init; }`
- `public ulong OriginClusterIdHash { get; init; }`
- `public int EntryCount { get; init; }`
- `public long BatchSequence { get; init; }`
- `public int AtomicBatchSpanCount { get; init; }`
- `public LatticeMergeMode Mode { get; init; }`
- `public LatticeCompression Compression { get; init; }`
- `public uint DictionaryId { get; init; }`
- `public void WriteTo(Span<byte> destination)`
- `public static EncodedBatchHeader ReadFrom(ReadOnlySpan<byte> source)`
- `public static ulong HashClusterId(string clusterId)`

### `Orleans.Lattice.Replication.FallOffLogDecision`

[Source](../../src/lattice.replication/FallOffLogDecision.cs) (line 60).

`public readonly record struct FallOffLogDecision( bool FellOffLog, HybridLogicalClock LocalHighWaterMark, bool BootstrapTriggered, bool Suppressed)`

- `Primary constructor / positional members: ( bool FellOffLog, HybridLogicalClock LocalHighWaterMark, bool BootstrapTriggered, bool Suppressed)`

### `Orleans.Lattice.Replication.IBootstrapSnapshotSource`

[Source](../../src/lattice.replication/IBootstrapSnapshotSource.cs) (line 25).

`public interface IBootstrapSnapshotSource : ISnapshotProvider`


### `Orleans.Lattice.Replication.IChangeFeed`

[Source](../../src/lattice.replication/IChangeFeed.cs) (line 67).

`public interface IChangeFeed`

- `IAsyncEnumerable<WalRecord> Subscribe( string treeName, HybridLogicalClock cursor, bool includeLocalOrigin = true, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<WalRecord> Subscribe( string treeName, ChangeFeedCursor cursor, bool includeLocalOrigin = true, CancellationToken cancellationToken = default)`
- `Task<ChangeFeedCursor> GetCurrentCursorAsync( string treeName, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeBootstrapCoordinator`

[Source](../../src/lattice.replication/ILatticeBootstrapCoordinator.cs) (line 33).

`public interface ILatticeBootstrapCoordinator`

- `Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken cancellationToken = default)`
- `Task<BootstrapCoordinatorStatus> GetStatusAsync(string treeName, CancellationToken cancellationToken = default)`
- `Task BootstrapAsync(string treeName, string sourceClusterId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeFallOffLogDetector`

[Source](../../src/lattice.replication/ILatticeFallOffLogDetector.cs) (line 48).

`public interface ILatticeFallOffLogDetector`

- `Task<FallOffLogDecision> CheckAndTriggerAsync( string treeName, string sourceClusterId, HybridLogicalClock senderOldestAvailableHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeReplicationAdmin`

[Source](../../src/lattice.replication/ILatticeReplicationAdmin.cs) (line 30).

`public interface ILatticeReplicationAdmin`

- `Task<OperatorReseedDecision> RequestSnapshotAsync( string treeName, string sourceClusterId, CancellationToken cancellationToken = default)`
- `Task<OperatorReseedDecision> ForceRequestSnapshotAsync( string treeName, string sourceClusterId, CancellationToken cancellationToken = default)`
- `Task<bool> ForceLiftBootstrapReadFenceAsync( string treeName, string reason, CancellationToken cancellationToken = default)`
- `Task<bool> ResolveCrossClusterSagaParticipantAsync( string sagaId, bool commit, string reason, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeReplicationConfigAuthority`

[Source](../../src/lattice.replication/ILatticeReplicationConfigAuthority.cs) (line 36).

`public interface ILatticeReplicationConfigAuthority`

- `Task<LatticeReplicationEnableResult> EnableReplicationAsync( string treeId, LatticeMergeMode mode, string? bootstrapSourceClusterId = null, CancellationToken cancellationToken = default)`
- `Task<LatticeReplicationDisableResult> DisableReplicationAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeReplicationTreeStatus?> GetTreeStatusAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, LatticeReplicationTreeStatus>> GetAllTreeStatusesAsync( CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeReplicationDeadLetters`

[Source](../../src/lattice.replication/ILatticeReplicationDeadLetters.cs) (line 9).

`public interface ILatticeReplicationDeadLetters`

- `Task<IReadOnlyList<DeadLetterEntry>> ListAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<bool> DiscardAsync(string treeId, long entryId, CancellationToken cancellationToken = default)`
- `Task<ApplyResult?> ReplayAsync(string treeId, long entryId, CancellationToken cancellationToken = default)`
- `Task<bool> PoisonSagaAsync( string treeId, string originClusterId, Guid transactionId, CancellationToken cancellationToken = default)`
- `Task<bool> ReleaseQuarantinedSagaAsync( string treeId, string originClusterId, Guid transactionId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeReplicationPeerDecommissioner`

[Source](../../src/lattice.replication/ILatticeReplicationPeerDecommissioner.cs) (line 27).

`public interface ILatticeReplicationPeerDecommissioner`

- `Task<LatticeReplicationPeerDecommissionOutcome> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeReplicationPreconditionValidator`

[Source](../../src/lattice.replication/ILatticeReplicationPreconditionValidator.cs) (line 20).

`public interface ILatticeReplicationPreconditionValidator`

- `LatticeReplicationPreconditionResult Validate(string treeId, LatticeMergeMode mode)`

### `Orleans.Lattice.Replication.ILatticeReplicationSecretSource`

[Source](../../src/lattice.replication/Security/ILatticeReplicationSecretSource.cs) (line 28).

`public interface ILatticeReplicationSecretSource`

- `ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)`
- `ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.ILatticeSagaControlHandler`

[Source](../../src/lattice.replication/ILatticeSagaControlHandler.cs) (line 19).

`public interface ILatticeSagaControlHandler`

- `Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> GetDecisionAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ILatticeWalIntrospection`

[Source](../../src/lattice.replication/ILatticeWalIntrospection.cs) (line 25).

`public interface ILatticeWalIntrospection`

- `Task<HybridLogicalClock?> GetOldestAvailableHlcAsync( string treeName, CancellationToken cancellationToken = default)`
- `Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetOldestAvailableHlcByOriginAsync( string treeName, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IReceiverFlowControlPolicy`

[Source](../../src/lattice.replication/IReceiverFlowControlPolicy.cs) (line 29).

`public interface IReceiverFlowControlPolicy`

- `ValueTask<ReceiverFlowControlHint> EvaluateAsync( ReceiverFlowControlContext context, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.IRemoteSnapshotItemTransport`

[Source](../../src/lattice.replication/IRemoteSnapshotItemTransport.cs) (line 9).

`public interface IRemoteSnapshotItemTransport : IRemoteSnapshotTransport`

- `IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IRemoteSnapshotTransport`

[Source](../../src/lattice.replication/IRemoteSnapshotTransport.cs) (line 57).

`public interface IRemoteSnapshotTransport`

- `Task<RemoteSnapshotMetadata> GetMetadataAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IReplicationApplier`

[Source](../../src/lattice.replication/IReplicationApplier.cs) (line 32).

`public interface IReplicationApplier`

- `Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)`
- `async Task<ApplyResult> ApplyBatchAsync( IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IReplicationBatchEncoder`

[Source](../../src/lattice.replication/IReplicationBatchEncoder.cs) (line 48).

`public interface IReplicationBatchEncoder`

- `string ContentType { get; }`
- `int CurrentWireVersion { get; }`
- `void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)`
- `ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)`
- `void EncodeFraming( in EncodedBatchHeader header, string treeName, string originClusterId, ReadOnlyMemory<ArraySegment<byte>> entries, IBufferWriter<byte> writer)`
- `bool TryDecodeFraming( ReadOnlyMemory<byte> payload, out EncodedBatchHeader header, out string treeName, out string originClusterId, out ReadOnlyMemory<ArraySegment<byte>> entries)`

### `Orleans.Lattice.Replication.IReplicationDigestProbeTransport`

[Source](../../src/lattice.replication/IReplicationDigestProbeTransport.cs) (line 18).

`public interface IReplicationDigestProbeTransport`

- `Task<DigestProbeResponse> ProbeDigestAsync( string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)`
- `Task<MerkleWalkProbeResponse> ProbeMerkleWalkAsync( string targetClusterId, MerkleWalkProbeRequest request, CancellationToken cancellationToken)`
- `Task<Orleans.Lattice.HybridLogicalClock> GetPeerHighWaterMarkAsync( string targetClusterId, string treeName, string originClusterId, CancellationToken cancellationToken)`
- `Task<ContentManifestResponse> ExchangeContentManifestAsync( string targetClusterId, ContentManifestRequest request, CancellationToken cancellationToken)`
- `Task<CompressionDictionaryPullResponse> PullCompressionDictionaryAsync( string targetClusterId, CompressionDictionaryPullRequest request, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.IReplicationLocalVcSeeder`

[Source](../../src/lattice.replication/IReplicationLocalVcSeeder.cs) (line 62).

`public interface IReplicationLocalVcSeeder`

- `Task<LocalVcSeedReport> SeedFromTreeAsync(string treeName, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IReplicationTenantIsolationGate`

[Source](../../src/lattice.replication/IReplicationTenantIsolationGate.cs) (line 31).

`public interface IReplicationTenantIsolationGate`

- `bool IsActive { get; }`
- `ValueTask<ReplicationTenantIsolationDecision> EvaluateAsync( string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.IReplicationTopology`

[Source](../../src/lattice.replication/IReplicationTopology.cs) (line 64).

`public interface IReplicationTopology`

- `IReadOnlyCollection<string> CurrentPeers { get; }`
- `IDisposable Subscribe(Action<PeerChanged> onChange)`

### `Orleans.Lattice.Replication.IReplicationTransport`

[Source](../../src/lattice.replication/IReplicationTransport.cs) (line 52).

`public interface IReplicationTransport`

- `Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.ISagaControlChannel`

[Source](../../src/lattice.replication/ISagaControlChannel.cs) (line 13).

`public interface ISagaControlChannel`

- `Task<SagaControlResponse> PrepareAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> CommitAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> AbortAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> GetStatusAsync(string clusterId, SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaControlResponse> GetDecisionAsync(string coordinatorClusterId, SagaControlRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ISagaParticipant`

[Source](../../src/lattice.replication/ISagaParticipant.cs) (line 70).

`public interface ISagaParticipant`

- `Task<SagaParticipantPrepareResult> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `Task<SagaPhase> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ISagaPeerAuthorizer`

[Source](../../src/lattice.replication/ISagaPeerAuthorizer.cs) (line 19).

`public interface ISagaPeerAuthorizer`

- `Task<bool> IsAuthorizedAsync(string? originClusterId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.ISnapshotProvider`

[Source](../../src/lattice.replication/ISnapshotProvider.cs) (line 22).

`public interface ISnapshotProvider`

- `Task<SnapshotStream> ExportAsync( string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)`
- `Task<SnapshotStream> ExportAsync( string treeName, string sourceClusterId, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)`
- `Task<SnapshotStream> ExportAsync( string treeName, IReadOnlyList<LeafReReplayRange> ranges, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.LatticeBootstrapState`

[Source](../../src/lattice.replication/LatticeBootstrapState.cs) (line 14).

`public enum LatticeBootstrapState`

- `Idle = 0`
- `RequestingSnapshot = 1`
- `ApplyingSnapshot = 2`
- `IncrementalHandoff = 3`
- `LiveIncremental = 4`
- `Failed = 5`

### `Orleans.Lattice.Replication.LatticeBootstrapTransientFaultClassifier`

[Source](../../src/lattice.replication/LatticeBootstrapTransientFaultClassifier.cs) (line 72).

`public static class LatticeBootstrapTransientFaultClassifier`

- `public static bool IsTransient(Exception exception)`

### `Orleans.Lattice.Replication.LatticeRemoteSnapshotService`

[Source](../../src/lattice.replication/LatticeRemoteSnapshotService.cs) (line 52).

`public sealed class LatticeRemoteSnapshotService : IRemoteSnapshotItemTransport`

- `public LatticeRemoteSnapshotService( ISnapshotProvider provider, ILogger<LatticeRemoteSnapshotService> logger)`
- `public LatticeRemoteSnapshotService( ISnapshotProvider provider, ILatticeReplicationContext replicationContext, ILogger<LatticeRemoteSnapshotService> logger)`
- `public async Task<RemoteSnapshotMetadata> GetMetadataAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`
- `public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`
- `public async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync( string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.LatticeReplicationAcceptedSecrets`

[Source](../../src/lattice.replication/Security/LatticeReplicationSecrets.cs) (line 20).

`public sealed class LatticeReplicationAcceptedSecrets`

- `public LatticeReplicationAcceptedSecrets(IReadOnlyList<string> secrets, string version)`
- `public IReadOnlyList<string> Secrets { get; }`
- `public string Version { get; }`
- `public static LatticeReplicationAcceptedSecrets Empty { get; }`

### `Orleans.Lattice.Replication.LatticeReplicationConfigEntry`

[Source](../../src/lattice.replication/LatticeReplicationConfigEntry.cs) (line 60).

`public sealed class LatticeReplicationConfigEntry : ICrdt<LatticeReplicationConfigEntry>`

- `public RwFlag Enabled { get; set; }`
- `public MvRegister Mode { get; set; }`
- `public bool IsEnabled`
- `public bool HasAmbiguousMode`
- `public IReadOnlyList<LatticeMergeMode> Modes { get; }`
- `public bool IsBottom`
- `public void Enable(string replicaId, long counter)`
- `public void Disable(string replicaId, long counter)`
- `public void SetMode(string replicaId, LatticeMergeMode mode)`
- `public bool TryGetMode(out LatticeMergeMode mode)`
- `public void MergeFrom(LatticeReplicationConfigEntry other)`
- `public LatticeReplicationConfigEntry Clone()`
- `public static byte[] EncodeMode(LatticeMergeMode mode)`
- `public static LatticeMergeMode DecodeMode(byte[] value)`

### `Orleans.Lattice.Replication.LatticeReplicationDisableResult`

[Source](../../src/lattice.replication/LatticeReplicationDisableResult.cs) (line 19).

`public readonly record struct LatticeReplicationDisableResult( string TreeId, bool AlreadyDisabled)`

- `Primary constructor / positional members: ( string TreeId, bool AlreadyDisabled)`

### `Orleans.Lattice.Replication.LatticeReplicationEnableResult`

[Source](../../src/lattice.replication/LatticeReplicationEnableResult.cs) (line 27).

`public readonly record struct LatticeReplicationEnableResult( string TreeId, LatticeMergeMode Mode, bool AlreadyEnabled, bool BootstrapRequested)`

- `Primary constructor / positional members: ( string TreeId, LatticeMergeMode Mode, bool AlreadyEnabled, bool BootstrapRequested)`

### `Orleans.Lattice.Replication.LatticeReplicationEnrollmentSource`

[Source](../../src/lattice.replication/LatticeReplicationEnrollmentSource.cs) (line 20).

`public enum LatticeReplicationEnrollmentSource`

- `Runtime = 0`
- `Static = 1`
- `RuntimeAndStatic = 2`

### `Orleans.Lattice.Replication.LatticeReplicationEnvironmentVariables`

[Source](../../src/lattice.replication/Security/LatticeReplicationEnvironmentVariables.cs) (line 18).

`public static class LatticeReplicationEnvironmentVariables`

- `public const string Prefix`
- `public const string Secret`
- `public const string AcceptedSecrets`
- `public const string PeerSecretPrefix`
- `public const string AllowSourceTreeSecrets`

### `Orleans.Lattice.Replication.LatticeReplicationHealthCheckOptions`

[Source](../../src/lattice.replication/LatticeReplicationHealthCheckOptions.cs) (line 25).

`public sealed class LatticeReplicationHealthCheckOptions`

- `public LongTier? EntriesBehind { get; set; }`
- `public DoubleTier? LastContactSeconds { get; set; }`
- `public LongTier? ConsecutiveErrors { get; set; }`
- `public TimeSpan UnhealthyAfter { get; set; }`
- `public TimeSpan InboundDegradedAfter { get; set; }`
- `public TimeSpan InboundCriticalAfter { get; set; }`
- `public static readonly LongTier DefaultEntriesBehind`
- `public static readonly DoubleTier DefaultLastContactSeconds`
- `public static readonly LongTier DefaultConsecutiveErrors`
- `public static readonly TimeSpan DefaultUnhealthyAfter`
- `public static readonly TimeSpan DefaultInboundDegradedAfter`
- `public static readonly TimeSpan DefaultInboundCriticalAfter`
- `public const string DefaultName`

### `Orleans.Lattice.Replication.LatticeReplicationMetrics`

[Source](../../src/lattice.replication/LatticeReplicationMetrics.cs) (line 35).

`public static class LatticeReplicationMetrics`

- `public const string MeterName`
- `public const string TagTree`
- `public const string TagPeer`
- `public const string TagOutcome`
- `public const string TagDirection`
- `public const string DirectionOutbound`
- `public const string DirectionInbound`
- `public const string OutcomeSuccess`
- `public const string OutcomeDedup`
- `public const string OutcomeFailure`
- `public const string OutcomeParkedCausalBuffer`
- `public const string OutcomeRejectedDependencyLost`
- `public const string OutcomeBootstrapFloorDropped`
- `public const string OutcomeBootstrapFloorDeferred`
- `public const string OutcomeShadowForwardDedup`
- `public const string OutcomeRejectedNotReplicated`
- `public const string OutcomeRejectedModeMismatch`
- `public const string OutcomeRejectedForeignTenant`
- `public const string OutcomeRejectedTenantOffline`
- `public const string OutcomeRejectedSuspendedTenant`
- `public const string TagReason`
- `public const string TagShard`
- `public const string TagOrigin`
- `public const string ReasonDiscarded`
- `public const string ReasonReplayed`
- `public const string ReasonEvicted`
- `public const string ReasonSchema`
- `public const string ReasonHlcSkew`
- `public const string ReasonOversized`
- `public const string ReasonModeMismatch`
- `public const string ReasonForeignTenant`
- `public const string ReasonTenantOffline`
- `public const string ReasonSuspendedTenant`
- `public const string ReasonUnknown`
- `public static readonly Meter Meter`
- `public static readonly Histogram<double> ShipDuration`
- `public static readonly Histogram<double> ApplyDuration`
- `public static readonly Histogram<double> ApplyLag`
- `public static readonly Counter<long> WalEntriesShipped`
- `public static readonly Counter<long> ShipRedundantPayloads`
- `public static readonly Counter<long> ShipRedundantPayloadBytes`
- `public const string ShipRedundantPayloadsName`
- `public const string ShipRedundantPayloadBytesName`
- `public static readonly Counter<long> ShipWireVersionDownStamp`
- `public const string ShipWireVersionDownStampName`
- `public const string DownStampReasonCompressionDropped`
- `public const string DownStampReasonBlockedCrdtMode`
- `public const string DownStampReasonBlockedUnsupportedVersion`
- `public static readonly Counter<long> CompressDictionaryBytesIn`
- `public static readonly Counter<long> CompressDictionaryBytesOut`
- `public const string CompressDictionaryBytesInName`
- `public const string CompressDictionaryBytesOutName`
- `public static readonly Counter<long> CoalesceEntriesElided`
- `public static readonly Counter<long> CoalesceBytesElided`
- `public const string CoalesceEntriesElidedName`
- `public const string CoalesceBytesElidedName`
- `public static readonly Counter<long> DoorbellRung`
- `public static readonly Counter<long> DoorbellCoalesced`
- `public const string DoorbellRungName`
- `public const string DoorbellCoalescedName`
- `public static readonly Counter<long> CoalesceDeltasMerged`
- `public const string CoalesceDeltasMergedName`
- `public static readonly Counter<long> ShipElidedPayloads`
- `public static readonly Counter<long> ShipElidedPayloadBytes`
- `public static readonly Counter<long> ManifestExchanges`
- `public const string ShipElidedPayloadsName`
- `public const string ShipElidedPayloadBytesName`
- `public const string ManifestExchangesName`
- `public static readonly Counter<long> ReceiverContentManifestExchanges`
- `public static readonly Counter<long> ReceiverContentEntriesElided`
- `public static readonly Counter<long> ReceiverContentHwmAdvances`
- `public const string ReceiverContentManifestExchangesName`
- `public const string ReceiverContentEntriesElidedName`
- `public const string ReceiverContentHwmAdvancesName`
- `public static readonly Counter<long> DeadLetterEnqueued`
- `public static readonly Counter<long> DeadLetterRemoved`
- `public static readonly Counter<long> SagaApplyDeferred`
- `public static readonly Counter<long> ReceiverSagaPoisoned`
- `public const string ReasonPoisonedSaga`
- `public const string ReceiverSagaPoisonedName`
- `public const string OutcomeReceiverSagaPoisonedTimeout`
- `public const string OutcomeReceiverSagaPoisonedTerminalTimeout`
- `public const string OutcomeReceiverSagaQuarantined`
- `public const string OutcomeReceiverSagaQuarantineFull`
- `public const string OutcomeReceiverSagaQuarantineReleased`
- `public const string OutcomeReceiverSagaPoisonedOperator`
- `public const string OutcomeReceiverSagaPoisonRefusedDecided`
- `public const string OutcomeReceiverSagaPoisonRefusedFull`
- `public const string ReasonDependencyLost`
- `public static readonly Counter<long> DeadLetterRefused`
- `public const string EntriesBehindName`
- `public const string BytesBehindName`
- `public const string ConsecutiveErrorsName`
- `public const string LastContactSecondsName`
- `public const string ShipInFlightName`
- `public const string WireVersionNegotiatedName`
- `public const string WireVersionDowngradeActiveName`
- `public const string ApplyLagName`
- `public const string ApplyDurationName`
- `public const string WalEntriesShippedName`
- `public static readonly UpDownCounter<long> CausalFrontierOrigins`
- `public static readonly Counter<long> SourceRestoreUncoordinated`
- `public static readonly Counter<long> TombstoneReapBound`
- `public const string TombstoneReapBoundName`
- `public const string ReapBoundDegradedOrigin`
- `public const string ReapBoundOriginFrontier`
- `public const string ReapBoundHeldEntry`
- `public const string ReapBoundPeerFrontier`
- `public static readonly Counter<long> ApplySourceLineageRefused`
- `public const string ApplySourceLineageRefusedName`
- `public const string SourceLineageRefusedStale`
- `public const string SourceLineageRefusedReplaced`
- `public static readonly UpDownCounter<long> ApplyBufferedEntries`
- `public static readonly UpDownCounter<long> ApplyBufferBytes`
- `public static readonly Histogram<double> ApplyDependencyWaitMs`
- `public static readonly Counter<long> ApplyCausalViolationsBlocked`
- `public const string ApplyBufferedEntriesName`
- `public const string ApplyBufferBytesName`
- `public const string ApplyDependencyWaitMsName`
- `public const string ApplyCausalViolationsBlockedName`
- `public static readonly Counter<long> ApplyFifoViolations`
- `public const string ApplyFifoViolationsName`
- `public static readonly Histogram<int> ApplyParallelRuns`
- `public const string ApplyParallelRunsName`
- `public static readonly Counter<long> PeerFellOffLog`
- `public const string PeerFellOffLogName`
- `public static readonly Counter<long> PeerFellOffLogSuppressed`
- `public const string PeerFellOffLogSuppressedName`
- `public static readonly Counter<long> BootstrapEntriesReceived`
- `public const string BootstrapEntriesReceivedName`
- `public static readonly Counter<long> BootstrapBytesReceived`
- `public const string BootstrapBytesReceivedName`
- `public static readonly Histogram<double> BootstrapDuration`
- `public const string BootstrapDurationName`
- `public const string BootstrapOutcomeLive`
- `public const string BootstrapOutcomeFailed`
- `public const string BootstrapOutcomeTimedOut`
- `public const string BootstrapReconcileOutcomeReconciled`
- `public const string BootstrapReconcileOutcomeSkippedScoped`
- `public const string BootstrapReconcileOutcomeSkippedUnstable`
- `public const string BootstrapReconcileOutcomeSkippedDeleted`
- `public const string BootstrapReconcileOutcomeSkippedUnknown`
- `public const string BootstrapReconcileOutcomeSkippedLineageMismatch`
- `public const string BootstrapReconcileOutcomeSkippedNeverAligned`
- `public const string BootstrapReconcileOutcomeSkippedNotLww`
- `public const string BootstrapReconcileOutcomeOwedRetry`
- `public const string BootstrapReconcileOutcomeAligned`
- `public static readonly Counter<long> BootstrapTransientRetries`
- `public const string BootstrapTransientRetriesName`
- `public static readonly Counter<long> BootstrapReadFenceForceLifted`
- `public const string BootstrapReadFenceForceLiftedName`
- `public static readonly Counter<long> BootstrapReconcile`
- `public const string BootstrapReconcileName`
- `public static readonly Counter<long> DigestProbeMismatch`
- `public const string DigestProbeMismatchName`
- `public static readonly Counter<long> DigestProbeCompared`
- `public const string DigestProbeComparedName`
- `public const string DigestProbeOutcomeMatch`
- `public const string DigestProbeOutcomeMismatch`
- `public const string DigestProbeOutcomeVersionSkew`
- `public const string DigestProbeOutcomeRemoteUnavailable`
- `public static string DigestProbeOutcomeTag(DigestProbeOutcome outcome)`
- `public static readonly Histogram<int> ShipEffectiveBatchSize`
- `public const string ShipEffectiveBatchSizeName`
- `public static readonly Histogram<double> ShipAckLatency`
- `public const string ShipAckLatencyName`
- `public const string TagDepth`
- `public static readonly Counter<long> MerkleWalkLocalised`
- `public const string MerkleWalkLocalisedName`
- `public static readonly Counter<long> MerkleWalkAborted`
- `public const string MerkleWalkAbortedName`
- `public const string MerkleWalkAbortDepthCap`
- `public const string MerkleWalkAbortByteBudget`
- `public const string MerkleWalkAbortRemoteUnavailable`
- `public const string MerkleWalkAbortVersionSkew`
- `public static string MerkleWalkAbortReasonTag(MerkleWalkAbortReason reason)`
- `public static readonly Counter<long> LeafReReplayEntries`
- `public const string LeafReReplayEntriesName`
- `public static readonly Counter<long> LeafReReplaySkipped`
- `public const string LeafReReplaySkippedName`
- `public const string LeafReReplaySkipDisabled`
- `public const string LeafReReplaySkipRangeEmpty`
- `public const string LeafReReplaySkipWalTrimmed`
- `public static string LeafReReplaySkipReasonTag(LeafReReplaySkipReason reason)`
- `public static readonly Counter<long> BootstrapFallbackTriggered`
- `public const string BootstrapFallbackTriggeredName`
- `public static readonly Counter<long> BootstrapFallbackEntries`
- `public const string BootstrapFallbackEntriesName`
- `public static readonly Counter<long> BootstrapFallbackSkipped`
- `public const string BootstrapFallbackSkippedName`
- `public const string BootstrapFallbackSkipDisabled`
- `public const string BootstrapFallbackSkipRangeEmpty`
- `public const string BootstrapFallbackSkipEmpty`
- `public static string BootstrapFallbackSkipReasonTag(BootstrapFallbackSkipReason reason)`
- `public static readonly Counter<long> DigestRemediationSkipped`
- `public const string DigestRemediationSkippedName`
- `public const string DigestRemediationDisabledName`
- `public const string DigestRemediationReasonOptOut`
- `public const string DigestRemediationReasonBudgetExhausted`
- `public const string DigestRemediationReasonCircuitOpen`
- `public static string DigestRemediationDisabledReasonTag(RemediationDisabledReason reason)`
- `public const string TagDictionary`
- `public static readonly Counter<long> DictionaryNegotiation`
- `public const string DictionaryNegotiationName`
- `public static readonly Counter<long> DictionaryBatches`
- `public const string DictionaryBatchesName`
- `public const string DictionaryNegotiationOutcomeMatched`
- `public const string DictionaryNegotiationOutcomeFellBack`
- `public const string DictionaryNegotiationOutcomeUnknown`
- `public const string DictionaryNegotiationOutcomeFingerprintMismatch`
- `public const string DictionaryBatchWith`
- `public const string DictionaryBatchWithout`
- `public static string DictionaryNegotiationOutcomeTag(SharedDictionaryNegotiationResult result)`
- `public static readonly Counter<long> DictionaryConvergence`
- `public const string DictionaryConvergenceName`
- `public const string DictionaryConvergenceOutcomeInstalled`
- `public const string DictionaryConvergenceOutcomeRejected`
- `public const string DictionaryConvergenceOutcomeUnavailable`
- `public const string TagPhase`
- `public const string TagCause`
- `public const string TagMode`
- `public const string SagaPhasePrepare`
- `public const string SagaPhaseCommit`
- `public const string SagaPhaseAbort`
- `public const string SagaCauseVoteAbort`
- `public const string SagaCauseCoordinatorLoss`
- `public const string SagaReasonCommit`
- `public const string SagaReasonEngineUnavailable`
- `public const string SagaReasonInfeasible`
- `public const string SagaReasonPrecondition`
- `public const string SagaReasonBuildFailed`
- `public const string SagaReasonNotReplicated`
- `public const string SagaReasonSingle`
- `public const string SagaReasonSet`
- `public static readonly Histogram<double> SagaPhaseDuration`
- `public const string SagaPhaseDurationName`
- `public static readonly Counter<long> SagaParticipantVotes`
- `public const string SagaParticipantVotesName`
- `public static readonly Counter<long> SagaParticipantCommits`
- `public const string SagaParticipantCommitsName`
- `public static readonly Counter<long> SagaParticipantAborts`
- `public const string SagaParticipantAbortsName`
- `public static readonly Histogram<double> SagaFenceDuration`
- `public const string SagaFenceDurationName`
- `public static readonly Counter<long> SagaCompensations`
- `public const string SagaCompensationsName`

### `Orleans.Lattice.Replication.LatticeReplicationModeChangeRejectedException`

[Source](../../src/lattice.replication/LatticeReplicationModeChangeRejectedException.cs) (line 36).

`public sealed class LatticeReplicationModeChangeRejectedException : InvalidOperationException, ILatticeDomainFault`

- `public string TreeId { get; }`
- `public LatticeMergeMode RequestedMode { get; }`
- `public LatticeMergeMode CurrentMode { get; }`
- `public bool CurrentModeAmbiguous { get; }`
- `public LatticeReplicationModeChangeRejectedException()`
- `public LatticeReplicationModeChangeRejectedException(string message)`
- `public LatticeReplicationModeChangeRejectedException(string message, Exception innerException)`
- `public LatticeReplicationModeChangeRejectedException( string message, string treeId, LatticeMergeMode requestedMode, LatticeMergeMode currentMode, bool currentModeAmbiguous)`

### `Orleans.Lattice.Replication.LatticeReplicationOptions`

[Source](../../src/lattice.replication/LatticeReplicationOptions.cs) (line 15).

`public class LatticeReplicationOptions`

- `public string ClusterId { get; set; }`
- `public IReadOnlyDictionary<string, LatticeMergeMode>? ReplicatedTrees { get; set; }`
- `public Func<string, bool>? KeyFilter { get; set; }`
- `public IReadOnlyCollection<string>? KeyPrefixes { get; set; }`
- `public int ReplogPartitions { get; set; }`
- `public Func<string, IWalStorageProvider>? WalStorageProvider { get; set; }`
- `public int WalMaxBatchEntries { get; set; }`
- `public long WalMaxBatchBytes { get; set; }`
- `public int WalMaxPendingBatches { get; set; }`
- `public int MaxApplyRetries { get; set; }`
- `public TimeSpan SagaDeferralTimeout { get; set; }`
- `public int DeadLetterQueueCapacity { get; set; }`
- `public int CausalBufferMaxEntries { get; set; }`
- `public int CausalAppliedIdentityCapacity { get; set; }`
- `public const int DefaultCausalAppliedIdentityCapacity`
- `public long CausalBufferMaxBytes { get; set; }`
- `public int ShadowForwardDedupeCacheSize { get; set; }`
- `public int ApplyMaxParallelRuns { get; set; }`
- `public bool ContentHashDedupEnabled { get; set; }`
- `public int ContentHashDedupCacheSize { get; set; }`
- `public bool PreShipCoalescingEnabled { get; set; }`
- `public bool ContentHashDedupElisionEnabled { get; set; }`
- `public TimeSpan? WalRetention { get; set; }`
- `public bool AllowWalRetentionWithoutAntiEntropy { get; set; }`
- `public bool AutoBootstrapOnFallOffLog { get; set; }`
- `public TimeSpan OperatorReseedMinInterval { get; set; }`
- `public BoundedExponentialRetryPolicyOptions? BootstrapTransientRetry { get; set; }`
- `public IReadOnlyCollection<string>? ReplicationPeers { get; set; }`
- `public int ShipBatchSize { get; set; }`
- `public int ShipPartitionPageSize { get; set; }`
- `public int ShipCursorWriteInterval { get; set; }`
- `public TimeSpan ShipCursorWriteMaxDelay { get; set; }`
- `public int ShipMaxInFlight { get; set; }`
- `public TimeSpan ShipBackoffInitial { get; set; }`
- `public TimeSpan ShipPhaseTimerPeriod { get; set; }`
- `public TimeSpan ShipSourceIdentityBackstopInterval { get; set; }`
- `public TimeSpan LivenessProbeInterval { get; set; }`
- `public TimeSpan ShipBackoffMax { get; set; }`
- `public double ShipBackoffJitter { get; set; }`
- `public TimeSpan MaintenanceGcInterval { get; set; }`
- `public TimeSpan MaintenanceFallOffCheckInterval { get; set; }`
- `public bool DigestProbeEnabled { get; set; }`
- `public TimeSpan DigestProbeInterval { get; set; }`
- `public double DigestProbeJitter { get; set; }`
- `public bool MerkleWalkEnabled { get; set; }`
- `public int MerkleWalkMaxDepth { get; set; }`
- `public long MerkleWalkMaxBytes { get; set; }`
- `public bool LeafReReplayEnabled { get; set; }`
- `public int LeafReReplayMaxEntries { get; set; }`
- `public long LeafReReplayMaxBytes { get; set; }`
- `public bool BootstrapFallbackEnabled { get; set; }`
- `public int BootstrapFallbackMaxEntries { get; set; }`
- `public long BootstrapFallbackMaxBytes { get; set; }`
- `public bool ShipDoorbellEnabled { get; set; }`
- `public LatticeCompression FramingCompression { get; set; }`
- `public int FramingCompressionLevel { get; set; }`
- `public long MaxInboundDecompressedBytes { get; set; }`
- `public int FramingCompressionMinBatchBytes { get; set; }`
- `public uint FramingCompressionDictionaryId { get; set; }`
- `public bool DictionaryNegotiationEnabled { get; set; }`
- `public bool AutoSharedDictionaryEnabled { get; set; }`
- `public bool WireVersionNegotiationEnabled { get; set; }`
- `public int MinimumSupportedWireVersion { get; set; }`
- `public int UnknownPeerWireVersionFloor { get; set; }`
- `public bool AdaptiveBatchSizingEnabled { get; set; }`
- `public int AdaptiveBatchIncrement { get; set; }`
- `public double AdaptiveBatchDecreaseFactor { get; set; }`
- `public TimeSpan AdaptiveBatchLatencyThreshold { get; set; }`
- `public int AdaptiveBatchWindowLength { get; set; }`
- `public bool AutoRemediateOnDigestMismatch { get; set; }`
- `public double RemediationTrafficBudgetFraction { get; set; }`
- `public TimeSpan RemediationTrafficWindow { get; set; }`
- `public int RemediationFailureThreshold { get; set; }`
- `public TimeSpan RemediationCircuitResetInterval { get; set; }`
- `public const string DefaultClusterId`
- `public const int DefaultReplogPartitions`
- `public const int DefaultWalMaxBatchEntries`
- `public const long DefaultWalMaxBatchBytes`
- `public const long DefaultMaxInboundDecompressedBytes`
- `public const int DefaultWalMaxPendingBatches`
- `public const int DefaultMaxApplyRetries`
- `public static readonly TimeSpan DefaultSagaDeferralTimeout`
- `public const int DefaultDeadLetterQueueCapacity`
- `public const int DefaultCausalBufferMaxEntries`
- `public const long DefaultCausalBufferMaxBytes`
- `public const int DefaultShadowForwardDedupeCacheSize`
- `public const int DefaultApplyMaxParallelRuns`
- `public const bool DefaultContentHashDedupEnabled`
- `public const int DefaultContentHashDedupCacheSize`
- `public const bool DefaultContentHashDedupElisionEnabled`
- `public const bool DefaultPreShipCoalescingEnabled`
- `public const bool DefaultAutoBootstrapOnFallOffLog`
- `public static readonly TimeSpan DefaultOperatorReseedMinInterval`
- `public const int DefaultBootstrapMaxAttempts`
- `public static readonly TimeSpan DefaultBootstrapInitialRetryDelay`
- `public static readonly TimeSpan DefaultBootstrapMaxRetryDelay`
- `public const int DefaultShipBatchSize`
- `public const int DefaultShipPartitionPageSize`
- `public const int DefaultShipCursorWriteInterval`
- `public static readonly TimeSpan DefaultShipCursorWriteMaxDelay`
- `public const int DefaultShipMaxInFlight`
- `public static readonly TimeSpan DefaultShipBackoffInitial`
- `public static readonly TimeSpan DefaultShipPhaseTimerPeriod`
- `public static readonly TimeSpan DefaultShipSourceIdentityBackstopInterval`
- `public static readonly TimeSpan DefaultLivenessProbeInterval`
- `public static readonly TimeSpan DefaultShipBackoffMax`
- `public const double DefaultShipBackoffJitter`
- `public static readonly TimeSpan DefaultMaintenanceGcInterval`
- `public static readonly TimeSpan DefaultMaintenanceFallOffCheckInterval`
- `public const bool DefaultDigestProbeEnabled`
- `public const bool DefaultAllowWalRetentionWithoutAntiEntropy`
- `public static readonly TimeSpan DefaultDigestProbeInterval`
- `public const double DefaultDigestProbeJitter`
- `public const bool DefaultMerkleWalkEnabled`
- `public const int DefaultMerkleWalkMaxDepth`
- `public const long DefaultMerkleWalkMaxBytes`
- `public const bool DefaultLeafReReplayEnabled`
- `public const int DefaultLeafReReplayMaxEntries`
- `public const long DefaultLeafReReplayMaxBytes`
- `public const bool DefaultBootstrapFallbackEnabled`
- `public const int DefaultBootstrapFallbackMaxEntries`
- `public const long DefaultBootstrapFallbackMaxBytes`
- `public const bool DefaultShipDoorbellEnabled`
- `public const LatticeCompression DefaultFramingCompression`
- `public const int DefaultFramingCompressionLevel`
- `public const int DefaultFramingCompressionMinBatchBytes`
- `public const uint DefaultFramingCompressionDictionaryId`
- `public const bool DefaultDictionaryNegotiationEnabled`
- `public const bool DefaultAutoSharedDictionaryEnabled`
- `public const bool DefaultWireVersionNegotiationEnabled`
- `public const int DefaultMinimumSupportedWireVersion`
- `public const int DefaultUnknownPeerWireVersionFloor`
- `public const bool DefaultAdaptiveBatchSizingEnabled`
- `public const int DefaultAdaptiveBatchIncrement`
- `public const double DefaultAdaptiveBatchDecreaseFactor`
- `public static readonly TimeSpan DefaultAdaptiveBatchLatencyThreshold`
- `public const int DefaultAdaptiveBatchWindowLength`
- `public const bool DefaultAutoRemediateOnDigestMismatch`
- `public const double DefaultRemediationTrafficBudgetFraction`
- `public static readonly TimeSpan DefaultRemediationTrafficWindow`
- `public const int DefaultRemediationFailureThreshold`
- `public static readonly TimeSpan DefaultRemediationCircuitResetInterval`

### `Orleans.Lattice.Replication.LatticeReplicationPeerDecommissionOutcome`

[Source](../../src/lattice.replication/ILatticeReplicationPeerDecommissioner.cs) (line 74).

`public readonly record struct LatticeReplicationPeerDecommissionOutcome(string PeerClusterId, int TreeCount, bool AlreadyDecommissioned)`

- `Primary constructor / positional members: (string PeerClusterId, int TreeCount, bool AlreadyDecommissioned)`

### `Orleans.Lattice.Replication.LatticeReplicationPeerStillConfiguredException`

[Source](../../src/lattice.replication/LatticeReplicationPeerStillConfiguredException.cs) (line 27).

`public sealed class LatticeReplicationPeerStillConfiguredException : InvalidOperationException, ILatticeDomainFault`

- `public string PeerClusterId { get; }`
- `public LatticeReplicationPeerStillConfiguredException()`
- `public LatticeReplicationPeerStillConfiguredException(string message)`
- `public LatticeReplicationPeerStillConfiguredException(string message, Exception innerException)`
- `public LatticeReplicationPeerStillConfiguredException(string message, string peerClusterId)`

### `Orleans.Lattice.Replication.LatticeReplicationPreconditionFailedException`

[Source](../../src/lattice.replication/LatticeReplicationPreconditionFailedException.cs) (line 35).

`public sealed class LatticeReplicationPreconditionFailedException : InvalidOperationException, ILatticeDomainFault`

- `public string TreeId { get; }`
- `public LatticeMergeMode RequestedMode { get; }`
- `public LatticeReplicationPreconditionFailedException()`
- `public LatticeReplicationPreconditionFailedException(string message)`
- `public LatticeReplicationPreconditionFailedException(string message, Exception innerException)`
- `public LatticeReplicationPreconditionFailedException( string message, string treeId, LatticeMergeMode requestedMode)`

### `Orleans.Lattice.Replication.LatticeReplicationPreconditionResult`

[Source](../../src/lattice.replication/LatticeReplicationPreconditionResult.cs) (line 10).

`public readonly record struct LatticeReplicationPreconditionResult`

- `public bool IsSatisfied { get; init; }`
- `public string? FailureReason { get; init; }`
- `public static LatticeReplicationPreconditionResult Satisfied { get; }`
- `public static LatticeReplicationPreconditionResult Rejected(string reason)`

### `Orleans.Lattice.Replication.LatticeReplicationSecurityOptions`

[Source](../../src/lattice.replication/Security/LatticeReplicationSecurityOptions.cs) (line 30).

`public sealed class LatticeReplicationSecurityOptions`

- `public bool RequireAuthentication { get; set; }`
- `public bool BindCredentialToOriginCluster { get; set; }`
- `public TimeSpan SecretRefreshInterval { get; set; }`
- `public bool ScanConfigurationForSecrets { get; set; }`

### `Orleans.Lattice.Replication.LatticeReplicationSecurityServiceCollectionExtensions`

[Source](../../src/lattice.replication/Security/LatticeReplicationSecurityServiceCollectionExtensions.cs) (line 16).

`public static class LatticeReplicationSecurityServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeReplicationSecrets<TSource>(this ISiloBuilder builder) where TSource : class, ILatticeReplicationSecretSource`
- `public static ISiloBuilder AddLatticeReplicationSecrets<TSource>( this ISiloBuilder builder, Func<IServiceProvider, TSource> factory) where TSource : class, ILatticeReplicationSecretSource`
- `public static ISiloBuilder AddLatticeReplicationSecretsFromConfiguration( this ISiloBuilder builder, IConfiguration section)`
- `public static ISiloBuilder ConfigureLatticeReplicationSecurity( this ISiloBuilder builder, Action<LatticeReplicationSecurityOptions> configure)`

### `Orleans.Lattice.Replication.LatticeReplicationServiceCollectionExtensions`

[Source](../../src/lattice.replication/LatticeReplicationServiceCollectionExtensions.HealthChecks.cs) (line 7).

`public static partial class LatticeReplicationServiceCollectionExtensions`

- `public static IHealthChecksBuilder AddLatticeReplicationHealthCheck( this IHealthChecksBuilder builder, string? name = null, HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)`

[Source](../../src/lattice.replication/LatticeReplicationServiceCollectionExtensions.ReplicationConfig.cs) (line 11).

`public static partial class LatticeReplicationServiceCollectionExtensions`


[Source](../../src/lattice.replication/LatticeReplicationServiceCollectionExtensions.SystemTrees.cs) (line 6).

`public static partial class LatticeReplicationServiceCollectionExtensions`

- `public static ISiloBuilder ReplicateLatticeSystemTrees( this ISiloBuilder builder, bool includeAudit = false)`

[Source](../../src/lattice.replication/LatticeReplicationServiceCollectionExtensions.WalSaturationFlowControl.cs) (line 6).

`public static partial class LatticeReplicationServiceCollectionExtensions`

- `public static Orleans.Hosting.ISiloBuilder AddWalSaturationReceiverFlowControl( this Orleans.Hosting.ISiloBuilder builder, Action<WalSaturationReceiverFlowControlOptions>? configure = null)`

[Source](../../src/lattice.replication/LatticeReplicationServiceCollectionExtensions.cs) (line 16).

`public static partial class LatticeReplicationServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeReplication( this ISiloBuilder builder, Action<LatticeReplicationOptions> configure, bool enableRuntimeConfig = false)`
- `public static ISiloBuilder ConfigureLatticeReplication( this ISiloBuilder builder, Action<LatticeReplicationOptions> configure)`
- `public static ISiloBuilder ConfigureLatticeReplication( this ISiloBuilder builder, string treeName, Action<LatticeReplicationOptions> configure)`
- `public static ISiloBuilder AddLatticeAutoSharedDictionary( this ISiloBuilder builder, Action<CompressionDictionaryTrainingOptions>? configureTraining = null)`

[Source](../../src/lattice.replication/LatticeSagaParticipantRegistrationExtensions.cs) (line 7).

`public static partial class LatticeReplicationServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeSagaParticipant<TParticipant>( this ISiloBuilder builder, string? name = null) where TParticipant : class, ISagaParticipant`

### `Orleans.Lattice.Replication.LatticeReplicationSharedSecret`

[Source](../../src/lattice.replication/Security/LatticeReplicationSharedSecret.cs) (line 14).

`public static class LatticeReplicationSharedSecret`

- `public const int MinimumLength`
- `public static string Generate(int byteLength = 32)`
- `public static bool IsWellFormed(string? secret)`
- `public static bool FixedTimeEquals(string? a, string? b)`

### `Orleans.Lattice.Replication.LatticeReplicationTreeStatus`

[Source](../../src/lattice.replication/LatticeReplicationTreeStatus.cs) (line 37).

`public readonly record struct LatticeReplicationTreeStatus( string TreeId, bool Enabled, LatticeMergeMode? Mode, bool Ambiguous)`

- `Primary constructor / positional members: ( string TreeId, bool Enabled, LatticeMergeMode? Mode, bool Ambiguous)`
- `public LatticeReplicationEnrollmentSource Source { get; init; }`

### `Orleans.Lattice.Replication.LatticeSystemTreeNames`

[Source](../../src/lattice.replication/LatticeSystemTreeNames.cs) (line 38).

`public static class LatticeSystemTreeNames`

- `public const string MembershipGroups`
- `public const string MembershipEdges`
- `public const string AuthPolicy`
- `public const string AuthAudit`
- `public const string ReplicationConfig`
- `public const string ReplicationConfigMapKey`
- `public static IReadOnlyDictionary<string, LatticeMergeMode> BuildReplicationConfigEnrolmentMap()`
- `public static IReadOnlyDictionary<string, LatticeMergeMode> BuildEnrolmentMap(bool includeAudit)`

### `Orleans.Lattice.Replication.LeafReReplayOutcome`

[Source](../../src/lattice.replication/LeafReReplayOutcome.cs) (line 9).

`public readonly record struct LeafReReplayOutcome`

- `public bool Attempted { get; init; }`
- `public int RangesProcessed { get; init; }`
- `public int EntriesReReplayed { get; init; }`
- `public LeafReReplaySkipReason SkipReason { get; init; }`
- `public static LeafReReplayOutcome NotAttempted`

### `Orleans.Lattice.Replication.LeafReReplayRange`

[Source](../../src/lattice.replication/LeafReReplayRange.cs) (line 17).

`public readonly record struct LeafReReplayRange`

- `public string? StartKey { get; init; }`
- `public string? EndKey { get; init; }`
- `public bool Contains(string? key)`

### `Orleans.Lattice.Replication.LeafReReplaySkipReason`

[Source](../../src/lattice.replication/LeafReReplaySkipReason.cs) (line 10).

`public enum LeafReReplaySkipReason`

- `None = 0`
- `Disabled = 1`
- `RangeEmpty = 2`
- `WalTrimmed = 3`

### `Orleans.Lattice.Replication.LocalVcSeedReport`

[Source](../../src/lattice.replication/LocalVcSeedReport.cs) (line 45).

`public readonly record struct LocalVcSeedReport( string TreeName, VersionVector? Frontier, long EntriesScanned, bool SeedApplied)`

- `Primary constructor / positional members: ( string TreeName, VersionVector? Frontier, long EntriesScanned, bool SeedApplied)`

### `Orleans.Lattice.Replication.LongTier`

[Source](../../src/lattice.replication/LatticeReplicationHealthCheckOptions.cs) (line 164).

`public readonly record struct LongTier(long Degraded, long Unhealthy)`

- `Primary constructor / positional members: (long Degraded, long Unhealthy)`

### `Orleans.Lattice.Replication.MerkleWalkAbortReason`

[Source](../../src/lattice.replication/MerkleWalkAbortReason.cs) (line 8).

`public enum MerkleWalkAbortReason`

- `None = 0`
- `DepthCapExceeded = 1`
- `ByteBudgetExceeded = 2`
- `RemoteUnavailable = 3`
- `VersionSkew = 4`

### `Orleans.Lattice.Replication.MerkleWalkOutcome`

[Source](../../src/lattice.replication/MerkleWalkOutcome.cs) (line 9).

`public readonly record struct MerkleWalkOutcome`

- `public bool Localised { get; init; }`
- `public int LeavesLocalised { get; init; }`
- `public IReadOnlyList<LeafReReplayRange> LocalisedRanges { get; init; }`
- `public int DepthReached { get; init; }`
- `public MerkleWalkAbortReason AbortReason { get; init; }`
- `public long BytesInspected { get; init; }`
- `public static MerkleWalkOutcome NotLocalised`

### `Orleans.Lattice.Replication.MerkleWalkProbeRequest`

[Source](../../src/lattice.replication/MerkleWalkProbeRequest.cs) (line 11).

`public readonly record struct MerkleWalkProbeRequest`

- `public string TreeName { get; init; }`
- `public int ShardIndex { get; init; }`
- `public string? RangeStartKey { get; init; }`
- `public string? RangeEndKey { get; init; }`
- `public int Depth { get; init; }`

### `Orleans.Lattice.Replication.MerkleWalkProbeResponse`

[Source](../../src/lattice.replication/MerkleWalkProbeResponse.cs) (line 12).

`public readonly record struct MerkleWalkProbeResponse`

- `public bool Available { get; init; }`
- `public LeafProjectionDigest Digest { get; init; }`
- `public static MerkleWalkProbeResponse Unavailable`

### `Orleans.Lattice.Replication.NoOpReceiverFlowControlPolicy`

[Source](../../src/lattice.replication/NoOpReceiverFlowControlPolicy.cs) (line 12).

`public sealed class NoOpReceiverFlowControlPolicy : IReceiverFlowControlPolicy`

- `public static NoOpReceiverFlowControlPolicy Instance { get; }`
- `public ValueTask<ReceiverFlowControlHint> EvaluateAsync( ReceiverFlowControlContext context, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.NoParticipantSagaControlHandler`

[Source](../../src/lattice.replication/NoParticipantSagaControlHandler.cs) (line 13).

`public sealed class NoParticipantSagaControlHandler : ILatticeSagaControlHandler`

- `public Task<SagaControlResponse> PrepareAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `public Task<SagaControlResponse> CommitAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `public Task<SagaControlResponse> AbortAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`
- `public Task<SagaControlResponse> GetStatusAsync(SagaControlRequest request, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.OperatorReseedDecision`

[Source](../../src/lattice.replication/OperatorReseedDecision.cs) (line 36).

`public readonly record struct OperatorReseedDecision( bool Triggered, DateTimeOffset? LastRequestedAt, TimeSpan? RetryAfter)`

- `Primary constructor / positional members: ( bool Triggered, DateTimeOffset? LastRequestedAt, TimeSpan? RetryAfter)`

### `Orleans.Lattice.Replication.PeerChangeKind`

[Source](../../src/lattice.replication/PeerChangeKind.cs) (line 13).

`public enum PeerChangeKind`

- `Added = 0`
- `Removed = 1`

### `Orleans.Lattice.Replication.PeerChanged`

[Source](../../src/lattice.replication/PeerChanged.cs) (line 27).

`public readonly record struct PeerChanged(string PeerClusterId, PeerChangeKind Kind)`

- `Primary constructor / positional members: (string PeerClusterId, PeerChangeKind Kind)`

### `Orleans.Lattice.Replication.PeerHighWaterMarkRequest`

[Source](../../src/lattice.replication/PeerHighWaterMarkRequest.cs) (line 12).

`public readonly record struct PeerHighWaterMarkRequest`

- `public string TreeName { get; init; }`
- `public string OriginClusterId { get; init; }`

### `Orleans.Lattice.Replication.PeerHighWaterMarkResponse`

[Source](../../src/lattice.replication/PeerHighWaterMarkResponse.cs) (line 15).

`public readonly record struct PeerHighWaterMarkResponse`

- `public HybridLogicalClock Clock { get; init; }`

### `Orleans.Lattice.Replication.ReceiverFlowControlContext`

[Source](../../src/lattice.replication/ReceiverFlowControlContext.cs) (line 16).

`public readonly record struct ReceiverFlowControlContext`

- `public string TreeName { get; init; }`
- `public string OriginClusterId { get; init; }`
- `public int EntryCount { get; init; }`
- `public double ApplyDurationMs { get; init; }`

### `Orleans.Lattice.Replication.ReceiverFlowControlHint`

[Source](../../src/lattice.replication/ReceiverFlowControlHint.cs) (line 21).

`public readonly record struct ReceiverFlowControlHint`

- `public int? SuggestedBatchSize { get; init; }`
- `public int? PauseForMs { get; init; }`
- `public static ReceiverFlowControlHint None`

### `Orleans.Lattice.Replication.RemediationDisabledReason`

[Source](../../src/lattice.replication/RemediationDisabledReason.cs) (line 16).

`public enum RemediationDisabledReason`

- `OptOut = 0`
- `BudgetExhausted = 1`
- `CircuitOpen = 2`

### `Orleans.Lattice.Replication.RemediationGuard`

[Source](../../src/lattice.replication/RemediationGuard.cs) (line 39).

`public sealed class RemediationGuard`

- `public RemediationGuard()`
- `public bool TryBeginRemediation(string peer, int windowBudget, long windowTicks, long nowTicks)`
- `public void RecordEntriesShipped(string peer, int entries)`
- `public bool IsCircuitBlocking(string peer, long cooldownTicks, long nowTicks)`
- `public void RecordSuccess(string peer)`
- `public bool RecordFailure(string peer, int failureThreshold, long nowTicks)`
- `public static void PublishDisabled(string tree, string peer, RemediationDisabledReason reason)`
- `public static void ClearDisabled(string tree, string peer)`

### `Orleans.Lattice.Replication.RemoteSnapshotMetadata`

[Source](../../src/lattice.replication/RemoteSnapshotMetadata.cs) (line 33).

`public readonly record struct RemoteSnapshotMetadata`

- `public string TreeName { get; init; }`
- `public string SourceClusterId { get; init; }`
- `public HybridLogicalClock AsOfHlc { get; init; }`
- `public VersionVector CausalStableFrontier { get; init; }`
- `public long ExportEpoch { get; init; }`
- `public SnapshotSourceGeneration? OpenGeneration { get; init; }`

### `Orleans.Lattice.Replication.RemoteSnapshotMetadataRequest`

[Source](../../src/lattice.replication/RemoteSnapshotMetadataRequest.cs) (line 21).

`public readonly record struct RemoteSnapshotMetadataRequest`

- `public string TreeName { get; init; }`
- `public string SourceClusterId { get; init; }`
- `public HybridLogicalClock FromAsOfHlc { get; init; }`

### `Orleans.Lattice.Replication.RemoteSnapshotProvider`

[Source](../../src/lattice.replication/RemoteSnapshotProvider.cs) (line 45).

`public sealed class RemoteSnapshotProvider : IBootstrapSnapshotSource`

- `public RemoteSnapshotProvider( IRemoteSnapshotTransport transport, ILogger<RemoteSnapshotProvider> logger)`
- `public Task<SnapshotStream> ExportAsync( string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)`
- `public async Task<SnapshotStream> ExportAsync( string treeName, string sourceClusterId, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Replication.RemoteSnapshotStreamItem`

[Source](../../src/lattice.replication/RemoteSnapshotStreamItem.cs) (line 20).

`public readonly record struct RemoteSnapshotStreamItem`

- `public SnapshotEntry Entry { get; init; }`
- `public SnapshotSourceGeneration? CloseGeneration { get; init; }`

### `Orleans.Lattice.Replication.ReplicationAck`

[Source](../../src/lattice.replication/ReplicationAck.cs) (line 13).

`public readonly record struct ReplicationAck`

- `public bool Accepted { get; init; }`
- `public HybridLogicalClock HighestAppliedHlc { get; init; }`
- `public HybridLogicalClock? BlockedAtHlc { get; init; }`
- `public int? SuggestedBatchSize { get; init; }`
- `public int? PauseForMs { get; init; }`
- `public int? SupportedWireVersion { get; init; }`
- `public uint[]? AdvertisedDictionaryIds { get; init; }`
- `public AdvertisedCompressionDictionary[]? AdvertisedDictionaries { get; init; }`
- `public long? BootstrapEpoch { get; init; }`
- `public Guid? ReceiverLineage { get; init; }`
- `public bool SourceLineageRefused { get; init; }`

### `Orleans.Lattice.Replication.ReplicationBatch`

[Source](../../src/lattice.replication/ReplicationBatch.cs) (line 17).

`public readonly record struct ReplicationBatch`

- `public string TargetClusterId { get; init; }`
- `public string TreeName { get; init; }`
- `public string OriginClusterId { get; init; }`
- `public ReadOnlyMemory<byte> Payload { get; init; }`
- `public ReplicationBatchEnvelope? Envelope { get; init; }`
- `public ReplicationBatchEncodedEnvelope? EncodedEnvelope { get; init; }`

### `Orleans.Lattice.Replication.ReplicationBatchEncodedEnvelope`

[Source](../../src/lattice.replication/ReplicationBatchEncodedEnvelope.cs) (line 31).

`public readonly record struct ReplicationBatchEncodedEnvelope`

- `public EncodedBatchHeader Header { get; init; }`
- `public System.ReadOnlyMemory<System.ArraySegment<byte>> EncodedEntries { get; init; }`

### `Orleans.Lattice.Replication.ReplicationBatchEnvelope`

[Source](../../src/lattice.replication/ReplicationBatchEnvelope.cs) (line 22).

`public readonly record struct ReplicationBatchEnvelope`

- `public int WireVersion { get; init; }`
- `public string TreeName { get; init; }`
- `public string OriginClusterId { get; init; }`
- `public IReadOnlyList<WalRecord> Entries { get; init; }`
- `public const int CurrentVersion`
- `public const int CurrentMinorVersion`

### `Orleans.Lattice.Replication.ReplicationContactDirection`

[Source](../../src/lattice.replication/ReplicationPeerStats.cs) (line 13).

`public enum ReplicationContactDirection`

- `Outbound = 0`
- `Inbound = 1`

### `Orleans.Lattice.Replication.ReplicationPeerSnapshot`

[Source](../../src/lattice.replication/ReplicationPeerStats.cs) (line 556).

`public readonly record struct ReplicationPeerSnapshot( string Tree, string Peer, long EntriesBehind, long BytesBehind, long ConsecutiveErrors, double LastContactSeconds)`

- `Primary constructor / positional members: ( string Tree, string Peer, long EntriesBehind, long BytesBehind, long ConsecutiveErrors, double LastContactSeconds)`
- `public ReplicationContactDirection Direction { get; init; }`
- `public long InFlight { get; init; }`

### `Orleans.Lattice.Replication.ReplicationPeerStats`

[Source](../../src/lattice.replication/ReplicationPeerStats.StatusRead.cs) (line 3).

`public partial class ReplicationPeerStats`


[Source](../../src/lattice.replication/ReplicationPeerStats.cs) (line 52).

`public partial class ReplicationPeerStats`

- `public ReplicationPeerStats()`
- `public void RecordBacklog(string tree, string peer, long entriesBehind, long bytesBehind)`
- `public void RecordInFlight(string tree, string peer, long depth)`
- `public void RecordSuccess(string tree, string peer)`
- `public void RecordError(string tree, string peer)`
- `public void RecordInboundSuccess(string tree, string originPeer)`
- `public void RecordInboundError(string tree, string originPeer)`
- `public IReadOnlyCollection<ReplicationPeerSnapshot> Snapshot()`

### `Orleans.Lattice.Replication.ReplicationTenantIsolationDecision`

[Source](../../src/lattice.replication/ReplicationTenantIsolationDecision.cs) (line 18).

`public enum ReplicationTenantIsolationDecision`

- `Admit = 0`
- `RejectUnknownTenant = 1`
- `RejectOutOfRegion = 2`
- `RejectSuspendedTenant = 3`

### `Orleans.Lattice.Replication.ReplicationTypeAliases`

[Source](../../src/lattice.replication/ReplicationTypeAliases.cs) (line 12).

`public static class ReplicationTypeAliases`


### `Orleans.Lattice.Replication.SagaControlRequest`

[Source](../../src/lattice.replication/SagaControlRequest.cs) (line 18).

`public readonly record struct SagaControlRequest`

- `public string SagaId { get; init; }`
- `public string TargetTree { get; init; }`
- `public string ManifestId { get; init; }`
- `public string CoordinatorClusterId { get; init; }`
- `public string? SetId { get; init; }`
- `public string? RequesterClusterId { get; init; }`

### `Orleans.Lattice.Replication.SagaControlResponse`

[Source](../../src/lattice.replication/SagaControlResponse.cs) (line 15).

`public readonly record struct SagaControlResponse`

- `public string SagaId { get; init; }`
- `public SagaPhase Phase { get; init; }`
- `public SagaVote Vote { get; init; }`
- `public string Detail { get; init; }`

### `Orleans.Lattice.Replication.SagaParticipantPrepareResult`

[Source](../../src/lattice.replication/SagaParticipantPrepareResult.cs) (line 19).

`public readonly record struct SagaParticipantPrepareResult(SagaVote Vote, string? Detail = null)`

- `Primary constructor / positional members: (SagaVote Vote, string? Detail = null)`

### `Orleans.Lattice.Replication.SagaPhase`

[Source](../../src/lattice.replication/SagaPhase.cs) (line 10).

`public enum SagaPhase`

- `None = 0`
- `Prepared = 1`
- `Committed = 2`
- `Aborted = 3`

### `Orleans.Lattice.Replication.SagaVote`

[Source](../../src/lattice.replication/SagaVote.cs) (line 9).

`public enum SagaVote`

- `None = 0`
- `Commit = 1`
- `Abort = 2`

### `Orleans.Lattice.Replication.SharedDictionaryNegotiation`

[Source](../../src/lattice.replication/SharedDictionaryNegotiation.cs) (line 15).

`public static class SharedDictionaryNegotiation`

- `public static SharedDictionaryNegotiationResult Negotiate( uint configuredDictionaryId, IReadOnlyCollection<uint>? peerAdvertisedIds)`
- `public static SharedDictionaryNegotiationResult Negotiate( uint configuredDictionaryId, ulong configuredFingerprint, IReadOnlyCollection<AdvertisedCompressionDictionary>? peerAdvertised)`

### `Orleans.Lattice.Replication.SharedDictionaryNegotiationResult`

[Source](../../src/lattice.replication/SharedDictionaryNegotiationResult.cs) (line 44).

`public readonly record struct SharedDictionaryNegotiationResult( uint EffectiveDictionaryId, bool Matched, bool PeerCapabilityKnown, bool FellBack, bool FingerprintMismatch = false)`

- `Primary constructor / positional members: ( uint EffectiveDictionaryId, bool Matched, bool PeerCapabilityKnown, bool FellBack, bool FingerprintMismatch = false)`

### `Orleans.Lattice.Replication.SharedDictionaryNegotiationSnapshot`

[Source](../../src/lattice.replication/SharedDictionaryNegotiationSnapshot.cs) (line 31).

`public readonly record struct SharedDictionaryNegotiationSnapshot( string Tree, string Peer, uint EffectiveDictionaryId, bool Matched, bool PeerCapabilityKnown, bool FellBack, bool FingerprintMismatch = false)`

- `Primary constructor / positional members: ( string Tree, string Peer, uint EffectiveDictionaryId, bool Matched, bool PeerCapabilityKnown, bool FellBack, bool FingerprintMismatch = false)`

### `Orleans.Lattice.Replication.SharedDictionaryNegotiationState`

[Source](../../src/lattice.replication/SharedDictionaryNegotiationState.cs) (line 23).

`public sealed class SharedDictionaryNegotiationState`

- `public void Record(string tree, string peer, SharedDictionaryNegotiationResult result)`
- `public IReadOnlyCollection<SharedDictionaryNegotiationSnapshot> Snapshot()`

### `Orleans.Lattice.Replication.SnapshotEntry`

[Source](../../src/lattice.replication/SnapshotEntry.cs) (line 55).

`public readonly record struct SnapshotEntry`

- `public string Key { get; init; }`
- `public byte[] Value { get; init; }`
- `public HybridLogicalClock Timestamp { get; init; }`
- `public bool IsPrepared { get; init; }`
- `public bool IsTombstone { get; init; }`
- `public Guid TransactionId { get; init; }`
- `public int SourceShardIndex { get; init; }`
- `public int AtomicBatchSize { get; init; }`
- `public int AtomicBatchIndex { get; init; }`
- `public long ExpiresAtTicks { get; init; }`
- `public byte[]? Delta { get; init; }`
- `public Orleans.Lattice.LatticeMergeMode Mode { get; init; }`
- `public bool? SettledDecision { get; init; }`
- `public bool IsDecision`
- `public bool Equals(SnapshotEntry other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Replication.SnapshotSourceGeneration`

[Source](../../src/lattice.replication/SnapshotSourceGeneration.cs) (line 8).

`public readonly record struct SnapshotSourceGeneration`

- `public string? PhysicalTreeId { get; init; }`
- `public long? ShardMapVersion { get; init; }`
- `public Guid? Lineage { get; init; }`
- `public long? DeleteEpoch { get; init; }`
- `public bool? IsDeleted { get; init; }`

### `Orleans.Lattice.Replication.SnapshotStream`

[Source](../../src/lattice.replication/SnapshotStream.cs) (line 18).

`public sealed class SnapshotStream`

- `public string TreeName { get; }`
- `public HybridLogicalClock AsOfHlc { get; }`
- `public VersionVector CausalStableFrontier { get; }`
- `public SnapshotSourceGeneration? OpenGeneration { get; init; }`
- `public SnapshotSourceGeneration? CloseGeneration { get; internal set; }`
- `public IAsyncEnumerable<SnapshotEntry> Entries { get; }`
- `public SnapshotStream( string treeName, HybridLogicalClock asOfHlc, VersionVector causalStableFrontier, IAsyncEnumerable<SnapshotEntry> entries)`

### `Orleans.Lattice.Replication.WalSaturationReceiverFlowControlOptions`

[Source](../../src/lattice.replication/WalSaturationReceiverFlowControlOptions.cs) (line 24).

`public sealed class WalSaturationReceiverFlowControlOptions`

- `public const double DefaultThrottledBatchRatio`
- `public const int DefaultThrottledPauseMs`
- `public const int DefaultSaturatedBatchSize`
- `public const int DefaultSaturatedPauseMs`
- `public double ThrottledBatchRatio { get; set; }`
- `public int ThrottledPauseMs { get; set; }`
- `public int SaturatedBatchSize { get; set; }`
- `public int SaturatedPauseMs { get; set; }`

### `Orleans.Lattice.Replication.WalSaturationReceiverFlowControlPolicy`

[Source](../../src/lattice.replication/WalSaturationReceiverFlowControlPolicy.cs) (line 43).

`public sealed class WalSaturationReceiverFlowControlPolicy : IReceiverFlowControlPolicy`

- `public WalSaturationReceiverFlowControlPolicy( IWalSaturationSignal? signal, IOptionsMonitor<LatticeReplicationOptions> replicationOptions, IOptionsMonitor<WalSaturationReceiverFlowControlOptions> flowControlOptions)`
- `public ValueTask<ReceiverFlowControlHint> EvaluateAsync( ReceiverFlowControlContext context, CancellationToken cancellationToken)`

### `Orleans.Lattice.Replication.WireVersionDownEncoder`

[Source](../../src/lattice.replication/ReplicationAck.cs) (line 520).

`public static class WireVersionDownEncoder`

- `public const int MinimumDownEncodableWireVersion`
- `public static void EnsureDownEncodable( int effectiveWireVersion, LatticeMergeMode mode, LatticeCompression compression)`
- `public static EncodedBatchHeader PrepareHeader( in EncodedBatchHeader header, int effectiveWireVersion)`

### `Orleans.Lattice.Replication.WireVersionNegotiation`

[Source](../../src/lattice.replication/ReplicationAck.cs) (line 284).

`public static class WireVersionNegotiation`

- `public static WireVersionNegotiationResult Negotiate( int localCurrentVersion, int minimumSupportedVersion, int unknownPeerFloorVersion, int? peerAdvertisedVersion)`

### `Orleans.Lattice.Replication.WireVersionNegotiationResult`

[Source](../../src/lattice.replication/ReplicationAck.cs) (line 408).

`public readonly record struct WireVersionNegotiationResult`

- `public int EffectiveWireVersion { get; init; }`
- `public bool DowngradeActive { get; init; }`
- `public bool PeerCapabilityKnown { get; init; }`

### `Orleans.Lattice.Replication.WireVersionNegotiationSnapshot`

[Source](../../src/lattice.replication/ReplicationPeerStats.cs) (line 752).

`public readonly record struct WireVersionNegotiationSnapshot( string Tree, string Peer, int NegotiatedVersion, bool DowngradeActive, bool PeerCapabilityKnown)`

- `Primary constructor / positional members: ( string Tree, string Peer, int NegotiatedVersion, bool DowngradeActive, bool PeerCapabilityKnown)`

### `Orleans.Lattice.Replication.WireVersionNegotiationState`

[Source](../../src/lattice.replication/ReplicationPeerStats.cs) (line 605).

`public class WireVersionNegotiationState`

- `public WireVersionNegotiationState()`
- `public void Record(string tree, string peer, WireVersionNegotiationResult result)`
- `public IReadOnlyCollection<WireVersionNegotiationSnapshot> Snapshot()`
