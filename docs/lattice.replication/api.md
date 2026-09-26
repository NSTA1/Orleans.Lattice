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
| `FallOffLogDecision` | readonly record struct | Result of a receiver-side fall-off check: whether the receiver's per-origin high-water mark is older than the sender's oldest retained WAL entry, and whether a bootstrap was triggered or absorbed. | `FellOffLog`, `LocalHighWaterMark`, `BootstrapTriggered`, `Suppressed` |
| `OperatorReseedDecision` | readonly record struct | Result of an operator snapshot request. | `Triggered`, `LastRequestedAt`, `RetryAfter` |

`ReplicatedTrees` is the static opt-in map from tree id to `LatticeMergeMode`. Trees not in the map do not ship unless runtime replication config is enabled (`AddLatticeReplication(..., enableRuntimeConfig: true)`), in which case a tree enabled at runtime through `ILatticeReplicationConfigAuthority` replicates too - see [Runtime Replication Config](runtime-config.md). `KeyFilter` and `KeyPrefixes` narrow which keys the shipper emits from opted-in trees; snapshot exports do not apply them.

## Change feed

See [Change Feed](change-feed.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IChangeFeed` | interface | Cursor-driven, pull-based read of locally-authored WAL entries for a tree. | `Subscribe(string, HybridLogicalClock, bool, CancellationToken)`, `Subscribe(string, ChangeFeedCursor, bool, CancellationToken)`, `GetCurrentCursorAsync` |
| `ChangeFeedCursor` | readonly struct | Per-partition offset cursor for lossless WAL consumption. | `Initial`, constructor from offsets, `GetOffsetForPartition`, `PartitionOffsets` |

The feed is for locally-authored writes. Entries installed by inbound apply are visible in local state but are not re-emitted through this feed; consumers that need to observe receiver-side installs should decorate `IReplicationApplier`.

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
| `ApplyResult` | readonly record struct | Apply outcome and high-water-mark visibility. | `Applied`, `HighWaterMark`, `Deferred` (the batch was held back by an inbound receive fence and must be retried) |
| `IReplicationLocalVcSeeder` | interface | Rebuilds a tree's local vector clock from the vector-clock slots on its values after an intra-cluster snapshot restore, so inbound dependency checks do not run against a zeroed vector. A no-op for a non-replicated tree. | `SeedFromTreeAsync(string, CancellationToken)` returning `LocalVcSeedReport` |
| `LocalVcSeedReport` | readonly record struct | Observable result of local version-vector seeding. | `TreeName`, `Frontier`, `EntriesScanned`, `SeedApplied` |

`ApplyAsync` preserves the source cluster HLC and origin id. `ApplyBatchAsync` is the preferred batch seam because implementations can collapse high-water-mark updates and drain causal buffers once per batch.

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
| `ILatticeReplicationDeadLetters` | interface | Lists, discards, and replays quarantined apply failures. | `ListAsync`, `CountAsync`, `DiscardAsync`, `ReplayAsync` |
| `DeadLetterEntry` | readonly record struct | Retained failed apply entry. | `EntryId`, `Entry`, `FailureReason`, `RetryCount`, `EnqueuedAtTicks` |

Replay runs the parked entry through the canonical applier and removes it only when the replay returns successfully.

## Operator, admin, and WAL introspection

See [Auto-Bootstrap](auto-bootstrap.md), [WAL](wal.md), and [Observability](observability.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationAdmin` | interface | Operator-driven snapshot re-seed controls. | `RequestSnapshotAsync`, `ForceRequestSnapshotAsync` |
| `ILatticeWalIntrospection` | interface | Sender-side view of retained WAL availability. | `GetOldestAvailableHlcAsync`, `GetOldestAvailableHlcByOriginAsync` |
| `ILatticeFallOffLogDetector` | interface | Receiver-side check of the local per-origin high-water mark against a sender's oldest retained WAL entry; on fall-off it records the metric and, when `AutoBootstrapOnFallOffLog` is on, starts a bootstrap. | `CheckAndTriggerAsync` returning `FallOffLogDecision` |

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
| `LatticeReplicationHealthCheckOptions` | sealed class | Health-check thresholds. | `EntriesBehind`, `LastContactSeconds`, `ConsecutiveErrors`, `UnhealthyAfter`, `InboundDegradedAfter`, `InboundCriticalAfter`; the nested `LongTier` / `DoubleTier` threshold records; `DefaultName` |

## Flow control

See [Receiver Flow Control](receiver-flow-control.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReceiverFlowControlPolicy` | interface | Maps receiver state to ack hints. | `EvaluateAsync` |
| `ReceiverFlowControlContext` | readonly record struct | Input to a flow-control policy. | `TreeName`, `OriginClusterId`, `EntryCount`, `ApplyDurationMs` |
| `ReceiverFlowControlHint` | readonly record struct | Suggested sender limits. | `SuggestedBatchSize`, `PauseForMs`, `None` |
| `NoOpReceiverFlowControlPolicy` | sealed class | Policy that returns no hints. | `Instance`, `EvaluateAsync` |
| `WalSaturationReceiverFlowControlOptions` | sealed class | Tunes how the throttled and saturated WAL states map to hints (a healthy WAL returns no hint). | `ThrottledBatchRatio`, `ThrottledPauseMs`, `SaturatedBatchSize`, `SaturatedPauseMs` |
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
| `LatticeReplicationSecurityOptions` | sealed class | Shared-secret authentication options. | `RequireAuthentication`, `SecretRefreshInterval`, `ScanConfigurationForSecrets` |
| `LatticeReplicationEnvironmentVariables` | static class | Environment-variable names for replication secrets. | `Prefix`, `Secret`, `AcceptedSecrets`, `PeerSecretPrefix`, `AllowSourceTreeSecrets` |
| `LatticeReplicationSharedSecret` | static class | Shared-secret generation and validation helpers. | `MinimumLength`, `Generate`, `IsWellFormed`, `FixedTimeEquals` |

The gRPC binding requires HTTPS endpoints unless `AllowPlaintextEndpoints` is enabled. Shared-secret authentication is configured through the security extension methods above.

## Cross-cluster saga participation

The saga service-provider interfaces let a host join the coordinated cross-cluster commit protocol that backs a fleet-wide restore. Register an implementation with `AddLatticeSagaParticipant`. See [Coordinated restore](coordinated-restore.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ISagaParticipant` | interface | Service-provider interface a host implements to take part in a cross-cluster saga: it votes on prepare, then applies or discards its staged work. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` |
| `ISagaControlChannel` | interface | Outbound control channel the coordinator uses to drive a named peer cluster through the saga phases. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` (each taking the target `clusterId`) |
| `ISagaPeerAuthorizer` | interface | Fail-closed gate deciding whether an inbound saga control request from a claimed origin cluster is accepted. | `IsAuthorizedAsync(string? originClusterId, CancellationToken)` |
| `ILatticeSagaControlHandler` | interface | Server-side delegation seam for the inbound saga control channel. The gRPC saga service validates the request and enforces peer authorization, then delegates each imperative RPC to this handler. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` (each taking a `SagaControlRequest`) |
| `NoParticipantSagaControlHandler` | sealed class | The transport-only fallback handler the gRPC binding registers with `TryAddSingleton`. `AddLatticeReplication` registers its own durable handler - which routes each inbound saga call to the per-saga participant - with `TryAddSingleton` too, so on a silo that calls it before the gRPC binding (as the setup above does) the durable handler is the effective one and this class is never used. Holds no participant state: it reports `SagaPhase.None` for every saga and votes `SagaVote.Abort` on prepare, because a participant that cannot durably prepare must not let the coordinator commit. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` |
| `SagaPhase` | enum | The durable phase a participant reports for a saga. | `None = 0`, `Prepared = 1`, `Committed = 2`, `Aborted = 3` |
| `SagaVote` | enum | A participant's prepare-phase vote. | `None = 0`, `Commit = 1`, `Abort = 2` |
| `SagaControlRequest` | readonly record struct | The request every saga control call carries. | `SagaId`, `TargetTree`, `ManifestId`, `CoordinatorClusterId`, `SetId` |
| `SagaControlResponse` | readonly record struct | A participant cluster's answer to a saga control call. | `SagaId`, `Phase`, `Vote`, `Detail` |
| `SagaParticipantPrepareResult` | readonly record struct | An `ISagaParticipant` prepare vote. | `Vote`, `Detail` |
| `LatticeSystemTreeNames` | static class | The reserved system-tree names the replication package owns. | `MembershipGroups`, `MembershipEdges`, `AuthPolicy`, `AuthAudit`, `ReplicationConfig`, `ReplicationConfigMapKey`, `BuildEnrolmentMap`, `BuildReplicationConfigEnrolmentMap` |

## Tenant isolation and runtime configuration authority

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationTenantIsolationGate` | interface | Evaluates whether an inbound replicated entry is admissible for the tenant its tree id names - the tenant must exist, be resident in this region, and be active - so a peer can never widen tenant or region scope. The core default is inactive; the tenancy add-on supplies a real gate. | `IsActive`, `EvaluateAsync(string, CancellationToken)` returning `ReplicationTenantIsolationDecision` |
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
