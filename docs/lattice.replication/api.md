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
| `LatticeReplicationServiceCollectionExtensions` | static class | Registers replication services on an Orleans silo. | `AddLatticeReplication`, `ConfigureLatticeReplication`, `AddLatticeAutoSharedDictionary`, `AddLatticeReplicationHealthCheck`, `AddWalSaturationReceiverFlowControl`, `AddLatticeSagaParticipant` |
| `LatticeReplicationSecurityServiceCollectionExtensions` | static class | Registers shared-secret sources and security options. | `AddLatticeReplicationSecrets`, `AddLatticeReplicationSecretsFromConfiguration`, `ConfigureLatticeReplicationSecurity` |

`AddLatticeReplication` wires the replication pipeline and default no-op transport. A production deployment replaces the transport by adding the gRPC binding or a custom `IReplicationTransport`. `ConfigureLatticeReplication` follows .NET named options: the overload without a tree name sets global defaults; the `treeName` overload overrides a single tree.

The gRPC transport and the Azure Table WAL backend ship as separate packages with their own API references - see [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/api.md) (`AddLatticeReplicationGrpc`, `MapLatticeReplicationGrpc`, `LatticeReplicationGrpcOptions`) and [Orleans.Lattice.Storage.AzureTable](../lattice.storage.azuretable/api.md) (`AddAzureTableWalStorage`, `AzureTableWalStorageProvider`, `AzureTableWalStorageOptions`).

## Replication modes and configuration types

See [Replication Modes](replication-modes.md) and [Replication Drivers](replication-drivers.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeReplicationOptions` | class | Main replication options. | Identity, tree opt-in, WAL, apply, ship, bootstrap, compression, wire-version, and remediation properties. See [Configuration](configuration.md). |
| `FallOffLogDecision` | readonly record struct | Result of checking whether a peer cursor is older than the sender's retained WAL. | Decision status slots exposed by the record. |
| `OperatorReseedDecision` | readonly record struct | Result of an operator snapshot request. | `Triggered`, retry timing slots exposed by the record. |

`ReplicatedTrees` is the public opt-in map from tree id to `LatticeMergeMode`. Trees not in the map do not ship. `KeyFilter` and `KeyPrefixes` narrow which keys are emitted from opted-in trees.

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
| `ReplicationBatch` | readonly record struct | Logical outbound batch routing metadata plus payload. | Record properties for target cluster, origin cluster, tree, entries or encoded payload. |
| `ReplicationBatchEnvelope` | readonly record struct | Decoded transport envelope. | Header, routing, and batch payload slots. |
| `ReplicationBatchEncodedEnvelope` | readonly record struct | Pre-encoded envelope used by transport implementations. | Encoded header and payload slots. |
| `ReplicationAck` | readonly record struct | Receiver acknowledgement and hints. | `Accepted`, `HighestAppliedHlc`, flow-control hints, dictionary and wire-version hints. |
| `EncodedBatchHeader` | readonly record struct | Fixed wire framing header. | Wire-version, compression, dictionary, and length fields. |

A transport must be idempotent at the batch boundary: sender retries can redeliver a batch, and the receiver deduplicates by origin and HLC. The gRPC binding that implements this seam (`LatticeReplicationGrpcOptions` and the registration helpers) is documented in [Orleans.Lattice.Replication.Grpc](../lattice.replication.grpc/api.md).

## Replication apply

See [Replication Apply](replication-apply.md) and [Deltas](deltas.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationApplier` | interface | Applies inbound WAL records to the local tree. | `ApplyAsync`, `ApplyBatchAsync` |
| `ApplyResult` | readonly record struct | Apply outcome and high-water-mark visibility. | `Applied`, `HighWaterMark` |
| `IReplicationLocalVcSeeder` | interface | Seeds local version-vector state before live apply. | Public seeding method returning `LocalVcSeedReport` |
| `LocalVcSeedReport` | readonly record struct | Observable result of local version-vector seeding. | Record slots for seeded state and counts. |

`ApplyAsync` preserves the source cluster HLC and origin id. `ApplyBatchAsync` is the preferred batch seam because implementations can collapse high-water-mark updates and drain causal buffers once per batch.

## Bootstrap and snapshots

See [Snapshot Bootstrap](snapshot-bootstrap.md), [Auto-Bootstrap](auto-bootstrap.md), and [Automatic Drift Remediation](automatic-drift-remediation.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ISnapshotProvider` | interface | Exports a streaming as-of-HLC view of a tree. | `ExportAsync(string, HybridLogicalClock, CancellationToken)`, source-cluster overload, range-scoped overload |
| `IBootstrapSnapshotSource` | interface | Marker and specialization for bootstrap snapshot sources. | Inherits `ISnapshotProvider` |
| `ILatticeBootstrapCoordinator` | interface | Drives receiver-side snapshot bootstrap. | `GetStateAsync`, `GetStatusAsync`, `BootstrapAsync` |
| `LatticeBootstrapState` | enum | Bootstrap state-machine state. | `Idle`, request, apply, handoff, live, and failed states |
| `BootstrapCoordinatorStatus` | readonly record struct | Observable bootstrap phase and source cluster. | State and source-cluster slots |
| `SnapshotEntry` | readonly record struct | One row in a snapshot stream. | Key, value, timestamp, prepared/tombstone flags, transaction id, source-shard index, atomic-batch size/index, TTL expiry, delta, and merge-mode slots |
| `SnapshotStream` | sealed class | Async snapshot stream wrapper. | As-of HLC, causal-stable frontier, and entry stream |
| `IRemoteSnapshotTransport` | interface | Fetches snapshot metadata and stream items from a remote cluster. | Metadata and streaming fetch methods |
| `LatticeRemoteSnapshotService` | sealed class | Public remote snapshot transport service. | Implements `IRemoteSnapshotTransport` |
| `RemoteSnapshotProvider` | sealed class | Snapshot provider backed by a remote transport. | Implements `IBootstrapSnapshotSource` |
| `RemoteSnapshotMetadata` | readonly record struct | Remote snapshot metadata response. | As-of HLC and causal frontier slots |
| `RemoteSnapshotMetadataRequest` | readonly record struct | Remote metadata request. | Tree, source, and upper-bound slots |
| `RemoteSnapshotStreamItem` | readonly record struct | One item returned by remote snapshot streaming. | Entry and stream-control slots |

Bootstrap applies snapshot entries through the same public apply seam as incremental replication, then hands off to live shipping at the snapshot frontier.

## Dead-letter queue

See [Dead-Letter Queue](dead-letter-queue.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationDeadLetters` | interface | Lists, discards, and replays quarantined apply failures. | `ListAsync`, `CountAsync`, `DiscardAsync`, `ReplayAsync` |
| `DeadLetterEntry` | readonly record struct | Retained failed apply entry. | Entry id, source record, failure details, and retry metadata slots |

Replay runs the parked entry through the canonical applier and removes it only when the replay returns successfully.

## Operator, admin, and WAL introspection

See [Auto-Bootstrap](auto-bootstrap.md), [WAL](wal.md), and [Observability](observability.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ILatticeReplicationAdmin` | interface | Operator-driven snapshot re-seed controls. | `RequestSnapshotAsync`, `ForceRequestSnapshotAsync` |
| `ILatticeWalIntrospection` | interface | Sender-side view of retained WAL availability. | `GetOldestAvailableHlcAsync`, `GetOldestAvailableHlcByOriginAsync` |
| `ILatticeFallOffLogDetector` | interface | Detects whether a peer cursor has fallen behind retained WAL. | Public check method returning `FallOffLogDecision` |

The admin surface rate-limits routine re-seeds through `OperatorReseedMinInterval`; the force method bypasses that rate limit for disaster-recovery and scheduled re-seed scenarios.

## Cursor and GC surface

Public cursor state is represented by `ChangeFeedCursor` and by `ILatticeWalIntrospection`. WAL retention is controlled through `LatticeReplicationOptions.WalRetention` and `MaintenanceGcInterval`; the public contract is that consumers advance cursors and GC trims only what policy permits. See [WAL](wal.md).

## Encoder and wire-version surface

See [Wire Format](wire-format.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationBatchEncoder` | interface | Encodes and decodes replication batches. | Public encode and decode methods |
| `EncodedBatchHeader` | readonly record struct | Fixed frame header. | Header fields and current wire-version metadata |
| `WireVersionNegotiation` | static class | Computes an effective wire version from local and peer capabilities. | Public negotiation helpers |
| `WireVersionNegotiationResult` | readonly record struct | Negotiation result. | Effective version and status slots |
| `WireVersionDownEncoder` | static class | Down-stamps frames for older receivers. | Public down-encoding helpers |
| ReplicationTypeAliases | static class | Stable Orleans serialization alias constants for public replication wire types. | Public alias constants |

Use this surface when writing a transport or compatibility shim. Most application hosts only configure the related options in [Configuration](configuration.md#wire-version-and-adaptive-batch-sizing).

## Metrics types

See [Observability](observability.md) and [Health Check](health-check.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeReplicationMetrics` | static class | Meter, counter, histogram, and tag names for replication telemetry. | Public constants and instrument names |
| `ReplicationPeerStats` | class | Per-peer metrics accumulator. | `RecordBacklog`, `RecordInFlight`, `RecordSuccess`, `RecordError`, `RecordInboundSuccess`, `RecordInboundError`, `Snapshot` |
| `ReplicationPeerSnapshot` | readonly record struct | Point-in-time per-peer telemetry. | Backlog, lag, contact, error, and direction slots |
| `ReplicationContactDirection` | enum | Direction tag for peer contact. | Outbound and inbound values |
| `WireVersionNegotiationState` | class | Runtime wire-version telemetry state. | Public record/update methods and `Snapshot` |
| `WireVersionNegotiationSnapshot` | readonly record struct | Wire-version telemetry snapshot. | Local, peer, effective version, and status slots |
| `LatticeReplicationHealthCheckOptions` | sealed class | Health-check thresholds. | Entries-behind, age, consecutive-error, and grace-window properties |

## Flow control

See [Receiver Flow Control](receiver-flow-control.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReceiverFlowControlPolicy` | interface | Maps receiver state to ack hints. | `EvaluateAsync` |
| `ReceiverFlowControlContext` | readonly record struct | Input to a flow-control policy. | Tree, origin, batch size, duration, and receiver-state slots |
| `ReceiverFlowControlHint` | readonly record struct | Suggested sender limits. | `SuggestedBatchSize`, `PauseForMs`, `None` |
| `NoOpReceiverFlowControlPolicy` | sealed class | Policy that returns no hints. | `EvaluateAsync` |
| `WalSaturationReceiverFlowControlOptions` | sealed class | Maps WAL saturation states to hints. | Healthy, throttled, and saturated hint properties |
| `WalSaturationReceiverFlowControlPolicy` | sealed class | Built-in WAL-saturation-aware policy. | `EvaluateAsync` |

Flow-control hints are advisory. The sender clamps its next batch size and pause to the ack it receives, but a receiver must still tolerate redelivery and retries.

## Anti-entropy and remediation

See [Automatic Drift Remediation](automatic-drift-remediation.md), [digest probes](anti-entropy-digest-probe.md), [Merkle walks](anti-entropy-merkle-walk.md), [leaf re-replay](anti-entropy-leaf-rereplay.md), [bootstrap fallback](anti-entropy-bootstrap-fallback.md), and [remediation guards](anti-entropy-remediation-guards.md).

| Type | Kind | Purpose |
|---|---|---|
| `IReplicationDigestProbeTransport` | interface | Transport seam for digest, manifest, Merkle, high-water-mark, replay, and fallback probes. |
| `DigestProbeComparer` | static class | Compares digest probe responses. |
| `DigestProbeOutcome` | enum | Digest comparison outcome. |
| `DigestProbeRequest`, `DigestProbeResponse` | readonly record structs | Digest probe request and response. |
| `ContentManifestRequest`, `ContentManifestResponse`, `ContentManifestEntry` | readonly record structs | Content-manifest probe shapes. |
| `MerkleWalkProbeRequest`, `MerkleWalkProbeResponse`, `MerkleWalkOutcome` | readonly record structs | Merkle walk request, response, and outcome. |
| `MerkleWalkAbortReason` | enum | Reason a Merkle walk stopped before repair. |
| `PeerHighWaterMarkRequest`, `PeerHighWaterMarkResponse` | readonly record structs | High-water-mark probe shapes. |
| `LeafReReplayRange`, `LeafReReplayOutcome` | readonly record structs | Targeted leaf replay range and result. |
| `LeafReReplaySkipReason` | enum | Reason targeted replay was skipped. |
| `BootstrapFallbackOutcome` | readonly record struct | Result of snapshot fallback repair. |
| `BootstrapFallbackSkipReason` | enum | Reason snapshot fallback was skipped. |
| `RemediationGuard` | sealed class | Budget and circuit-breaker guard for automatic repair. |
| `RemediationDisabledReason` | enum | Reason automatic remediation is disabled. |

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
| `CompressionDictionaryAdvertisement` | static class | Encodes dictionary advertisements. |
| `CompressionDictionaryFingerprint` | static class | Computes dictionary fingerprints. |
| `CompressionDictionaryConvergence` | static class | Checks convergence of shared dictionary state. |
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
| `ILatticeReplicationSecretSource` | interface | Supplies shared secrets for peer authentication. | Public secret lookup method |
| `ConfigurationBindingSecretSource` | sealed class | Secret source backed by configuration. | Constructor and secret lookup method |
| `LatticeReplicationAcceptedSecrets` | sealed class | Accepted shared-secret set. | Secret collection properties |
| `LatticeReplicationSecurityOptions` | sealed class | Shared-secret authentication options. | Required-secret and accepted-secret settings |
| `LatticeReplicationEnvironmentVariables` | static class | Environment-variable names for replication secrets. | Public constant names |
| `LatticeReplicationSharedSecret` | static class | Shared-secret header and validation helpers. | Public helper methods and constants |

The gRPC binding requires HTTPS endpoints unless `AllowPlaintextEndpoints` is enabled. Shared-secret authentication is configured through the security extension methods above.

## Cross-cluster saga participation

The saga service-provider interfaces let a host join the coordinated cross-cluster commit protocol that backs a fleet-wide restore. Register an implementation with `AddLatticeSagaParticipant`. See [Coordinated restore](coordinated-restore.md).

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `ISagaParticipant` | interface | Service-provider interface a host implements to take part in a cross-cluster saga: it votes on prepare, then applies or discards its staged work. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` |
| `ISagaControlChannel` | interface | Outbound control channel the coordinator uses to drive a named peer cluster through the saga phases. | `PrepareAsync`, `CommitAsync`, `AbortAsync`, `GetStatusAsync` (each taking the target `clusterId`) |
| `ISagaPeerAuthorizer` | interface | Fail-closed gate deciding whether an inbound saga control request from a claimed origin cluster is accepted. | `IsAuthorizedAsync(string? originClusterId, CancellationToken)` |
| `SagaPhase` | enum | The durable phase a participant reports for a saga. | `None = 0`, `Prepared = 1`, `Committed = 2`, `Aborted = 3` |
| `SagaVote` | enum | A participant's prepare-phase vote. | `None = 0`, `Commit = 1`, `Abort = 2` |
| `LatticeSystemTreeNames` | static class | The reserved system-tree names the replication package owns. | Public constant tree-name members |

## Tenant isolation and runtime configuration authority

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `IReplicationTenantIsolationGate` | interface | Evaluates whether an inbound replicated entry is admissible for its tenant and region, so a peer can never widen tenant or region scope. | `EvaluateAsync(...)` returning `ReplicationTenantIsolationDecision` |
| `ReplicationTenantIsolationDecision` | enum | The gate's verdict. | `Admit = 0`, `RejectUnknownTenant = 1`, `RejectOutOfRegion = 2` |
| `ILatticeReplicationConfigAuthority` | interface | Supplies the authoritative runtime replication configuration a silo applies. See [Runtime configuration](runtime-config.md). | Public configuration-resolution members |
| `LatticeReplicationConfigEntry` | sealed class | One authoritative runtime replication configuration entry. | Public configuration properties |
| `ILatticeReplicationPreconditionValidator` | interface | Validates that a requested replication mode change is admissible before it is applied. | Public validation member |
| `LatticeReplicationModeChangeRejectedException` | exception | Thrown when a replication mode change is rejected. | Standard exception members |
| `LatticeReplicationPreconditionFailedException` | exception | Thrown when a replication precondition fails. | Standard exception members |

## Azure Table WAL durability

The durable Azure Table WAL backend ships as the separate `Orleans.Lattice.Storage.AzureTable` package. Use `AddAzureTableWalStorage` when the replication WAL must survive silo restarts and support production retention, bootstrap, and replay windows. Its public surface (`AzureTableWalStorageProvider`, `AzureTableWalStorageOptions`, and the retry policies) and configuration are documented in [Orleans.Lattice.Storage.AzureTable](../lattice.storage.azuretable/README.md) - see its [API Reference](../lattice.storage.azuretable/api.md) and [Configuration](../lattice.storage.azuretable/configuration.md). For the core WAL provider seam, see [WAL](wal.md) and [core WAL Storage Providers](../lattice/wal-storage-providers.md).
