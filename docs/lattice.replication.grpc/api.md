---
agent_spec: "docs/agents/api/replication.json"
---

# gRPC Transport Public API Reference

This document is the contract for the public `Orleans.Lattice.Replication.Grpc` surface. It describes caller-visible behaviour: what to register, which options shape the binding, and which replication seams the package connects. It does not name product-internal implementation types.

## Setup

Install the transport package beside the core replication package:

```shell
dotnet add package Orleans.Lattice.Replication
dotnet add package Orleans.Lattice.Replication.Grpc
```

Import the gRPC namespace:

```csharp verify
using Orleans.Lattice.Replication.Grpc;
```

Register replication first, then add the gRPC binding:

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

On the receiving ASP.NET Core pipeline, call `MapLatticeReplicationGrpc` on the endpoint route builder.

## Registration and DI

| Type or member | Kind | Purpose |
|---|---|---|
| `LatticeReplicationGrpcServiceCollectionExtensions` | static class | Extension-method holder for registering and mapping the gRPC binding. |
| `AddLatticeReplicationGrpc` | extension method | Registers the gRPC sender, receiver endpoint dependencies, snapshot transport, anti-entropy probe transport, cross-cluster saga control channel and its receiving service, channel options, and security defaults. |
| `MapLatticeReplicationGrpc` | extension method | Maps inbound replication routes on an ASP.NET Core endpoint route builder and returns the builder for chaining. |
| `LatticeReplicationGrpcOptions` | sealed class | Configures peer endpoints, plaintext policy, channel customization, and local origin header override. |

`AddLatticeReplicationGrpc` is idempotent for the public transport seams: it replaces the default no-op `IReplicationTransport` installed by `AddLatticeReplication` with the gRPC binding, and projects the same `Peers` map and hardened channel defaults onto every related outbound transport. It is not idempotent as a whole, so call it once: every call adds the shared-secret interceptor to the gRPC pipeline again, and after two calls the secret check runs twice on every replication RPC. The live-push seam and the peer-probe seam (`IReplicationDigestProbeTransport`: digest, Merkle-walk, and high-water-mark probes, the content-manifest exchange, and the dictionary pull) share one per-peer channel cache; snapshot bootstrap and the saga control channel each keep their own per-peer channel.

## Transport seam

See [Transport](../lattice.replication/transport.md) and [Wire Format](../lattice.replication/wire-format.md).

| Public type | Purpose |
|---|---|
| `IReplicationTransport` | Sender-side contract used by the replication shipper to send a batch and await an ack. |
| `ReplicationBatchEnvelope` | The decoded transport envelope written onto the gRPC call body. |
| `ReplicationAck` | Receiver acknowledgement containing acceptance, high-water mark, and optional flow-control hints. |
| `IReplicationApplier` | Receiver-side seam invoked after the endpoint decodes a batch. |
| `IReplicationDigestProbeTransport` | Sender-side contract for the peer probes (digest, Merkle-walk, and peer high-water mark), the content-manifest exchange, and the compression-dictionary pull. The binding serves it from the same instance, and per-peer channel cache, as live push. |
| `IRemoteSnapshotTransport` | Receiver-driven snapshot bootstrap: pulls a peer's cut-point metadata and snapshot entry stream. The binding registers a gRPC implementation. |
| `ISagaControlChannel` | Outbound cross-cluster saga control (prepare, commit, abort, status). The binding routes a call addressed to the local cluster in-process and a call to any other participant over gRPC. |
| `ISagaPeerAuthorizer` | The gate the inbound saga service consults, with the origin cluster id stamped on the call, before it runs a call. The binding's default admits only cluster ids present in `Peers`. |
| `ILatticeSagaControlHandler` | The participant-side handler the inbound saga service calls. `AddLatticeReplication` registers the durable participant handler; on a host without it the binding falls back to a handler that holds no participant state and votes to abort on prepare. |
| `IChangeFeed` | In-process pull feed over the locally-authored WAL for custom consumers. The replication shipper does not read it; it tails the WAL partitions directly before transport dispatch. |

The transport does not interpret application payloads. It moves encoded replication envelopes between clusters and relies on the receiver apply path for idempotency, causal buffering, and CRDT-aware merge semantics.

## Options

See [Configuration](configuration.md).

| Member | Type | Purpose |
|---|---|---|
| `Peers` | `IDictionary<string, Uri>` | Maps remote cluster ids to the endpoint URI used for outbound live push, bootstrap, anti-entropy probe, and saga control traffic. |
| `AllowPlaintextEndpoints` | `bool` | Allows `http://` peer endpoints for loopback or diagnostic use. Default is `false`. |
| `ConfigureChannel` | `Action<string, GrpcChannelOptions>?` | Lets the host customize each peer channel after package defaults are applied. |
| `LocalClusterId` | `string?` | Overrides the outbound origin header, one value per peer channel shared by every tree. When unset, the cluster-wide `LatticeReplicationOptions.ClusterId` is used. Each live push, content-manifest exchange, and peer high-water-mark probe names its tree's own `ClusterId`, and the receiver refuses one whose origin differs from the header, so leave this unset (or equal to `ClusterId`) and give no replicated tree a per-tree `ClusterId` that differs from the cluster-wide value. See [Security](#security). |

## Endpoint mapping

`MapLatticeReplicationGrpc` exposes the receiver routes for live push and related replication traffic as three code-first gRPC services. There is no `.proto`: the push request is written by the replication batch encoder (the `ReplicationBatchEnvelope` wire format described in [Wire Format](../lattice.replication/wire-format.md)), and every other message is Orleans-serialized.

| Service | RPC | Kind | Purpose |
|---|---|---|---|
| `orleans.lattice.replication.LatticeReplication` | `Push` | unary | Live push: one `ReplicationBatchEnvelope` in, one `ReplicationAck` out. |
| `orleans.lattice.replication.LatticeReplication` | `ProbeDigest` | unary | Anti-entropy digest probe. |
| `orleans.lattice.replication.LatticeReplication` | `ProbeMerkleWalk` | unary | Anti-entropy Merkle-walk digest over a key range. |
| `orleans.lattice.replication.LatticeReplication` | `GetPeerHighWaterMark` | unary | The peer's applied watermark for a tree and origin, bounding leaf re-replay. |
| `orleans.lattice.replication.LatticeReplication` | `ExchangeContentManifest` | unary | Content-hash payload-elision manifest exchange. |
| `orleans.lattice.replication.LatticeReplication` | `PullCompressionDictionary` | unary | Shared compression-dictionary pull. |
| `orleans.lattice.replication.LatticeRemoteSnapshot` | `GetMetadata` | unary | Snapshot-bootstrap cut-point metadata. |
| `orleans.lattice.replication.LatticeRemoteSnapshot` | `RequestSnapshot` | server-streaming | The snapshot entries at that cut-point. |
| `orleans.lattice.replication.LatticeSaga` | `Prepare`, `Commit`, `Abort`, `GetStatus`, `GetDecision` | unary | Cross-cluster saga control. `GetDecision` is a prepared participant asking the coordinator cluster for the saga's decision ([#4637](https://github.com/NSTA1/Orleans.Lattice/issues/4637)). |

A host that only sends to peers can omit endpoint mapping. A host that only receives can call `AddLatticeReplicationGrpc` with an empty `Peers` map and still map the endpoint; with the default saga peer gate it then refuses every inbound saga control call (see Security below).

## Security

The binding requires HTTPS endpoints unless `AllowPlaintextEndpoints` is enabled. Shared-secret authentication and custom secret sources are part of the replication security surface; while `LatticeReplicationSecurityOptions.RequireAuthentication` is on (the default), the receiver-side shared-secret check covers every RPC in the endpoint table above and no other gRPC service on the host. While `LatticeReplicationSecurityOptions.BindCredentialToOriginCluster` is also on (the default), that check also refuses, with `PermissionDenied`, a call whose secret is not bound to the origin cluster it stamps in the `x-lattice-replication-origin` header; see [`BindCredentialToOriginCluster`](../lattice.replication/configuration.md#transport-security---latticereplicationsecurityoptions). Three further receiver-side gates apply whether or not authentication is on: the peer-read RPCs (the digest and Merkle-walk probes, the peer high-water mark, the content-manifest exchange) and the snapshot RPCs refuse, with `PermissionDenied`, a tree that is not enrolled for replication on the receiving cluster; `Push`, `ExchangeContentManifest`, and `GetPeerHighWaterMark` refuse, with `PermissionDenied`, a call that carries no origin header or whose request names an origin cluster id (for `Push`, the batch envelope's) that differs from it; and the saga control RPCs refuse, with `PermissionDenied`, a call that carries no origin header or whose request names a different coordinator cluster id, then pass the header's cluster id to an `ISagaPeerAuthorizer` gate that by default admits only cluster ids present in `Peers`. See [Transport Security](../lattice.replication/transport-security.md).

## Observability

Successful and failed sends are observed through the replication metrics surface. Per-peer lag, consecutive errors, entries behind, and last contact are owned by the replication shipper; the gRPC binding contributes the send outcome and duration (`orleans.lattice.replication.ship.duration`, tagged `outcome` = `ok` when the peer returns an ack - accepted or not - and `error` when the gRPC call throws) and the shipped-entry count (`orleans.lattice.replication.wal.entries_shipped`, added when the ack for a non-empty batch returns, whether or not it is accepted) at the `IReplicationTransport` boundary. A send that fails before the call is issued - an unknown peer, or a non-`https` endpoint without the plaintext opt-in - records neither. On the receiving side, the binding's content-manifest exchange handler records the receiver content-hash counters on the same replication meter: `orleans.lattice.replication.receiver.content_manifest_exchanges` (one per exchange answered), `orleans.lattice.replication.receiver.content_entries_elided` (entries the receiver already holds), and `orleans.lattice.replication.receiver.content_hwm_advances` (metadata-only high-water-mark advances), each tagged `tree`, `peer` (the requesting origin), and `tenant`. See [Observability](../lattice.replication/observability.md).

The package also publishes its own meter, `orleans.lattice.replication.grpc`, with one instrument: the `orleans.lattice.replication.grpc.insecure_channel` counter (unit `{channel}`, tagged `peer`, `transport` = `push` / `snapshot` / `saga_control`, and `tenant` = `_platform_`), incremented - alongside a warning log - whenever `AllowPlaintextEndpoints` causes a channel to be built against a non-`https` endpoint. Subscribe to it with `AddMeter("orleans.lattice.replication.grpc")`; the bundled Replication Transport (gRPC) dashboard charts it.
