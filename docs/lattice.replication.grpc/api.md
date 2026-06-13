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
| `AddLatticeReplicationGrpc` | extension method | Registers the gRPC sender, receiver endpoint dependencies, snapshot transport, anti-entropy probe transport, channel options, and security defaults. |
| `MapLatticeReplicationGrpc` | extension method | Maps inbound replication routes on an ASP.NET Core endpoint route builder and returns the builder for chaining. |
| `LatticeReplicationGrpcOptions` | sealed class | Configures peer endpoints, plaintext policy, channel customization, and local origin header override. |

`AddLatticeReplicationGrpc` is idempotent for the public transport seams: it replaces the default no-op `IReplicationTransport` installed by `AddLatticeReplication` with the gRPC binding and uses the same peer channel cache for related outbound replication traffic.

## Transport seam

See [Transport](../lattice.replication/transport.md) and [Wire Format](../lattice.replication/wire-format.md).

| Public type | Purpose |
|---|---|
| `IReplicationTransport` | Sender-side contract used by the replication shipper to send a batch and await an ack. |
| `ReplicationBatchEnvelope` | The decoded transport envelope written onto the gRPC call body. |
| `ReplicationAck` | Receiver acknowledgement containing acceptance, high-water mark, and optional flow-control hints. |
| `IReplicationApplier` | Receiver-side seam invoked after the endpoint decodes a batch. |
| `IChangeFeed` | Producer-side feed read by the replication shipper before transport dispatch. |

The transport does not interpret application payloads. It moves encoded replication envelopes between clusters and relies on the receiver apply path for idempotency, causal buffering, and CRDT-aware merge semantics.

## Options

See [Configuration](configuration.md).

| Member | Type | Purpose |
|---|---|---|
| `Peers` | `IDictionary<string, Uri>` | Maps remote cluster ids to the endpoint URI used for outbound live push, bootstrap, and probe traffic. |
| `AllowPlaintextEndpoints` | `bool` | Allows `http://` peer endpoints for loopback or diagnostic use. Default is `false`. |
| `ConfigureChannel` | `Action<string, GrpcChannelOptions>?` | Lets the host customize each peer channel after package defaults are applied. |
| `LocalClusterId` | `string?` | Overrides the outbound origin header. When unset, `LatticeReplicationOptions.ClusterId` is used. |

## Endpoint mapping

`MapLatticeReplicationGrpc` exposes the receiver routes for live push and related replication traffic. A host that only sends to peers can omit endpoint mapping. A host that only receives can call `AddLatticeReplicationGrpc` with an empty `Peers` map and still map the endpoint.

## Security

The binding requires HTTPS endpoints unless `AllowPlaintextEndpoints` is enabled. Shared-secret authentication and custom secret sources are part of the replication security surface; see [Transport Security](../lattice.replication/transport-security.md).

## Observability

Successful and failed sends are observed through the replication metrics surface. Per-peer lag, consecutive errors, entries behind, and last contact are owned by the replication shipper; the gRPC binding contributes the send outcome and duration at the `IReplicationTransport` boundary. See [Observability](../lattice.replication/observability.md).
