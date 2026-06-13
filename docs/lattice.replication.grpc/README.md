# Orleans.Lattice.Replication.Grpc

Canonical gRPC transport binding for [Orleans.Lattice.Replication](../lattice.replication/README.md) - wires the public `IReplicationTransport` seam to ASP.NET Core gRPC endpoints so clusters can push live batches, exchange replication acks, and use the same peer endpoints for bootstrap and anti-entropy traffic.

## What is it?

`Orleans.Lattice.Replication.Grpc` is the opt-in transport package for multi-cluster replication. Hosts reference it when they want a production network binding instead of an in-process or custom `IReplicationTransport`.

It provides:

- **Outbound live push.** The canonical sender sends one unary RPC per `ReplicationBatchEnvelope` over a cached HTTP/2 channel per peer cluster.
- **Inbound apply.** The receiver endpoint decodes the envelope and drives `IReplicationApplier`, returning a `ReplicationAck` with the applied high-water mark and flow-control hints.
- **Shared endpoint shape.** The same peer map is used for live push, remote snapshot bootstrap, and anti-entropy probes exposed by the replication package.
- **Security defaults.** HTTPS endpoints are required by default, with shared-secret authentication documented in [Transport Security](../lattice.replication/transport-security.md).

The package has no external broker and no `.proto` file to maintain.

## Core Properties

- **Public seam only.** Callers configure `LatticeReplicationGrpcOptions`, send through `IReplicationTransport`, and receive through `IReplicationApplier`.
- **Long-lived channels.** Each peer endpoint gets a cached HTTP/2 channel that multiplexes concurrent calls.
- **Idempotent delivery.** Sender retries may redeliver a batch; receiver high-water-mark dedup makes repeat `(origin, hlc)` records no-ops.
- **Ack-driven progress.** Senders advance only to the `ReplicationAck.HighestAppliedHlc` reported by the receiver.
- **Transport-neutral payload.** The wire bytes are the normal `ReplicationBatchEnvelope` encoding described in [Wire Format](../lattice.replication/wire-format.md).

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **gRPC live push** | Sub-second push path over HTTP/2 using the public `IReplicationTransport` seam. | [Architecture](architecture.md) |
| **Unified peer options** | One `LatticeReplicationGrpcOptions` instance configures peer endpoints, TLS policy, channel customization, and origin header override. | [Configuration](configuration.md) |
| **Receiver endpoint mapping** | `MapLatticeReplicationGrpc` exposes the inbound replication endpoints on an ASP.NET Core route builder. | [API Reference](api.md) |
| **Bootstrap and anti-entropy transport** | The same peer endpoint carries snapshot bootstrap and read-only drift probes used by the replication package. | [Replication docs](../lattice.replication/README.md) |
| **Transport chaos coverage** | Fault-injected channel tests prove retry convergence with no batch loss and no duplicate apply. | [Chaos Tests](chaos-tests.md) |

## Quick Start

Register the replication package on the silo, then add the gRPC binding and map its endpoints on the ASP.NET Core host:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo => silo
    .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
    .AddLatticeReplication(opts =>
    {
        opts.ClusterId = "site-a";
        opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
        {
            ["orders"] = LatticeMergeMode.LwwRegister,
        };
        opts.ReplicationPeers = new[] { "site-b" };
    }));

builder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.Peers["site-b"] = new Uri("https://site-b.example:5001");
});

var app = builder.Build();
app.MapLatticeReplicationGrpc();
app.Run();
```

## Reference

For day-to-day use and operations:

- [API Reference](api.md) - public registration helpers and option types.
- [Configuration](configuration.md) - every `LatticeReplicationGrpcOptions` member and operational guidance.
- [Chaos Tests](chaos-tests.md) - the transport chaos suite and what it proves.
- [Transport Security](../lattice.replication/transport-security.md) - shared-secret authentication, HTTPS defaults, and secret sources.
- [Replication transport](../lattice.replication/transport.md) - the `IReplicationTransport` contract and batch ack model.

For internals (the "how"):

- [Architecture](architecture.md) - sender, endpoint, applier, and channel topology in behavioural terms.
- [Wire Format](../lattice.replication/wire-format.md) - `ReplicationBatchEnvelope` encoding and wire-version compatibility.
- [Replication Apply](../lattice.replication/replication-apply.md) - receiver high-water-mark dedup and causal apply.
- [Replication package index](../lattice.replication/README.md) - the full producer, WAL, shipper, apply, and bootstrap pipeline.

## Feature tracking

Transport work is tracked with the rest of replication on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice.replication). The grouped summary lives in the [replication feature index](../lattice.replication/features.md).
