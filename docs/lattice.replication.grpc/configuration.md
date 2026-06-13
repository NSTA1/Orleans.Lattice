# Configuration

This document covers the public configuration surface for `Orleans.Lattice.Replication.Grpc`. For replication-wide options such as `LatticeReplicationOptions.ClusterId`, `ReplicationPeers`, shipping cadence, wire version, and flow control, see the [replication configuration reference](../lattice.replication/configuration.md).

## Registering the binding

Call `AddLatticeReplicationGrpc` after registering the replication package. Configure `Peers` for every remote cluster this silo dials:

```csharp verify
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicationPeers = new[] { "site-b" };
});

siloBuilder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.Peers["site-b"] = new Uri("https://site-b.example:5001");
});
```

Map endpoints on any ASP.NET Core host that accepts inbound replication calls:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Orleans.Lattice.Replication.Grpc;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeReplicationGrpc();

var app = builder.Build();
app.MapLatticeReplicationGrpc();
```

## Options Reference - `LatticeReplicationGrpcOptions`

| Option | Type | Default |
|---|---|---|
| [`Peers`](#peers) | `IDictionary<string, Uri>` | empty ordinal dictionary |
| [`AllowPlaintextEndpoints`](#allowplaintextendpoints) | `bool` | `false` |
| [`ConfigureChannel`](#configurechannel) | `Action<string, GrpcChannelOptions>?` | `null` |
| [`LocalClusterId`](#localclusterid) | `string?` | `null` |

### `Peers`

Maps remote cluster id to the endpoint URI that cluster exposes for replication gRPC calls. The keys should match the cluster ids used by `LatticeReplicationOptions.ReplicationPeers` and by outbound batch `TargetClusterId` values.

Each peer is resolved into a cached HTTP/2 channel on first use. The map is read when that channel is created; runtime edits are not a topology update mechanism. Restart or use a higher-level deployment rollout when peer endpoints change.

A send to a cluster id missing from `Peers` fails instead of silently dropping the batch.

### `AllowPlaintextEndpoints`

Controls whether `http://` peer URIs are accepted. The default is `false`, which requires `https://` and fails closed when a peer endpoint is not protected by TLS.

Set this to `true` only for loopback tests, local diagnostics, or another explicitly trusted environment:

```csharp verify
using Orleans.Lattice.Replication.Grpc;

siloBuilder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.AllowPlaintextEndpoints = true;
    grpc.Peers["loopback"] = new Uri("http://127.0.0.1:5000");
});
```

### `ConfigureChannel`

Optional callback invoked when a peer channel is constructed. Use it for host-owned gRPC settings such as mTLS credentials, custom `HttpHandler` instances, keep-alive, retry policy, and message-size bounds.

```csharp verify
using Grpc.Net.Client;
using Orleans.Lattice.Replication.Grpc;

siloBuilder.Services.AddLatticeReplicationGrpc(grpc =>
{
    grpc.Peers["site-b"] = new Uri("https://site-b.example:5001");
    grpc.ConfigureChannel = (peer, channel) =>
    {
        channel.MaxReceiveMessageSize = 8 * 1024 * 1024;
        channel.MaxSendMessageSize = 8 * 1024 * 1024;
    };
});
```

The callback runs after package defaults are applied. If a host needs to replace credentials or handlers, assign the desired values directly in the callback.

### `LocalClusterId`

Optional override for the origin metadata header stamped onto outbound calls. Leave it `null` for normal deployments so the binding uses `LatticeReplicationOptions.ClusterId`.

Use this only when an advanced host has a deliberate reason to expose a different transport origin than the replication cluster id. Keep the value stable and unique within the topology.

## Relationship to replication options

`LatticeReplicationGrpcOptions.Peers` answers "where do I dial this peer?" `LatticeReplicationOptions.ReplicationPeers` answers "which peer ids should this tree ship to?" Configure both for a normal sender. A receiver-only host can leave `Peers` empty and still call `MapLatticeReplicationGrpc`.

For wire-version, compression, adaptive batch sizing, flow-control hints, and security secret sources, use the replication package options and security extensions described in [Configuration](../lattice.replication/configuration.md) and [Transport Security](../lattice.replication/transport-security.md).
