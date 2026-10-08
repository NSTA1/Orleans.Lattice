---
agent_spec: "docs/agents/api/replication.json"
---

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

Each outbound transport resolves a peer into its own cached HTTP/2 channel on first use: live push and the `IReplicationDigestProbeTransport` peer probes share one, and snapshot bootstrap and saga control each keep another. The map is read when a channel is created; runtime edits are not a topology update mechanism. Restart or use a higher-level deployment rollout when peer endpoints change.

A send to a cluster id missing from `Peers` fails instead of silently dropping the batch.

The keys also serve as the inbound allow-list for cross-cluster saga control. By default the receiving saga service accepts a `Prepare`, `Commit`, `Abort`, or `GetStatus` call only when the caller's origin cluster id - the origin header the sender stamps - is a key in `Peers`, and refuses it with `PermissionDenied` otherwise. A call without that header, or whose request names a coordinator cluster id that differs from it, is refused with `PermissionDenied` before the gate runs: the request's coordinator cluster id is never used in place of the header. A `GetDecision` call is the one exception to the coordinator check, because its caller is a participant asking the coordinator - the request names this cluster as coordinator - so it is gated on the header alone: the header must be present and authorized, and it overwrites the request's requester cluster id, which the coordinator checks against the saga's recorded participants ([#4637](https://github.com/NSTA1/Orleans.Lattice/issues/4637)). Register your own `ISagaPeerAuthorizer` to replace the peer-map gate; it receives the header's cluster id. See [Transport Security](../lattice.replication/transport-security.md).

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

The callback runs after package defaults are applied. If a host needs to replace credentials or handlers, assign the desired values directly in the callback. The package default for `Credentials` is a composite that carries the shared-secret call credentials (the call-time injection of the secret and origin headers), so assigning `Credentials` in the callback removes that injection: the peer then sees no shared secret and, with receiver authentication on, rejects the call as `Unauthenticated`. With receiver authentication off, the missing origin header still gets a live push, a content-manifest exchange, a peer high-water-mark probe, or a saga control call refused as `PermissionDenied`. Client certificates for mTLS can instead be attached through a custom `HttpHandler`, which leaves `Credentials` in place.

### `LocalClusterId`

Optional override for the origin metadata header stamped onto outbound calls. Leave it `null` for normal deployments so the binding uses the cluster-wide `LatticeReplicationOptions.ClusterId`. The header value is fixed when a peer's channel is built, so every tree that talks to that peer sends the same value.

A receiver requires this header on a live push, a content-manifest exchange, or a peer high-water-mark probe, compares it with the origin cluster id the request names, and refuses the call with `PermissionDenied` when the header is absent or the two differ. The request names the sending tree's own `ClusterId`, resolved per tree (for a live push, in the batch envelope), so a tree's calls are refused whenever that value differs from the header: a `LocalClusterId` that differs from `ClusterId`, or a per-tree `ClusterId` override that differs from the cluster-wide value (see [`ClusterId`](../lattice.replication/configuration.md#clusterid)). The receiving saga service's default peer gate also reads this header: a peer admits saga control calls only when its own `Peers` map lists the value. The header value is also the cluster id a receiver binds the presented secret to while receiver authentication and [`BindCredentialToOriginCluster`](../lattice.replication/configuration.md#transport-security---latticereplicationsecurityoptions) are on (the defaults). Leave `LocalClusterId` unset, or set it to the same value as `ClusterId`, and give no replicated tree a per-tree `ClusterId` that differs from the cluster-wide value.

## Relationship to replication options

`LatticeReplicationGrpcOptions.Peers` answers "where do I dial this peer?" `LatticeReplicationOptions.ReplicationPeers` answers "which peer ids should this tree ship to?" Configure both for a normal sender. A receiver-only host can leave `Peers` empty and still call `MapLatticeReplicationGrpc` to accept live push, peer probes, and snapshot pulls, but with the default saga peer gate it then refuses every inbound saga control call (see [`Peers`](#peers)).

For wire-version, compression, adaptive batch sizing, flow-control hints, and security secret sources, use the replication package options and security extensions described in [Configuration](../lattice.replication/configuration.md) and [Transport Security](../lattice.replication/transport-security.md).
