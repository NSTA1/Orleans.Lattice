# gRPC streaming push transport (`Orleans.Lattice.Replication.Grpc`)

The `Orleans.Lattice.Replication.Grpc` sub-package ships the canonical sender + receiver pair for the [`IReplicationTransport`](transport.md) seam. It replaces the default no-op transport with a long-lived, HTTP/2-multiplexed `GrpcChannel` per peer cluster on the sender side, and exposes an ASP.NET Core endpoint that drives [`IReplicationApplier`](replication-apply.md) on the receiver side.

The sub-package is opt-in: hosts that need the transport reference `Orleans.Lattice.Replication.Grpc` and call the DI extensions below; hosts that don't never pay the dependency cost.

## Topology

```text
Site A silo                          Site B host (ASP.NET Core)
───────────                          ──────────────────────────
ChangeFeed                           Kestrel (HTTP/2)
   │                                   │
   ▼                                   ▼
GrpcPushTransport                    LatticeReplicationGrpcService
   │                                   │
   │  unary Push(ReplicationBatchEnvelope)
   │  → ReplicationAck
   │ ───────────────────────────────▶  │
   │                                   ▼
   │                                 IReplicationApplier
   │                                   │
   │ ◀─────── ReplicationAck ──────────┤
   ▼
peer cursor advances strictly to ack.HighestAppliedHlc
```

One unary `Push` RPC per batch, a single long-lived `GrpcChannel` per peer cluster id, HTTP/2 multiplexing concurrent batches across the channel.

## Wire format

The on-the-wire shape is the same `ReplicationBatchEnvelope` (alias `olr.be`, wire version 1) the [`IReplicationBatchEncoder`](wire-format.md) seam frames. The gRPC marshaller hands the gRPC stream's `IBufferWriter<byte>` straight to `IReplicationBatchEncoder.Encode(envelope, writer)`, so the envelope's bytes are written directly into the underlying network buffer without an intermediate managed allocation. The encoder is the canonical Orleans-binary encoder by default; swapping the DI registration for a different encoder (e.g. JSON for HTTP debuggability) is the only knob needed to change the wire bytes.

The gRPC service definition is **not** generated from `.proto` — it is hand-rolled via custom `Marshaller<T>` instances backed by the encoder. There is no `Grpc.Tools` build dependency, no `.proto` file to keep in sync, and no language-binding artefact to ship: a non-.NET peer that wants to talk to the receiver implements the same encoded envelope shape directly.

## Sender-side registration

```text
siloBuilder.ConfigureServices(services =>
{
    services.AddLatticeReplicationGrpcPushTransport(options =>
    {
        options.PeerEndpoints["site-b"] = new Uri("https://site-b.example:5001");
        options.PeerEndpoints["site-c"] = new Uri("https://site-c.example:5001");
    });
});
```

`AddLatticeReplicationGrpcPushTransport` replaces the default `NoOpReplicationTransport` registered by `AddLatticeReplication`. Subsequent calls are idempotent — the registration uses `IServiceCollection.Replace`, not `Add`.

### `GrpcPushTransportOptions`

| Member | Semantics |
|---|---|
| `PeerEndpoints` | `IDictionary<string, Uri>` keyed by `TargetClusterId`. Every peer the silo intends to ship to must be present before the first `SendAsync` call. A batch whose `TargetClusterId` is not in the map causes `SendAsync` to throw `InvalidOperationException`. The map is read once per peer (on the first dispatch) and cached — runtime edits are not observed. |
| `ConfigureChannel` | Optional `Action<string, GrpcChannelOptions>?` invoked when the transport constructs the per-peer `GrpcChannel`. Hosts attach mTLS credentials, custom `HttpHandler`s, retry policies, and keep-alive settings here. The default (`null`) leaves channel options at `Grpc.Net.Client` defaults, which is sufficient for plaintext-loopback tests but not for production. |

A future item standardises mTLS / token-rotation across both transports; until then the host is responsible for wiring transport security via `ConfigureChannel`.

## Receiver-side registration

```text
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddLatticeReplication(o => o.ClusterId = "site-b");
builder.Services.AddLatticeReplicationGrpcServer();

var app = builder.Build();
app.MapLatticeReplicationGrpcService();
app.Run();
```

`AddLatticeReplicationGrpcServer` registers the receiver service + the gRPC method singleton. `MapLatticeReplicationGrpcService` exposes the `Push` route on the endpoint builder. The two calls split because `AddGrpc()` itself splits service registration from route mapping, and the receiver follows the same pattern.

The receiver service requires the standard `AddLatticeReplication` registrations (encoder + `IReplicationApplier`); call it before `AddLatticeReplicationGrpcServer`.

## Concurrency and idempotency

The transport is safe for concurrent invocation across distinct `(TargetClusterId, TreeName)` pairs — the canonical outbound shipper fans out across peers and trees in parallel. Concurrent invocation against the same pair is implementation-defined; the canonical shipper serialises calls per pair.

Idempotency is the receiver's responsibility. The transport retries are configured via `GrpcChannelOptions.ServiceConfig` (per gRPC's standard retry policy mechanism), and the per-origin high-water-mark dedup in `IReplicationApplier` makes re-deliveries of the same `(origin, hlc)` tuple a no-op. The sender advances its per-peer cursor strictly to `ack.HighestAppliedHlc`, never to a value the sender chose locally.

## Observability

Each `SendAsync` records a `LatticeReplicationMetrics.ShipDuration` sample tagged with:

- `tree` — the `ReplicationBatch.TreeName`.
- `peer` — the `ReplicationBatch.TargetClusterId`.
- `outcome` — `ok` on a successful ack, `error` on any thrown exception.

Per-peer gauges (`entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`) are owned by the outbound shipper, not the transport — they aggregate across batches and are not the transport's concern.

## Implementation notes

- **Box wrappers.** gRPC's `Method<TRequest, TResponse>` has a `class` constraint; the public `ReplicationBatchEnvelope` and `ReplicationAck` are `readonly record struct`. Internal sealed-class wrappers (`ReplicationBatchEnvelopeBox`, `ReplicationAckBox`) carry the value across the gRPC call boundary. The wrappers are an internal implementation detail; callers never see them.
- **`[BindServiceMethod]` codegen-style topology.** The receiver service is split into an internal `LatticeReplicationGrpcServiceBase` (carries the `[BindServiceMethod]` attribute and the static `BindService` callback) and a sealed derived `LatticeReplicationGrpcService` (the DI-resolved per-request handler). This mirrors the topology `Grpc.Tools` codegen produces and is required because `Grpc.AspNetCore` invokes `BindService(binder, null)` once at startup to record method metadata before resolving the per-request instance.
- **`LatticeReplicationGrpcMethodHolder`.** The static `BindService` callback cannot accept DI dependencies, so a process-wide `LatticeReplicationGrpcMethodHolder.Current` bridges the DI-resolved `LatticeReplicationGrpcMethod` into the static binding hook. The DI factory populates the holder when the method singleton first resolves; `MapLatticeReplicationGrpcService` pre-resolves it before invoking `MapGrpcService<LatticeReplicationGrpcServiceBase>` so the holder is populated before gRPC reflects on the type.

## Caveats

- **`PeerEndpoints` is read once per peer.** Adding or removing peers at runtime is **not** observed by the transport. A future item (observable topology) addresses this; until then, host restarts are required to change the peer set.
- **No transport-level authentication is configured by default.** The default `GrpcChannelOptions` accept any server certificate over HTTPS and any client over HTTP. Production hosts must configure mTLS or bearer-token authentication via `ConfigureChannel` and the receiver's ASP.NET Core authentication middleware.
- **The transport does not interpret the payload.** A transport-level decision based on payload contents (e.g. shed-load on oversize batches) is out of scope. `GrpcChannelOptions.MaxSendMessageSize` and `MaxReceiveMessageSize` cap the wire size at the transport boundary; the receiver-side flow-control work will surface batch-size hints on the ack envelope.
