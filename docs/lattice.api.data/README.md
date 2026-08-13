# Orleans.Lattice.Api.Data

A write-capable external data-plane add-on for [Orleans.Lattice](../../README.md) - set, delete, read, and atomically batch-mutate the entries of a running lattice cluster from a non-.NET client, over a transport-agnostic facade with a code-first gRPC binding.

## What is it?

`Orleans.Lattice.Api.Data` is the **outward-facing read-write surface** of a lattice cluster. The core library is reached through .NET grain interfaces; this package adds the external data plane a non-.NET service, a language-agnostic worker, or an edge component needs to mutate and read tree entries over the wire - without embedding the Orleans client.

It is the write-capable sibling of the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) package, and is built the same way, in two layers:

- **A transport-agnostic facade.** `ILatticeDataApi` (a public contract in the shared `Orleans.Lattice.Api.Abstractions` package) exposes point set/delete, single-tree atomic batch, cross-tree atomic batch, point read, and a single bounded range-read page over plain request/response records. The facade has no wire dependency, so the same surface serves an in-process consumer and a remote one.
- **A code-first gRPC binding.** `Orleans.Lattice.Api.Data.Grpc` projects the facade onto a gRPC service whose messages are the same Orleans-serialized records, plus a public `LatticeDataApiGrpcClient`. Remote consumers talk to the cluster over HTTP/2 with no hand-rolled `.proto`.

Every operation is served by fetching the cluster grain with `GetGrain<ILattice>(treeId)` and calling the **same public `ILattice` method** the in-cluster client calls. Authorization is therefore inherited, not re-implemented: the per-tree / per-key enforcement wired at the core grain fires automatically once the caller identity flows on the ambient credential context.

## Core properties

- **Opt-in and absent by default.** Nothing registers unless the host calls `AddLatticeDataApi()` on the silo and `AddLatticeDataApiGrpc()` / `MapLatticeDataApiGrpc()` on the web host. A cluster that does not add the package has no external write surface.
- **Fail-closed.** An unresolved or anonymous caller is denied every mutation and read. Because calls route through the gated `ILattice` surface with the caller identity on the credential context, an anonymous subject is default-denied by the core authorization gate - the package adds no bypass.
- **Authorization inherited, never re-implemented.** Writes and deletes throw on denial; point reads of a denied key report absent; range reads prune to the authorized subset; atomic and cross-tree batches authorize every leg before any apply. None of this logic lives in this package - it is the core gate, reached through `ILattice`.
- **Transport-agnostic.** The facade is the contract; gRPC is one binding. The same records flow to an in-process consumer and a remote one.

## Surface (v1)

| Operation | Facade method | gRPC RPC | `ILattice` method it routes to |
|---|---|---|---|
| Point write | `SetAsync` | `Set` | `SetAsync(key, value)` |
| Point delete | `DeleteAsync` | `Delete` | `DeleteAsync(key)` |
| Range delete | `DeleteRangeAsync` | `DeleteRange` | the resilient range-delete drain (`DeleteRangeAsync(startInclusive, endExclusive)` extension over the delete-range cursor) |
| Single-tree atomic batch | `SetManyAtomicAsync` | `SetManyAtomic` | `SetManyAtomicAsync(upserts, deletes, operationId)` |
| Cross-tree atomic batch | `SetManyAtomicCrossTreeAsync` | `SetManyAtomicCrossTree` | the cross-tree atomic coordinator surface |
| Point read | `GetAsync` | `Get` | `GetAsync(key)` |
| Bounded range-read page | `ReadRangeAsync` | `ReadRange` | the paged entry-cursor surface (`OpenEntryCursorAsync` / `NextEntriesAsync` / `CloseCursorAsync`) |

### Explicitly deferred

The following are **not** in v1 and are deliberately left out so a caller cannot mistake their absence for a bug:

- **Live streaming scan / change feed.** There is no server-streamed entry tail or mutation feed on this surface. A caller that needs to observe live change should use the read-only [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) change-observation surface. The bounded `ReadRange` page here is a one-shot, continuation-token-paged read, not a live stream.

## Quick start

Add the data API on top of an existing `Orleans.Lattice` silo, then add the gRPC binding and map its routes. The transport-level authorizer denies every call until a real authorizer is registered (or enforcement is explicitly turned off behind an outer boundary):

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeDataApi();
});

// Expose the read-write data surface over gRPC. The default authorizer denies
// every call, so register a real one (or disable enforcement behind an outer
// boundary) before the endpoint serves traffic. Per-tree / per-key rights are
// still enforced by the core gate regardless of this coarse switch.
builder.Services.AddLatticeDataApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeDataApiAuthorizer, AllowAllDataApiAuthorizer>();

var app = builder.Build();
app.MapLatticeDataApiGrpc();
```

From a remote consumer, build a `LatticeDataApiGrpcClient` over a gRPC channel and mutate or read entries. The client needs a service provider with Orleans serialization registered (`AddSerializer()`) so its wire marshallers match the server:

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var dataClient = LatticeDataApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

// Write a value, then read it back. The caller identity travels on the request
// metadata (see Security below); the core gate authorizes each call.
await dataClient.SetAsync(new DataSetRequest
{
    TreeId = "orders",
    Key = "order-42",
    Value = new byte[] { 1, 2, 3 },
}, cancellationToken);

var read = await dataClient.GetAsync(new DataGetRequest
{
    TreeId = "orders",
    Key = "order-42",
}, cancellationToken);

if (read.Found)
{
    Console.WriteLine($"{read.Key} = {read.Value.Length} bytes");
}
```

## Security

This is a write-capable external surface, so its default posture is closed:

- **Two independent gates.** A coarse transport-level authorizer (`ILatticeDataApiAuthorizer`, default `DenyAllDataApiAuthorizer`) runs first and rejects the whole call before it reaches the facade; then the per-tree / per-key core gate authorizes every leg of the actual operation. The coarse gate is the endpoint-level on/off switch; the core gate is the fine-grained rights check. Both must pass.
- **Anonymous is denied.** A call with no resolvable credential is default-denied by the core gate (writes and reads alike), because the anonymous subject has no grant. This is verified by tests rather than by a bespoke check in this package.
- **Denial carries no value.** When the core gate denies a call, the gRPC service maps it to `PermissionDenied` and attaches only the non-sensitive fields of the denial - the tree id, the operation, the subject, and the reason - as response trailers. The entry value is never included in a denial.
- **Identity bridge.** The caller identity is lifted from request metadata by `ILatticeDataApiCredentialBridge`. The default is header-based: it reads the `authorization` header and strips a leading `Bearer ` prefix case-insensitively. Replace the seam to source identity differently.

## Reference

- [Configuration](configuration.md) - every public options property, its type, and its default.
- Facade registration: `AddLatticeDataApi()` on the silo builder, configured with `LatticeApiDataOptions`.
- gRPC registration: `AddLatticeDataApiGrpc()` and `MapLatticeDataApiGrpc()`, configured with `LatticeDataApiGrpcOptions`.
- Public client: `LatticeDataApiGrpcClient` (`Set`, `Delete`, `DeleteRange`, `SetManyAtomic`, `SetManyAtomicCrossTree`, `Get`, `ReadRange`).
- Authorization seam: `ILatticeDataApiAuthorizer` (`DenyAllDataApiAuthorizer`, `AllowAllDataApiAuthorizer`).
- Identity seam: `ILatticeDataApiCredentialBridge`.
