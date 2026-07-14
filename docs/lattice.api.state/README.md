# Orleans.Lattice.Api.State

A read-only cluster state-API add-on for [Orleans.Lattice](../../README.md) - query, observe, and subscribe to the live trees, structure, entries, and metrics of a running lattice cluster, over a transport-agnostic facade with a code-first gRPC binding.

## What is it?

`Orleans.Lattice.Api.State` is the **outward-facing read surface** of a lattice cluster. The core library is a write-and-query data plane reached through grain interfaces; this package adds the read-only introspection plane a dashboard, a CLI explorer, or the `Orleans.Lattice.Api.Mcp` MCP server needs - without granting any of them a mutation path.

It is built in two layers:

- **A transport-agnostic facade.** `ILatticeStateQuery`, `ILatticeStateObserver`, and `ILatticeStateMetricsObserver` expose discovery, structure, entry inspection, change observation, and metrics over plain request/response records. The facade has no wire dependency, so the same surface serves an in-process consumer and a remote one. The facade interfaces live in the shared `Orleans.Lattice.Api.Abstractions` contract package and are `public`, so an out-of-package in-process host reuses them by referencing that package and resolving them from DI directly, or by co-hosting the gRPC binding and dialing it over a loopback channel - see [Client](client.md#in-process-reuse).
- **A code-first gRPC binding.** `Orleans.Lattice.Api.State.Grpc` projects the facade onto a long-lived gRPC service whose messages are the same Orleans-serialized records, plus a public `LatticeStateApiGrpcClient`. Remote consumers talk to the cluster over HTTP/2 with no hand-rolled `.proto`.

It covers:

- **Discovery.** Enumerate the registered trees and materialised views as a deterministic, paged catalog, with optional per-view stats.
- **Structure.** Walk a tree's shard-root node graph - per-shard roots, child fan-out, live-key subtree counts, and depth.
- **Entries.** Scan a key-ordered, snapshot-isolated page of entries (forward or reverse, predicate-filtered, with a value-preview budget) or fetch one key's full record.
- **Change observation.** Subscribe to a tree's live mutation stream - point writes, deletes, and range deletes - as a server-streamed feed.
- **Metrics.** Read a one-shot metrics snapshot per tree, or subscribe to a delta-coalesced live metric feed (live keys, shard count, optional shard hotness and view lag).
- **Security.** A fail-closed authorization seam (`ILatticeStateApiAuthorizer`) gates the gRPC surface; the default denies every call until an authorizer is registered or enforcement is explicitly turned off.

The package is **strictly read-only**: every surface observes state, none of them mutates it.

## Core Properties

- **Read-only by construction.** There is no write, delete, split, or reconfigure verb anywhere on the surface. The facade and the gRPC service expose observation verbs only.
- **Strongly-consistent reads.** Entry scans run under the core library's snapshot-isolated cursor machinery, so a page reflects a coherent point-in-time view even during concurrent writes and rebalancing. Structure and metric counts are not cursor-bound: they come from the pushed-up topology digest and the metrics sampler, which report the latest published aggregate rather than a scan-pinned snapshot.
- **Transport-agnostic.** The facade is the contract; gRPC is one binding. The same records flow to an in-process consumer and a remote one, so the `Orleans.Lattice.Api.Mcp` MCP server reuses the facade with zero re-modelling.
- **Fail-closed.** The gRPC surface authorizes every call. Left unconfigured it denies all traffic, so an endpoint is never accidentally exposed unauthenticated.
- **Low ambient cost.** Discovery, structure, and metrics sampling coalesce shared work: many concurrent subscribers to the same metric request share a single sampling loop, and a cluster with no readers does no sampling at all.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Tree & view discovery** | A deterministic, paged catalog of every registered tree and materialised view, with optional per-view stats and system-tree inclusion. | [Surfaces](surfaces.md#discovery) |
| **Tree-structure query** | The shard-root node graph of a tree - per-shard roots, child fan-out, depth, and live-key subtree counts - bounded by depth and node limits. | [Surfaces](surfaces.md#structure) |
| **Entry inspection** | Key-ordered, snapshot-isolated entry scans (forward / reverse, predicate-filtered, value-preview-budgeted) and single-key record fetch. | [Surfaces](surfaces.md#entries) |
| **Change observation** | A server-streamed feed of a tree's live mutations - sets, deletes, and range deletes - with optional maintenance-rewrite inclusion. | [Surfaces](surfaces.md#change-observation) |
| **Metrics observation** | A one-shot per-tree metrics snapshot, or a delta-coalesced live feed of live keys, shard count, shard hotness, and view lag. | [Surfaces](surfaces.md#metrics) |
| **Code-first gRPC binding** | A long-lived gRPC service and a public typed client over the same Orleans-serialized records - no hand-written `.proto`. | [gRPC Contract](grpc-contract.md) |
| **Fail-closed authorization** | A per-call authorization seam that denies by default until an authorizer is registered or enforcement is explicitly disabled. | [Security](security.md) |
| **Shared sampling** | Concurrent subscribers to the same metric request share one sampling loop; a reader-less cluster samples nothing. | [Efficiency](efficiency.md) |

## Quick Start

Add the state API on top of an existing `Orleans.Lattice` silo. On the silo, register the facade with `AddLatticeStateApi`, then add the gRPC binding and map its routes:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi();
});

// Expose the read-only state surface over gRPC. The default authorizer denies
// every call, so register a real one (or disable enforcement behind an outer
// boundary) before the endpoint serves traffic.
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();

var app = builder.Build();
app.MapLatticeStateApiGrpc();
```

From a remote consumer, build a `LatticeStateApiGrpcClient` over a gRPC channel and walk the surface. The client needs a service provider with Orleans serialization registered (`AddSerializer()`) so its wire marshallers match the server:

```csharp verify
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

var serializerProvider = new ServiceCollection().AddSerializer().BuildServiceProvider();

using var channel = GrpcChannel.ForAddress("https://cluster.example:5001");
var stateClient = LatticeStateApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

// Discover the registered trees.
var catalog = await stateClient.ListTreesAsync(new CatalogRequest { PageSize = 50 }, cancellationToken);
foreach (var entry in catalog.Entries)
{
    Console.WriteLine($"{entry.TreeId}  shards={entry.ShardCount}  {entry.Lifecycle}");
}
```

For a complete, runnable journey - silo + gRPC host, discovery, structure, snapshot-isolated scan, and a live change tail - see the [`StateExplorer`](../../samples/StateExplorer) sample under [`samples/`](../../samples).

## Reference

For day-to-day use:

- [Setup](setup.md) - registering the facade, the gRPC binding, and mapping the endpoint routes.
- [gRPC Contract](grpc-contract.md) - the code-first service, its RPCs, the wire records, and the public client.
- [Surfaces](surfaces.md) - discovery, structure, entry inspection, change history, change observation, metrics, and cluster info, request by request.
- [Security](security.md) - the fail-closed authorization seam, the default-deny posture, and the transport story.
- [Efficiency](efficiency.md) - shared sampling, reader-less zero cost, and the overhead guardrails.
- [Client](client.md) - building and driving `LatticeStateApiGrpcClient`, and reusing the facade in-process.
