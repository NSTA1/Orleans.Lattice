# Orleans.Lattice.Api.State

A read-only cluster state-API add-on for [Orleans.Lattice](../../README.md) - query, observe, and subscribe to the live trees, structure, entries, and metrics of a running lattice cluster, over a transport-agnostic facade with a code-first gRPC binding.

## What is it?

`Orleans.Lattice.Api.State` is the **outward-facing read surface** of a lattice cluster. The core library is a write-and-query data plane reached through grain interfaces; this package adds the read-only introspection plane a dashboard, a CLI explorer, or the `Orleans.Lattice.Api.Mcp` MCP server needs - without granting any of them a mutation path.

It is built in two layers:

- **A transport-agnostic facade.** `ILatticeStateQuery`, `ILatticeStateObserver`, and `ILatticeStateMetricsObserver` expose discovery, structure, entry inspection, per-key change history, dead-letter inspection, change observation, metrics, and cluster identity over plain request/response records. The facade has no wire dependency, so the same surface serves an in-process consumer and a remote one. The facade interfaces live in the shared `Orleans.Lattice.Api.Abstractions` contract package and are `public`, so an out-of-package in-process host reuses them by referencing that package and resolving them from DI directly, or by co-hosting the gRPC binding and dialing it over a loopback channel - see [Client](client.md#in-process-reuse).
- **A code-first gRPC binding.** `Orleans.Lattice.Api.State.Grpc` projects the facade onto a long-lived gRPC service whose messages are Orleans-serialized C# records that wrap or reuse the facade DTOs, plus a public `LatticeStateApiGrpcClient`. Remote consumers talk to the cluster over HTTP/2 with no hand-rolled `.proto`.

It covers:

- **Discovery.** Enumerate the registered trees, materialised views, and tag indexes as deterministic, paged catalogs, with optional per-view stats, and browse a tag index's values, covered trees, and live members.
- **Structure.** Walk a tree's shard-root node graph - per-shard roots, child fan-out, live-key subtree counts, and depth.
- **Entries.** Scan a key-ordered page of entries (snapshot-isolated by default, or through a cheaper baseline-free live cursor; forward or reverse, predicate-filtered, with a value-preview budget) or fetch one key's full record.
- **Change history.** Page a single key's revision timeline - sets, deletes, CRDT deltas, and range tombstones - bounded by the tree's durable-history retention.
- **Change observation.** Subscribe to a tree's live mutation stream - point writes, deletes, and range deletes - as a server-streamed feed.
- **Metrics.** Read a one-shot metrics snapshot per tree, or subscribe to a delta-coalesced live metric feed (live keys, shard count, optional shard hotness and view lag).
- **Dead letters.** Count and page strict-mode schema-enforcement dead-letter queues without replaying or requeueing diverted items.
- **Cluster info.** Identify the connected cluster by its Orleans cluster id and service id.
- **Security.** A fail-closed authorization seam (`ILatticeStateApiAuthorizer`) gates the protected gRPC surface; the default denies protected calls until an authorizer is registered or enforcement is explicitly turned off. `GetAuthScheme` stays unauthenticated for scheme discovery.

The package is **strictly read-only**: every surface observes state, none of them mutates it.

## Core Properties

- **Read-only by construction.** There is no write, delete, split, or reconfigure verb anywhere on the surface. The facade and the gRPC service expose observation verbs only.
- **Strongly-consistent reads.** Entry scans run by default (`EntryScanMode.Snapshot`) under the core library's snapshot-isolated cursor machinery, so a page reflects a coherent point-in-time view even during concurrent writes and rebalancing; the opt-in `Live` and `LivePointInTime` modes trade that isolation for a baseline-free open, so later pages can reflect writes committed after the scan opened. Structure and metric counts are not cursor-bound: they come from the pushed-up topology digest and the metrics sampler, which report the latest published aggregate rather than a scan-pinned snapshot.
- **Transport-agnostic.** The facade is the contract; gRPC is one binding. The same records flow to an in-process consumer and a remote one, so the `Orleans.Lattice.Api.Mcp` MCP server reuses the facade with zero re-modelling.
- **Fail-closed.** The gRPC surface authorizes every protected state read or observation call. Left unconfigured it denies protected traffic, while `GetAuthScheme` remains open only to advertise how to sign in.
- **Low ambient cost.** Discovery, structure, and entry reads are pull-driven and do only the requesting call's work; the metrics surface coalesces shared work: many concurrent subscribers to the same metric request share a single sampling loop, and a cluster with no metric subscribers does no sampling at all.
- **Tenant-scoped tree names.** Every `TreeId` a request carries is a **tenant-local name**: the facade resolves it to its effective, tenant-scoped id through `ITenantContextResolver.ResolveEffectiveTreeIdAsync` at the entry point and uses that one id for both the authorization check and the read, so a call can never authorize one tree and observe another. With the tenancy add-on absent - or registered, but with no active tenant asserted, which resolves the default tenant - the bare name is returned unchanged, so behaviour is byte-for-byte as before. Under an asserted, non-default active tenant an unqualified name is scoped into that tenant's `t/{tenant}/{name}` namespace, and an already-qualified `t/` id or a `_lattice_` system-tree name passes through unchanged (a well-formed foreign `t/{other}/{name}` is left to the tenancy access gate to adjudicate). The call fails closed with a `LatticeTenantAccessDeniedException` when the asserted tenant fails validation against the caller's own membership, or when, outside a system-origin scope, it names a `sys-` tree or a malformed `t/` id that belongs to no tenant. See [`Orleans.Lattice.Tenancy`](../lattice.tenancy/README.md).

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Tree & view discovery** | A deterministic, paged catalog of every registered tree, materialised view, and tag index, with optional per-view stats and system-tree inclusion, plus tag-value, covered-tree, and tag-member browsing. | [Surfaces](surfaces.md#discovery) |
| **Tree-structure query** | The shard-root node graph of a tree - per-shard roots, child fan-out, depth, and live-key subtree counts - bounded by depth and node limits. | [Surfaces](surfaces.md#structure) |
| **Entry inspection** | Key-ordered entry scans (snapshot-isolated by default, or a cheaper live cursor; forward / reverse, predicate-filtered, value-preview-budgeted) and single-key record fetch. | [Surfaces](surfaces.md#entries) |
| **Change observation** | A server-streamed feed of a tree's live mutations - sets, deletes, and range deletes - with optional maintenance-rewrite inclusion. | [Surfaces](surfaces.md#change-observation) |
| **Per-key change history** | A continuation-paged timeline of one key's revisions with per-row retention metadata and a bound that reports whether the timeline is age-bounded, truncated, or a write-ahead-log-window fallback. | [Surfaces](surfaces.md#change-history) |
| **Metrics observation** | A one-shot per-tree metrics snapshot, or a delta-coalesced live feed of live keys, shard count, shard hotness, and view lag. | [Surfaces](surfaces.md#metrics) |
| **Cluster identity** | The connected cluster's Orleans cluster id and service id, for a client header or a multi-cluster picker. | [Surfaces](surfaces.md#cluster-info) |
| **Code-first gRPC binding** | A long-lived gRPC service and a public typed client over Orleans-serialized C# records that wrap or reuse facade DTOs - no hand-written `.proto`. | [gRPC Contract](grpc-contract.md) |
| **Fail-closed authorization** | A per-call authorization seam that denies protected calls by default until an authorizer is registered or enforcement is explicitly disabled. | [Security](security.md) |
| **Dead-letter inspection** | Count and page strict-mode schema-enforcement dead-letter queues as read-only state. | [Surfaces](surfaces.md#dead-letters) |
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
// protected calls, so register a real one (or disable enforcement behind an outer
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
- [Configuration](configuration.md) - every public options property, its type, and its default.
- [gRPC Contract](grpc-contract.md) - the code-first service, its RPCs, the wire records, and the public client.
- [Surfaces](surfaces.md) - discovery, structure, entry inspection, change history, change observation, metrics, and cluster info, request by request.
- [Security](security.md) - the fail-closed authorization seam, the default-deny posture, and the transport story.
- [Efficiency](efficiency.md) - shared sampling, reader-less zero cost, and the overhead guardrails.
- [Client](client.md) - building and driving `LatticeStateApiGrpcClient`, and reusing the facade in-process.
