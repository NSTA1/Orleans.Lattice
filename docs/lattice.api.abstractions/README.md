# Orleans.Lattice.Api.Abstractions

The shared **API contract** package for [Orleans.Lattice](../../README.md) - the transport-agnostic service interfaces of the seven API facades (state, data, auth, backup, schema, replication, tree administration) and their request / response models, and nothing else.

## What is it?

The Orleans.Lattice API surface is built in layers. Each **facade** package (`Orleans.Lattice.Api.State`, `.Api.Data`, `.Api.Auth`, `.Api.Backup`, `.Api.Schema`, `.Api.Replication`, `.Api.TreeAdmin`) exposes a single transport-agnostic service interface over plain request / response records; each **binding** (`...Grpc`) and the `Orleans.Lattice.Api.Mcp` server projects that same surface onto a wire protocol or tool set.

`Orleans.Lattice.Api.Abstractions` is the seam between the facades and their consumers. It carries only the contract:

- **The service interfaces** - `ILatticeStateQuery`, `ILatticeStateObserver`, and `ILatticeStateMetricsObserver` (state); `ILatticeDataApi` (data); `ILatticeAuthAdmin` (auth); `ILatticeBackupControl` (backup); `ILatticeSchemaControl` (schema); `ILatticeReplicationControl` (replication); `ILatticeTreeAdmin` (tree administration); and `ILatticeRegionCatalog` (region discovery).
- **Their request / response models** - the results, pages, records, and requests those interfaces exchange, each with its stable Orleans serialization alias.

### Region contract

The `Orleans.Lattice.Api.Region` namespace carries the transport-agnostic region-discovery contract, so any consumer - the MCP `lattice_list_regions` tool today, a future Explorer UI or gRPC binding - reads the same region model rather than a surface-specific type:

- `ILatticeRegionCatalog` - `ListRegionsAsync` lists the regions a server can route to, current region first.
- `LatticeRegionDescriptor` - one region's id, cluster id, whether it is the current region, and its per-group reachability.
- `LatticeRegionGroupReachability` - whether a facade group is reachable in a region and the endpoint it is served at.

The package has no implementation, no registration extension, and no background work. Facade packages reference it and implement the interfaces; binding and MCP packages reference it and consume them.

## Why it exists

Before this package the facade service interfaces were `internal` to each facade package, so every consumer that needed the contract - the gRPC bindings and the co-hosted MCP server - had to be granted `InternalsVisibleTo` into the facade assembly (and, for the MCP server, into the core assembly as well). That coupled a consumer to a producer's private surface across several assemblies.

Publishing the contract as a real, versioned public package removes those cross-package internal-visibility grants: a binding evolves against a stable public contract rather than another package's internals, and `internal` inside each facade goes back to meaning "safe to change". The interfaces keep their original `Orleans.Lattice.Api.{State,Data,Auth,Backup,Schema}` namespaces, so existing consumers compile unchanged.

## Core properties

- **Contract-only.** Interfaces and DTOs, no behaviour. There is nothing to register from this package directly.
- **Stable wire identity.** Every serializable model keeps its existing `[Alias]`, so the move between assemblies is wire-compatible: persisted and in-flight payloads are unaffected.
- **Source-compatible.** Namespaces are unchanged, so a consumer's `using` directives and type references keep resolving after the move.
- **Trusted system-origin seam.** A co-hosted infrastructure consumer that must run a trusted, gate-bypassing introspection uses the public `LatticeSystemOrigin` seam in the core library rather than an internal-visibility grant.

## Usage

You do not register anything from this package directly. Register a facade (which implements these contracts); the now-public interface then resolves straight from DI for a co-hosted, in-process consumer, with no `InternalsVisibleTo` grant into the facade package:

```csharp verify
var builder = WebApplication.CreateBuilder();

builder.Host.UseOrleans(silo =>
{
    silo
        .AddLattice((s, storageName) => s.AddMemoryGrainStorage(storageName))
        .AddLatticeStateApi();
});

var app = builder.Build();

// ILatticeStateQuery is public, so a co-hosted consumer resolves the contract
// directly from DI - no InternalsVisibleTo into Orleans.Lattice.Api.State.
ILatticeStateQuery stateQuery = app.Services.GetRequiredService<ILatticeStateQuery>();
```

For a remote surface, add the matching binding (which consumes the same contract). See the facade package docs for the full registration story:

- [`Orleans.Lattice.Api.State`](../lattice.api.state/README.md) / [`.Grpc`](../lattice.api.state.grpc/README.md)
- [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) / [`.Grpc`](../lattice.api.data.grpc/README.md)
- [`Orleans.Lattice.Api.Auth`](../lattice.api.auth/README.md) / [`.Grpc`](../lattice.api.auth.grpc/README.md)
- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) / [`.Grpc`](../lattice.api.backup.grpc/README.md)
- [`Orleans.Lattice.Api.Schema`](../lattice.api.schema/README.md) / [`.Grpc`](../lattice.api.schema.grpc/README.md)
- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) / [`.Grpc`](../lattice.api.replication.grpc/README.md)
- [`Orleans.Lattice.Api.TreeAdmin`](../lattice.api.treeadmin/README.md) / [`.Grpc`](../lattice.api.treeadmin.grpc/README.md)
- [`Orleans.Lattice.Api.Mcp`](../lattice.api.mcp/README.md)
