# Orleans.Lattice.Api.State

Optional read-only **cluster state API** add-on for [`Orleans.Lattice`](https://github.com/NSTA1/Orleans.Lattice). It is the outward-facing read surface of a lattice cluster: external clients can query, observe, and subscribe to live state and metadata over a transport-agnostic facade, without any mutation path.

## What it gives you

- **Discovery** - a deterministic, paged catalog of every registered tree and materialised view, with optional per-view stats.
- **Structure** - walk a tree's shard-root node graph: per-shard roots, child fan-out, depth, and live-key subtree counts, bounded by depth/node limits.
- **Entry inspection** - key-ordered, snapshot-isolated entry scans (forward or reverse, predicate-filtered, value-preview-budgeted) and single-key record fetch.
- **Change observation** - a server-streamed feed of a tree's live mutations (sets, deletes, range deletes).
- **Metrics observation** - a one-shot per-tree snapshot or a delta-coalesced live feed of live keys, shard count, shard hotness, and view lag.
- **Read-only by construction** - no write, delete, split, or reconfigure verb exists anywhere on the surface.
- **Low ambient cost** - zero cost when unregistered, negligible when idle; concurrent subscribers to the same request share one sampling loop.

Layered on top of `AddLattice(...)` with a single opt-in call:

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeStateApi();
```

For remote access, pair it with [`Orleans.Lattice.Api.State.Grpc`](https://www.nuget.org/packages/Orleans.Lattice.Api.State.Grpc), the code-first gRPC binding and typed client over the same Orleans-serialized records. The facade is intended to back a tree-explorer dashboard and the `Orleans.Lattice.Api.Mcp` MCP server.

See the [State API documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.api.state/README.md) for the full surface, security model, and client guide.
