# Orleans.Lattice.Api.State

Optional read-only **cluster state API** add-on for [`Orleans.Lattice`](https://github.com/NSTA1/Orleans.Lattice).

Layered on top of `AddLattice(...)` with a single opt-in call:

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeStateApi();
```

It exposes a transport-agnostic facade that lets external clients **query**, **observe**, and
**subscribe to** a cluster's lattice state and metadata - trees, their structure (shard roots,
internal nodes, leaves), entries, and materialised views - intended to back a tree-explorer
dashboard and a later MCP surface. The API is strictly read-only and is designed to add minimal
overhead: zero cost when unregistered, negligible cost when idle, and bounded cost under load.

This package is shipped and versioned in lock-step with the rest of the Orleans.Lattice family.
