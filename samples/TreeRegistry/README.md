# Tree Registry

## What it shows

Every tree you touch is tracked in a registry. This sample creates three trees
(a write auto-registers each), enumerates them with `GetAllTreeIdsAsync`, probes
existence with `TreeExistsAsync`, and reads each tree's **effective per-tree
configuration** through `IOptionsMonitor<LatticeOptions>` - including overrides
applied with `ConfigureLattice(treeName, ...)`.

## Run it

```
dotnet run --project samples/TreeRegistry
```

## Expected output

```
Silo starting... ready.

== Creating trees (first write auto-registers each) ==
  wrote to 'orders'
  wrote to 'audit'
  wrote to 'sessions'

== Registered trees (GetAllTreeIdsAsync) ==
  - audit
  - orders
  - sessions

== Existence checks (TreeExistsAsync) ==
  'orders' exists       = True
  'never-written' exists = False

== Per-tree config overrides ==
  orders    CacheTtl=00:00:30   TombstoneGracePeriod=1.00:00:00
  audit     CacheTtl=00:00:00   TombstoneGracePeriod=14.00:00:00
  sessions  CacheTtl=00:00:00   TombstoneGracePeriod=1.00:00:00

Note: 'orders' overrides CacheTtl, 'audit' overrides TombstoneGracePeriod,
      'sessions' shows the global defaults.
```

## When to use

- Operational tooling and dashboards that need to enumerate the live trees in a
  cluster and show how each one is configured.
- Multi-tenant deployments where different trees want different cache or
  tombstone policies, set once at startup via `ConfigureLattice(treeName, ...)`.

## When not to use

- As a data index. The registry lists *tree ids*, not keys - use a tag index or
  a scan to find data within a tree.

## Feature docs

[docs/lattice/tree-registry.md](../../docs/lattice/tree-registry.md)
