---
agent_spec: "docs/agents/api/treeadmin.json"
---

# Chaos tests

The tree-administration package has one chaos suite. It drives the facade's explicit alias swap (`SetTreeAliasAsync`) against trees an online reshard has re-mapped, while atomic batches are written through the logical tree and continuous readers poll it, and proves that atomic visibility survives every swap. It complements the core [chaos tests](../lattice/chaos-tests.md), which cover resharding and resizing on their own.

The suite is tagged `[Category("Chaos")]`, so the non-chaos development loop excludes it:

```powershell
dotnet test --filter "TestCategory!=Chaos"
```

## Explicit alias swap suite (`test/lattice.api.treeadmin/Chaos/`)

`ExplicitAliasSwapChaosTests` runs one scenario on a single-silo test cluster with in-memory grain storage and trees sized at four keys per leaf, so splits happen early. A probe seeds a universe of keys, then keeps writing it as a chain of atomic rounds through the logical tree while readers check that every read sees one whole round.

| Phase | What it does |
|---|---|
| Grow | Reshards the logical tree from 4 to 6 physical shards under load. |
| First swap | Prepares a target tree holding the whole universe at the last committed round, reshards it from 4 to 7 shards so its map differs, then swaps the logical tree's alias onto it under load. |
| Shrink through the alias | Reshards the logical tree to 4 shards; the reshard must re-map the aliased target, not the copy the alias left. |
| Second swap | Prepares a second target (5 shards resharded to 3) and swaps the alias onto it under load. |

| Assertion | What it proves |
|---|---|
| No atomic visibility violation | Every read across every phase saw a whole round. A swap rolls the logical tree back to the target's round, so the readers' lower bound is relaxed for the swap window only - never their uniformity - and every round written after the swap settles must stick. |
| Quiesced checks after each phase | With no write in flight, every key reads at the last committed round, and a count and a full key scan both see exactly the universe. |
| Alias resolution | After each swap the logical tree resolves to that swap's target. |
| Physical shard counts `7`, `4`, `3` | The logical tree routes by each target's own shard map, which the swap carries onto the logical tree, and a reshard driven through the alias re-maps the target. |

Each phase runs under a 90-second budget. A reshard step that faults with a retryable topology fault is tolerated and retried.

## Running locally

```powershell
dotnet test test\lattice.api.treeadmin\Orleans.Lattice.Api.TreeAdmin.Tests.csproj --filter "TestCategory=Chaos"
```

The suite needs no external dependency: it hosts its own test cluster in-process.

## See also

- [`Orleans.Lattice.Api.TreeAdmin`](README.md) - the facade, including Set tree alias and the reshard verbs.
- [Tree registry](../lattice/tree-registry.md) - how an alias and its shard map are recorded.
- [Core chaos tests](../lattice/chaos-tests.md) - resharding and resizing under load.
