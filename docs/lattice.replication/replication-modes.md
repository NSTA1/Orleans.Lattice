# Replication modes

Every tree replicated by `Orleans.Lattice.Replication` declares a
**`ReplicationMode`** at configuration time. The mode tells receivers how
to merge the captured value bytes; the producer stamps it onto every
emitted `ReplogEntry` so the receiver never has to guess.

There is no implicit fallback. A tree that is not declared in
`LatticeReplicationOptions.ReplicatedTrees` is **not replicated**. This is
deliberate — the core library stores every value as opaque `byte[]`, so
the producer cannot recognise CRDT primitives by inspection. Implicit
opt-in would silently fall back to last-writer-wins on bytes and risk
concurrent-update data loss; explicit declaration removes the footgun.

## Declaring a mode

```text
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, ReplicationMode>
    {
        ["users"] = ReplicationMode.LwwRegister,
        ["orders"] = ReplicationMode.LwwRegister,
    };
});
```

`null` and an empty dictionary both mean "no trees are replicated" — the
commit-time observer short-circuits before any sink call.

## Available modes

| Mode | Status | Convergence guarantee |
|------|--------|-----------------------|
| `LwwRegister` | **Available now** | Last-writer-wins ordered by `(HybridLogicalClock, OriginClusterId)`. Concurrent writes from different clusters silently drop the loser. |
| `OrSet` | Reserved | Observed-remove set. Rejected by the options validator until the core library exposes the typed primitive value surface. |
| `PnCounter` | Reserved | Positive-negative counter. Rejected by the options validator until the core library exposes the typed primitive value surface. |
| `VersionVector` | Reserved | Version vector. Rejected by the options validator until the core library exposes the typed primitive value surface. |

Declaring a tree with any reserved mode fails fast at first options
resolution with a clear validation error.

## When `LwwRegister` is the right choice

`LwwRegister` is the only mode currently usable end-to-end, and it is a
real convergence rule — but only under **single-writer-per-key
discipline**. Each key must have at most one authoritative cluster at any
given time (e.g. routed by tenant, by shard, or by ownership token).
Under this discipline, last-writer-wins is correct: there is never a
genuinely-concurrent write to resolve, and the HLC-plus-origin tiebreaker
just orders the unambiguous successor.

If your workload allows concurrent writes from multiple clusters to the
same key, last-writer-wins **silently drops the loser** — both writes
return success on their respective clusters, but only one survives the
merge. For those workloads, wait for the typed CRDT modes; do not paper
over the gap with `LwwRegister`.

## How the mode is resolved at commit time

The commit-time observer routes every mutation through
`IReplicationModeResolver.Resolve(treeId)`:

- The default implementation reads
  `LatticeReplicationOptions.ReplicatedTrees` and caches the per-tree
  outcome until `IOptionsMonitor.OnChange` fires.
- Hosts can replace the registration to source the mode map from
  elsewhere (a control plane, a feature flag system, or a permissive
  test stub that opts every tree in to `LwwRegister`).
- A `null` return value means "this tree is not replicated" and the
  observer returns immediately, before any `IReplogSink` call.

The resolved mode is written to `ReplogEntry.Mode` so receivers can pick
the correct apply algorithm without re-inspecting the value bytes.
