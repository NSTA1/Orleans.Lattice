# Conflict-Free Merges

## :warning: CRDTs are opt-in

Plain writes on a tree - `SetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, and
friends - are **last-writer-wins** (an `LwwRegister`): when two writers touch the
same key concurrently, the later timestamp silently overwrites the earlier one
and the losing update is discarded. That is the correct default for ordinary
key/value data, but it is **not** conflict-free.

To get the convergent, no-lost-update behaviour shown below you must
**explicitly opt in** by writing through one of the typed CRDT extension
accessors (`tree.PnCounter(key)`, `tree.OrSet(key)`, `tree.OrFlag(key)`, ...).
The accessor picks the right merge mode for the key; a plain `SetAsync` to the
same key would fall back to last-writer-wins.

## What it shows

Orleans.Lattice can store values as CRDTs, so independent writers can mutate the
same logical value concurrently - with no coordination - and the store always
converges to the **same** final state, regardless of thread interleaving or the
order updates are merged. This is opt-in (see the warning above); this sample
drives it entirely through the typed CRDT **extension** surface on `ILattice`
(`tree.PnCounter(key)`, `tree.OrSet(key)`, ...) rather than instantiating the
primitive types directly.

1. **Convergence under concurrent threads** - 100 writers, each its own replica,
   hammer one shared tree at the same time. Every PN-counter increment and
   OR-set add survives: the final count is exact, with zero lost updates.
2. **Every CRDT type converges regardless of merge order** - two replicas
   diverge in isolation, then exchange and merge each other's state. Each of the
   eight CRDT accessors reaches an identical result whichever side merges first.

## CRDT types (via the `ILattice` extension surface)

| Accessor | Semantics | Use case |
|---|---|---|
| `tree.PnCounter(key)` | Increment/decrement counter; merges by summing per-replica components. | Distributed counters: likes, view counts, inventory reservations. |
| `tree.OrSet(key)` | Observed-remove set; a concurrent add beats a concurrent remove (add-wins). | Membership sets: tags, followers, a shopping cart's contents. |
| `tree.OrFlag(key)` | Enable-wins boolean flag. | A toggle where turning something **on** should win a tie (e.g. activating a feature). |
| `tree.RwFlag(key)` | Disable-wins boolean flag. | A toggle where a **removal** must win a tie: revocation lists, blocklists, opt-outs. |
| `tree.VersionVector(key)` | Causal clock; merges pointwise-max per replica. | Tracking causality - deciding whether two updates are causal or concurrent. |
| `tree.MvRegister<T>(key)` | Multi-value register; concurrent writes survive as a conflict set instead of last-writer-wins. | Conflicting single-value edits the app (or a user) should resolve, e.g. a profile field edited in two places. |
| `tree.OrMap<TKey,TValue>(key)` | Add-wins map whose cells are themselves CRDTs, merged recursively. | Per-entity sub-state: per-user counters, per-city tallies, nested documents. |
| `tree.Sequence<T>(key)` | Replicated growable array; concurrent inserts converge on a deterministic order. | Ordered collaborative data: text buffers, ordered lists, activity feeds. |

## Run it

```
dotnet run --project samples/ConflictFreeMerges
```

## Expected output

```
Silo starting... ready.

== 1. Convergence under concurrent threads ==
  100 writers, each its own replica, all writing at once
  PnCounter 'votes'   = 100 (expected 100)
  OrSet 'members'     = 100 distinct members (expected 100)
  [OK] concurrent writers converged with zero lost updates

== 2. Every CRDT type converges regardless of merge order ==
  Two replicas diverge in isolation, then merge each other's state.

  PnCounter (add/subtract counter)
    after merge: a=7  b=7  [OK -> 7]

  OrSet (add-wins set)
    after merge: a.contains('green')=True  b.contains('green')=True  [add-wins]

  OrFlag (enable-wins flag)
    after merge: a.enabled=True  b.enabled=True  [enable-wins -> True]

  RwFlag (disable-wins flag)
    after merge: a.enabled=False  b.enabled=False  [disable-wins -> False]

  VersionVector (causal version tracker)
    after merge: a.replicas=2  b.replicas=2  identical=True  [both lanes kept]

  MvRegister (multi-value register)
    after merge: a=[left-edit, right-edit]  b=[left-edit, right-edit]  [both edits survive for the app to resolve]

  OrMap (map of CRDT cells)
    after merge: a has london=True paris=True  b has london=True paris=True

  Sequence / RGA (ordered list)
    after merge: a=[b1, a1, a2]  b=[b1, a1, a2]  identical=True  [deterministic order]

Done. Every replica reached the same state without locks, consensus, or a conflict prompt.
```

## When to use

- Multi-writer or multi-region data where you want availability over
  coordination: each writer proceeds locally and the state merges deterministically.
- Counters, sets, flags, registers, maps, and sequences whose concurrent updates
  should combine by algebra (sum, add-wins, disable-wins, latest-wins, union)
  rather than by a lock or a manual conflict prompt.

## When not to use

- Invariants that require a global check before a write (e.g. "never oversell
  stock"). CRDTs converge, but they do not enforce cross-key transactional
  constraints - use `SetManyAtomicAsync` or an external guard for those.

## Feature docs

[docs/lattice/state-primitives.md](../../docs/lattice/state-primitives.md)
