# CRDTs in Orleans.Lattice

A beginner-friendly tour of the conflict-free replicated data types (CRDTs)
that ship with Orleans.Lattice, and how to drive each one through the typed
`ILattice` accessor extensions.

## What is a CRDT (in one minute)

Imagine the same logical value lives on two clusters at once (say, one in
Europe and one in the US) and both accept writes at the same time. If Europe
writes `X` and the US writes `Y` to the same key concurrently, which one wins?

The naive answer is "last writer wins" - keep the newest timestamp and throw the
other away. That is simple, but it silently loses data whenever two sides write
at the same time.

> [!WARNING]
> This last-writer-wins behaviour is exactly what a plain `tree.SetAsync(...)`
> does: a concurrent write to the same key silently discards one side. Only use
> it when you know concurrent writes to a key cannot happen; otherwise reach for
> one of the CRDT accessors below.

A **CRDT** is a data type designed so that this question never needs a lock, a
coordinator, or a vote. Each replica applies writes locally and ships its state
(or a small delta) to the others. When two states meet they are combined with a
**merge** function that is:

- **Commutative** - `merge(a, b) == merge(b, a)`; order of arrival does not matter.
- **Associative** - regrouping merges does not matter.
- **Idempotent** - merging the same update twice is harmless; duplicates and
  re-deliveries are safe.

Because the merge obeys those three rules, every replica that has seen the same
set of updates ends up in **exactly the same state**, no matter what order the
updates arrived in. This property is called *strong eventual convergence*, and
it is what lets Orleans.Lattice replicate active-active across clusters without
consensus.

The trade-off is that a CRDT resolves conflicts *by construction* rather than by
asking you. A counter sums both sides; a set keeps both adds; a register may
surface both concurrent values so your application can pick. You choose the
primitive whose built-in resolution rule matches what your data means.

## The primitives

| Primitive | Accessor | Converges by | Reach for it when... |
| --- | --- | --- | --- |
| [G-Counter](gcounter.md) | `tree.GCounter(key)` | per-replica sum (grow-only) | you need a counter that only goes up (views, totals) and want the minimal, tombstone-free counter |
| [PN-Counter](pncounter.md) | `tree.PnCounter(key)` | per-replica sum | you need a counter that many clusters increment and decrement at once (likes, stock, quotas) |
| [G-Set](gset.md) | `tree.GSet(key)` | set union (grow-only) | you need an append-only set (tag sets, seen-ids) and never remove elements |
| [OR-Set](orset.md) | `tree.OrSet(key)` | add-wins observed-remove | you need a distributed set where a concurrent add and remove should keep the element |
| [RW-Set](rwset.md) | `tree.RwSet(key)` | remove-wins observed-remove | you need a set where a removal must win the tie (revocation lists, blocklists) |
| [MV-Register](mvregister.md) | `tree.MvRegister<T>(key)` | keep concurrent values | you hold a single typed value but want to *see* concurrent writes instead of silently dropping one |
| [Max-Register](maxregister.md) | `tree.MaxRegister<T>(key, orderKeySelector)` | directional max (monotone) | a value only moves up (high-water mark, version ceiling, max-seen reading) |
| [Min-Register](minregister.md) | `tree.MinRegister<T>(key, orderKeySelector)` | directional min (monotone) | a value only moves down (latency floor, first-seen timestamp) |
| [Version Vector](versionvector.md) | `tree.VersionVector(key)` | per-replica max | you track causal history / "who has seen what" to detect concurrency |
| [OR-Flag](orflag.md) | `tree.OrFlag(key)` | enable-wins | you track a boolean presence bit where a concurrent enable should beat a disable |
| [RW-Flag](rwflag.md) | `tree.RwFlag(key)` | disable-wins | you track presence where a removal must win the tie (revocation lists, blocklists) |
| [OR-Map](ormap.md) | `tree.OrMap<TKey,TValue>(key)` | recursive per-key merge | you need a dictionary whose values are themselves CRDTs merged per key |
| [Sequence (RGA)](sequence.md) | `tree.Sequence<T>(key)` | ordered insert/tombstone | you collaboratively edit an ordered list or text buffer |

Each linked page has a diagram of the convergence behaviour and a short,
runnable `ILattice` example.

## The catalogue at a glance

The catalogue is organised by data-structure family; each family offers the
variants whose built-in conflict resolution differs. Pick the family that matches
your data, then the variant whose resolution rule matches what a concurrent edit
should mean.

| Family | Variants |
| --- | --- |
| Counter | grow-only [`GCounter`](gcounter.md), positive-negative [`PnCounter`](pncounter.md) |
| Set | grow-only [`GSet`](gset.md), add-wins [`OrSet`](orset.md), remove-wins [`RwSet`](rwset.md) |
| Flag | enable-wins [`OrFlag`](orflag.md), disable-wins [`RwFlag`](rwflag.md) |
| Register | monotone [`MaxRegister`](maxregister.md) / [`MinRegister`](minregister.md), multi-value [`MvRegister`](mvregister.md), last-writer-wins mode |
| Map | [`OrMap`](ormap.md) |
| Sequence | [`Rga`](sequence.md) |
| Causality | [`VersionVector`](versionvector.md) |

> **Deliberate non-goals.** The sequence family ships only RGA; an
> interleaving-correct list variant (LSEQ / Fugue-style) is intentionally out of
> scope because RGA covers the collaborative-list workload and the
> interleaving-anomaly fix carries a cost the catalogue does not yet need. If your
> workload hits concurrent-insert interleaving, raise an issue and we will
> revisit.

> Not a CRDT: `LatticeMergeMode.LwwRegister` is plain last-writer-wins on the
> value bytes. It is safe only when a single cluster owns each key at a time; use
> a typed CRDT above whenever concurrent writers can target the same key.

## How you use them here

Every value in the store is opaque `byte[]`, so the tree cannot guess which CRDT
a key holds. Rather than declaring a type up front, you read and write through a
**typed accessor** hanging off `ILattice` - for example `tree.PnCounter("key")`
or `tree.MvRegister<string>("key")`. The accessor carries the merge mode, encodes
the delta for you, and exposes natural methods (`AddAsync`, `IncrementAsync`,
`SetAsync`) so you never hand-build byte arrays.

Each key records its own merge mode, so a single tree can **mix** CRDT types
freely, with different CRDT primitives and plain last-writer-wins values
alongside one another. There is nothing to configure per tree for local
(single-cluster) use.

> [!NOTE]
> The one exception is **cross-cluster replication**. An enrolled tree declares a
> single [`LatticeMergeMode`](../lattice/api.md) in its replication map, and a
> receiver dead-letters any inbound write whose mode differs - so a peer cannot
> override the merge algebra, and a declared-CRDT tree also rejects plain
> `SetAsync` writes. Mix modes freely on a local tree; keep a replicated tree to
> the single mode it was enrolled under.

The `replicaId` argument you pass to many of these methods names the writer.
Give each independent writer (cluster, silo, or logical actor) a stable, distinct
id so concurrent edits are attributed to different causal lineages.

## Who owns the bytes

Every primitive stores opaque `byte[]` payloads, and the library follows one rule
for who owns a given array. Ownership is decided by where the array came from,
not by which type is holding it:

| Seam | Rule | What it means for you |
|---|---|---|
| **You hand a value in** (`SetAsync`, `AddAsync`, and the primitives' `Set`/`Add`/`InsertAfter`) | The array is taken over, not copied | Do not keep writing into an array after you pass it in. Author it, hand it over, forget it. |
| **A peer or a delta is folded in** (`MergeFrom`, `MergeDelta`) | The incoming array is copied | A replica never ends up sharing a buffer with the peer it merged from. |
| **A value comes back out** (`Clone`, `OrMap.Get`, materialised projections) | The array is copied | A value you read is yours to mutate; doing so can never reach back into stored state. |

The first rule is why writes stay allocation-free on the hot path, and the last
two are why a read or a merge can never corrupt somebody else's state. If you are
implementing a CRDT against `ICrdt<TSelf>`, all three legs are part of the
contract.

Two things keep the cost of the copying legs down. An empty or tombstoned payload
reuses the shared `Array.Empty<byte>()` singleton, so it never allocates - which
matters because an aged sequence is mostly tombstones. And the set primitives
(`GSet`, `OrSet`, `RwSet`) never retain a caller's array at all: an element is
encoded to a string key on the way in and decoded fresh on the way out, so they
satisfy all three legs by construction.

The rule is enforced structurally rather than by review. A contract test walks
every registered CRDT's object graph and compares `byte[]` instances by reference
identity across a clone, a state fold, a delta fold, and each public projection.
It also fails when a CRDT type has no specimen registered, or grows a new public
`byte[]`-bearing projection that nothing covers - so a new primitive cannot join
the family without picking up the contract.

## Where a bounded register's direction lives

A `MaxRegister` and a `MinRegister` are the same primitive pointed in opposite
directions. That direction is owned by the **merge mode registered for the key**
(`LatticeMergeMode.MaxRegister` or `LatticeMergeMode.MinRegister`), which is what
the accessor you used selects. The `IsMin` bit you can see on the decoded
`BoundedRegister` state is a copy of that decision carried on the wire, not a
second source of truth: every decode re-stamps it from the registered mode.

This matters if you ever write register state as raw bytes rather than through
`tree.MaxRegister(...)` / `tree.MinRegister(...)` - a hand-authored payload whose
`IsMin` disagrees with the key's mode is corrected on read rather than silently
folding the wrong way for the rest of the key's life.
