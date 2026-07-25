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
| [OR-Set](orset.md) | `tree.OrSet(key)` | add-wins observed-remove | you need a distributed set where a concurrent add and remove should keep the element |
| [PN-Counter](pncounter.md) | `tree.PnCounter(key)` | per-replica sum | you need a counter that many clusters increment and decrement at once (likes, stock, quotas) |
| [MV-Register](mvregister.md) | `tree.MvRegister<T>(key)` | keep concurrent values | you hold a single typed value but want to *see* concurrent writes instead of silently dropping one |
| [Version Vector](versionvector.md) | `tree.VersionVector(key)` | per-replica max | you track causal history / "who has seen what" to detect concurrency |
| [OR-Flag](orflag.md) | `tree.OrFlag(key)` | enable-wins | you track a boolean presence bit where a concurrent enable should beat a disable |
| [RW-Flag](rwflag.md) | `tree.RwFlag(key)` | disable-wins | you track presence where a removal must win the tie (revocation lists, blocklists) |
| [OR-Map](ormap.md) | `tree.OrMap<TKey,TValue>(key)` | recursive per-key merge | you need a dictionary whose values are themselves CRDTs merged per key |
| [Sequence (RGA)](sequence.md) | `tree.Sequence<T>(key)` | ordered insert/tombstone | you collaboratively edit an ordered list or text buffer |

Each linked page has a diagram of the convergence behaviour and a short,
runnable `ILattice` example.

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
freely: an OR-Set under one key, a PN-Counter under another, and plain
last-writer-wins values alongside them. There is nothing to configure per tree
for local (single-cluster) use.

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
