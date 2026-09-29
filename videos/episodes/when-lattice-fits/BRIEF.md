# When Lattice fits, and when it doesn't - brief

- **Path:** Evaluate, first. It is the episode any evaluator may open first,
  so it begins in plain words.
- **Audience:** architects and tech leads deciding whether Orleans.Lattice
  belongs in a system, and anyone they bring with them: the first part assumes
  nothing at all, so a manager or a product owner can follow it; the rest
  assumes only that Microsoft Orleans is a .NET framework for distributed
  applications.
- **Length:** two to five minutes.
- **The one idea:** Orleans.Lattice takes the place of the database, cache and
  queue a system is usually assembled from, by taking three positions - the
  store lives in the cluster, conflict resolution is algebraic, everything else
  is a seam - and it does not fit a system that has no Orleans cluster, needs
  concurrent plain writes merged, needs one global order or one read across
  trees, stores very large values in place, or has not made its log durable.
- **Pages it introduces:** [What it is and why it exists](../../../README.md#what-is-it)
  and [A core plus seams](../../../README.md#architecture-a-core-plus-seams).
- **Afterwards the viewer can** say what Orleans.Lattice takes the place of in
  a system, name the kinds of system it composes into, and name, in plain
  words, the cases where it is the wrong choice.

## Beats

The plain-language opening, with no technical terms and one everyday example,
an online shop, drawn with the same keys the technical half uses:

1. What a shop remembers: every basket and every order.
2. The usual answer: a database to keep it, a cache to read it quickly and a
   queue to pass work along, each run on its own and each with its own idea of
   what is true.
3. What Orleans.Lattice does instead: the memory stays inside the application,
   in one store with one set of rules; it fits when many parts of an
   application, perhaps in many places, change what it remembers.
4. Where it does not fit, plainly: when two places change the same plain value
   at once, only one change is kept, unless the value is one of the kinds that
   merge.

Then, "in technical terms":

5. What it is: a sorted key-value store embedded in your Orleans cluster, keys
   strings and values byte arrays, with no external database, coordinator
   service or queue beside it; set against the usual assembly, its three
   positions.
6. The store lives in the cluster: a read is a grain call, not a round trip to
   a separate tier; and the read cache belongs to the store, refreshing on
   every read by default.
7. Conflict resolution is algebraic: no lock manager, no consensus round trip,
   and any cluster can accept a write to any key.
8. Everything else is a seam: companion packages behind documented seams, a
   capability left out costing nothing, and each seam open to your own
   implementation.
9. What it composes into: what the core alone covers, the categories it
   composes into (released packages only), and a platform rather than a
   product.
10. Where it does not fit, as plainly: it needs an Orleans cluster; concurrent
    writes to a plain value are not merged; no global order across atomic
    batches and no single read across trees; a value is bounded by its log
    provider's entry; the default log is in memory.
11. Where next: The guarantees, and the two README pages it introduces.

## Sources

The README has no "when not to use it" section, so the limits come from the
pages that state them, each cited against its cue in the script's Sources
table:

- [README.md](../../../README.md): "What is it?", "Why it exists", "What you
  can build", "Architecture: a core plus seams".
- [Consistency guarantees](../../../docs/lattice/consistency.md): "Single-key
  operations" and "Read-cache staleness" for the cache; "Clock skew" for the
  later clock winning; "Cross-tree (multi-tree) atomic visibility" and "What
  Lattice does not guarantee" for global order and reads across trees.
- [State primitives](../../../docs/lattice/state-primitives.md) and the
  [CRDT primitives](../../../docs/crdt/readme.md), for the values that merge.
- [Read caching](../../../docs/lattice/caching.md), for the per-silo cache.
- [Tree storage](../../../docs/lattice/tree-storage.md), on sizing a WAL row
  for a provider, for the bound on one value.
- [WAL](../../../docs/lattice/wal.md) and
  [WAL storage providers](../../../docs/lattice/wal-storage-providers.md), for
  the in-memory default log.
- [PACKAGES.md](../../../PACKAGES.md) and the release tags, for what has
  shipped.

## Not in this episode

- The deployment journey, which is the front door's to tell.
- The guarantees operation by operation, which are the next episode's.
- The Explorer, which is being redesigned.
- Anything unreleased: vector search, and so the README's AI memory category,
  which rests on it. Its search and indexing category is left out for length
  only, since materialised views and tag indexes are in the released core.
- Performance numbers, and any claim the corpus does not make.
