# Scans, filters and cursors - brief

- **Path:** Build, third episode (B3), after Values that merge and its deep dive.
- **Audience:** a developer reading and processing keys from an `ILattice` tree.
- **Length:** two to three minutes.
- **The one idea:** scan an ordered key range, filter values at the owning leaf, and use a durable cursor when work needs explicit resumable pages.
- **It introduces:** [Predicate operations](../../../docs/lattice/predicated-operations.md) and the [API reference](../../../docs/lattice/api.md).
- **Afterwards the viewer can** choose a resilient streaming scan for a long walk, add a supported typed predicate that runs on the server, or open a stateful cursor and save its identifier to resume paged work.

## Beats

1. Recap that Values that merge made the value's merge rule explicit; this episode chooses which keys to read and how to keep a walk going.
2. An ordered range: scans return live keys in strict lexicographic order; the lower bound is inclusive, the upper bound exclusive, and reverse changes direction.
3. Filter at the leaf: a typed lambda is translated into a serializable predicate; unsupported expressions fail on the client; leaves evaluate it against JSON values, returning only matching keys or entries.
4. Resilient streams: prefer `ScanKeysAsync` and `ScanEntriesAsync`; their wrappers recover from a reclaimed remote enumeration with the predicate intact, within a bounded retry budget.
5. Stateful cursors: open an opaque cursor ID, page through it, retain the ID for failover or client restart, and close the cursor when finished; point-in-time mode holds one saga-decision view across pages.
6. Where next: Atomic writes, plus the Predicate operations and API reference pages.

## Code on screen

Two compiled snippets from the companion page: a bounded predicate scan and a predicate entry cursor that drains pages.

## Sources

- [Predicate operations](../../../docs/lattice/predicated-operations.md): "How it works", "Supported expressions", "Streaming scans", and "Durable cursors with a predicate".
- [API reference](../../../docs/lattice/api.md): "Enumeration", "Scan reliability", "Stateful cursors", "Point-in-time cursors", and the predicate operation signatures.
- [Series plan](../../series.md): B3's idea, its introduced pages, and the ending printed by `npm run series -- ending B3`.

## Not in this episode

- Arbitrary C# execution on the server; only the documented predicate expression language is supported.
- A claim that live scans or live cursors freeze one view across every page; point-in-time mode is an explicit cursor option.
- Snapshot cursor internals, range deletion, materialised views, performance numbers, or any claim the corpus does not make.
