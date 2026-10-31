# Clocks and version vectors - brief

- **Path:** How it works, reached from Evaluate's "The guarantees".
- **Audience:** developers and architects who need to understand how replicas
  record causal history and recognise concurrent changes.
- **Length:** two to five minutes.
- **The one idea:** a version vector keeps the highest clock for each replica;
  comparing those entries shows whether one history contains another or the
  histories are concurrent.
- **Afterwards the viewer can** read a version vector as per-replica causal
  history, identify concurrent vectors, and use `DominatesOrEquals` to test
  whether one vector has seen everything in another.

## Beats

1. Recap what a caller can observe, then ask how replicas compare histories.
2. A local wall-clock timestamp alone does not establish that one replica saw
   another's write.
3. A version vector records one logical clock per replica, not application
   data.
4. Each replica advances its own entry and carries forward clocks it has seen.
5. Compare two vectors entry by entry: if each is ahead somewhere, they are
   concurrent.
6. Merge by taking the maximum in each replica entry; repeat delivery changes
   nothing.
7. A vector at least as high in every entry dominates the other and contains
   its causal history.
8. Show the typed accessor and leave conflict resolution to the application.
9. Return to Evaluate and name "The evidence".

## Sources

- [Version Vector](../../../docs/crdt/versionvector.md): causal history,
  per-replica clocks, pointwise-maximum merge, dominance, and the compiled
  accessor example.
- [CRDT primitives](../../../docs/crdt/readme.md): the role of version vectors
  in tracking causal history and detecting concurrency.
- `src/lattice/Primitives/VersionVector.cs`: stored per-replica
  `HybridLogicalClock` values, pointwise maximum, and `DominatesOrEquals`.
- `src/lattice/Primitives/HybridLogicalClock.cs`: each replica's clock combines
  wall-clock ticks with a monotonic counter.
- `videos/series.md` and `videos/series.json`: H2's idea, path, order, and
  ending.

## Not in this episode

- How the MV-Register uses causal dots to resolve a single value.
- A guarantee that wall clocks are synchronised across replicas.
- A policy for resolving application-level conflicts.
- Any claim that a version vector stores the application value itself.
