# Atomic commit without consensus - brief

- **Path:** How it works, a standalone episode reached from Atomic writes on
  Build.
- **Audience:** developers and architects who want to understand how an
  all-or-nothing batch is made visible across a tree's leaves.
- **Length:** two to five minutes.
- **The one idea:** prepared writes stay hidden until one tree-wide registry
  decision makes the whole batch visible or aborts it.
- **Afterwards the viewer can** describe the prepare, decision and terminal
  phases, explain why readers do not see a partial batch, and distinguish a
  single recorded decision from consensus among the leaves.
- **Introduces:** [Verified atomic commit](../../../docs/lattice/verified-atomic-commit.md).

## Beats

1. Recap the all-or-nothing batch from Build and pose the question of how
   multiple leaves expose it as one change.
2. Show a coordinator staging two keys in separate leaves, hidden from
   readers while the saga is in flight.
3. Show the tree's transaction registry recording the commit as the
   tree-wide visibility flip.
4. Show reads resolving pending keys against that decision, then terminal
   marks draining each leaf's bucket.
5. Show the failure path: retry, abort, and discard hidden writes without
   per-key rollback writes.
6. Clarify that the protocol coordinates work but does not ask the leaves to
   reach consensus; end at TTL and soft delete and the verification page.

## Sources

- [Atomic writes](../../../docs/lattice/atomic-writes.md): atomicity
  guarantees, saga coordinator, prepare, execute, compensation and the
  tree-wide visibility flip.
- [Consistency guarantees](../../../docs/lattice/consistency.md): atomic
  visibility for multi-key reads and batches.
- [Atomic-commit specification overview](../../../spec/atomic-commit/README.md):
  the abstract single-cluster protocol and its checked properties.
- [Production order](../../series.md#production-order): H4's ending and
  companion page.

## Not in this episode

- Cross-tree atomic writes, arbitrary external effects and the replicated
  receiver protocol.
- WAL durability, crash recovery details, the registry's sharding and
  admission limits.
- Claims that the TLA+ model is a refinement proof of the running system.
