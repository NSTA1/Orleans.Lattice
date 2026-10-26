# Values that merge - brief

- **Path:** Build, second episode (B2), after Hello, Lattice.
- **Audience:** developers who can register Lattice and read and write a typed
  value through `ILattice`.
- **Length:** two to five minutes.
- **The one idea:** a plain value keeps the last write, while a counter,
  register, set or map merges concurrent writes by construction; how to choose.
- **It introduces:** [CRDT primitives](../../../docs/crdt/readme.md).
- **Afterwards the viewer can** choose a primitive by its conflict rule,
  distinguish preserving concurrent updates from replacing a value, and find
  the typed accessor examples in the CRDT guide.
- **Release line:** the Orleans.Lattice 9.9 core package. No companion package
  or unreleased surface is shown.

## Beats

1. Recap Hello, Lattice, then distinguish replacement from merge.
2. Show the site's G-Counter scenario: separate writer components, value eight,
   and an unchanged value on re-delivery. Name PN-Counter for decrements.
3. Choose set semantics: grow-only, add-wins or remove-wins.
4. Show the site's multi-value register scenario: concurrent values are kept;
   an application resolves them with a later write after observing both.
5. Distinguish monotone registers and recursive map values.
6. Typed accessors encode deltas; independent writers need stable, distinct
   identities. Local mixed-mode trees differ from replicated trees.
7. End with the plan's next episode, deep dive and CRDT guide.

## Sources

- [CRDT overview](../../../docs/crdt/readme.md): the choice table, accessor
  model, replica identities and replicated-tree restriction.
- [G-Counter](../../../docs/crdt/gcounter.md) and
  [PN-Counter](../../../docs/crdt/pncounter.md): counter semantics.
- [G-Set](../../../docs/crdt/gset.md),
  [OR-Set](../../../docs/crdt/orset.md) and
  [RW-Set](../../../docs/crdt/rwset.md): set conflict rules.
- [MV-Register](../../../docs/crdt/mvregister.md),
  [Max-Register](../../../docs/crdt/maxregister.md) and
  [Min-Register](../../../docs/crdt/minregister.md): register choices.
- [OR-Map](../../../docs/crdt/ormap.md): recursive values and host registration.
- `docs-site/figures/join-figures.json`: the two animated scenarios.

## Not in this episode

- No code on screen: the guide holds the compiled accessor examples.
- No full catalogue recital, causal-dot implementation, sequence or flag tutorial.
- No replication setup, storage configuration, global ordering or performance
  claims. The mathematical explanation belongs to H1.
- No Explorer or unreleased package, and no change to the series plan.
