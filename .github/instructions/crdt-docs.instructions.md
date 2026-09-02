---
applyTo: "docs/crdt/**"
---

# docs/crdt is the beginner-friendly CRDT tour, not technical documentation

`docs/crdt/` exists to answer one question for an application developer who has
never met a CRDT: **which primitive should I use, and what will it do to my data
when two clusters write at once?** Its own opening line calls it "a
beginner-friendly tour". Treat that as a contract, not a description.

It is the only documentation area in this repository written for a reader who is
not already comfortable with the internals, so it is easy to spoil and hard to
notice you have spoiled it - every other instinct in this repo pushes toward
precision and depth.

## What belongs here

- What the primitive means, in plain language, and the conflict it resolves.
- When to reach for it over its siblings, framed by the reader's problem
  ("a removal must win the tie") rather than by mechanism.
- A short `mermaid` diagram of the convergence behaviour.
- One runnable ` ```csharp verify ` example against the typed `ILattice`
  accessor.
- Brief, plain-language reassurance where a newcomer would reasonably worry -
  for example "does this grow forever if I keep re-adding?" - phrased as an
  outcome, with a link out for the mechanism.

## What does not belong here

Anything a reader needs internals knowledge to follow:

- Storage layout, field-by-field state shapes, normal forms.
- Compaction, dot-history bookkeeping, causal-context representation.
- Predicates and invariants stated as formulae or pseudocode.
- Wire format, serialization, replication or write-ahead-log mechanics.
- Rollout, upgrade-ordering, `AppContext` switches, or other operator concerns.
- Rationale aimed at maintainers ("why we rejected the alternative").

Terms like *dot* and *tombstone* already appear on the per-primitive pages and
are fine where a page already establishes them, but do not introduce new
machinery, and never make understanding the mechanism a prerequisite for
choosing the primitive.

## Where the technical content goes instead

- **[`docs/lattice/state-primitives.md`](../../docs/lattice/state-primitives.md)** -
  the technical counterpart, and the default home. It explicitly points at
  `docs/crdt/` for the gentle introduction and offers itself as "a more detailed
  explanation", so the split is already documented in both directions.
- **XML doc comments on the type** - for the reasoning a maintainer needs while
  reading the code, including rejected alternatives.
- **`docs/lattice/tombstone-compaction.md`** - tree-level reclamation, which is a
  different mechanism from anything in the CRDT value itself.

## Before you add to this folder

Read the section you are about to edit and ask whether your addition sounds like
its neighbours. If it introduces a mechanism, a formula, or an operational
caveat, it belongs in `docs/lattice/state-primitives.md` - link to it from here
in one plain sentence instead, phrased as what the reader gets rather than how it
works.
