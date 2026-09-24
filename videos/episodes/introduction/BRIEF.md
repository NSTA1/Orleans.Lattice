# Orleans.Lattice in three minutes - brief

- **Path:** the front door, for everyone. Every path starts here.
- **Audience:** anyone meeting Orleans.Lattice for the first time. The first
  minute assumes nothing at all, so a manager, a product owner or a student
  can follow it; the rest is for developers, architects and operators, and
  assumes only that Microsoft Orleans is a .NET framework for distributed
  applications.
- **Length:** three minutes at most.
- **The one idea:** Orleans.Lattice is a store that lives in your Orleans
  cluster, converges without locks because its merges are algebraic, and grows
  from one machine to many regions by registering companion packages, with the
  same `ILattice` programming model throughout.
- **Afterwards the viewer can** say in plain words what problem it solves, and
  choose their way into the documentation: Build, Evaluate or Operate.

## Beats

The plain-language opening, with no technical terms:

1. What state is: the things an application has to remember.
2. Why it is hard: an application in more than one place, where two places
   change the same thing at the same moment, and the usual answer of taking
   turns and keeping the memory in a separate database.
3. What Orleans.Lattice does instead: the memory stays inside the application,
   every place reaches the same answer whatever order the changes arrive in,
   nobody waits, and it grows from one machine to many regions without a
   rewrite.

Then, "in technical terms":

4. What it is: a sorted key-value store inside your own cluster, with no
   external database, coordinator or queue.
5. Its three positions, as the README states them: the store lives in the
   cluster; conflict resolution is algebraic; everything else is a seam.
6. The join, shown with the site's G-Counter scenario, including the
   re-delivery that changes nothing. The opening's two places counting page views
   are the same story told plainly, with the same numbers added.
7. The core and its seams: one node per section of `PACKAGES.md`, as on the
   site, keeping only released packages.
8. Local, Team and Global, with `ILattice` unchanged at every stage.
9. The three ways into the documentation, and where to find it.

## Sources

Every claim is drawn from these, in their own words where possible:

- [README.md](../../../README.md): the opening paragraphs, "What is it?", "Why
  it exists", "The deployment journey", and "Architecture: a core plus seams".
- [reference-architecture.md](../../../reference-architecture.md), "Disaster
  recovery", for places that keep going when one fails.
- The documentation site's home page (`docs-site/pages/index.md`): the thesis,
  "Three ways in", "One programming model, Local to Global", and "A core plus
  seams".
- The G-Counter join figure in `docs-site/figures/join-figures.json`, which
  mirrors the Behaviour example in [the G-Counter guide](../../../docs/crdt/gcounter.md).
- [PACKAGES.md](../../../PACKAGES.md), for the seams.

## Not in this episode

- The Explorer, which is being redesigned.
- Anything unreleased: vector search, RepoContext.
- Performance numbers, and any claim the corpus does not make.
