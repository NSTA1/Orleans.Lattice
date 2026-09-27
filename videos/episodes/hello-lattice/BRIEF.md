# Hello, Lattice - brief

- **Path:** Build, first episode (B1). Reached from the front door's "Build".
- **Audience:** a developer who writes code against `ILattice` and knows that
  Microsoft Orleans is a .NET framework of silos, clients and grains. No
  plain-language opening: it starts in technical terms.
- **Length:** two to three minutes.
- **The one idea:** register Lattice on a silo, resolve a tree by name, and
  write and read typed values.
- **It introduces:** [Quick start](../../../README.md#quick-start) and the
  [API reference](../../../docs/lattice/api.md).
- **Afterwards the viewer can** add the package, register Lattice on a silo
  with in-memory storage, resolve a tree from a client or a grain, write and
  read a typed record, read an absent key, delete a key, and write only when a
  key is absent - and knows that production needs durable storage for both
  the grain state and the write-ahead log.

## Beats

1. What the episode does: register, resolve, write and read.
2. Register on a silo: add the package; one `AddLattice` call registers the
   grain catalogue, the grain storage its callback supplies, and the
   write-ahead log, which is in memory by default. Here the grain storage is
   in memory too: right for a first run; in production both must be durable,
   which a later Build episode covers.
3. A tree, by name: resolve `ILattice` from the grain factory, on a client or
   inside a grain. Resolution is idempotent: the same name always routes to
   the same tree.
4. Typed values: keys are strings and values are byte arrays at the core; the
   typed extensions serialize as JSON by default; pass a serializer of your
   own, or write the raw bytes, when you want to own the format. Keys stream
   back in strict lexicographic order; scans have an episode of their own.
5. When a key is not there: a read of an absent or deleted key returns null;
   a delete leaves a tombstone and returns whether the key was live; get or
   set writes only when the key is absent, with no read-then-write race.
6. Where next: Values that merge, and the two pages it introduces.

## Code on screen

Four panels, each a compiled snippet on the companion page
(`docs/videos/hello-lattice.md`): `hello-lattice/register`,
`hello-lattice/resolve`, `hello-lattice/typed` and `hello-lattice/absent`. The
lines being narrated are marked by a bar in the marker beside them.

## Sources

Every claim is drawn from these, in their own words where possible:

- [README.md](../../../README.md), "Quick Start": the registration, what it
  registers, the in-memory defaults and the need for durable storage, the
  typed extensions and JSON default, and the raw `byte[]` surface.
- [docs/lattice/api.md](../../../docs/lattice/api.md): "Setup" (the
  package), "Basic usage" (idempotent resolution, absent and tombstoned
  reads, delete, get or set, lexicographic scans), the `ILattice` table
  (`GetAsync`, `GetOrSetAsync`, `DeleteAsync`), and `TypedLatticeExtensions`
  (JSON by default, a serializer of your own).

## Not in this episode

- The Explorer, and anything unreleased.
- Durable storage (B6), scans in depth (B3), merging values (B2),
  compare-and-swap and atomic writes (B4), and time-to-live (B5).
- Consistency guarantees, which have a page of their own.
- Performance numbers, and any claim the corpus does not make.
