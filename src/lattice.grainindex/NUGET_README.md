# Orleans.Lattice.GrainIndex

Optional, opt-in **typed grain indexing** for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

An Orleans grain that holds typed state can be enrolled in a *grain index*: the
grain's projected state is written into a lattice tree owned by this package, so
the grains matching a property predicate can be discovered without a scan of the
cluster's grain directory or a separate secondary store.

Queries reuse the core server-side predicate surface, so filtering happens in the
tree shards rather than by pulling every candidate back to the caller.

This package is a pre-release (`0.x`) and is versioned independently of the core
`Orleans.Lattice` package. Its public surface is still being built out; see the
repository documentation for the current state.
