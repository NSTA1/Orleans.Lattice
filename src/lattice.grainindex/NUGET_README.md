# Orleans.Lattice.GrainIndex

Optional, opt-in **typed grain indexing** for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice).

An Orleans grain that holds typed state can be enrolled in a *grain index*: the
grain's projected state is written into a lattice tree owned by this package, so
the grains matching a property predicate can be discovered without a scan of the
cluster's grain directory or a separate secondary store.

Queries reuse the core server-side predicate surface, so filtering happens in the
tree shards rather than by pulling every candidate back to the caller.

- Declare an index in silo setup with `AddGrainIndex<TGrain, TState>(...)`,
  naming each projected property with `Include`, and annotate the grain's
  persistent state with `[Indexed]`.
- Query it through `IGrainIndexProvider`: `GetIndex<TGrain, TState>(name)`
  returns an index whose `Where(...)` plans a query that streams matching grain
  references, keys, or matches.
- A durable pending-projection outbox retries a failed index write until it
  lands, and a reminder-anchored backfill crawl onboards dormant grains from an
  `IGrainKeySource` registered with `AddGrainIndexKeySource`.
- Startup drift detection rejects (or, when opted in, rebuilds) a declaration
  change that would invalidate the stored entries, and `IGrainIndexAdmin`
  reports status and controls the crawl.

See the
[grain index documentation](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.grainindex/README.md).
