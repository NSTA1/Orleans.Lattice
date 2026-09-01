# Architecture

How a grain index is stored, how entries get written, and what consistency it
offers.

## The shape

One lattice tree backs a whole index - never one tree per property. Each index
tree lives under the reserved prefix `__grainindex/`, and the package's own
bookkeeping lives in a single system tree, `__grainindex/.registry`.

```
__grainindex/.registry      the definition records, drift fingerprints,
                            backfill checkpoints, and the pending-projection outbox
__grainindex/users          one application index
__grainindex/orders         another
```

## Key encoding

`GrainIndexKeyEncoder` is the single place that decides what an entry's key
looks like, and the only place the query planner goes to build a key range.

A key is three separator-delimited components:

```
{propertyName} SEP {valueComponent} SEP {encodedGrainKey}
```

The separator is `U+0000`, the lowest code unit, and the tree orders keys with
`StringComparer.Ordinal`. The property name leads, so each property occupies its
own contiguous range inside the shared tree.

The layout is **injective**: no value component ever contains the separator (the
string encoding escapes it away, and every other encoding is hexadecimal or a
single digit), so the first separator after the property prefix ends the value
and the second begins the grain key. An entry therefore names exactly one grain
and one property value. The grain key is last precisely so it may contain
anything at all.

### Ordering

For a property type with a total order the value component is **order
preserving**: ordinal comparison of two encoded keys for the same property
yields the same answer as comparing the two property values with
`Comparer<T>.Default` (ordinal comparison for `string`, matching the tree's own
key comparer).

That is what makes `Age >= 18` a single contiguous range scan rather than a full
scan. The order-preserving set is exactly `bool`, the integral types (`sbyte`,
`byte`, `short`, `ushort`, `int`, `uint`, `long`, `ulong`, and `char`), `float`,
`double`, `DateTime`, `DateTimeOffset`, and `string`. A `Nullable<T>` of any of
those is order preserving too, because the underlying type is what is tested.
`decimal` is not in the set and takes the unordered fallback below.

### Null

An ordered value component starts with a one-character presence flag - `U+0001`
for null, `U+0002` for present - so null sorts below every present value, as
`Comparer<T>.Default` orders it, and an empty string is never confused with
null.

### Fallback for unordered types

A property whose type has no total order gets a constant, empty value component,
so its entries collapse to one key per grain and the query side answers
predicates over it by scanning the property's range and evaluating the stored
payload - a payload-predicate scan.

A pleasant side effect is that such an entry's key never moves, so a value
change updates it in place with no tombstone.

## The registry tree

Each index's effective declaration is fingerprinted and stored as a registry
record in `__grainindex/.registry`. At silo start the
reconciler compares the incoming declaration against the stored record field by
field and branches on the [drift classification](configuration.md#drift-detection).

The fingerprint uses the same `XxHash128` digest construction the core uses for
its own content digests, so index identity is computed the same way as the rest
of Lattice rather than by a bespoke scheme.

## The activation and mutation path

`[Indexed]` on a grain's persistent state installs the projection on the grain's
own write path:

1. The grain activates, or calls `WriteStateAsync`.
2. The state is projected into the index's declared properties, producing the
   entry set that state *should* have.
3. That set is diffed against the projection stored for the grain, producing an
   update plan: entries to add, entries to remove.
4. The plan is applied with `SetManyAtomicAsync`, so a reader never sees a
   half-updated grain. Entry updates are all-or-nothing.

Steps 2 and 3 are what `projection.duration` measures.

Under the default `Synchronous` projection mode this happens as part of the
write path and a failure is surfaced to the caller. The mode is read once, when
the enrolment path is built, because it changes the shape of a grain's write
path rather than tuning it.

## The outbox

The outbox closes the window in which a grain's own state commit succeeds and
the index write that should follow it does not - because the tree rejected it,
or because the silo stopped in between.

Without a durable record of the intent, that failure is invisible: the grain's
state says one thing, the index says another, and nothing in the system knows
they disagree until a full backfill sweeps the grain again. With one, the drain
retries the exact same batch, under the exact same idempotency key, until it
lands.

A pending-projection marker is written to the registry tree *before*
the index write is issued, and cleared when the write is confirmed. A background
drain claims outstanding markers and retries them.

The marker carries **the whole plan**, not a "this grain is dirty" flag. A flag
would oblige the retry to re-read the grain's state, which means activating it -
so a fault that took the index down would be repaired by waking every affected
grain. The self-contained plan means the drain never activates a grain.

Because the retry replays the same plan under the same idempotency key, a
redelivered batch is not double-applied.

## The two onboarding routes converge

| Route | Covers | Trigger |
|---|---|---|
| Activation and mutation | Grains your traffic touches | Grain activation, and every state write |
| [Backfill](backfill.md) | Dormant grains, and the pre-existing population | An Orleans reminder, rate limited and checkpointed |

Both write through the same projection and plan-application path, so they
converge on one duplicate-free index. A grain reached by both is projected
idempotently: the diff against its stored projection is empty the second time.

## Consistency

The contract, stated precisely:

- **An index entry reflects the last *projected* state.** A grain that has
  mutated but whose projection has not yet landed can still match its old value.
- **A query reads the index, not the grains.** It never activates a grain to
  confirm a match. Re-read the grain if you need authoritative state.
- **Entry updates for one grain are atomic.** A reader never sees a grain
  half-way through a projection.
- **A committed state change cannot silently leave the index stale.** A failed
  index write is surfaced to the caller *and* leaves a durable marker that is
  retried until it lands.
- **A grain the backfill has not yet reached is absent**, not stale. Queries
  under-report during an incomplete backfill rather than returning wrong values.
- **`SnapshotCursor` gives page-to-page stability over the index**, not over
  grain state.

The eventual consistency is deliberate. Making a query authoritative over live
grain state would mean activating every candidate grain, which converts a range
scan into a cluster-wide activation storm.

## Cluster locality

An index entry points at a grain identity in *this* cluster, so index trees are
not replicated by default and startup audits that. See
[Grain indexes are cluster-local](configuration.md#grain-indexes-are-cluster-local).

## See also

- [Queries](queries.md#how-a-predicate-is-routed) - how the encoding is exploited by the planner.
- [Configuration](configuration.md) - the declaration and its guardrails.
- [Backfill](backfill.md) - the crawl that onboards dormant grains.
- [Observability](observability.md) - the instruments covering these paths.
