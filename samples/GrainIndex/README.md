# Grain Index

## What it shows

A **grain index** tracks a grain's typed state in a lattice tree, so you can ask
*"which `User` grains are 18 or over?"* without hand-maintaining a secondary
index and without activating every grain to find out.

This sample:

- Declares an index over `IUserGrain`'s `UserState`, projecting `Age` and
  `Country`. Only the properties named with `Include` are projected.
- Writes five users. Each grain enrols itself on its own write path, because its
  state is annotated with `[Indexed]` - there is no index maintenance code in
  the grain.
- Runs a **single-property comparison** (`Age >= 18`), which becomes one
  contiguous range scan over the order-preserving key encoding rather than a
  full scan plus a filter.
- Runs a **conjunction** (`Age >= 18 && Country == "UK"`). An index entry
  carries exactly one property, so the planner issues one range scan per
  property and intersects the resulting grain keys.
- Runs a **disjunction** (`Age < 18 || Age >= 60`), which unions its branches and
  de-duplicates, so a grain matching both is yielded once.
- Confirms a match against live grain state, because the index is eventually
  consistent with respect to grain state.

## Run it

```
dotnet run --project samples/GrainIndex
```

## Expected output

```
Wrote 5 users.

Adults (Age >= 18):
  carla
  alice
  dan

Users in the UK:
  alice
  bob
  dan

UK adults (Age >= 18 && Country == "UK"):
  alice (age 34)
  dan (age 61)

Under 18 or 60+: erin, bob, dan

Done.
```

Result order follows the index's key ordering, not insertion order: entries are
ordered by the encoded property value, and a conjunction's order comes from the
scan it intersects.

## Key points

- **Opt in twice.** The silo declares the index *and* the grain annotates its
  state with `[Indexed]`. Nothing is tracked by accident.
- **`Include` is the whole projection.** There is no index-everything mode,
  because every indexed property costs write amplification on the grain's write
  path.
- **`WriteStateAsync` republishes entries atomically.** A reader never sees a
  grain half-way through a projection.
- **Queries read the index, not the grains.** Re-read the grain when the answer
  must be authoritative - the sample does this in the conjunction query.
- **This sample relies on activation enrolment only.** A real deployment with a
  pre-existing grain population also registers an `IGrainKeySource` so the
  reminder-driven [backfill](../../docs/lattice.grainindex/backfill.md) can
  onboard grains that are dormant.

## Learn more

- [Orleans.Lattice.GrainIndex](../../docs/lattice.grainindex/README.md)
- [Queries](../../docs/lattice.grainindex/queries.md)
- [Configuration](../../docs/lattice.grainindex/configuration.md)
- [Backfill](../../docs/lattice.grainindex/backfill.md)
- [Architecture](../../docs/lattice.grainindex/architecture.md)
