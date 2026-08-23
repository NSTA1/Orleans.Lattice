# Verified WAL Durability

## What it shows

The write-ahead log's garbage collector may only trim log entries that every
consumer has durably acked, and it learns each consumer's progress from the WAL
cursor registry. This sample makes the two registry properties that keep the GC
safe observable at runtime, by driving the **real production registry**
(`InMemoryWalCursorRegistry`) under concurrency:

1. **Per-consumer monotonicity** - a consumer's acked cursor is a max-merge, so a
   concurrent stale or duplicate re-delivery of an *older* cursor never regresses
   it. The sample races many advancing reports against random stale
   re-deliveries and confirms no consumer's cursor is ever observed moving
   backwards.
2. **The min-cursor trim floor** - the GC-visible floor is the *minimum* cursor
   across all consumers, so a fast consumer sprinting ahead never lets the GC
   trim past a slower one and strand it. The sample pins one slow consumer, races
   the others far ahead, and confirms the floor stays pinned to the laggard until
   the laggard itself advances.

The same properties this sample observes are proven by the cores in
`src/lattice/InMemoryWalCursorRegistry.cs` and `src/lattice/WalGcTrimCore.cs`, and
model-checked by the Coyote models in `test/lattice/BPlusTree/Coyote/`
(`WalCursorMonotonicityModel`, `WalGcTrimFloorModel`).

## Run it

```
dotnet run --project samples/VerifiedWalDurability
```

## Expected output

The exact sample and re-delivery counts vary run-to-run (the monitor and the
reporters race), but the per-consumer regression count is always zero, the trim
floor always stays pinned to the laggard, and it only advances once the laggard
catches up.

```
== VerifiedWalDurability sample ==

1) Racing concurrent cursor reports (with stale re-deliveries) against the registry...
   Cursor advances issued : 1600
   Stale re-deliveries    : 192
   Registry samples taken : 4777
   Per-consumer REGRESSIONS: 0
   -> zero regressions: every stale re-delivery was max-merged away; each cursor only advanced.

2) Pinning a slow consumer while the others sprint ahead...
   fast:a, fast:b acked   : 5000
   laggard acked          : 100
   GC trim floor (min)    : 100
   -> floor pinned to the laggard: the GC cannot trim past the slowest consumer.

   laggard catches up to  : 4200
   GC trim floor (min)    : 4200
   -> floor advanced to the new minimum only after the laggard reported: safe forward progress.

These WAL cursor-safety properties are machine-checked, not just observed here:
  * cores    : src/lattice/InMemoryWalCursorRegistry.cs, src/lattice/WalGcTrimCore.cs
  * Coyote   : test/lattice/BPlusTree/Coyote/  (dotnet test --filter Category=Coyote)
  * docs     : docs/lattice/verified-wal.md

Done.
```

## When to use

- You want to see the runtime manifestation of the properties the WAL cursor
  registry cores and Coyote models verify: a consumer cursor never regresses
  under stale re-delivery, and the GC trim floor never passes the slowest
  consumer.
- You are extending the WAL shipping, GC, or cursor-registry logic and want a
  reproducible harness that races reporters against the real registry through its
  public API.

## When not to use

- You want the atomic-write visibility property (a snapshot read observes a
  multi-key saga all-or-nothing) - see the
  [VerifiedAtomicCommit](../VerifiedAtomicCommit/README.md) sample for that.

## Feature doc

[docs/lattice/verified-wal.md](../../docs/lattice/verified-wal.md)
