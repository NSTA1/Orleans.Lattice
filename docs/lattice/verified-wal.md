---
agent_spec: "docs/agents/invariants.yaml"
---

# Verified WAL Concurrency

The write-ahead log (WAL) is Orleans.Lattice's durability boundary: every
mutation lands in the log before it is acknowledged, and a background garbage
collector trims the log once every consumer has durably consumed past a point.
That machinery runs under heavy concurrency - producers appending, peer ship
loops and leaf materialisers reporting cursors, the GC trimming, and shard moves
quiescing and re-fencing the log - and a handful of small decision points keep it
safe. Orleans.Lattice drives those decision points from a set of **verified
cores** - pure, deterministic functions that both the production grains and a
Coyote verification layer execute - so the WAL's safety properties are
machine-checked, not just asserted by prose and integration tests.

This document describes the verification apparatus for the WAL: the proven-core
pattern applied to the WAL seams, and the Coyote concurrency tier that
model-checks each core under adversarial interleavings. It is an assurance
document; the runtime behaviour it protects is documented in
[the Write-Ahead Log](wal.md) and
[Cross-cluster replication](../lattice.replication/README.md). It shares the
proven-core pattern, harness, and tier policy with the
[Verified Atomic-Commit Protocol](verified-atomic-commit.md); read that first for
the full description of the pattern and the `[Category("Coyote")]` tier.

## The proven-core pattern, applied to the WAL

As with the atomic-commit protocol, each WAL decision point is extracted into a
pure core - a single function (or small pure type) that takes explicit inputs and
returns a verdict, with no `Task`/`await`, no wall-clock or HLC read, no Orleans
types, and no storage. The production grain hot path calls the core to make the
real decision, and a Coyote model calls the *same* core to check it under every
explored ordering, so a property proven of the core is a property of production. The
extracted cores are `internal` and exposed to the test assembly through
`InternalsVisibleTo`; the in-memory cursor registry the cursor models drive
directly is the public `InMemoryWalCursorRegistry`.

### The extracted WAL cores

| Core | Decision it owns |
|------|------------------|
| `WalShippingWatermark` | The durable-contiguous tail of a WAL shard that has several flushes in flight and out of completion order - the start of the oldest in-flight flush, or the next offset when none is in flight, which `WalShardGrain` further bounds by every abandoned flush that can still land and by one past the highest stored offset (issue #4621) - and whether an offset may be shown to a cursor-advancing reader (the replication shipper, the view maintainer, leaf replay): only offsets strictly below the tail, so no reader is ever handed an offset above a still-unfilled prefix hole. |
| `WalGcTrimCore.IsEntryEligible` | Whether one log entry may be trimmed, given the GC's min-acked cursor, the optional retention ceiling, the durable materialiser offset admission (which can also refuse an entry only the in-memory cursor would admit), the causal-stable frontier, and any buffer-pin blocked-floor - the exact per-entry predicate the GC scan applies. |
| `InMemoryWalCursorRegistry` (driven directly) | The per-consumer cursor max-merge and the `min(cursor)` GC floor scan - a consumer cursor never regresses under a stale re-delivery, and the floor is the minimum across consumers. |
| `WalMoveFenceCore` | Whether an append is admitted while a shard move has fenced the log (`!moveFenced`), and whether a stale quiesce observation must abort (`observed > expected`) - the fence check that must be atomic with the offset assignment. |
| `WalAdmissionGateCore.IsDispatchRefused` | Whether the commit-log writer refuses a new dispatch because it is draining for shutdown - the pre-admission gate paired with a drain that must release every parked caller. |
| `WalOffsetAllocationCore.Assign` | The per-shard log-offset handed to an append and the single-step advance of the offset counter - the read-and-advance that must be atomic so two concurrent appends never share an offset and the sequence stays dense. |
| `WalOffsetAllocationCore.RecoveredNextOffset` | The offset a recovering shard activation resumes from: one past the highest stored offset, so a recovered allocator never reissues an acknowledged offset. Activation, the post-failure resync and the test seam all recover through it. |
| `WalBlockedFloorCore.Meet` | The lowest buffer-pin HLC across consumers - the meet (minimum) each consumer's live pin is folded into, so the GC's blocked floor tracks the slowest buffering consumer and never trims an entry a live buffer still needs. |
| `WalMoveResumeCore` | Whether a move's target is a clean prefix of the source tail, and the offset a crashed-and-re-driven copy resumes just past - the resume arithmetic that makes an interrupted placement move copy each retained offset exactly once. |
| `LeafDurablePinCore` | The durable materialiser pin a leaf publishes for a partition: a release for a partition with nothing to lose, the never-written release (#3453), the Zero block pin for a prefix whose only durable copy is the WAL, or a trim entitlement of `min(persisted checkpoint, covered)` - never the pending checkpoint (#3476). `BPlusLeafGrain.ResolveDurablePinForPartition` gathers the inputs and maps the verdict. |
| `WalFallOffCore` | Whether the WAL has been trimmed past the first offset a replay resuming after a checkpoint still needs (`checkpoint >= 0 && tail > checkpoint + 1`; a durably recorded checkpoint of 0 is a real read position, issue #4433). Both the fall-off-log detector and the activation's cold-replay guard route through it, so the two cannot disagree. |

The core files live under `src/lattice/`, `src/lattice/BPlusTree/`, and
`src/lattice/BPlusTree/Grains/` next to the grains that call them.

## The Coyote concurrency tier

The WAL cores are model-checked with [Microsoft Coyote](https://github.com/microsoft/coyote)
using the same shared harness (`CoyoteModelHarness`) and the same explicit
cooperative step-ordering style (a model implements `ICoyoteModel` and advances
the steps itself; Coyote drives `runtime.RandomBoolean()` to explore the
resulting choice space, which is not a thread schedule space - the models run at
a concurrency degree of zero) described in the
[atomic-commit verification doc](verified-atomic-commit.md#the-coyote-concurrency-tier).
There is no `coyote rewrite` pass; the concurrency is encoded as data so it is
fully enumerable.

The WAL models live under `test/lattice/BPlusTree/Coyote/`:

| Model | Core(s) exercised | Property checked |
|-------|-------------------|------------------|
| `WalShippingWatermarkModel` | `WalShippingWatermark` | Under every explored order of out-of-order flush completions and reader polls, a reader that advances its cursor to an offset has every lower offset already persisted - no prefix hole is ever shipped. |
| `WalGcTrimFloorModel` | `WalGcTrimCore` | The GC trims only past the *minimum* acked cursor across all peers; flooring under the maximum strands a lagging consumer. |
| `WalCursorMonotonicityModel` | `InMemoryWalCursorRegistry` (real) | A consumer's cursor never regresses below its highest report; a stale re-delivery is max-merged away, not applied last-writer-wins. |
| `WalMoveQuiesceModel` | `WalMoveFenceCore` | The fence check and the offset assignment are atomic, so no append is assigned an offset once a shard move has raised the fence - every offset lands at or below the stable tail the move copies. |
| `WalCommitLogWriterDrainModel` | `WalAdmissionGateCore` | A shutdown drain releases every parked admission caller; observing the drain token in the wait set (rather than sampling it before parking) closes the lost-wakeup. |
| `WalOffsetContiguityModel` | `WalOffsetAllocationCore` | Reading and advancing the offset counter is atomic, so two concurrent appends never receive the same offset and the assigned sequence stays dense and strictly ascending. |
| `WalBlockedFloorLifecycleModel` | `WalBlockedFloorCore` | The GC's blocked floor is the minimum live buffer pin across consumers, so through every interleaving of pin-take, pin-raise, and pin-clear it never rises above a live pin and never trims an entry a buffering consumer still needs. |
| `WalMoveRedriveModel` | `WalMoveResumeCore` | A placement move's tail copy resumes just past what the target already holds, so a coordinator that crashes and re-drives at any offset boundary copies every retained offset exactly once with no duplicate and no gap. |
| `WalDurabilityLifecycleModel` | `WalOffsetAllocationCore`, `WalShippingWatermark`, `LeafDurablePinCore`, `WalGcTrimCore`, `WalFallOffCore` | End to end, with a leaf stopping at any step: appends, out-of-order flushes, read-position replay, checkpoint persists and snapshot captures that can fail, pin publication and the GC trim, with a variant in which one leaf owns nothing so the never-written release is exercised. No acknowledged write is lost or skipped, no leaf falls off the log, no trim entitlement exceeds snapshot coverage, a failed persist is rolled back, and no pin exceeds the persisted checkpoint. |
| `WalPartitionReleaseModel` | `LeafDurablePinCore`, `WalGcTrimCore` | One leaf whose writes span two partitions, with HLC stamps and a retention TTL: the empty release of a partition the leaf has applied nothing in, writes stamped below the leaf's clock (replication, carried copies), and every GC arm that admits by stamp or by age. No acknowledged write leaves durable state, under the replay barrier of #4669 and the override hold of #4641. |

### Every model ships a non-vacuous guard test

As in the atomic-commit tier, a model that checks a property only has value if
the property can actually fail. Every WAL model therefore ships a companion
**guard test** that removes exactly the one fix the property depends on and
asserts Coyote *finds* the resulting violation
(`AssertViolationFoundInSomeExploredRun`):

- `WalShippingWatermarkModel` - the guard clamps the reader at the raw
  next-offset tail, ignoring in-flight flushes, and Coyote finds the order in
  which a higher window persists first, the reader advances past the hole, and
  the still-in-flight lower offset is stranded.
- `WalGcTrimFloorModel` - the guard floors the trim at the *maximum* consumer
  cursor, and Coyote finds the schedule that strands a lagging consumer.
- `WalCursorMonotonicityModel` - the guard replaces the max-merge with a
  last-writer-wins assignment, and Coyote finds the stale re-delivery that
  regresses a consumer cursor.
- `WalMoveQuiesceModel` - the guard splits the atomic fence-check-and-assign into
  two steps, and Coyote finds the schedule where a quiesce fences between them.
- `WalCommitLogWriterDrainModel` - the guard samples the drain token before
  parking, and Coyote finds the lost-wakeup that leaves a caller parked after the
  drain.
- `WalOffsetContiguityModel` - the guard splits the atomic read-and-advance of the
  offset counter, and Coyote finds the schedule where two appends are handed the
  same offset.
- `WalBlockedFloorLifecycleModel` - the guard joins the floor at the *maximum*
  live buffer pin instead of the minimum, and Coyote finds the schedule where the
  floor rises above a lagging consumer's pin and the GC trims an entry it is still
  buffering.
- `WalMoveRedriveModel` - the guard resumes every re-drive from the source floor
  instead of past what the target already holds, and Coyote finds the crash point
  after which the copy re-appends an offset the target already has (a duplicate).
- `WalDurabilityLifecycleModel` - nine guards, each removing one fix and each
  required to be caught by the assertion that fix protects, not merely by some
  violation:
  - resolving the pin against the pending checkpoint (`[PublishedPinWithinPersistedBelief]`);
  - not rolling back a failed checkpoint persist (`[PersistedBeliefHonest]`);
  - reading past the watermark (`[ShippingNeverSkips]`);
  - flooring the trim at the highest pin (`[TrimCoveredBySnapshot]`);
  - releasing a never-written leaf's pin regardless of its snapshot coverage
    (`[ReleaseBackedBySnapshot]` at the publication; with that assertion off and
    one leaf owning nothing, `[RecoveryNeverFallsOffLog]` after the trim and the
    restart);
  - acknowledging an append before its flush lands (`[AckedWriteDurable]`);
  - a cold start resuming at the persisted checkpoint over an empty projection
    (`[ReadPositionHonest]`);
  - a replay that never reads past the persisted checkpoint
    (`[EveryAckedWriteMaterialised]`);
  - a read position that advances only over the leaf's own entries, as before
    #2270 (`[ReclamationEventuallyAdvances]`, with one leaf owning nothing).
- `WalPartitionReleaseModel` - seven guards, each caught by its one assertion,
  `[AckedWriteDurable]`: an empty release before the partition's replay (#4669),
  a retention ceiling that yields only to a Zero pin (#4622), and, for the override
  hold of #4641, skipping it, dropping it without a real offset, clearing it on a
  persisted checkpoint, reading the holds before the GC's head bound, and a trigger
  on a stamp below the clock alone, which misses a merge saturated at the HLC
  counter ceiling.

A model with a green fix test and a green guard test is proven load-bearing for the
assertions its guards name, and only for those. Every assertion of both
end-to-end models is the reporter of at least one guard: disabling any one of them
turns a guard red. The confirmation pass of issue #4433 found four lifecycle
assertions that were never the reporter; each now has its own guard.

### Running the tier

The Coyote tier is opt-in and held out of the fast development loop and the
deterministic CI step. Every model and guard test is tagged
`[Category("Coyote")]`.

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj -c Release --filter "Category=Coyote"
```

See the "Coyote concurrency tier" section of
[`.github/instructions/testing.instructions.md`](../../.github/instructions/testing.instructions.md)
for the tier policy and the procedure for adding a new model.

## The TLA+ specification, and the scope of the assurance

The cores above are each model-checked in isolation, and
`WalDurabilityLifecycleModel` composes five of them. Above both sits a design-level
TLA+ specification in [`spec/wal/`](../../spec/wal/README.md):

- `WalDurability.tla`, the leaf lifecycle under crash-anywhere recovery, checked
  with every property at one fault and, through a second configuration, with every
  safety property at two. It includes flushes abandoned at their deadline that land
  late or settle as holes (`LogPrefixApplied`, issue #4621). A third
  configuration lets the environment destroy a leaf's snapshot or its state row,
  and the operator purge a tree, and checks that the loss is never silent: a leaf
  whose evidence is gone fails closed, and only the writes a purge actually
  deleted are carved out of `ReadPositionHonest`;
- `WalMove.tla`, a shard move with a durable fence, a shard crash and the loss of
  its coordinator, and, through a second configuration, two moves contending for the
  same stream (one taking over the other's lapsed fence).

It follows the pattern of the atomic-commit specification: every property and every
action has a mutation that makes a property fire, and a refinement note maps each
construct to production and to tests proven to go red when production regresses.

The specification found four durability defects, each filed with a reproduction and
kept as a standing mutation until it is fixed. Three were reproduced against the real
`BPlusLeafGrain` before they were filed; #4467 was reproduced by the model's trace
only, and its fix's grain test came with the fix:

| Issue | Defect |
|-------|--------|
| #4450 | A snapshot that fails to load falls through to a cold replay of a trimmed WAL. Fixed: the replay now fails closed. |
| #4451 | A capture during a cold rebuild claims more coverage than its rows hold. Fixed: the claim stops at what has been re-read. |
| #4456 | A never-written leaf releases its block pin above its snapshot's coverage. Fixed: the release is bounded by coverage. |
| #4467 | A faulted cold rebuild re-arms warm over a partial projection. Fixed: the retry stays cold. |

All four are fixed, and each one's mutation is now an ordinary regression check.

The independent review of the specification found two more, each hidden by a bound
or an abstraction of the first version. Both are fixed:

| Issue | Defect |
|-------|--------|
| #4523 | A never-written leaf with no snapshot releases its block at its persisted checkpoint; after a cold-rebuild capture below that release and a trim, its next activation latches stale. It needs two faults. Fixed: the release fires only under durable coverage. |
| #4525 | A move's fence lives only in the source activation's memory and the flip re-checks nothing, so a source re-activated after the copy acknowledges writes the flip discards. Fixed: the fence is a durable record every new activation re-derives, and the flip requires it still held. |

Since then the models have found eight more, each reproduced before its fix:

| Issue | Defect |
|-------|--------|
| #4621 | A flush abandoned at its deadline lands after readers have passed its offset. Fixed: the watermark holds below every unsettled abandoned call, and a provider trim watermark tells a hole from a trim. |
| #4622 | A retention TTL trims a write made after an empty release. Fixed: the TTL ceiling is capped at the lowest uncovered frontier. |
| #4634 | A cold start over a vanished snapshot replays a trimmed WAL. Fixed: the leaf row records held coverage and the activation fails closed. |
| #4654 | A leaf whose state row vanishes comes up empty. Fixed: a rowless leaf serves data only under a create intent. |
| #4669 | A cold leaf releases a partition its replay has not read. Fixed: no empty release before the replay barrier latches. |
| #4641 | A write stamped below an empty release's frontier is trimmed by every stamp-based GC arm. Fixed: a durable override hold the GC reads as a block. |
| #4699 | A move's quiesce ignores an abandoned call a predecessor activation left. Fixed: the drain check reads the process-wide registry. |
| #4700 | A purge's shard-wide recovery flag re-creates empty a leaf the purge never reached, and strands cleared leaves when the reseed stops early. Fixed: each leaf carries its own purge marker, and recovery re-creates only a marked leaf, visiting every one. |

**What is covered:** one WAL partition shared by two leaves, with two faults per
behaviour for safety and one for liveness, and a variant with snapshot and row loss
and a purge; one move with one shard crash and one coordinator crash, and a variant
with two contending moves; and, in Coyote, a leaf spanning two partitions with HLC
stamps and a retention TTL (`WalPartitionReleaseModel`).

**What is not covered:**

- more than two partitions, or several partitions in TLA+;
- splits, resharding and saga state;
- interleavings inside a grain turn's awaits;
- replication consumers, beyond their effect on the trim floor;
- liveness under two faults, or any property under three.

Coverage of the leaf lifecycle does not imply coverage of any of those. Each module's
refinement note lists its gaps in full.

## Related

- [Verified Atomic-Commit Protocol](verified-atomic-commit.md) - the sibling
  verification effort whose proven-core pattern, harness, and tier policy this
  work reuses.
- [Verified WAL Durability sample](../../samples/VerifiedWalDurability/README.md) -
  a runnable demonstration of the cursor-monotonicity and trim-floor properties
  these models prove.
- [Chaos Tests](chaos-tests.md) - the end-to-end integration contract that
  exercises the same WAL guarantees against a live cluster under fault injection.
