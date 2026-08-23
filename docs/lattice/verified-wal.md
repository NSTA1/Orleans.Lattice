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
interleaving, so a property proven of the core is a property of production. The
cores are `internal` and exposed to the test assembly through
`InternalsVisibleTo`.

### The extracted WAL cores

| Core | Decision it owns |
|------|------------------|
| `WalShippingWatermark` | The highest contiguous offset a shipper may advance its shipped-watermark to, given the acked set - never past a gap, so a reader behind the watermark never misses an entry. |
| `WalGcTrimCore.IsEntryEligible` | Whether one log entry may be trimmed, given the GC's min-acked cursor, the optional retention ceiling, the causal-stable frontier, and any buffer-pin blocked-floor - the exact per-entry predicate the GC scan applies. |
| `InMemoryWalCursorRegistry` (driven directly) | The per-consumer cursor max-merge and the `min(cursor)` GC floor scan - a consumer cursor never regresses under a stale re-delivery, and the floor is the minimum across consumers. |
| `WalMoveFenceCore` | Whether an append is admitted while a shard move has fenced the log (`!moveFenced`), and whether a stale quiesce observation must abort (`observed > expected`) - the fence check that must be atomic with the offset assignment. |
| `WalAdmissionGateCore.IsDispatchRefused` | Whether the commit-log writer refuses a new dispatch because it is draining for shutdown - the pre-admission gate paired with a drain that must release every parked caller. |
| `WalOffsetAllocationCore.Assign` | The per-shard log-offset handed to an append and the single-step advance of the offset counter - the read-and-advance that must be atomic so two concurrent appends never share an offset and the sequence stays dense. |
| `WalBlockedFloorCore.Meet` | The lowest buffer-pin HLC across consumers - the meet (minimum) each consumer's live pin is folded into, so the GC's blocked floor tracks the slowest buffering consumer and never trims an entry a live buffer still needs. |
| `WalMoveResumeCore` | Whether a move's target is a clean prefix of the source tail, and the offset a crashed-and-re-driven copy resumes just past - the resume arithmetic that makes an interrupted placement move copy each retained offset exactly once. |

The core files live under `src/lattice/` and `src/lattice/BPlusTree/` next to the
grains that call them.

## The Coyote concurrency tier

The WAL cores are model-checked with [Microsoft Coyote](https://github.com/microsoft/coyote)
using the same shared harness (`CoyoteModelHarness`) and the same explicit
cooperative-interleaving style (a model implements `ICoyoteModel` and yields
decision points; Coyote drives `runtime.RandomBoolean()` to explore the schedule
space) described in the
[atomic-commit verification doc](verified-atomic-commit.md#the-coyote-concurrency-tier).
There is no `coyote rewrite` pass; the concurrency is encoded as data so it is
fully enumerable.

The WAL models live under `test/lattice/BPlusTree/Coyote/`:

| Model | Core(s) exercised | Property checked |
|-------|-------------------|------------------|
| `WalShippingWatermarkModel` | `WalShippingWatermark` | The shipped watermark never advances past a gap in the acked set, so a lagging reader never skips an unshipped entry. |
| `WalGcTrimFloorModel` | `WalGcTrimCore` | The GC trims only past the *minimum* acked cursor across all peers; flooring under the maximum strands a lagging consumer. |
| `WalCursorMonotonicityModel` | `InMemoryWalCursorRegistry` (real) | A consumer's cursor never regresses below its highest report; a stale re-delivery is max-merged away, not applied last-writer-wins. |
| `WalMoveQuiesceModel` | `WalMoveFenceCore` | The fence check and the offset assignment are atomic, so a shard move that quiesces the log can never fence an append that has already taken an offset. |
| `WalCommitLogWriterDrainModel` | `WalAdmissionGateCore` | A shutdown drain releases every parked admission caller; observing the drain token in the wait set (rather than sampling it before parking) closes the lost-wakeup. |
| `WalOffsetContiguityModel` | `WalOffsetAllocationCore` | Reading and advancing the offset counter is atomic, so two concurrent appends never receive the same offset and the assigned sequence stays dense and strictly ascending. |
| `WalBlockedFloorLifecycleModel` | `WalBlockedFloorCore` | The GC's blocked floor is the minimum live buffer pin across consumers, so through every interleaving of pin-take, pin-raise, and pin-clear it never rises above a live pin and never trims an entry a buffering consumer still needs. |
| `WalMoveRedriveModel` | `WalMoveResumeCore` | A placement move's tail copy resumes just past what the target already holds, so a coordinator that crashes and re-drives at any offset boundary copies every retained offset exactly once with no duplicate and no gap. |

### Every model ships a non-vacuous guard test

As in the atomic-commit tier, a model that checks a property only has value if
the property can actually fail. Every WAL model therefore ships a companion
**guard test** that removes exactly the one fix the property depends on and
asserts Coyote *finds* the resulting violation
(`AssertInterleavingViolationFound`):

- `WalShippingWatermarkModel` - the guard advances the watermark to the highest
  acked offset ignoring gaps, and Coyote finds the schedule where a reader skips
  an unshipped entry.
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

A model with a green fix test and a green guard test is proven load-bearing.

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

## Related

- [Verified Atomic-Commit Protocol](verified-atomic-commit.md) - the sibling
  verification effort whose proven-core pattern, harness, and tier policy this
  work reuses.
- [Verified WAL Durability sample](../../samples/VerifiedWalDurability/README.md) -
  a runnable demonstration of the cursor-monotonicity and trim-floor properties
  these models prove.
- [Chaos Tests](chaos-tests.md) - the end-to-end integration contract that
  exercises the same WAL guarantees against a live cluster under fault injection.
