# Live-advance module interface

The contract between the five modules that replace the design model
`design/WalLiveAdvance.tla` (epic #5605, member 6). Each module checks its
guarantees as invariants over its own state and, in its refinement note's
`## Interface` section, maps them onto the vocabulary below. A module that assumes
another module's guarantee models it as an environment constraint in exactly the
form stated here. It never imports that module's state and never re-derives the
guarantee. The design authority remains [`design/LiveAdvanceDesign.md`](design/LiveAdvanceDesign.md).

## The graph

| Module | Name | Assumes | Guarantees |
|--------|------|---------|------------|
| E | `WalAppendEpoch` | nothing | `StaleEpochNeverAppends`, `ClaimHeadCoversPriorAppends`, `RaisedEpochUnique`, `OnlySupersededRefused`, and the entry check |
| H | `WalHandOffFence` | E's entry check | `HandOffBound`, `AbsentMarkerHolds`, `CloseHeadMonotone` |
| R | `WalRoutingFloor` | E's entry check | `StraddlerBelowInit`, `LiveGenerationAboveFloor` |
| B | `WalBulkSwitch` | nothing | `BulkHintBound`, `ContestedKeyAgrees` |
| C | `WalLiveAdvanceCore` | every guarantee above | `ReadPositionHonest`, `SnapshotHonest`, `NoTrimPastAcked`, and liveness |

The edges are E <- H, E <- R, and {E, H, R, B} <- C. No other edge exists, so
the graph is acyclic. A module that finds it needs a further assumption raises it
with the design owner before adding it. Adding an edge that closes a cycle is a
design change, not a modelling choice.

## Vocabulary

- **L** is the owner leaf whose live advance is in question, and **a** an
  activation of it.
- **The partition** has an allocator `next`, and each allocated offset `o` has an
  author of one of four kinds:
  - `Own(a)`: an append by an activation of L, stamped with its epoch.
  - `Foreign(i)`: a stamped append of a key in L's range by another author `i`: a
    hand-off initiator (a donor leaf, a shard root, the range-delete initiator, or
    the bulk switch's shard root), under `i`'s epoch or routing generation.
  - `Straddler(i)`: a stamped append by an initiator `i` of a purged incarnation of
    the tree.
  - `Unstamped`: an append admitted with no stamp. That is a 10.2.0 silo, a closed
    gate view, the coordinator-terminal class, or an unreachable leaf (#5709). It
    is always reported.
- **An initiator's entry and floor** at the partition are `entry(i)` and
  `floor(i)`. For a leaf epoch the floor is the partition's shared retired floor;
  for a routing generation it is the initiator's own routing floor.
  - **An append** stamped `s` is admitted if and only if `entry(i)` is loaded and
    `s >= entry(i)` (`IsEpochAdmitted`, `IsGenerationAdmitted`). The floor is
    **never** checked on the append path: checking it there refuses a live
    activation another leaf's retirement overtook (#5635), and would not refine
    production.
  - **A claim** of `s` is written and raised only above `floor(i)`, and above
    `entry(i)` or equal to it as a re-presentation (`DecideClaimWrite`,
    `IsRaiseAdmitted`).
  - **Retirement** raises `floor(i)` and unloads `entry(i)` before deleting it, so
    a retired stamp's next append is refused as unloaded, and the re-claim it then
    makes meets the floor.
- **L's marker** has the states `initialised`, `open`, and `closedAt`: per
  partition, the **maximum** of the heads every close recorded on it, the
  re-created incarnation's first initialisation included, since it records its
  heads as a close. It is a maximum, not the last close's heads: production
  max-merges every retained close with the compacted heads of evicted ones
  (`HandOffMarkerCore.RequiredHead`). A purge discards the marker, so `closedAt`
  starts again from nothing in the re-created incarnation.
- **Per activation**, `claimHead[a]` is the head a's claim returned, and
  `seenU[a]` is the highest unstamped offset reported to `a`.

## Guarantees

Each guarantee is stated in the vocabulary. The owning module checks it as an
invariant (or, for the entry check, by the action guards its mutations break, and,
for `CloseHeadMonotone`, as an action property).

**E, `WalAppendEpoch`**
- `StaleEpochNeverAppends`: an `Own(a)` offset is allocated only while `a`'s epoch
  is the highest raised epoch of L and above every epoch L's clear retired.
- `ClaimHeadCoversPriorAppends`: when `a`'s claim is raised, every offset already
  allocated lies below `claimHead[a]`.
- `RaisedEpochUnique`: no two activations that can still append hold the same
  raised epoch.
- `OnlySupersededRefused`: only a superseded activation is ever refused.
- **The entry check**, which H and R assume: the first two guarantees,
  generalised to any initiator `i`. An append stamped below `entry(i)`, or with
  `entry(i)` unloaded, is never allocated; a claim at or below `floor(i)` is never
  raised; and a re-claim raises `entry(i)` in the same step that reads the
  allocator head it returns, so every allocation before the re-claim lies below
  that head. `WalAppendEpoch` checks it for leaf epochs; its note gives the
  refinement argument for routing generations, which run the same claim code.

**H, `WalHandOffFence`**
- `HandOffBound`: a `Foreign(i)` offset is allocated only while L's marker for the
  window is open, or below the `closedAt` the window's close recorded. Equivalently,
  after a close with heads `h`, no write admitted before the close is allocated at
  or above `h`, whatever stamp, kind (point, batch, range-delete walk) or path
  (stamp at admission, admission mark, a claim reset by a restart) it took.
- `AbsentMarkerHolds`: L's marker reads closed only once it is initialised. An
  absent or uninitialised marker holds L.
- `CloseHeadMonotone`: within an incarnation of L's marker, `closedAt` never
  decreases. A close whose own heads are lower, such as a window opened before the
  re-created incarnation's first initialisation and closed after it, leaves the
  heads an earlier close or the initialisation recorded in force. H checks it as
  an action property.

**R, `WalRoutingFloor`**
- `StraddlerBelowInit`: a `Straddler(i)` offset is allocated only below the
  `closedAt` that the re-created incarnation's first initialisation recorded, or
  not at all.
- `LiveGenerationAboveFloor`: every live initiator's current generation is above
  its own floor.

**B, `WalBulkSwitch`**
- `BulkHintBound`: a bulk-born leaf's birth read position is at or below every
  offset of a record in its range that it does not hold at birth. Rows relocated
  into it at birth are held.
- `ContestedKeyAgrees`: the owner resolves a contested key to the value every WAL
  reader resolves it to: last writer wins by version.

## What C assumes

C models the other modules only as environment constraints:
- **Own appends** satisfy E's three guarantees.
- **Foreign appends** satisfy `HandOffBound` and `AbsentMarkerHolds`, and every
  close and the first initialisation max-merge their heads into `closedAt`
  (`CloseHeadMonotone`).
- **Straddler appends** satisfy `StraddlerBelowInit`.
- **A bulk-born leaf's birth** satisfies `BulkHintBound`.
- **Unstamped appends** are always reported, and raise every reader's `seenU` before
  it arms. This is the stamp census's obligation, and the core's own unstamped
  watermark and self-report model it.

C then checks `ReadPositionHonest`, `SnapshotHonest` and `NoTrimPastAcked` over
`Armed(a)` and the live advance, and member 6b adds the armed-newborn liveness
property.

## Where each design mutation goes

Each design mutation goes to the module that owns the guard it removes. It is
retargeted at that module's property, and its anchors are rewritten:

| Module | Design mutations |
|--------|------------------|
| E | `StaleEpochNeverAppends*`, `OnlySupersededRefused*`, `NoTrimPastAckedNoEpochCheck`, `NoTrimPastAckedLazyFence`, `ReadPositionHonestClaimReadableHead`, `ReadPositionHonestClaimAdoptsNoCas`, `ReadPositionHonestFloorFromEntryOnly`, `ReadPositionHonestRecreateDropsFloor` |
| H | `ReadPositionHonestNoOpenCheck`, `ReadPositionHonestRearmWithoutReplay`, `ReadPositionHonestCloseHeadUnderstated`, `ReadPositionHonestDrainWithoutFence`, `ReadPositionHonestDrainPointOnly`, `ReadPositionHonestStampAtAppend`, `ReadPositionHonestGenerationNotDurable`, `ReadPositionHonestRangeDeletePerShardClose`, `ReadPositionHonestRangeDeleteWithoutIntent`, `ReadPositionHonestRestampDuringClose`, `ReadPositionHonestRestampUnderAnyIntent`, `ReadPositionHonestResumeWithoutMarker` |
| R | `ReadPositionHonestRouting*`, `ReadPositionHonestStraddlerRaiseRecreatesMarker`, `LiveGenerationAboveFloor*` |
| B | `ReadPositionHonestBulkWithoutHint`, `ReadPositionHonestBulkAppendDropsExistingRows`, `ContestedKeyAgreesStrandedValueKept` |
| C | `NoTrimPastAckedLiveAdvance5578`, `ReadPositionHonestNoClaimHeadReplay`, `SnapshotHonestNoOwnTracker`, `ReadPositionHonestNoSelfReport`, `ReadPositionHonestUnstampedNotReported`, `ReadPositionHonestRestartForgetsUnstamped`, `OldNeverRejectedRejectedOnceFenced` |

A mutation that removes a guarantee goes red in the module that owns the
guarantee. A mutation that removes the matching assumption goes red in C. The
design model's redundancy checks go to the module that owns the guard: they are
kept as documented redundancy, never as mutation files.

## Rules for modellers

- Use the property names above exactly. They are the interface.
- A module keeps its base configuration at or under about 1M distinct states, and
  every CI case (the base, each variant, each mutation's clean control and mutant)
  at or under about 5M distinct states and 20 minutes.
- `WalAppendEpoch` and its `AppendEpochRefinement.md` are the worked template.
- **The composite check** (member 6c) is one tiny configuration composing the five
  modules at minimal constants, well under 1M states, run in CI. It catches a
  guarantee that does not meet the assumption that relies on it.
