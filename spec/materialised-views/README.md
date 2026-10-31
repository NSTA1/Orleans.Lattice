# Materialised view tailing

`MaterialisedViewTail` checks a materialised view tailing its source tree's
write-ahead log across the operations that change what the view reads from, or
what the WAL keeps for it. It composes the steady drain and its checkpoint, the
read position and cursor pin that hold WAL GC, the cursor-entitled trim and the
retention-TTL trim that can overtake a reader, fall-off detection, lag
eviction, a maintainer crash and replay, the shadow-generation rebuild and its
swap, reconcile after out-of-band drift, generation reclaim, decommission, and a
source identity swap followed by the view's rebind onto the new physical log.

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `MaterialisedViewTail` | 7 | 5 | 17 | 22 | 27 | 121,488 |
| `MaterialisedViewFence` | 4 | 2 | 9 | 9 | 13 | 194 |
| `MaterialisedViewHorizon` | 5 | 8 | 19 | 23 | 30 | 16,792 |
| `MaterialisedViewAggregation` | 9 | 2 | 12 | 18 | 21 | 24,072 |
| `MaterialisedViewRebind` | 8 | 4 | 17 | 20 | 27 | 21,086 |

The `ThreeWrites` variant (`MaterialisedViewTail.ThreeWrites.cfg`) raises the
source write budget to three, which lets a trim, a swap and a rebuild each fall
between writes on both copies; it reaches 1,937,776 distinct states.

## Instance and interactions

Two keys, two source writes, and two physical copies: `P1`, the copy the
logical tree names at creation, and `P2`, the copy an online resize, a
reshard's physical rebinding or a shadow-cutover restore swaps in. The swap is
one environment step that snapshots the old copy's state into the new copy and
moves the alias; the alias reservation, copy and fence ordering that make the
swap safe are `TreeLifecycle`'s, and this model takes them as given. Values are
per-key versions. A view-tree write is a plain overwrite (`ApplyAsync`), not a
merge by source HLC, so replay is not idempotent step by step: re-applying an
entry the view already reflects (a crash replay from the checkpoint, or a
rebuild catching up from its resume floor) can transiently serve an older value
for a key. The model tracks the prefix the served view already reflects (`hw`)
and checks that the window closes once the replay passes it (`ReplayConverges`)
and that no key regresses outside it (`ServedMonotone`).

The maintainer drains from its durable applied offset and checkpoints in a
separate step, so a crash between them replays the gap. The checkpoint
publishes the read position and reports the GC cursor pin, both keyed by the
**bound physical** copy. A restarted activation publishes its persisted bound
log at its persisted resume point before it drains, and answers for no other
log. GC trims either within that entitlement, or by the retention TTL, which
may overtake the view; the drain then detects fall-off and rebuilds from source
state. Lag past the budget evicts and rebuilds. Drift writes the view tree out
of band; reconcile detects it by digest and rebuilds.

The rebuild captures a resume floor, scans the source into a new generation,
and swaps the generation in only once complete, so readers never see an empty
or partial view. The superseded generation is reclaimed afterwards. A swap of
the source's identity is detected on the next drain or by notification; the
maintainer rebinds (by rebuilding against the new physical copy), moves its pin
and read position, and releases the old copy's. Decommission withdraws the read registration,
releases every pin under both the logical and the bound physical id, and
deletes every generation, including the physical shadow tree a rebuild that
was interrupted after it began scanning may have left behind (`shadow`).

Decommission is modelled as waiting for an idle maintainer, and that is the
production behaviour rather than a scope cut. `ViewMaintainerGrain` is not
`[Reentrant]`. Its two `[AlwaysInterleave]` methods mutate nothing a rebuild
reads: `GetDurableReadPositionsAsync` reads an immutable published snapshot, and
`NotifySourceIdentityChangedAsync` only records that the source binding is
stale for the next drain to re-resolve. Its drain timer does not interleave. A `DecommissionAsync`
call therefore queues behind an in-progress rebuild turn and runs after it. The
one way a rebuild can stop part-way is for the activation to fail or crash,
which is the model's `Crash` with `shadow` still set; the next activation's
decommission must then delete that partial shadow, which is what
`DecommissionReleases` requires.

Environment steps (writes, the swap, both trims, drift, the crash, and
decommission) are not fair, and their budgets are finite. Weak fairness
applies to every maintainer step.

## Focused replay and horizon companions

`MaterialisedViewFence` checks the lifetime of an already-persisted fence through
bounded replay, peer readiness, supersession, repeated activation and physical
rebind. Its fixed adversarial prefix has TWO two-key batches: the first waits
on a peer and the second flushes, then ordinary writes with LOWER HLCs occur
at HIGHER WAL offsets. Resolving the first batch can empty staging before the
old head is consumed. `UnpassedFenceCleared` restores the #4881 defect and must
violate `QuiescentExact`, not just a fence bookkeeping assertion. Its
`NewerOrdinary` variant changes the ordinary HLC from 10 to 30, raises the
crash budget from one to two and narrows the window from three to two; it
reaches 359 distinct states. The prefix compresses the first complete pass;
it does not replace the general staging model. See
[RefinementFence.md](RefinementFence.md) for the mapping and honest limits.

`MaterialisedViewHorizon` checks two source keys and two source shards spread
over THREE physical WAL partitions. Prepares are on p0/p2 and terminals on
p0/p1, so reading both terminals does not imply consuming the prepare on p2.
All prepares precede all terminals, but source installation and each shard's
terminal append are separate actions. `installed` means the corresponding
prepared source row is installed, not that a WAL terminal was appended. For the
visibility seam, the model allows the first shard terminal before source-decision
confirmation but gates the final distinct terminal on confirmation; it does not
validate the production producer's stronger decision-before-any-terminal order.
A regular orphan can recover cardinality
from one actual retained prepare carrying a positive batch-size witness even if
the other prepare payload is trimmed; it still needs every distinct terminal
partition and consumption through the post-terminal horizon. The bounded
retained-history cursor resumes across per-drain examination budgets, including
when terminal evidence is already below a checkpoint. Fresh terminal-only
history cannot infer cardinality, and a retention gap that removes every
prepare witness is explicitly outside the convergence claim. Abandoned batches
follow a separate commit-terminal-triggered rebuild path without the regular
orphan history entitlement. Rebuild visibility is gated by the transaction
decision, not by individual row installation or shard-terminal arrival; this is
a two-key visibility seam, not a whole-tree snapshot guarantee. The `FullScan`
variant visits every partition while allowing two crashes; it reaches 29,932
distinct states. The base reaches 16,792. See
[RefinementHorizon.md](RefinementHorizon.md).

These are exhaustive finite checks, not simulations and not an exhaustive
Cartesian product of every feature. On the bounded two-CPU Java 17 toolchain,
standalone Horizon TLC runs took 7.180 seconds for the base and 8.645 seconds
for the FullScan variant, including JVM startup, using one worker and a 2 GiB
maximum heap. Each mutant has a paired clean single-property control, and every
behavioural action has an action-targeted mutation. State counts are in the
counts table and manifests. Actual harness controls contain mutation-specific
headers; same-target grouping does not eliminate their paired clean runs.

The final serial no-build `TlcModelCheckTests` run executed 25 Horizon cases:
the base, FullScan variant, and all 23 mutation/control cases. It passed 25/25
with zero skipped, inconclusive, or not-executed tests in 196.4 seconds (3
minutes 16.4 seconds), using one TLC JVM slot, two visible CPUs, and a 2 GiB
maximum heap. A later isolated run passes the same 25 cases in 113.521 seconds;
the planner retains the earlier conservative measured weight. Named actual
caller controls now establish the bounded production mappings, including
cardinality, distinct terminals, consumed horizons and live rebind retirement.
These are not machine-checked implementation refinements or whole-tree scans.

## Scope and companion models

This is the view's **tail and lifecycle** composition. The following are
deliberately outside it, and no guarantee about them is claimed here:

- General atomic and cross-tree staging. The focused fence companion covers
  the persisted replay/supersession lifetime and the horizon companion covers
  multi-partition hold-back, rebuild and abandonment; neither claims the full
  coordinator, aggregation or projection interaction space.
- Replication: shipping views between regions, the in-place rebuild a
  replicated view performs, and anti-entropy, which is the anti-entropy area's
  scope.
- `TreeLifecycle` internals: the reservation, copy, fence and undo phases.
- History retention shaping and aggregation folds. The aggregation drain keys
  its cursor exactly like the steady drain; that shared keying is the part
  modelled.
- Reader handle grace on a reclaimed generation, and the per-partition WAL,
  which is abstracted to one log per physical copy.
- Reconcile is modelled as rebuilding only on a real digest mismatch.

A clean run does not imply the Cartesian product of every view feature with
every topology operation was checked; the instance is the one stated above.

## Defects found

Writing this model found the following production defects. Each was fixed in a
separate change with a regression test, and the test is cited as the detector
in [RefinementTail.md](RefinementTail.md).

- #4853 - the view's WAL cursor pin and read position were keyed by the
  logical tree id rather than the bound physical copy, so after a resize,
  reshard or shadow-cutover restore GC did not see the view's floor on the log
  it actually read (`Checkpoint`, `PinOnlyOnBinding`, `PinConverges`), and
  decommission released only the logical pin (`DecommissionReleases`).
- #4856 - decommission left behind the physical shadow generation of a rebuild
  that was interrupted after it began scanning (`DecommissionReleases`, the
  `shadow` variable).
- #4857 - source back-pressure was read under the logical id although WAL
  saturation is sampled under the physical id, so after a resize, undo,
  restore or revert the maintainer missed saturation and drained at full rate.
  This is a rate-control seam with no correctness state in this model; it is
  listed because it came out of the same binding review.

## Mutation controls

Every checked invariant and temporal property has a mutation, and every
behavioural action is perturbed by at least one. `TlcModelCheckTests` builds
mutants from the current source, checks a clean control under the same
single-property cfg, and requires the mutant to fail on that **named
property**, not merely any error. Progress mutations keep fairness and strand a
real step.

See [RefinementTail.md](RefinementTail.md) for the production seams and their
detectors.

## Focused aggregation and physical-rebind companions

`MaterialisedViewAggregation` checks numeric membership and count/sum slots at
every intermediate state using retract/add arithmetic, including regrouping,
retraction, duplicate replay, a crash after internal commit, rebuild overlap and
ordinary-versus-prepared replacement. HLC stamps and WAL order are independent.
Its revision-set composition deliberately distinguishes accumulative history:
superseded Set/Delete revisions are retained within a generation rather than
source-latest filtered. Explicit rebuild may collapse history to its scan seed;
it does not invent revisions no longer recoverable from source or retained WAL.
The `Delete` variant retracts the ordinary member and disables crash injection;
its exhaustive state census is 7,176, distinct from the base.

Source confirmation is not itself an atomicity proof. The aggregation companion
also gates a batch omitting a superseded member until its replacement is already
visible, and checks that pre-flush observation with a separate invariant.
Public group materialisation is a separate step for ordinary writes: the
internal membership/slot invariant is not a claim that every ordinary public
group row changes atomically with its internal rows. See
[RefinementAggregation.md](RefinementAggregation.md) for exact contracts and
production remediation evidence.

`MaterialisedViewRebind` captures a physical source/floor tuple, holds it while
scanning, rejects a stale source candidate, and publishes the generation,
physical binding and resume point together while discarding retired staging.
It composes source prepare decisions, physically addressed WAL writes, cursor
GC, concurrent alias moves (including after validation but before publication),
interrupted scans and crash recovery. Its action
property checks each intermediate cutover, not merely eventual self-heal.
`NoCrash` isolates the same interactions without activation failure and reaches
8,143 distinct states. See [RefinementRebind.md](RefinementRebind.md).

Both companions are bounded intended protocols. Aggregation production refinement
remains pending. Rebind now has accepted captured-tuple, restart, actual GC,
physical staging and moving-log resume detectors, with anchored negative controls;
controlled grain transport does not establish cluster or registry durability.
Neither companion contains a resolved transaction cache. The separate
[resolved-identity companion](../view-resolved-identity/README.md) checks #4895
through two logs reusing one ID, actual tail ownership, same-log rebuild and
uncertain publication responses. Its queue-only production discriminator reaches
the real cache capacity, without claiming a full eviction-algorithm proof.
[ReadyClosure and SourceAdmission](../view-staging/README.md) separately check
independent-clock replacement closure and coherent source admission; their
production refinement remains pending. A clean finite model never substitutes
for discriminating production evidence.

### Measured independent shard weights

Each command selected one module through `TlcModelCheckTests` with `--no-build`,
`LATTICE_TLC_CONCURRENCY=1`, two visible CPUs and an 1800 MB JVM heap. The
measurements are whole-command wall times, not sums of individual test durations
(which include waiting for the TLC slot and shared control runs).

| Module | Harness cases | Whole-command seconds | Exhaustive state census |
|--------|---------------|-----------------------|-------------------------|
| `MaterialisedViewAggregation` | 20 (18 paired mutations, base, Delete) | 95.42 | 24,072 / 7,176 |
| `MaterialisedViewRebind` | 22 (20 paired mutations, base, NoCrash) | 117.55 | 21,086 / 8,143 |

These are separate shard groups, not one directory-wide materialised-view group.
The repository CI routing and final matrix balance are integrated separately.
