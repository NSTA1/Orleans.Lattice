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
| `MaterialisedViewHorizon` | 4 | 5 | 15 | 16 | 22 | 20,794 |

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
`[Reentrant]`; its only `[AlwaysInterleave]` method,
`GetDurableReadPositionsAsync`, reads an immutable published snapshot and
mutates nothing, and its drain timer does not interleave. A `DecommissionAsync`
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
terminal append are separate actions. A rebuild's bounded planning scan may
miss a prepare; a post-terminal all-partition horizon distinguishes genuinely
missing prepares from a partition not yet read. Backstop abandonment persists
across crashes and rebuilds and is retired only after every distinct shard
terminal lies below the captured heads, never after just one commit. The
`FullScan` variant visits every partition while allowing two crashes; it
reaches 33,456 distinct states. See [RefinementHorizon.md](RefinementHorizon.md).

These are exhaustive finite checks, not simulations and not an exhaustive
Cartesian product of every feature. On the bounded two-CPU Java 17 toolchain,
the raw fence base/variant take about one to two seconds each and the horizon
base/variant about four to eight seconds each, including JVM startup. Each mutant
has a paired clean single-property control, and every behavioural action has
an action-targeted mutation. State counts are in the counts table and manifests;
CI must retain whole target groups where controls can be shared.

The actual `TlcModelCheckTests` runs pass 11 fence cases and 18 horizon cases
(base, variant and every paired mutant), with zero skipped. A final serial run
with one JVM slot and two visible CPUs takes 41.083/114.483 seconds from first
case start to last case end, and 48.434/124.659 seconds for the corresponding
no-build commands on a shared host. Heap is capped at 2 GiB; peak usage was not
measured. The production detector mappings are explicitly pending acceptance,
not certified refinements; a clean finite model is not that acceptance.

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
