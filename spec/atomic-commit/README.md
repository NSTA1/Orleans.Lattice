# TLA+ specification of the Orleans.Lattice atomic-commit protocol

This directory holds a TLA+ specification of the distributed atomic-commit
protocol - the multi-leaf prepare / commit / abort saga, the per-tree
transaction-registry decision, and reader visibility - together with a TLC
model configuration that checks its safety and liveness properties
exhaustively over a small bounded instance.

It is the deliverable of level-C epic #1588, Phase 7 (#1596), lever (c): a
design-level specification above the code, checked by TLC, with a refinement
note ([`Refinement.md`](Refinement.md)) mapping it to the extracted Coyote
protocol cores.

It is one of the modules indexed in [`spec/README.md`](../README.md). The layout every
module follows, how to run TLC, and why TLC runs in CI are described there;
this README covers only what is particular to the atomic-commit module.

The directory holds a second module, `AtomicCommitCrossCluster`, which instances
this one for the origin cluster and specifies the replicated half: what a peer
cluster receiving the saga does to keep it all-or-nothing visible. It is
described under [The cross-cluster module](#the-cross-cluster-module). Every
property `AtomicCommit` checks is a claim about one cluster; none of them covers
the receiver, and the cross-cluster module's properties cover nothing else.

## Files

| File | What it is |
|------|-----------|
| [`AtomicCommit.tla`](AtomicCommit.tla) | The specification: state, actions, safety invariants, liveness properties. |
| [`AtomicCommit.cfg`](AtomicCommit.cfg) | The TLC model: the bounded instance and the invariant / property list to check. |
| [`mutations/`](mutations/) | One deliberate defect per checked property, each of which must make that property fire. See [`mutations/README.md`](mutations/README.md). |
| [`Refinement.md`](Refinement.md) | The refinement note: each spec variable, action and checked property mapped to its protocol counterpart in the code cores, or excluded with a reason. |
| [`AtomicCommit.manifest.json`](AtomicCommit.manifest.json) | The module manifest: where the mutations and refinement note live, which actions are non-behavioural, and the counts the gates assert (see [Counts](#counts)). |
| [`AtomicCommitCrossCluster.tla`](AtomicCommitCrossCluster.tla) | The cross-cluster module: the origin saga (this module, instanced) replicated to a receiver. |
| [`AtomicCommitCrossCluster.cfg`](AtomicCommitCrossCluster.cfg) | Its TLC model: every property, with no loss path and a receiver that follows the stream. |
| `AtomicCommitCrossCluster.<Variant>.cfg` | Its variant configurations: a receiver joining through a bootstrap, and each loss path, checked with every property on slices of the instance that the cross-cluster refinement note describes. The manifest's `variants` lists them. |
| [`mutations-cross-cluster/`](mutations-cross-cluster/) | Its mutation catalogue. See [`mutations-cross-cluster/README.md`](mutations-cross-cluster/README.md). |
| [`RefinementCrossCluster.md`](RefinementCrossCluster.md) | Its refinement note, mapping it to the replication apply seam, the receiver registry and the cross-tree receiver barrier. |
| [`AtomicCommitCrossCluster.manifest.json`](AtomicCommitCrossCluster.manifest.json) | Its manifest. |
| `README.md` | This file. |

## What is modelled

The specification is deliberately abstract: keys, participant leaves, and a
transaction status. There is no serialization, no timers, no HLC, no WAL - the
issue scopes those out. The abstraction is chosen so TLC can enumerate every
interleaving of the protocol's decision and broadcast steps.

- **Coordinator** (`PrepareTx`, `DecideTx`, `BroadcastStep`) - the saga:
  prepare fan-out into hidden per-leaf pending buckets, a single terminal
  decision, then the per-leaf terminal broadcast one leaf at a time.
- **Transaction registry** (`decision`, `forgotten`, `revision`) - the single
  tree-wide commit / abort decision, whether its row has since been retired,
  and the monotonic revision. Recording the decision *before* the broadcast is
  the linearization point. `ForgetDecision` models the saga's post-fan-out
  cleanup: it retires the row (so `RegistryView` reverts to in-flight) without
  changing the outcome, and only once every participant has drained.
  `RegistryMask` models the registry declining to report the saga's outcome
  (`masked`, production's `Indeterminate`: an aged-out row it still stores, or
  a delegated cross-tree txid whose coordinator cannot be dialled), with no
  ordering against the participants or even the decision.
- **Reader visibility** (`Observed`, `SurfaceViaGate`) - the per-key gate that
  resolves how a read of a key carrying a pending mutation is answered, resolved
  against one registry view so a saga is all-or-nothing visible. A read has
  three outcomes, not two: the post-saga value, the pre-saga value, or
  `"hidden"` when the registry has declined to report the decision - the gate's
  `Indeterminate` arm, which asserts nothing about the saga and so is never
  treated as a pre-saga read.
- **Reshard / migration** (`ShadowForwardOrphan`, `OrphanDrain`) - an abstract
  online shard-split step that shadow-forwards a stale prepared write onto a
  leaf that already applied the saga's terminal, and the leaf's discard of
  that late bucket. Until it is discarded, the gate's orphan guard
  (`AlreadyTerminal`, which `SurfaceViaGate` reads) makes the bucket fall
  through instead of shadowing the authoritative value (the #1584 class at
  design level).

## Properties checked

Safety invariants (checked at every reachable state):

| Invariant | Meaning |
|-----------|---------|
| `TypeOK` | State stays well-typed. |
| `AllOrNothing` | Atomicity: within one saga a snapshot reader never sees one key post-saga and another pre-saga - never a split view. A hidden key is compatible with either. |
| `VisibilityMatchesDecision` | A key is post-saga-visible only when the tree-wide decision is committed, and pre-saga-visible only when it is not (sharpest safety statement; implies `AllOrNothing` and `StrictIsolation`). |
| `StrictIsolation` | An in-flight or aborted saga is never surfaced as committed. |
| `CommitIntegrity` | Commit implies every participant acked; abort implies at least one nack. |
| `LinearizedTerminals` | No leaf applies a commit / abort terminal before the registry recorded that decision (decision-before-broadcast). |
| `NoMixedTerminals` | A saga never applies commit on one leaf and abort on another. |

Action and temporal properties (`DecisionDurability` and `RevisionMonotonic` are
single-step action properties; `MonotonicVisibility` is a safety property stated
over whole behaviours; the last three are liveness properties):

| Property | Meaning |
|----------|---------|
| `DecisionDurability` | Once terminal, the registry decision never flips to the other terminal, and its row is never retired while a written key has not yet applied its terminal (its prepared bucket is still undrained). |
| `MonotonicVisibility` | Once a key has been observed post-saga it is never observed pre-saga at any later state (even across a reshard, or while the registry declines to report the decision). Going hidden is not a reversion, but it cannot launder one: post, then hidden, then pre is a violation, which is why the property is stated over the behaviour rather than over one step. |
| `RevisionMonotonic` | The registry revision counter never decreases. |
| `Termination` | Every saga terminates (under weak fairness of saga progress). Fails on a protocol defect under that fairness, not only without it. |
| `EveryCommittedKeyReadable` | Every committed saga's keys are eventually all materialised at their post-saga value on their own leaf, and served post-saga from then on, whatever the registry reports and whatever late orphan bucket lands (issue #4428). Not entailed by any invariant; for a committed saga it coincides with `NoStrandedPrepare` (see its comment in the spec). |
| `NoStrandedPrepare` | Every participant of a decided saga eventually applies the saga's terminal. The only one of the three liveness properties that sees an aborted saga's stranded bucket. |

All three liveness properties fail on protocol defects under the fairness the
spec asserts. `Termination` and `NoStrandedPrepare` each have one the other two
miss; `EveryCommittedKeyReadable`'s is also caught by `NoStrandedPrepare`, with
which it coincides for a committed saga. The paired mutations in
[`mutations/`](mutations/README.md) are the standing demonstration.

## The bounded instance

`AtomicCommit.cfg` fixes a concrete instance:

- 2 concurrent sagas (`t1`, `t2`),
- 3 keys (`k1`, `k2`, `k3`),
- `t1` writes `{k1, k2}`, `t2` writes `{k2, k3}` - 2 participants each,
  overlapping on `k2`,
- a bounded reshard orphan step per key (used-once budget).

**What the `k2` overlap does and does not buy.** The two sagas do share a key,
and TLC does interleave their two lifecycles. What the overlap does *not* do is
exercise any *cross-saga* claim, because every property above is stated
per-saga - bar `TypeOK` and `RevisionMonotonic`, which
constrain only the variables' domains and the shared revision counter: each
quantifies `\A t \in Txns` and then resolves that saga's keys against that
saga's own `decision[t]`, `terminal[t]` and `pend[t]`. No property relates
`t1`'s state to `t2`'s, and no protocol action's guard does either: every
variable but the shared revision counter is indexed by saga, so each saga's
properties are checked exactly as they would be for that saga alone. Read the
overlap as naming a shared key, not as evidence that concurrent sagas
contending for one key have been checked.

Such a property is *unexpressed here*, not inexpressible, and the price is
worth stating rather than hand-waving. The natural one is
`NoConcurrentPreparedWriters`: at most one saga holds a pending bucket on a key
at a time. It would strengthen the design rather than mirror the code: the
implementation takes no per-key admission lock, so two sagas writing one key
each stage their own per-transaction pending bucket on the leaf, and
overlapping sagas are resolved pairwise by last-writer-wins ("Ordering across
distinct sagas" in [atomic writes](../../docs/lattice/atomic-writes.md)). As an
invariant alone it is false here for the same reason: `PrepareTx` has no
cross-saga precondition and both sagas may hold a bucket on `k2`. Making it
true costs one conjunct on `PrepareTx` requiring no other saga's bucket on any
key it writes.

That one conjunct is not free, and the reason is specific rather than general
caution: `ShadowForwardOrphan` may re-install a bucket on `k2` after `t1` is
done, `OrphanDrain` is **deliberately not fair** (see the fairness note in
`AtomicCommit.tla`), so a behaviour exists in which that orphan is never drained
and the gated `PrepareTx(t2)` is never enabled - which would break `Termination`.
Adding the property therefore also means deciding whether `OrphanDrain` becomes
fair, which weakens the "every safety property holds whether or not the orphan
fires" guarantee that its unfairness currently buys. Two coupled changes and a
re-run of TLC, not one conjunct.

The deeper limit is that the model abstracts values away entirely: even with the
conjunct in place, "which of two committed writers does a reader of `k2` observe" is
not a question this instance can ask, because `Observed` returns which side of
the saga a read lands on rather than a value. A cross-saga *visibility* property needs a value
domain, which is a larger change than that conjunct.

To widen the instance, declare the new model values on the `CONSTANTS` line of
`AtomicCommit.tla`, extend `TxWrites`, `Txns`, and `Keys` there, and add the
matching model-value assignments to `AtomicCommit.cfg`. The state space stays
small for the default instance (see [Counts](#counts)), but no
protocol action's guard refers to another saga, so the sagas' reachable states
combine as a product: every saga added multiplies the count by what one saga
alone can reach, and larger instances grow quickly.

## Claims in this directory that open issues own

No issue that is still **open** owns claims made in this directory.

The four that used to appear here - **#2319** (verification artefacts named for
what they could not exercise; the Coyote concurrency degree was deliberately not
raised, see `CoyoteModelHarness`), **#2320** (the unordered decision-masking
action, now `RegistryMask`), **#2325** (documentation and API overclaims in the
atomicity surface, including the `k2` overlap discussed above) and **#2333**
(the `DecisionDurability` prose and its refinement seam) - are all resolved.
Further issues filed while closing them are about production behaviour, not
claims made here: #4428, #4445 and #4448.

The boundary is recorded in full under
[territory owned by other open issues](Refinement.md#territory-owned-by-other-open-issues)
in the refinement note, which is where a census of that note's Detector column
meets it. Read it before filing any of these findings as new.

## How to run TLC

From this directory, with the toolchain described in
[how to run TLC](../README.md#how-to-run-tlc):

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

A clean run ends with `Model checking completed. No error has been found.`
and reports the distinct-state count in [Counts](#counts).
### Confirming the model is non-vacuous

The invariants are load-bearing, not trivially true. To convince yourself,
temporarily weaken `BroadcastStep` so a leaf may apply a commit terminal while
the saga is still in `phase = "prepared"` (i.e. before `DecideTx` records the
decision): admit `"prepared"` to its phase guard and make the terminal kind
commit for every phase but `"aborting"`, which is what
[`mutations/LinearizedTerminalsBroadcastBeforeDecision.mutation`](mutations/LinearizedTerminalsBroadcastBeforeDecision.mutation)
does. Widening the guard alone is not enough: the unchanged kind rule then
applies an abort terminal, which TLC reports as
`Invariant LinearizedTerminals is violated` rather than as a split view. With
both changes, TLC reports `Invariant AllOrNothing is violated` with a
counterexample trace: a reader observes one key at its post-saga value while a
sibling key still shows pre-saga - exactly the split view the linearization
point exists to prevent. Revert the weakening to restore the clean run.

## The refinement note is gated for staleness, not for truth

[`Refinement.md`](Refinement.md) names production C# symbols in backticks. A
rename or deletion in `src/lattice/` would leave those references pointing at
code that no longer exists, while the note went on reading as authoritative.
`RefinementMappingStalenessTests` (in `test/lattice/Formal/`) closes that gap:
it parses the backticked `Type.Member` references out of the three mapping
tables and fails if any of them no longer resolves in `src/`. It is
toolchain-free, needs no JVM, and runs in the deterministic tier in
milliseconds. It skips the `Detector` column, whose test names
`RefinementDetectorMappingTests` resolves against `test/` instead.

Resolution handles three forms deliberately: an ordinary type member, a nested
type, and a **partial-class file suffix**. The mapping tables rely on the first
and the last - `ShardRootGrain.TxTerminal` is not a member at all but the file
`src/lattice/BPlusTree/Grains/ShardRootGrain.TxTerminal.cs`. A checker that
assumed `Type.Member` would report that (and `ShardRootGrain.Split` and
`BPlusLeafGrain.PendingTx`) as missing and be wrong. The gate reads source text
rather than using reflection, because several mapped symbols are `private` or
`internal` and the file-suffix form has no reflective existence at all.

**What a green run does and does not mean.** The gate checks that each named
symbol **exists**. It does not check that the row's claim about that symbol is
**true**. A row can name a dozen perfectly resolvable symbols and still assert
behaviour the code does not have; nothing here would notice. Verifying the
behavioural claims is separate work. Do not read a passing run as the note
having been validated, only as the note not naming code that has disappeared.

Bare backticked identifiers are not checked. In these tables they are
indistinguishable from TLA+ variables, spec-level string values, enum members
quoted without their type, and parameter names, so checking them would produce
false alarms; a staleness gate that cries wolf gets suppressed and is then
worse than no gate. The honest cost is that a rename of a symbol the note
mentions only in bare form is not caught.

A second toolchain-free gate, `RefinementPropertyCoverageTests`, checks the
note from the model's side: every property `AtomicCommit.cfg` checks must have
a row in the note's property-mapping table or, with a stated reason, in its
excluded-properties table, and a prose mention alone does not count. Like the
staleness gate, it checks coverage, not the truth of a mapping.

## Last checked

This specification was checked with **TLC 2.19** (tla2tools, rev 5a47802) on a
Temurin 21 JRE when it was added (#1597):

```
Model checking completed. No error has been found.
7649 states generated, 2809 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 17.
```

All seven invariants and all five temporal properties held; no deadlock. That run
predates #2612, which added the `forgotten` variable and the `ForgetDecision`
action and strengthened `DecisionDurability`, so these counts describe the earlier
model, not the current one. After #2320 added `masked` and `RegistryMask` and
#2321 added `NoStrandedPrepare`, a local run on tla2tools v1.7.4 checked all seven
invariants and all six temporal properties over 29,929 distinct states at depth 21
(124,625 generated), clean. The review that followed (PR #4424) dropped
`RegistryMask`'s decision guard and restated `MonotonicVisibility` and
`EveryCommittedKeyReadable`; the same toolchain then checked all seven
invariants and all six action and temporal properties over 31,684 distinct
states at depth 21 (134,633 generated), clean, with deadlock checking on. The current specification is model-checked in CI by
`TlcModelCheckTests.The_base_specification_holds`, against the tla2tools v1.7.4
release the workflows pin (see [CI decision](../README.md#ci-decision)).

## The cross-cluster module

`AtomicCommitCrossCluster` (issue #4436, epic #4430) instances `AtomicCommit`
for the origin and drives its saga `t1`, over `k1` and `k2`, through
`AtomicCommit`'s prepare, decide, broadcast and forget steps; the orphan and
registry-mask steps never run for it (an abstraction gap of the note). What it
adds starts at the origin's WAL: every prepare and every per-source-shard
terminal becomes a replication record, delivered to a receiver by a transport
that may reorder, lose a delivery (the record is shipped again) and lose an ack
(the record is delivered again), and never loses a record by itself: every way
production loses one to a peer is a loss-path action with its fix - a shipper
gap (#4534, #4651), a detach and re-add (#4652), a receiver poison (#4633), a
decommission and fresh re-add (#4684, #4698, #4701), and both trees off the log
at one boundary - together with the re-seed, the replay filter and the purge
holds that bring the saga back (#4577, #4631, #4652, #4666). The receiver stages prepares, tallies terminals
per source shard (including the legacy path for a terminal with no count),
hands a cross-tree saga to the receiver barrier through a delegation its
registry can fail to dial, fans terminals out to its leaves, and may instead join
through a snapshot bootstrap, after which the origin re-ships whatever its WAL
retains from before the cut and the receiver settles it against the exported
decisions. A cross-tree import - a bootstrap or a re-seed - records the tree's
arrival with the receiver barrier and holds the tree's read fence until no
barrier of its operations is undecided and every sibling tree has passed its
boundary (#4683, fixed by #4706; #4684). Each behaviour fixes one shape: a
single-tree saga over two source shards, or a cross-tree saga over two trees. A
behaviour takes at most one loss path, in either shape; the base cfg checks the
instance with none and a receiver that follows the stream, and a joining
receiver and each loss path are checked, with every property, in the variant
configurations, which slice the instance by how the receiver joins, the saga's
shape and outcome, and the loss path's own bounds so every run fits the TLC
budget; the refinement note says which bounds each path is checked under.

Its properties are all claims about the receiver: `RAllOrNothing`,
`RStrictIsolation`, `RLinearizedTerminals` and `DelegationsDisjoint` as
invariants, and `RMonotonicVisibility`, `RCommittedEventuallyVisible`,
`RNoStrandedPrepare` and `RImportFenceLifts` as temporal properties under a fair
transport. The base
holds with deadlock checking on, and so does every variant configuration.

The module models the protocol's intended design. Every place where
production departed from it was filed as a defect and is now fixed, and each
fix is kept as a regression-check mutation whose detectors
[`RefinementCrossCluster.md`](RefinementCrossCluster.md) cites: a terminal
that overtakes its shard's prepare (#4480, fixed by the shipper's terminal
hold), the snapshot read paths' handling of an undiallable delegation (#4448,
fixed by #4461), a bootstrap over a stranded origin prepare (#4481, fixed by
#4501), pre-cut saga records re-shipped after a bootstrap (#4482, fixed by
#4510's settle), and a decision purged while its saga's prepare could still be
re-shipped (#4508, fixed by #4553), a per-tree import that settles a cross-tree
sub-saga without the receiver barrier (#4683, fixed by #4706), and a barrier
left waiting by a tree that stopped being replicated (#4698). Issue #4684's
fixes - the cross-tree purge hold, the export precondition, the uniform arrival
and the boundary the fence waits on - are fixed by #4716 and #4721; the hold's
release on a decommission is issue #4723. Every loss path is kept the same way: a
shipper that stays on the log after a gap, a detach that does not take the peer
off the log, a re-add that does not re-mark, a parked prepare whose saga is not
poisoned (#4591), a poisoned saga's terminal applied,
the poison re-seed discarding a carried saga, the re-seed clear left out or
widened, a decision row not fanned out, a stale export decision (#4627), the
replay filter removed, clearing early or reading the decision alone, records withheld but not
retained, a purge that ignores the holds, and a replay or a re-seed settled while
a silo predates the purge hold (#4664, fixed by #4666). The base models the fixed
design, and no part of production's transport is left outside it.

The extracted cores it maps to are `TerminalArrivalTally` (now including the
ungated test) and `CrossTreeReceiverBarrier`, both executed by the production
grains and by the Coyote models `CrossClusterReceiverTallyModel` and
`CrossTreeReceiverBarrierModel`.

## Counts

The module's current totals. `SpecModuleDiscoveryTests` checks this table
against [`AtomicCommit.manifest.json`](AtomicCommit.manifest.json), and the
other Formal gates check the manifest against the specification, the cfg, the
mutation catalogue, the refinement note and TLC's own state count.
This table is the one place this directory states them; see
[module layout](../README.md#module-layout).

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `AtomicCommit` | 7 | 6 | 8 | 21 | 17 | 31,684 |
| `AtomicCommitCrossCluster` | 5 | 4 | 30 | 56 | 37 | 58,304 |
