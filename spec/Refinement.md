# Refinement note: TLA+ spec to code cores

This note maps the abstract TLA+ specification in
[`AtomicCommit.tla`](AtomicCommit.tla) to the Orleans.Lattice atomic-commit
protocol as it exists in code, so the abstract model and the runtime artifact
are traceably the same protocol and any divergence is visible.

It is a **documented mapping, not a machine-checked refinement proof** - the
latter (a theorem-prover effort) is explicitly out of scope for #1596. The
value is that when the protocol design changes, this table shows which spec
action and which code seam must move together.

## A note on stability of names

The per-key visibility decision already lives in a dependency-free core today
(`AtomicVisibilityGate` + `TxDecisionView` + `PendingReadOutcome`, from level B
/ #1585). The remaining protocol pieces - the saga coordinator transition
logic, the registry decision-plus-revision, the reshard orphan guard - are
being extracted into their own verified cores across level-C Phases 1-4, which
land after this specification. This note therefore maps to the **protocol
role** (coordinator decision, registry snapshot + revision, orphan guard)
rather than to class names that may still be in flight; where a concrete
artifact exists today it is named, and where a core is still landing the
production seam it will be extracted from is named instead.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `phase[t]` | Coordinator saga lifecycle | `AtomicWritePhase` persisted in `AtomicWriteState`, driven by `AtomicWriteGrain.RunSagaAsync`. Spec `init` -> `prepared` -> `committing`/`aborting` -> `done` abstracts `NotStarted`/`Prepare`/`Execute` -> decision -> `Compensate`/`Completed`. |
| `vote[t][k]` | Per-participant prepare outcome | The success / failure of each per-key prepare write in `ExecutePhaseAsync` (an ack is a staged prepared mutation; a nack is a precondition-guard miss or write failure that pivots the saga to `Compensate`). |
| `decision[t]` | Tree-wide commit / abort decision | `TxRegistryState.Decisions[txid]` (`TxStatus` = `InFlight` / `Committed` / `Aborted` / `Indeterminate`), read through `TxDecisionView`. Absent txid resolving to `InFlight` is the spec's default `decision = "inflight"`. `Indeterminate` is outside the spec's decision domain: it is not a fourth outcome but the registry declining to report one it still holds (a decision row masked by the tombstone retention window, or a cross-tree coordinator that could not be dialled), and the read gate hides the saga's keys rather than resolving them either way. |
| `terminal[t][k]` | Per-leaf applied terminal + orphan-guard flag | The leaf's `_recentlyTerminal` / applied-terminal state after `AppendTxTerminalAsync`; `terminal # "none"` is `AtomicVisibilityGate.ResolveKey`'s `alreadyTerminal` input. |
| `pend[t][k]` | Hidden pending bucket on a leaf | The leaf `_pendingTx[txid]` bucket installed by a prepared mutation (`BPlusLeafGrain.PendingTx`). |
| `orphanDone[t][k]` | Bounded reshard-orphan budget | Modelling device only (keeps the state space finite): it bounds how many times `ShadowForwardOrphan` / `OrphanDrain` may cycle on one key. It has no production counterpart - nothing in the code drains or discards an orphan bucket "at most once per key", and the discard the model's drain abstracts (see `OrphanDrain` below) is idempotent rather than budgeted. |
| `revision` | Monotonic registry revision | `TxRegistryState.DecisionsRevision`, bumped on every `Decisions` mutation. The token reader fast paths actually probe is a composite that adds the count of tombstones currently past their retention boundary plus two persisted compensating epochs (`TombstoneRetirementEpoch`, `TombstonePinUnmaskEpoch`), so the probe also announces the surface changes that happen with no write: a tombstone ageing out, a batch prune retiring several at once, and a snapshot pin un-masking rows. The spec models only the abstract monotonicity the composite provides. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `PrepareTx(t)` | Prepare fan-out | `AtomicWriteGrain.PrepareAsync` + `ExecutePhaseAsync`: stage every write into per-leaf pending buckets (hidden), collecting per-key ack / nack. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path` pins the saga side - the execute phase dispatches its batch under an active prepared scope carrying the saga's persisted transaction id, which is exactly what routes the write into a per-leaf pending bucket instead of the visible projection - and `BPlusLeafGrainTests.GetAsync_with_in_flight_pending_uses_pre_saga_visibility` pins the leaf-level hiding of the resulting bucket. |
| `DecideTx(t)` | Record the single terminal decision | `AtomicWriteGrain.RecordTerminalDecisionAsync` -> `ITxRegistryGrain.MarkCommittedAsync` / `MarkAbortedAsync`. Commit iff every participant acked; this write is issued **before** the broadcast - the linearization point. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `BroadcastStep(t,k)` | Per-leaf terminal fan-out (one leaf at a time) | `AtomicWriteGrain.BroadcastTerminalsAsync` -> per-shard / per-leaf `AppendTxTerminalAsync`. Modelling it one leaf per step is what lets TLC explore the post-decision window in which some leaves have flipped and others have not. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard` and `ShardRootGrainTxTerminalTests.AppendTxTerminalAsync_fans_out_terminal_to_every_leaf`. |
| `ShadowForwardOrphan(t,k)` | Reshard shadow-forward of a stale prepared write | A prepared write reaching a destination leaf that has already applied the saga's terminal, re-installing a pending bucket. Two production paths do this: the hot-path shadow-forward on an active split (`ShardRootGrain.Split`, prepared branch) and the retroactive sweep `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`, which replays the source's prepared mutations onto the destination. `ShardRootGrain.TxTerminal` is the terminal fan-out that races them, not a forwarder. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` covers the retroactive sweep, and `ShardRootGrainSplitShadowForwardTests.Hot_path_shadow_forward_installs_orphan_pending_bucket_on_destination_leaf_that_already_applied_the_terminal` covers the hot-path shadow-forward on an active split. The hot path was already asserted as a forwarder; what that second test adds is the ordering this row actually models, applying the terminal to the destination first so the forward produces the orphan bucket rather than merely arriving. |
| `OrphanDrain(t,k)` | Discard of a late orphan bucket, terminal already applied | `MigrationTerminalCore.DecideBucketAction` returning `DiscardOrphan` - the leaf holds a pending bucket for a saga whose terminal has already landed here, so `BPlusLeafGrain.ApplyTxTerminalAsync` discards the bucket instead of draining it. The guards correspond exactly: the action's `terminal[t][k] # "none"` is the core's `alreadyTerminal`, and `pend' = "none"` with everything else `UNCHANGED` is the discard. **This action does not model the split coordinator's post-sweep cleanup pass, and no rewording of this row can make it do so**: that pass acts on a bucket whose terminal has *not* reached the leaf, which is the complement of this action's guard. See the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps). | Yes: `MigrationTerminalCoreTests.Pending_already_terminal_discards_orphan_regardless_of_verdict` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `AllOrNothing` / `VisibilityMatchesDecision` | The all-or-nothing visibility that `TxDecisionView` delivers by resolving every key of a fan-out against one registry snapshot. This is the invariant the reshard split-view bug (#1584) turned on. | Yes: `AtomicVisibilityChaosTests.Continuous_reader_observes_zero_or_all_keys_for_every_poll`. Chaos tier, so it runs in CI and not in the local dev loop. |
| `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never returning `SurfacePrepared` unless `status = Committed` - the strict-isolation default that an in-flight or aborted saga stays invisible. | Yes: `AtomicVisibilityGateTests.InFlight_always_falls_through`, `AtomicVisibilityGateTests.Aborted_always_falls_through` and `AtomicVisibilityGateTests.Indeterminate_always_hides_key`. |
| `LinearizedTerminals` | The decision-before-broadcast ordering in `RunSagaAsync`: `RecordTerminalDecisionAsync` precedes `BroadcastTerminalsAsync`, so no leaf surfaces a committed value before the tree-wide decision exists. | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals`, `AtomicWriteGrainTests.RunSagaAsync_abort_records_the_decision_before_broadcasting_terminals` and `AtomicWriteGrainTests.FinalizeAsync_records_the_decision_before_broadcasting_terminals` - one per ordering site, so a reversal at any of the three is independently falsifiable. |
| `NoMixedTerminals` | A saga records exactly one `TxStatus`, so its per-leaf terminals are uniformly commit or uniformly abort. | Partial: `AtomicWriteIntegrationTests.SetManyAtomicAsync_commits_all_entries` covers the uniform-commit outcome. The uniform-abort outcome is unasserted. Gap filed as #2552. |
| `DecisionDurability` | `TxStatus` transitions are terminal: `MarkCommittedAsync` / `MarkAbortedAsync` never flip a recorded decision, and treat a repeat of the same outcome as an idempotent no-op for as long as the decision is still recorded - including while it is merely tombstoned, since classification runs before the tombstone is cleared. Once a tombstone has been physically purged the registry has no row to recognise, so a late same-outcome terminal records afresh rather than being absorbed; the recorded outcome is unchanged either way, which is what the spec property asserts. | Yes: `TxRegistryGrainTests.MarkCommittedAsync_throws_when_previously_aborted` and `TxRegistryGrainTests.MarkAbortedAsync_throws_when_previously_committed`. |
| `MonotonicVisibility` | A committed value never reverts to pre-saga - protected in code by the terminal-stable decision plus the orphan guard (`alreadyTerminal`) that stops a late shadow-forward bucket from re-hiding an applied value. The protection is conditional on the registry still holding the saga's decision row; a prepared write that is still resident when that row is physically purged reverts, which the spec does not express (see the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps)). | Yes: `AtomicVisibilityGateTests.Committed_but_already_terminal_orphan_falls_through`. |
| `RevisionMonotonic` | The composite comparison token is monotonically non-decreasing. `DecisionsRevision` on its own only ever increments, but it is not the whole token, and the live-expired-tombstone term it is summed with falls when a batch prune retires several tombstones at once or when a snapshot pin un-masks already-masked rows; `TombstoneRetirementEpoch` and `TombstonePinUnmaskEpoch` compensate for exactly those two drops, so the sum never revisits a value it previously carried under a different readable surface. | Yes: `TxRegistryGrainTests.GetDecisionsRevisionAsync_never_decreases_across_a_batch_prune` and `TxRegistryGrainTests.GetDecisionsRevisionAsync_does_not_fall_when_a_pin_covers_an_expired_tombstone`. |
| `Termination` / `EveryCommittedKeyReadable` | The saga always drives to `Completed` / `Compensate` (reminder-driven resume after a crash), and a committed saga's terminal fan-out reaches every leaf recorded as a participant. It is not unconditionally every leaf holding a bucket: an online split can install a prepared bucket on a destination that the coordinator's participant query had already passed over, which is the orphan window `RetroactiveSweepPreparedMutationsAsync` documents and its post-sweep cleanup pass narrows but does not close. | Yes: `AtomicWriteGrainTests.BroadcastTerminals_late_arrival_fires_second_terminal` and `AtomicWriteGrainTests.ReceiveReminder_resumes_execute_from_persisted_progress`. |

## The Detector column

Every behaviour-asserting row above carries a Detector cell naming the test
that would go red if the production behaviour it abstracts regressed. Three
decisions shaped that column, recorded here because each was a real fork.

**Framing: a detector is a test over production code, never a TLA+ mutation.**
Every property the base model checks already has a paired `spec/mutations` file,
guaranteed by `SpecMutationCatalogueTests`. It is tempting to cite those here,
and it would be wrong. A mutation perturbs the *spec*: if production regressed
tomorrow, every mutation would still fail in exactly the same way and nothing
would notice. A refinement row does not claim the spec is non-vacuous, it claims
the spec abstracts a *production* behaviour, so only a production test can
falsify it.

**Mechanism: name the test in the note and resolve the name in CI.** Four
options were weighed. (A) An attribute on each test pointing back at its row was
rejected: it scatters the mapping across the suite, so the note can no longer be
read as a whole. (B) A separate machine-readable manifest was rejected as a
second artefact to drift against the first. (C) Deriving the mapping from
coverage data was rejected because coverage proves a line executed, not that a
property was asserted, which is the exact conflation the parent audit exists to
catch. (D) The chosen option keeps the claim in the note, where a reader meets
it, and gates it with `RefinementDetectorMappingTests`.

**CI lane: the existing non-chaos suite, with no new job.** The gate is source
text analysis with no cluster and no I/O beyond reading `test/`, so it rides the
per-package fan-out that already runs on every pull request. A dedicated lane
would add wall-clock and a second place for the check to be skipped.

What the gate proves is bounded, and saying so plainly matters. It proves the
column cannot rot into prose: every behaviour-asserting row declares a verdict,
every test named still exists under `test/`, and every row admitting a gap cites
an issue. It does not, and cannot, prove that a named test is a *good* detector.
That judgement was made by reading each test against the row it answers, and
revising it means redoing that reading.

No census result is recorded here, deliberately. The census is *derived from*
the table rather than *asserted about* it, so a reader re-derives it instead of
trusting a figure that no gate evaluates. The method: take every
behaviour-asserting row, which is every row of the action and property tables
except `Stutter` (it asserts no production behaviour, so the Detector column's
question does not apply to it); read the verdict token each row's `Detector`
cell opens with, one of `Yes`, `Partial` or `None`; and tally those tokens. The
rows reporting `Partial` or `None` are the open gaps, and each cites the issue
that closes it, so the gap list is whatever those cells say today rather than
whatever this paragraph said when it was written.

A hand-maintained tally in this note would be a drift generator, because the
tallies move every time a gap closes and nothing would re-derive them. That is
not hypothetical: an earlier revision of this paragraph stated counts the
Detector column did not support, and the same wrong counts were restated in the
gate's own comment (#2560). `RefinementDetectorMappingTests` checks the parts
that can be checked mechanically - the behaviour-asserting denominator, that
every row declares a verdict, that every admitted gap cites an issue, and that
every behaviour-asserting row names a test that still resolves - and
`The_note_records_no_hand_maintained_census_count` keeps a tally from being
written back into this prose.

The census was performed; this paragraph declines to repeat its result, which
is not the same as the result never existing. It is recorded in the body of
epic #2556, pinned to the commit it was derived from, in a document that does
not change as the column does. That is the whole argument in one line: the
tally taken at that commit was already falsified twice over within a day of
being written, as gap issues landed, and it will be falsified again by the ones
still open. A reader who wants today's figure derives it by the method above,
which cannot be stale.

The gaps are the point of the exercise rather than a blemish on it. A row
reading "None" is a stronger artefact than a row reading nothing at all,
because only the first can be closed.

## Territory owned by other open issues

Two issues that are still **open** own claims made in this directory. Neither is
in scope for the refinement note's own work - epic #2556 states both exclusions
explicitly - and neither should be re-filed as a fresh finding by a later census
of the Detector column.

Read this before grading a row or filing a gap. A finding that lands inside one
of the boundaries below belongs to that issue, and opening a second issue for it
splits one fix across two changes whose authors cannot see each other. That is
not hypothetical: the census that produced the Detector column above ran without
this boundary written down anywhere, because the acceptance criterion that asked
for it (#2525's third) was never discharged. #2562 discharges it here, in the
document the census actually reads, rather than in an issue comment.

This section records the boundary and nothing else. It does not fix either
issue, and nothing here asserts that the claims they own are currently correct.

### #2325 owns documentation and API overclaims in the atomicity surface

#2325 is about explanations that do not match the mechanism they name. In each
case the behaviour users depend on is present; what is wrong is the account of
*why*, which is worse than it sounds, because a change that removed the real
mechanism would leave the wrong account standing and looking like cover. It owns
three findings:

- **The `SnapshotPin` guarantee is attributed to the wrong mechanism.** The
  guarantee the documentation attributes to the pin is delivered in practice by
  an unrelated fast path. The remedy is to name the mechanism that actually
  delivers it and pin that mechanism with a test.
- **The advertised two-saga overlap is nominal.** The bounded instance in
  [`README.md`](README.md) advertises two sagas overlapping on a shared key, but
  state is per-saga private and **no property relates two sagas**, so the
  overlap exercises nothing. The remedy is to add a property that relates two
  sagas, or to rename the scenario. #2325 is explicit that the model can carry
  such a property: the finding is to be worded as "unexpressed", with its price
  stated, never as "cannot express".
- **The Coyote model harness claims schedule exploration it does not perform.**
  Two members of `CoyoteModelHarness` are named for interleaving and schedule
  exploration while the measured concurrency degree is zero, and a user-facing
  sample source header and the assurance document repeat the claim. Raising the
  concurrency degree, as opposed to correcting the names, is #2319's rather than
  #2325's.

The test to apply: a finding that an artefact in the atomicity surface describes
a guarantee some other mechanism delivers, or advertises an interaction or an
exploration it does not perform, is #2325's.

### #2333 owns `DecisionDurability`'s prose and its refinement seam

#2333 owns the `DecisionDurability` row of the property mapping table above **by
name**, together with the same property's prose wherever else it is stated. It
reverses the remedy an earlier reading was converging on, so its direction
matters as much as its scope:

- **The TLA+ formula is correct as written, and must not be weakened or
  scoped.** `DecisionDurability` in [`AtomicCommit.tla`](AtomicCommit.tla)
  already forbids a committed decision going absent as well as going aborted,
  because absent is not `"committed"`.
- **Every site that states the property in prose is strictly weaker than the
  formula.** All of them narrow it to a *flip*: the comment above the formula in
  [`AtomicCommit.tla`](AtomicCommit.tla), the property table in
  [`README.md`](README.md), and the `DecisionDurability` row here. The same
  wording appears in `docs/lattice/verified-atomic-commit.md`, which is the same
  claim and belongs to the same issue. The remedy is to say the decision never
  flips **or is unset**, not to narrow the formula to match the prose.
- **The seam points at a path that cannot violate the property.** The row maps
  the property onto the repeat same-outcome registry call, where the removal and
  the re-apply both precede a single state write, so the property holds there by
  construction. The paths that can reach a violation - `ForgetAsync`,
  `PruneExpired`, and the zero-retention branch - are named nowhere in the
  mapping. Re-pointing the seam at those is #2333's. Note that the row's
  same-outcome clause has already been qualified once since #2333 was filed, by
  the #2299 fix wave: it now says a purged tombstone makes a late terminal record
  afresh. That narrows the clause #2333 called false; it does not discharge the
  issue, because the seam still names no mechanism that can unset a decision, and
  the row still concludes that the recorded outcome is unchanged either way.
- **Adding the model action that can unset a decision is #2320's.** #2333 relies
  on it to exercise the property but does not own it.

Two consequences for grading the Detector column:

- The `DecisionDurability` row's Detector cell is graded against the row's claim
  **as it stands**, which is the narrowed flip reading, and the two registry
  tests it names do detect a flip. That a flip-detecting test does not detect an
  *unset* is not a new detector gap to file: it restates #2333, and it resolves
  when #2333 corrects the claim. Re-grade the row after #2333 lands, not before.
- Do not correct any part of `DecisionDurability` here piecemeal. #2333's finding
  is that the formula, the prose sites and the seam **compose** into the defect,
  each step being locally defensible, so changing one of them alone can leave the
  artefact more inconsistent rather than less.

The test to apply: a finding that this note or the specification understates
`DecisionDurability`, or maps it onto a path that cannot violate it, is #2333's.

## Deliberate abstraction gaps

These are modelled abstractly or not at all, by design; the Coyote cores and
the reshard chaos suite cover them at the implementation level:

- **No serialization, timers, HLC, or WAL.** The spec has no wall-clock; the
  tombstone / TTL "hidden" branch of `AtomicVisibilityGate.ResolveKey`
  (prepared value hidden by a tombstone or expiry) is out of scope, so
  `ObservedPrepared` models only the commit / abort visibility dimension.
  This gap is about a TTL on the prepared **value**. It does **not** cover the
  retention window on the registry's **decision record**, which is a different
  clock and is declared separately below.
- **The registry decision record's retention window.** `decision[t]` is a total
  function assigned once and read directly, so in the model the registry cannot
  misreport or lose a decision it made. Production reaches two states the model
  does not express: once `TxDecisionRetention` elapses,
  `TxRegistryGrain.GetStatusAsync` reports a still-stored decision as
  `Indeterminate` (the read gate then hides the key), and once `PruneExpired`
  physically drops the row it reports `InFlight`, which the gate reads as an
  affirmative "did not commit" and falls through to the pre-saga value. Both
  outcomes are reachable while a prepared bucket is still resident, and the
  second is a committed key reverting to pre-saga - the hazard
  `MonotonicVisibility` and `VisibilityMatchesDecision` are worded to catch.
  Nothing in the spec reaches either state, and the orphan actions cannot
  substitute: both are guarded on `terminal # "none"`, so a prepare whose
  terminal never arrives is not a behaviour of this model at all. Closing the
  gap needs a variable interposed between the stored decision and the reader
  plus an action that unsets a decision; that is issue #2320, and it is where
  this would be modelled. Until it lands, no conclusion about a stranded or
  forgotten-decision prepare may be drawn from this specification.
  The production mitigations - the split coordinator's post-sweep cleanup pass
  in `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync` and the
  leaf's activation-time `SelfTerminaliseResolvedPreparesAsync` sweep - are
  best-effort re-checks against the registry, not ordering guarantees against
  the retention window. The cleanup pass runs once per sweep and acts only on a
  status of `Committed` or `Aborted`, leaving the bucket resident for anything
  else. The leaf sweep runs once per activation and does see past the retention
  mask, asking the registry for the recorded verdict behind an `Indeterminate`
  answer; neither can act once the row has been physically pruned, because the
  registry then has nothing left to report.
- **Per-saga projection.** Each saga's visible value is modelled independently
  per key; inter-saga last-writer-wins ordering on a shared key (and the
  cross-migration LWW backstop) is orthogonal to all-or-nothing visibility and
  is left to the CRDT / LWW cores.
- **Cross-tree and cross-cluster delegation.** The `ExternalAuthorities` /
  `ReceiverDecisionAuthorities` delegation and the `Prepared` park-and-wait
  phase are not modelled; the spec covers the single-tree saga, whose
  decision variable is the coordinator's verdict.
- **Crash / recovery.** Modelled only through idempotent re-entry being
  safe (repeated `DecideTx` / `BroadcastStep` converge to the same state);
  the reminder-driven resume mechanics are a code-level concern.
