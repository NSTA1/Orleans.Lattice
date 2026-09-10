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
| `forgotten[t]` | Registry row retired after cleanup | Whether the saga's row has left the registry **view**: `ITxRegistryGrain.ForgetAsync` dropping it, the lazy `PruneExpired` purge behind it, or the `TxDecisionRetention = 0` branch that never tombstones at all. Kept as a variable of its own rather than writing `"inflight"` back over `decision[t]`, so the outcome the saga recorded stays available to every property that needs it and only `RegistryView(t)` - what a reader actually resolves - reverts. |
| `revision` | Monotonic registry revision | `TxRegistryState.DecisionsRevision`, bumped on every `Decisions` mutation. The token reader fast paths actually probe is a composite that adds the count of tombstones currently past their retention boundary plus two persisted compensating epochs (`TombstoneRetirementEpoch`, `TombstonePinUnmaskEpoch`), so the probe also announces the surface changes that happen with no write: a tombstone ageing out, a batch prune retiring several at once, and a snapshot pin un-masking rows. The spec models only the abstract monotonicity the composite provides. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `PrepareTx(t)` | Prepare fan-out | `AtomicWriteGrain.PrepareAsync` + `ExecutePhaseAsync`: stage every write into per-leaf pending buckets (hidden), collecting per-key ack / nack. | Yes: `AtomicWriteGrainTests.ExecuteAsync_routes_execute_phase_writes_through_the_prepared_path` pins the saga side - the execute phase dispatches its batch under an active prepared scope carrying the saga's persisted transaction id, which is exactly what routes the write into a per-leaf pending bucket instead of the visible projection - and `BPlusLeafGrainTests.GetAsync_with_in_flight_pending_uses_pre_saga_visibility` pins the leaf-level hiding of the resulting bucket. |
| `DecideTx(t)` | Record the single terminal decision | `AtomicWriteGrain.RecordTerminalDecisionAsync` -> `ITxRegistryGrain.MarkCommittedAsync` / `MarkAbortedAsync`. Commit iff every participant acked; this write is issued **before** the broadcast - the linearization point. | Yes: `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard`. |
| `BroadcastStep(t,k)` | Per-leaf terminal fan-out (one leaf at a time) | `AtomicWriteGrain.BroadcastTerminalsAsync` -> per-shard / per-leaf `AppendTxTerminalAsync`. Modelling it one leaf per step is what lets TLC explore the post-decision window in which some leaves have flipped and others have not. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard` and `ShardRootGrainTxTerminalTests.AppendTxTerminalAsync_fans_out_terminal_to_every_leaf`. |
| `ShadowForwardOrphan(t,k)` | Reshard shadow-forward of a stale prepared write | A prepared write reaching a destination leaf that has already applied the saga's terminal, re-installing a pending bucket. Two production paths do this: the hot-path shadow-forward on an active split (`ShardRootGrain.Split`, prepared branch) and the retroactive sweep `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`, which replays the source's prepared mutations onto the destination. `ShardRootGrain.TxTerminal` is the terminal fan-out that races them, not a forwarder. | Yes: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` covers the retroactive sweep, and `ShardRootGrainSplitShadowForwardTests.Hot_path_shadow_forward_installs_orphan_pending_bucket_on_destination_leaf_that_already_applied_the_terminal` covers the hot-path shadow-forward on an active split. The hot path was already asserted as a forwarder; what that second test adds is the ordering this row actually models, applying the terminal to the destination first so the forward produces the orphan bucket rather than merely arriving. |
| `OrphanDrain(t,k)` | Discard of a late orphan bucket, terminal already applied | `MigrationTerminalCore.DecideBucketAction` returning `DiscardOrphan` - the leaf holds a pending bucket for a saga whose terminal has already landed here, so `BPlusLeafGrain.ApplyTxTerminalAsync` discards the bucket instead of draining it. The guards correspond exactly: the action's `terminal[t][k] # "none"` is the core's `alreadyTerminal`, and `pend' = "none"` with everything else `UNCHANGED` is the discard. **This action does not model the split coordinator's post-sweep cleanup pass, and no rewording of this row can make it do so**: that pass acts on a bucket whose terminal has *not* reached the leaf, which is the complement of this action's guard. See the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps). | Yes: `MigrationTerminalCoreTests.Pending_already_terminal_discards_orphan_regardless_of_verdict` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. |
| `ForgetDecision(t)` | Post-fan-out cleanup retires the registry row | The saga's cleanup once the terminal fan-out is finished: `ITxRegistryGrain.ForgetAsync`, plus the lazy `PruneExpired` purge behind it and the zero-retention branch that skips tombstoning. Its enabling conditions are production's ordering guarantee rather than a modelling convenience - every written key has applied its terminal and holds no pending bucket, which is what the late-pickup loop in `BroadcastTerminalsAsync` establishes before the saga completes. **Only the ordered path is modelled.** An unordered retention window masking a row while a bucket is still live is a different event with no such guarantee, and is #2320's: see [territory owned by other open issues](#territory-owned-by-other-open-issues). | Yes: `TxRegistryGrainTests.ForgetAsync_drops_recorded_decision` pins the retirement itself and `AtomicWriteGrainTests.BroadcastTerminals_late_arrival_fires_second_terminal` pins the drain-before-cleanup ordering the guard abstracts. That retiring the row *early* is a property violation rather than merely untidy is pinned by `AtomicCommitInvariantCoyoteTests.Forgetting_the_decision_before_every_leaf_drained_violates_decision_durability`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `AllOrNothing` / `VisibilityMatchesDecision` | The all-or-nothing visibility that `TxDecisionView` delivers by resolving every key of a fan-out against one registry snapshot. This is the invariant the reshard split-view bug (#1584) turned on. | Yes: `AtomicVisibilityChaosTests.Continuous_reader_observes_zero_or_all_keys_for_every_poll`. Chaos tier, so it runs in CI and not in the local dev loop. |
| `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never returning `SurfacePrepared` unless `status = Committed` - the strict-isolation default that an in-flight or aborted saga stays invisible. | Yes: `AtomicVisibilityGateTests.InFlight_always_falls_through`, `AtomicVisibilityGateTests.Aborted_always_falls_through` and `AtomicVisibilityGateTests.Indeterminate_always_hides_key`. |
| `LinearizedTerminals` | The decision-before-broadcast ordering in `RunSagaAsync`: `RecordTerminalDecisionAsync` precedes `BroadcastTerminalsAsync`, so no leaf surfaces a committed value before the tree-wide decision exists. | Yes: `AtomicWriteGrainTests.RunSagaAsync_commit_records_the_decision_before_broadcasting_terminals`, `AtomicWriteGrainTests.RunSagaAsync_abort_records_the_decision_before_broadcasting_terminals` and `AtomicWriteGrainTests.FinalizeAsync_records_the_decision_before_broadcasting_terminals` - one per ordering site, so a reversal at any of the three is independently falsifiable. |
| `NoMixedTerminals` | A saga records exactly one `TxStatus`, so its per-leaf terminals are uniformly commit or uniformly abort. | Yes: `AtomicWriteGrainTests.Aborting_saga_broadcasts_its_single_recorded_abort_verdict_to_every_touched_shard` and `AtomicWriteGrainTests.Committing_saga_broadcasts_its_single_recorded_commit_verdict_to_every_touched_shard`, which pin the recorded decision and the whole terminal fan-out together, over a batch routed to three distinct shards. One correction to the census while closing this: it recorded the uniform-abort outcome as unasserted, which was too strong - `CompensationContinuousReaderTests.Compensation_broadcasts_TxAbort_to_every_touched_shard` already pinned it, and a perturbation that hands one shard the opposite verdict reds that test too. What was genuinely undetected was the antecedent this row names, that the verdict every terminal carries is the single `TxStatus` the saga recorded: flipping `RecordTerminalDecisionAsync` to record the opposite outcome leaves every fan-out-only test green, because none of them wires the registry. |
| `DecisionDurability` | `TxStatus` transitions are terminal **and** the decision outlives every participant that depends on it. *No flip*: `MarkCommittedAsync` / `MarkAbortedAsync` never turn a recorded decision into the other terminal, and treat a repeat of the same outcome as an idempotent no-op for as long as the decision is still recorded - including while it is merely tombstoned, since classification runs before the tombstone is cleared. Once a tombstone has been physically purged the registry has no row to recognise, so a late same-outcome terminal records afresh rather than being absorbed; the recorded outcome is unchanged either way. *No premature unset*: the formula forbids a committed decision becoming **absent** exactly as firmly as it forbids it becoming aborted, because absent is not `"committed"` either, and an absent row hides a committed value from a reader just as a flip does. Three production paths retire a row - `ITxRegistryGrain.ForgetAsync`, `PruneExpired`, and the zero-retention branch that skips tombstoning altogether - and each is safe only because it runs after the terminal fan-out has drained every participant's pending bucket. That safety is an *ordering* guarantee rather than an idempotence one: `AtomicWriteGrain` reaches `ForgetAsync` only from post-fan-out cleanup, and the interface contract states the precondition in as many words ("by which point no leaf has the txid in its pending bucket anymore"). Retire a row earlier than that and a committed key goes invisible. | Yes, for both halves. Flip: `TxRegistryGrainTests.MarkCommittedAsync_throws_when_previously_aborted` and `TxRegistryGrainTests.MarkAbortedAsync_throws_when_previously_committed`. Unset: `AtomicCommitInvariantCoyoteTests.Forgetting_the_decision_before_every_leaf_drained_violates_decision_durability`, whose model action retires the row under production's real precondition and whose guard arm retires it while a leaf is still undrained. The two registry tests do not cover the unset - an absent row is not a flip, so both pass unchanged when it happens - which is how an earlier census graded this row against prose narrower than the formula it abstracts. |
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

Some claims made in this directory are owned by an issue that is still **open**,
and are not in scope for the refinement note's own work. This section records
that boundary, so that a later census of the Detector column does not re-file
a finding that already has an owner.

Read it before grading a row or filing a gap. A finding that lands inside one of
the boundaries below belongs to that issue, and opening a second issue for it
splits one fix across two changes whose authors cannot see each other. That is
not hypothetical: the census that produced the Detector column above ran without
this boundary written down anywhere, because the acceptance criterion that asked
for it (#2525's third) was never discharged. #2562 discharged it here, in the
document the census actually reads, rather than in an issue comment.

This section records the boundary and nothing else. It does not fix any of the
issues below, and nothing here asserts that the claims they own are currently
correct. **Re-populate it rather than deleting it** when its entries close: the
job it does outlives any particular set of issues, and an empty section still
tells a reader the question was asked.

### #2320 owns the unguarded decision-masking action

`ForgetDecision(t)` in [`AtomicCommit.tla`](AtomicCommit.tla) retires a registry
row, so the specification now has *an* action that unsets a decision. It is the
**ordered** one: the saga's post-fan-out cleanup, enabled only once every written
key has applied its terminal and holds no pending bucket, which is the ordering
`ITxRegistryGrain.ForgetAsync` states as its own precondition. It was added to
make `DecisionDurability` falsifiable, and it is, by the guard arm the row's
Detector cell names.

What it deliberately does **not** model is the **unordered** path: a retention
window aging out and masking a decision row while a prepared bucket is still
live. That event has no ordering guarantee behind it, and #2320 records that
adding it violates `MonotonicVisibility` and `VisibilityMatchesDecision` at depth
4 - the production hazard, not a modelling artefact.

The distinction is easy to lose and expensive to lose. The guarded action passes
every property precisely because its conjuncts make every observation independent
of the decision before it fires; reading that pass as evidence about the
unguarded hazard inverts #2320's finding. **`ForgetDecision` does not discharge
#2320.** The test to apply: a finding that the specification cannot reach a state
where a reader sees a decision the registry still holds is #2320's.

### #2319 owns verification artefacts named for what they cannot exercise

Two members of `CoyoteModelHarness` were renamed by #2325 to describe the
single-operation determinism they actually establish, because their previous
names promised interleaving and schedule exploration at a measured concurrency
degree of zero. Renaming was the correction available to a documentation issue.

**Raising the concurrency degree above zero, so that names promising exploration
would be honest, is #2319's and remains open.** A finding that a verification
artefact in the atomicity surface explores fewer schedules than its purpose
implies is #2319's; a finding that its *name or documentation* overstates what it
does was #2325's and is closed.

### Closed: #2325 and #2333

Both are resolved, and both are recorded here rather than deleted because a
census that predates their fixes will still turn their findings up.

- **#2333** owned `DecisionDurability`'s prose and its refinement seam. Its
  finding was that the TLA+ formula is correct and every prose site was strictly
  weaker, narrowing it to a *flip*, while the row's seam pointed at the one path
  that cannot violate it. Prose, seam, model action and detector were corrected
  together, which was the issue's own instruction: because the three layers
  *compose* into the defect, fixing prose alone would have converted an honest
  narrow claim into a false broad one.
- **#2325** owned three documentation and API overclaims in the atomicity
  surface - the `SnapshotPin` guarantee attributed to the wrong mechanism, the
  nominal two-saga overlap advertised in [`README.md`](README.md), and the
  harness members above. In each case the behaviour users depend on was present
  and the account of *why* was wrong, which is worse than it sounds: a change
  removing the real mechanism would leave the wrong account standing and looking
  like cover.

Do not re-file either as a fresh detector gap. If a census finds one of these
claims still stated somewhere this note does not reach, that is a missed site of
a closed fix, and belongs on a new issue naming the site.

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
