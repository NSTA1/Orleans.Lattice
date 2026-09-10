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
| `ShadowForwardOrphan(t,k)` | Reshard shadow-forward of a stale prepared write | A prepared write reaching a destination leaf that has already applied the saga's terminal, re-installing a pending bucket. Two production paths do this: the hot-path shadow-forward on an active split (`ShardRootGrain.Split`, prepared branch) and the retroactive sweep `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`, which replays the source's prepared mutations onto the destination. `ShardRootGrain.TxTerminal` is the terminal fan-out that races them, not a forwarder. | Partial: `TreeShardSplitGrainTests.RetroactiveSweep_replays_prepare_when_saga_in_flight` covers the retroactive sweep. The hot-path shadow-forward branch on an active split is unasserted, so only one of the two production paths this row names is detected. Gap filed as #2554. |
| `OrphanDrain(t,k)` | Discard of a late orphan bucket, terminal already applied | `MigrationTerminalCore.DecideBucketAction` returning `DiscardOrphan` - the leaf holds a pending bucket for a saga whose terminal has already landed here, so `BPlusLeafGrain.ApplyTxTerminalAsync` discards the bucket instead of draining it. The guards correspond exactly: the action's `terminal[t][k] # "none"` is the core's `alreadyTerminal`, and `pend' = "none"` with everything else `UNCHANGED` is the discard. **This action does not model the split coordinator's post-sweep cleanup pass, and no rewording of this row can make it do so**: that pass acts on a bucket whose terminal has *not* reached the leaf, which is the complement of this action's guard. See the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps). | Yes: `MigrationTerminalCoreTests.Pending_already_terminal_discards_orphan_regardless_of_verdict` and `BPlusLeafGrainTests.ApplyTxTerminalAsync_with_already_terminalled_txid_discards_orphan_pending_bucket`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `AllOrNothing` / `VisibilityMatchesDecision` | The all-or-nothing visibility that `TxDecisionView` delivers by resolving every key of a fan-out against one registry snapshot. This is the invariant the reshard split-view bug (#1584) turned on. | Yes: `AtomicVisibilityChaosTests.Continuous_reader_observes_zero_or_all_keys_for_every_poll`. Chaos tier, so it runs in CI and not in the local dev loop. |
| `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never returning `SurfacePrepared` unless `status = Committed` - the strict-isolation default that an in-flight or aborted saga stays invisible. | Yes: `AtomicVisibilityGateTests.InFlight_always_falls_through`, `AtomicVisibilityGateTests.Aborted_always_falls_through` and `AtomicVisibilityGateTests.Indeterminate_always_hides_key`. |
| `LinearizedTerminals` | The decision-before-broadcast ordering in `RunSagaAsync`: `RecordTerminalDecisionAsync` precedes `BroadcastTerminalsAsync`, so no leaf surfaces a committed value before the tree-wide decision exists. | None. Nothing pins the decision write ahead of the broadcast, so swapping the two calls at any of the three ordering sites in `AtomicWriteGrain` would leave the suite green. Gap filed as #2551. |
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

The census found ten rows detected, three partial or undetected. The gaps are
the point of the exercise rather than a blemish on it: `LinearizedTerminals`
(#2551), `NoMixedTerminals` (#2552), `PrepareTx(t)` (#2553) and
`ShadowForwardOrphan(t,k)` (#2554). A row reading "None" is a stronger artefact
than a row reading nothing at all, because only the first can be closed.

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
