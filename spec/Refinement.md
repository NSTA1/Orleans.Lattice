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

| Spec action | Protocol step | Code counterpart |
|-------------|---------------|------------------|
| `PrepareTx(t)` | Prepare fan-out | `AtomicWriteGrain.PrepareAsync` + `ExecutePhaseAsync`: stage every write into per-leaf pending buckets (hidden), collecting per-key ack / nack. |
| `DecideTx(t)` | Record the single terminal decision | `AtomicWriteGrain.RecordTerminalDecisionAsync` -> `ITxRegistryGrain.MarkCommittedAsync` / `MarkAbortedAsync`. Commit iff every participant acked; this write is issued **before** the broadcast - the linearization point. |
| `BroadcastStep(t,k)` | Per-leaf terminal fan-out (one leaf at a time) | `AtomicWriteGrain.BroadcastTerminalsAsync` -> per-shard / per-leaf `AppendTxTerminalAsync`. Modelling it one leaf per step is what lets TLC explore the post-decision window in which some leaves have flipped and others have not. |
| `ShadowForwardOrphan(t,k)` | Reshard shadow-forward of a stale prepared write | A prepared write reaching a destination leaf that has already applied the saga's terminal, re-installing a pending bucket. Two production paths do this: the hot-path shadow-forward on an active split (`ShardRootGrain.Split`, prepared branch) and the retroactive sweep `TreeShardSplitGrain.RetroactiveSweepPreparedMutationsAsync`, which replays the source's prepared mutations onto the destination. `ShardRootGrain.TxTerminal` is the terminal fan-out that races them, not a forwarder. |
| `OrphanDrain(t,k)` | Discard of a late orphan bucket, terminal already applied | `MigrationTerminalCore.DecideBucketAction` returning `DiscardOrphan` - the leaf holds a pending bucket for a saga whose terminal has already landed here, so `BPlusLeafGrain.ApplyTxTerminalAsync` discards the bucket instead of draining it. The guards correspond exactly: the action's `terminal[t][k] # "none"` is the core's `alreadyTerminal`, and `pend' = "none"` with everything else `UNCHANGED` is the discard. **This action does not model the split coordinator's post-sweep cleanup pass, and no rewording of this row can make it do so**: that pass acts on a bucket whose terminal has *not* reached the leaf, which is the complement of this action's guard. See the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps). |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. |

## Property mapping

| Spec property | Code-level property it abstracts |
|---------------|----------------------------------|
| `AllOrNothing` / `VisibilityMatchesDecision` | The all-or-nothing visibility that `TxDecisionView` delivers by resolving every key of a fan-out against one registry snapshot. This is the invariant the reshard split-view bug (#1584) turned on. |
| `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never returning `SurfacePrepared` unless `status = Committed` - the strict-isolation default that an in-flight or aborted saga stays invisible. |
| `LinearizedTerminals` | The decision-before-broadcast ordering in `RunSagaAsync`: `RecordTerminalDecisionAsync` precedes `BroadcastTerminalsAsync`, so no leaf surfaces a committed value before the tree-wide decision exists. |
| `NoMixedTerminals` | A saga records exactly one `TxStatus`, so its per-leaf terminals are uniformly commit or uniformly abort. |
| `DecisionDurability` | `TxStatus` transitions are terminal: `MarkCommittedAsync` / `MarkAbortedAsync` never flip a recorded decision, and treat a repeat of the same outcome as an idempotent no-op for as long as the decision is still recorded - including while it is merely tombstoned, since classification runs before the tombstone is cleared. Once a tombstone has been physically purged the registry has no row to recognise, so a late same-outcome terminal records afresh rather than being absorbed; the recorded outcome is unchanged either way, which is what the spec property asserts. |
| `MonotonicVisibility` | A committed value never reverts to pre-saga - protected in code by the terminal-stable decision plus the orphan guard (`alreadyTerminal`) that stops a late shadow-forward bucket from re-hiding an applied value. The protection is conditional on the registry still holding the saga's decision row; a prepared write that is still resident when that row is physically purged reverts, which the spec does not express (see the decision-record retention window under [abstraction gaps](#deliberate-abstraction-gaps)). |
| `RevisionMonotonic` | The composite comparison token is monotonically non-decreasing. `DecisionsRevision` on its own only ever increments, but it is not the whole token, and the live-expired-tombstone term it is summed with falls when a batch prune retires several tombstones at once or when a snapshot pin un-masks already-masked rows; `TombstoneRetirementEpoch` and `TombstonePinUnmaskEpoch` compensate for exactly those two drops, so the sum never revisits a value it previously carried under a different readable surface. |
| `Termination` / `EveryCommittedKeyReadable` | The saga always drives to `Completed` / `Compensate` (reminder-driven resume after a crash), and a committed saga's terminal fan-out reaches every leaf recorded as a participant. It is not unconditionally every leaf holding a bucket: an online split can install a prepared bucket on a destination that the coordinator's participant query had already passed over, which is the orphan window `RetroactiveSweepPreparedMutationsAsync` documents and its post-sweep cleanup pass narrows but does not close. |

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
