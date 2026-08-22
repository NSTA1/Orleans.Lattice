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
| `decision[t]` | Tree-wide commit / abort decision | `TxRegistryState.Decisions[txid]` (`TxStatus` = `InFlight` / `Committed` / `Aborted`), read through `TxDecisionView`. Absent txid resolving to `InFlight` is the spec's default `decision = "inflight"`. |
| `terminal[t][k]` | Per-leaf applied terminal + orphan-guard flag | The leaf's `_recentlyTerminal` / applied-terminal state after `AppendTxTerminalAsync`; `terminal # "none"` is `AtomicVisibilityGate.ResolveKey`'s `alreadyTerminal` input. |
| `pend[t][k]` | Hidden pending bucket on a leaf | The leaf `_pendingTx[txid]` bucket installed by a prepared mutation (`BPlusLeafGrain.PendingTx`). |
| `orphanDone[t][k]` | Bounded reshard-orphan budget | Modelling device only (keeps the state space finite). Corresponds to the split sweep's own post-sweep cleanup pass draining an orphan bucket at most once per key. |
| `revision` | Monotonic registry revision | `TxRegistryState.DecisionsRevision`, bumped on every `Decisions` mutation and used by reader fast paths as a cheap version probe. |

## Action mapping

| Spec action | Protocol step | Code counterpart |
|-------------|---------------|------------------|
| `PrepareTx(t)` | Prepare fan-out | `AtomicWriteGrain.PrepareAsync` + `ExecutePhaseAsync`: stage every write into per-leaf pending buckets (hidden), collecting per-key ack / nack. |
| `DecideTx(t)` | Record the single terminal decision | `AtomicWriteGrain.RecordTerminalDecisionAsync` -> `ITxRegistryGrain.MarkCommittedAsync` / `MarkAbortedAsync`. Commit iff every participant acked; this write is issued **before** the broadcast - the linearization point. |
| `BroadcastStep(t,k)` | Per-leaf terminal fan-out (one leaf at a time) | `AtomicWriteGrain.BroadcastTerminalsAsync` -> per-shard / per-leaf `AppendTxTerminalAsync`. Modelling it one leaf per step is what lets TLC explore the post-decision window in which some leaves have flipped and others have not. |
| `ShadowForwardOrphan(t,k)` | Reshard shadow-forward of a stale prepared write | The online shard-split sweep (`ShardRootGrain.TxTerminal` / `ShardRootGrain.Split`) forwarding a prepared write to a destination leaf that already applied the terminal, orphaning a pending bucket. |
| `OrphanDrain(t,k)` | Post-sweep orphan cleanup | The sweep's cleanup pass that drains an orphan pending bucket before the registry decision's tombstone TTL elapses. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. |

## Property mapping

| Spec property | Code-level property it abstracts |
|---------------|----------------------------------|
| `AllOrNothing` / `VisibilityMatchesDecision` | The all-or-nothing visibility that `TxDecisionView` delivers by resolving every key of a fan-out against one registry snapshot. This is the invariant the reshard split-view bug (#1584) turned on. |
| `StrictIsolation` | `AtomicVisibilityGate.ResolveKey` never returning `SurfacePrepared` unless `status = Committed` - the strict-isolation default that an in-flight or aborted saga stays invisible. |
| `LinearizedTerminals` | The decision-before-broadcast ordering in `RunSagaAsync`: `RecordTerminalDecisionAsync` precedes `BroadcastTerminalsAsync`, so no leaf surfaces a committed value before the tree-wide decision exists. |
| `NoMixedTerminals` | A saga records exactly one `TxStatus`, so its per-leaf terminals are uniformly commit or uniformly abort. |
| `DecisionDurability` | `TxStatus` transitions are terminal: `MarkCommittedAsync` / `MarkAbortedAsync` treat repeat same-outcome calls as idempotent no-ops and never flip a recorded decision. |
| `MonotonicVisibility` | A committed value never reverts to pre-saga - protected in code by the terminal-stable decision plus the orphan guard (`alreadyTerminal`) that stops a late shadow-forward bucket from re-hiding an applied value. |
| `RevisionMonotonic` | `DecisionsRevision` is monotonically non-decreasing. |
| `Termination` / `EveryCommittedKeyReadable` | The saga always drives to `Completed` / `Compensate` (reminder-driven resume after a crash), and a committed saga's terminal fan-out reaches every touched leaf. |

## Deliberate abstraction gaps

These are modelled abstractly or not at all, by design; the Coyote cores and
the reshard chaos suite cover them at the implementation level:

- **No serialization, timers, HLC, or WAL.** The spec has no wall-clock; the
  tombstone / TTL "hidden" branch of `AtomicVisibilityGate.ResolveKey`
  (prepared value hidden by a tombstone or expiry) is out of scope, so
  `ObservedPrepared` models only the commit / abort visibility dimension.
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
