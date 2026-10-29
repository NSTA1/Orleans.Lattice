# Trace validation for AtomicCommit: spike report

Issue #5159, item 11 of the formal verification pipeline epic (#5148). This is
research. It produces evidence and a recommendation, not a CI gate.

## Question

The refinement notes connect `AtomicCommit.tla` to production code by a
human-curated mapping, and detector tests pin each row. Nothing mechanically
checks that what production *does* is a behaviour the specification allows.
Trace validation does: record the protocol events a real run emits, map them
onto the specification's actions, and ask the model checker whether the
recorded sequence is a behaviour of the specification.

The spike asks whether that is worth doing here: what it costs to map, what it
costs to run, what it catches, and what it cannot see.

## What was built

| Piece | Where | What it does |
|-------|-------|--------------|
| Test-only event seam | `src/lattice/BPlusTree/AtomicCommitTrace.cs`, `IAtomicCommitTraceSink.cs` | A static `SinkForTest` field, `null` in production. |
| Coordinator hook | `AtomicWriteGrain.DecideSagaCommit` | Emits the resolved prepare outcome: the spec's `PrepareTx`. |
| Registry hooks | `TxRegistryGrain.MarkCommittedAsync` / `MarkAbortedAsync` / `ForgetAsync` | Emit a newly recorded decision (`DecideTx`) and a retired one (`ForgetDecision`), each only after the write is durable. Idempotent repeats emit nothing. |
| Leaf hooks | `BPlusLeafGrain.ApplyTxCommit` / `ApplyTxAbort` | Emit the keys of a drained or discarded pending bucket: one `BroadcastStep` per key, or one `OrphanDrain` per key when the leaf had already applied the terminal. |
| Recorder and mapping | `test/lattice/BPlusTree/TraceValidation/AtomicCommitTraceRecorder.cs` | Collects events, keeps one saga's, and maps the transaction id to `t1` and its keys, in ordinal order, to `k1`, `k2`. |
| Trace specification | the [two blocks below](#the-trace-specification) | Constrains `AtomicCommit`'s behaviours to the recorded steps. |
| Prototype test | `AtomicCommitTraceValidationTests` | Records one committed two-key saga on a test cluster, checks it, and checks three perturbed copies. |

### Zero cost when unused

Every emission site is `AtomicCommitTrace.SinkForTest?.OnX(...)`. With no sink
installed that is one static field load and a null test: C# does not evaluate
the arguments of a null-conditional call, so nothing is allocated and no
behaviour changes. The one site that needs a value from before the step it
reports (`ApplyTxAbort`, which must know whether the leaf had already applied
the terminal) reads the sink into a local and short-circuits on it before the
dedup-set lookup, so the unused cost is the same load and test.

## The trace specification

TLC or Apalache? **TLC**, for three reasons. The repository already pins and
provisions it (`tla2tools.jar` v1.7.4, JDK 17), so the spike adds no tool. The
trace is short and fully determined, so TLC explores exactly one state per event
and finishes in about a second, leaving nothing for Apalache's symbolic
encoding to win. And TLC's error trace is the readable rejection the issue asks
for. Apalache would earn its keep only for traces too long or too weakly
constrained for explicit-state search. These traces are neither; follow-up F2
below, which lets the specification take unobserved steps between events, is
the point to revisit that.

The approach is the standard one for TLC: a module that extends the
specification, adds a cursor `l` into the recorded `TraceLog`, and allows a step
only when it is the specification's own action for event `l` and lands in the
values the event recorded. Every event pins every choice its action makes (the
prepare event fixes the vote set), so an accepted trace of n events explores
exactly n + 1 distinct states; the harness requires that count, which stops an
initial predicate matching nothing (zero states, also a clean exit) from passing
as acceptance. The specification's safety invariants and its single-step and
whole-behaviour safety properties are checked along the trace too.

Neither block is a file under `spec/`. Any non-markdown file there is a TLC
verdict input (`tier-scope.py`'s `spec/*` glob) and would be discovered as a
module (`SpecModuleCatalogue`), so the prototype extracts them from this page,
by their fence info strings, into a scratch directory beside a copy of
`AtomicCommit.tla` and the generated `AtomicCommitTraceData` module. What is
shown here is exactly what runs.

```tla AtomicCommitTrace.tla
------------------------- MODULE AtomicCommitTrace -------------------------
(***************************************************************************)
(* Trace validation for AtomicCommit (issue #5159). Constrains the         *)
(* specification's behaviours to the steps of a recorded production trace, *)
(* TraceLog, supplied by the generated module AtomicCommitTraceData. TLC   *)
(* accepts the trace when it can consume every event; it rejects it by     *)
(* failing TraceAccepted at the first event no action admits.              *)
(***************************************************************************)
EXTENDS AtomicCommit, AtomicCommitTraceData, Naturals, Sequences, TLC

TxName  == (t1 :> "t1") @@ (t2 :> "t2")
KeyName == (k1 :> "k1") @@ (k2 :> "k2") @@ (k3 :> "k3")
Tx(s)  == CHOOSE t \in Txns : TxName[t] = s
Key(s) == CHOOSE k \in Keys : KeyName[k] = s

VARIABLE l
traceVars == <<vars, l>>

Ev == TraceLog[l]

PrepareEvent ==
    /\ Ev.event = "Prepare"
    /\ {KeyName[k] : k \in Written(Tx(Ev.tx))} = Ev.keys
    /\ PrepareTx(Tx(Ev.tx))
    /\ \A k \in Written(Tx(Ev.tx)) :
          vote'[Tx(Ev.tx)][k] = IF KeyName[k] \in Ev.acks THEN "ack" ELSE "nack"

DecideEvent ==
    /\ Ev.event = "Decide"
    /\ DecideTx(Tx(Ev.tx))
    /\ decision'[Tx(Ev.tx)] = Ev.outcome

TerminalEvent ==
    /\ Ev.event = "Terminal"
    /\ BroadcastStep(Tx(Ev.tx), Key(Ev.key))
    /\ terminal'[Tx(Ev.tx)][Key(Ev.key)] = Ev.outcome

OrphanDrainEvent ==
    /\ Ev.event = "OrphanDrain"
    /\ OrphanDrain(Tx(Ev.tx), Key(Ev.key))

ForgetEvent ==
    /\ Ev.event = "Forget"
    /\ ForgetDecision(Tx(Ev.tx))

TraceInit == Init /\ l = 1

TraceNext ==
    \/ /\ l <= Len(TraceLog)
       /\ \/ PrepareEvent
          \/ DecideEvent
          \/ TerminalEvent
          \/ OrphanDrainEvent
          \/ ForgetEvent
       /\ l' = l + 1
    \/ /\ l > Len(TraceLog)
       /\ UNCHANGED traceVars

TraceSpec == TraceInit /\ [][TraceNext]_traceVars

\* The saga state the rejected event was checked against, for the message.
SagaState(t) ==
    [phase |-> phase[t], decision |-> decision[t],
     terminal |-> terminal[t], forgotten |-> forgotten[t]]

TraceAccepted ==
    l <= Len(TraceLog) =>
        Assert(ENABLED TraceNext,
               <<"TRACE REJECTED: no AtomicCommit action admits event", l, TraceLog[l],
                 "in saga state", SagaState(Tx(TraceLog[l].tx))>>)
=============================================================================
```

```text AtomicCommitTrace.cfg
\* TLC configuration for the AtomicCommit trace specification (issue #5159).
SPECIFICATION TraceSpec

CONSTANTS
    t1 = t1
    t2 = t2
    k1 = k1
    k2 = k2
    k3 = k3

INVARIANTS
    TraceAccepted
    TypeOK
    AllOrNothing
    VisibilityMatchesDecision
    StrictIsolation
    CommitIntegrity
    LinearizedTerminals
    NoMixedTerminals

PROPERTIES
    DecisionDurability
    MonotonicVisibility
    RevisionMonotonic
```

The liveness properties (`Termination`, `EveryCommittedKeyReadable`,
`NoStrandedPrepare`) are deliberately absent: a recorded trace is a finite
prefix that ends in stuttering, and the trace specification carries no fairness,
so a liveness verdict on it would say nothing about production.

## Results

Measured on a Windows workstation (16 cores), JDK 17.0.21, TLC 2.19
(`tla2tools.jar` v1.7.4), `-workers 1`, over three consecutive runs of the
prototype.

### A recorded trace validates

The prototype writes two keys with `SetManyAtomicAsync` on a two-silo test
cluster. Production emitted five grain-level events; the keys sat on two leaves,
so each leaf reported one key:

```text
Prepare(alpha,beta;True) Decide(True) Terminal(alpha;True) Terminal(beta;True) Forget(False)
```

mapped to this trace, which TLC accepted with 6 distinct states for 5 events:

```text
[event |-> "Prepare", tx |-> "t1", keys |-> {"k1", "k2"}, acks |-> {"k1", "k2"}]
[event |-> "Decide", tx |-> "t1", outcome |-> "committed"]
[event |-> "Terminal", tx |-> "t1", key |-> "k1", outcome |-> "commit"]
[event |-> "Terminal", tx |-> "t1", key |-> "k2", outcome |-> "commit"]
[event |-> "Forget", tx |-> "t1"]
```

The second run recorded the two terminals in the other order, which the
specification also admits, and was accepted too. Along every accepted trace the
seven safety invariants and the three safety properties in the configuration
held, so the run also checked, on production's own states, that the decision
was recorded before any leaf applied a terminal and that the row was retired
only after every leaf had.

### A perturbed trace is rejected, readably

Three perturbations of the recorded trace, one per defect class the
specification's properties exist to forbid. Each was rejected at the event the
perturbation made illegal, never earlier and never later:

| Perturbation | Defect class | Rejected at |
|--------------|--------------|-------------|
| A terminal moved ahead of the decision | broadcast before decision (`LinearizedTerminals`) | event 2, the moved terminal |
| The cleanup moved ahead of the last terminal | early forget (`DecisionDurability`) | event 4, the cleanup |
| One participant nacks, the decision still commits | commit without unanimity (`CommitIntegrity`) | event 2, the decision |

TLC's report names the event and the saga state it was checked against, then
prints the behaviour up to that point:

```text
Error: Evaluating invariant TraceAccepted failed.
The first argument of Assert evaluated to FALSE; the second argument was:
<< "TRACE REJECTED: no AtomicCommit action admits event",
   4,
   [event |-> "Forget", tx |-> "t1"],
   "in saga state",
   [ phase |-> "committing",
     decision |-> "committed",
     terminal |-> (k1 :> "commit" @@ k2 :> "none" @@ k3 :> "none"),
     forgotten |-> FALSE ] >>
Error: The behavior up to this point is:
...
```

It says *which* event no action admits and the state it met, not *which guard*
of the action failed; for a two-line action that is enough to read the cause
off directly (here, `ForgetDecision` needs every written key's terminal, and
`k2`'s is `none`).

### Costs

| Cost | Measured |
|------|----------|
| TLC per trace check, process wall clock | 1.2 to 1.9 s (TLC itself reports under 1 s; the rest is JVM start and parsing) |
| States explored | n + 1 for a trace of n events |
| Prototype fixture, five tests including cluster start | about 13 s |
| Production hooks | 2 new files (a static field and a four-method interface), 6 emission sites of 1 to 5 lines in 3 grains |

Mapping effort per action, in the two places a mapping has to live:

| Spec action | Production event | Hook sites | TLA+ mapping | C# mapping | Notes |
|-------------|------------------|-----------|--------------|------------|-------|
| `PrepareTx` | coordinator's resolved prepare outcome | 1 | 6 lines | 1 line | Production reports the aggregate only (below). |
| `DecideTx` | registry records a new decision | 2 (commit, abort) | 4 lines | 1 line | Emitted after durability; idempotent repeats emit nothing. |
| `BroadcastStep` | leaf drains or discards a pending bucket | 2 (drain, discard) | 4 lines | 4 lines | One leaf step covering several keys becomes one spec step per key. |
| `OrphanDrain` | leaf discards a bucket behind an applied terminal | shared with the discard | 3 lines | 4 lines | Not exercised by the prototype. |
| `ForgetDecision` | registry tombstones or drops the row | 1 | 3 lines | 1 line | A tombstone maps to the forget: the spec abstracts the retention window. |
| `ShadowForwardOrphan` | a forwarder re-installs a prepare | not hooked | - | - | Needs hooks in both forwarders (`ShardRootGrain.Split`, `TreeShardSplitGrain`). |
| `RegistryMask` | none | not hookable | - | - | See the limits. |

The fixed scaffolding (identity maps, the cursor, `TraceNext`, the rejection
assertion) is about 25 lines of TLA+; the recorder, renderer and TLC runner
about 340 lines of test C#, beside a 170-line prototype fixture.

### What the spike found

- **The commit path conforms.** Production's ordering on the happy path matched
  the specification on every run. No discrepancy was found, which is the
  expected result on a path the detector tests already pin.
- **The prepare vote is not observable per participant.** The coordinator
  collapses its batched prepare to one boolean before it reaches
  `SagaCoordinatorCore`, so the hook can report only that, and the mapping has
  to invent a witness nack on an abort. `CommitIntegrity` is therefore checked
  against the coordinator's own summary, not against what each participant did.
- **Production has an abort no participant voted for.** The caller-gone rollback
  (`AtomicWriteGrain.RollBackUndecidedIfCallerGoneAsync`) can pivot a saga to
  compensation after every write was staged, and the compensation branch then
  feeds the core a nack (`DecideSagaCommit(everyParticipantPrepared: false)`)
  that no participant cast. The specification has no such behaviour - `DecideTx`
  aborts only on a nack, and `CommitIntegrity` says an aborted saga had one - and
  the `vote[t][k]` row of [Refinement.md](Refinement.md) describes a nack only as
  a write failure. It is not a safety problem (aborting is always safe), but it is
  a refinement inaccuracy. Trace validation would *not* have caught it with these
  hooks, because the hook reports the same synthetic nack the core receives; it
  surfaced while choosing the hook point. Recorded here as follow-up F3.

### Limits

- **Only discrete production events can be traced.** Six of the
  specification's seven non-stutter actions correspond to points in the code,
  and five are hooked here. `RegistryMask` does not: a tombstone ageing past its retention window is
  not an event anywhere, it is a classification each read makes against the
  clock, and a dial failure exists only inside the read that observed it. A trace
  that covers it has to let the specification take mask steps between recorded
  events, which turns trace checking from following one path into a (small)
  search.
- **The abort path was not driven.** No deterministic test drives an
  execute-phase failure through a real cluster; the prototype covers the commit
  path, and the three perturbations cover the abort-side defect classes only on
  the specification side.
- **Replay emits again.** The leaf hooks sit where the projection applies a
  terminal, so an activation that replays a terminal from the WAL would report a
  second terminal for the same key, which the specification rejects. A tracing
  run across a reactivation has to drop replay emissions (the leaf already knows
  when it is replaying).
- **One saga, fixed shape.** `AtomicCommit.tla` fixes `t1`'s participants as
  `{k1, k2}`, so the mapping accepts a two-key saga only; a wider saga, or two
  concurrent sagas, needs constants generated from the trace.
- **In-process only.** The seam is a static field, which reaches every silo of an
  in-process test cluster and nothing out of process.
- **Emission order is a linearisation, not a clock.** Events are appended when
  emitted, immediately after the step they report and before the emitting turn
  replies, so causal order is preserved; events with no causal order land in
  either order, which is only sound because the specification admits both.

## How to run it

The prototype is `AtomicCommitTraceValidationTests`. It needs a JVM and the
pinned `tla2tools.jar`, located the way the Formal harness locates them
(`TLA_TOOLS_JAR`, else `tools/tla2tools.jar`; `JAVA_HOME`, else `PATH`):

```powershell
$env:TLA_TOOLS_JAR = "<path>\tla2tools.jar"   # v1.7.4, SHA-256 936a2620...
$env:JAVA_HOME = "<path>\jdk-17"
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=TraceValidation"
```

It ignores itself, with the reason, when either is missing.

### No CI lane runs it

The fixture carries the `TraceValidation` category and NUnit's `[Explicit]`.
It is the `[Explicit]` that keeps it out of every lane, and it does so without a
change to any workflow:

| Lane | Filter shape (each also scoped by the lane's shard expression) | Tests of the fixture executed |
|------|-----------------------------------------------------------------|------|
| Deterministic tier (`plan-test-matrix.py` `TIERS`) | `TestCategory!=Chaos&TestCategory!=Coyote` | 0 |
| Coyote tier | `TestCategory=Coyote` | 0 (not matched) |
| Chaos tier | `TestCategory=Chaos&TestCategory!=AzureStorageEmulator` | 0 (not matched) |
| Nightly coverage (`plan-coverage-matrix.py` `COVERAGE_FILTER`) | `TestCategory!=Chaos&TestCategory!=Coyote&TestCategory!=Tlc` | 0 |
| `ci.yml` base filter | `TestCategory!=Chaos&TestCategory!=AzureStorageEmulator` | 0 |

Measured with `dotnet test --no-build`, each lane filter additionally narrowed
with `FullyQualifiedName~AtomicCommitTraceValidationTests` so the run stayed
small: VSTest matched the fixture's five tests where the shard expression
covers it, and NUnit3TestAdapter 6.2.0 executed none of them (`Total: 0`); the
category-positive Coyote and chaos tiers do not match it at all. A filter
containing a negation never selects an explicit test, and every CI lane's filter
for `test/lattice` contains one: the package is sharded, so it never runs
unfiltered, and an unfiltered run would not select an explicit test either. A purely positive selection -
`TestCategory=TraceValidation`, a bare `FullyQualifiedName~` name filter, or the
local Tier 3 loop's `TestCategory=Integration|TestCategory=Docs` - does run it;
no workflow uses one that matches it.

Why not a filter clause instead: excluding a category in every lane means
editing `plan-test-matrix.py`, `plan-coverage-matrix.py`, `ci.yml` and their
self-tests, and every workflow edit currently costs a full exhaustive TLC run.
`[Explicit]` gives the same exclusion with none of that, and the `Integration`
category it also carries satisfies `IntegrationCategoryHygieneTests` for a
fixture that starts a cluster.

## Recommendation

**Go for a bounded follow-up; no-go for a CI gate now.**

Mechanically, trace validation works here and is cheap: five of seven actions
mapped in a few lines each (a sixth needs only two more hooks), a check takes about a second, a recorded trace
validated first time and every perturbation was rejected at the right event with
a readable report. What it adds over the detector tests is not on the happy path,
which those tests already pin and which conformed. It is on paths whose
interleavings a human mapping cannot enumerate - reshard shadow-forwards, orphan
drains, and aborts racing a split - and the prototype does not reach them yet.
A gate built on the commit path alone would cost a JVM in a deterministic lane
and check what is already checked.

Proposed follow-up issues:

- **F1. Trace the reshard paths.** Hook the two shadow-forwarders
  (`ShadowForwardOrphan`), drop replay emissions, and record traces from the
  existing topology fixtures (`ShardSplitTopologyTests`, `ReshardTopologyTests`),
  where `OrphanDrain` and concurrent sagas occur. This is where the method can
  find something the detectors do not.
- **F2. Let the trace specification take unobserved `RegistryMask` steps**
  between recorded events, and generate the participant constants from the
  trace, so wider and concurrent sagas validate.
- **F3. Model or document the caller-gone abort.** Either add an abort with no
  nack to `AtomicCommit.tla` (and weaken `CommitIntegrity`'s second conjunct
  accordingly) or record it under the deliberate abstraction gaps of
  [Refinement.md](Refinement.md), and report real per-participant votes to the
  trace if the former.
- **F4. Revisit gating after F1.** If the reshard traces find a discrepancy or
  stay clean across the topology fixtures' interleavings, decide then whether a
  sampled trace check belongs beside the atomic TLC shard, which already
  provisions the JVM.
