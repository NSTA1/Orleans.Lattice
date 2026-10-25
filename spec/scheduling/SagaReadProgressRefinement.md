# Refinement note: saga and reader scheduling progress

## Model scope in one paragraph

A continuously reusable opaque saga prepares, records a durable decision,
delivers terminals individually in arbitrary leaf order, durably checkpoints
completion and then acknowledges its caller.
One multi-key reader captures a decision view, reads leaves independently and
validates the view. After bounded optimistic retries it holds a finite,
unrenewed decision gate. An optional expiry invalidates the read, never silently
certifies it. AtomicCommit supplies protocol safety (acknowledged prepares,
write-once decisions and visibility resolution); this module checks the
scheduling seam rather than re-proving that protocol. WalBackPressure supplies
the cached scoped verdict consumed by data admission. Terminal and completion
opportunities are independent of that verdict.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `registryVersion`, `snapshot`, `crossed` | Finite version identity and a ghost witness of any intervening commit, including wrap-around. | `TxRegistryGrain.SnapshotWithRevisionAsync`, `ReaderStabilityGate.ClassifySnapshot`. |
| `reader`, `attempts`, `observed` | Fan-out continuation, bounded optimistic attempts and the returned batch. | `LatticeGrain.GetManyAsyncCore`, `LatticeGrain.GetManyDecisionGatedAsync`. |
| `held`, `leaseLeft` | Unrenewed read hold; zero marks lost certification. | `TxRegistryGrain.AcquireReadCaptureGateAsync`, `TxRegistryGrain.ReleaseCaptureGateAsync`. |
| `saga`, `drained`, `materialised` | Opaque prepared/decided lifecycle and irreversible per-leaf drain. | `AtomicWriteGrain.RunSagaAsync`, `BPlusLeafGrain.ApplyTxTerminalAsync`. |
| `checkpointed`, `completed`, `terminalWitness` | Distinct durable completion checkpoint, caller acknowledgement and ghost witness of all terminal deliveries; in-memory Phase alone is not durability. | `AtomicWriteGrain.CompleteSagaAsync`. |
| `walState` | Current cached scoped back-pressure verdict. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`, `WalSaturationSignal.GetCurrentState`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `ReaderOpportunity` | Deterministic capture/read/validate/fallback continuation; guards are disjoint. | `LatticeGrain.GetManyAsyncCore`, `LatticeGrain.GetManyDecisionGatedCoreAsync`. | Yes: `BPlusLeafGrainTests.GetManyAsync_continuous_commits_and_irreversible_drains_complete_atomically_under_a_bounded_gate`, `BPlusLeafGrainTests.Sustained_writer_delivery_cannot_exhaust_the_actual_GetMany_reader`. |
| `WriterOpportunity` | Data prepares obey pressure; new decisions obey the live reader gate. | `AtomicWriteGrain.QuiesceOnSaturatedAsync`, `TxRegistryGrain.MarkCommittedAsync`. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`, `TxRegistryGrainTests.Gate_refuses_a_new_commit_decision_as_retryable`. |
| `TerminalOpportunity` | Deliver any undrained leaf's terminal, persist completion, or acknowledge the caller, regardless of saturation. | `AtomicWriteGrain.BroadcastTerminalsAsync`, `AtomicWriteGrain.CompleteSagaAsync`. | Yes: `AtomicWriteGrainTests.ExecuteAsync_decision_is_not_caller_completion_until_terminals_and_checkpoint_finish`, `AtomicWriteGrainTests.ExecuteAsync_original_caller_waits_for_completion_checkpoint_durability`, `AtomicWriteGrainTests.Saga_delivery_opportunities_complete_the_actual_caller_after_decision_and_terminal`, `AtomicWriteGrainTests.Terminal_resumption_choices_never_wait_for_pressure`. |
| `Sample` | Environment changes the cached signal input; no classifier is reimplemented here. | `WalSaturationSignal.UpdateState`, `AtomicWriteGrain.QuiesceOnSaturatedAsync`. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`. |
| `Expire` | Environment lapses the finite lease; a lost hold cannot certify a result. | `TxRegistryGrain.ReleaseCaptureGateAsync`, `LatticeGrain.GetManyDecisionGatedCoreAsync`. | Yes: `BPlusLeafGrainTests.GetManyAsync_registry_high_water_widening_during_the_gated_read_fails_closed`, `TxRegistryGrainTests.GetCaptureGateSnapshotAsync_fails_closed_after_expiry_or_reactivation`. |
| `Stutter` | No observable step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
| --- | --- | --- |
| `ReadSnapshotConsistent` | A returned batch belongs to one decision cut, despite irreversible drains between leaf reads. | Yes: `BPlusLeafGrainTests.GetManyAsync_continuous_commits_and_irreversible_drains_complete_atomically_under_a_bounded_gate`, `BPlusLeafGrainTests.Sustained_writer_delivery_cannot_exhaust_the_actual_GetMany_reader`. |
| `CallerOnlyAfterTerminals` | Durable decision telemetry is not caller success; terminal delivery and completion checkpoint must finish. | Yes: `AtomicWriteGrainTests.ExecuteAsync_decision_is_not_caller_completion_until_terminals_and_checkpoint_finish`, `AtomicWriteGrainTests.ExecuteAsync_original_caller_waits_for_completion_checkpoint_durability`, `AtomicWriteGrainTests.Saga_delivery_opportunities_complete_the_actual_caller_after_decision_and_terminal`. |
| `QuiesceNeverAdmitsDuringSaturation` | A still-saturated captured data partition refuses without consuming Execute progress; this does not gate terminal relief. | Yes: `AtomicWriteGrainTests.Quiesce_choices_preserve_captured_scope_expiry_reread_and_resumable_progress`, `AtomicWriteGrainTests.ExecuteAsync_signal_Saturated_past_budget_does_not_dispatch_SetManyAsync`. |
| `NoDecisionUnderReadGate` | New local decisions cannot cross a live read hold; repeating a decision that predates the hold is permitted. | Yes: `TxRegistryGrainTests.Gate_refuses_a_new_commit_decision_as_retryable`, `TxRegistryGrainTests.Gate_admits_a_repeat_of_a_decision_recorded_before_it`. |
| `ReaderEventuallyAnswers` | Writer rate alone cannot cause repeated retry exhaustion; with available stable routing and a valid gate, the fallback returns a consistent batch. | Yes: `BPlusLeafGrainTests.GetManyAsync_continuous_commits_and_irreversible_drains_complete_atomically_under_a_bounded_gate`, `BPlusLeafGrainTests.Sustained_writer_delivery_cannot_exhaust_the_actual_GetMany_reader`. |
| `ReaderEventuallyResolves` | A lost lease/coverage fails explicitly rather than looping forever or certifying mixed data. | Yes: `BPlusLeafGrainTests.GetManyAsync_registry_high_water_widening_during_the_gated_read_fails_closed`, `BPlusLeafGrainTests.GetManyAsync_original_reader_resolves_at_deadline_before_stalled_gate_delivery`. |
| `SagaCompletionVisible` | Decided sagas finish terminals and acknowledge the actual caller when those collaborators complete. | Yes: `AtomicWriteGrainTests.ExecuteAsync_decision_is_not_caller_completion_until_terminals_and_checkpoint_finish`, `AtomicWriteGrainTests.ExecuteAsync_original_caller_waits_for_completion_checkpoint_durability`, `AtomicWriteGrainTests.Saga_delivery_opportunities_complete_the_actual_caller_after_decision_and_terminal`, `AtomicWriteGrainTests.Terminal_resumption_choices_never_wait_for_pressure`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Model representation sanity, not a production representation; both sampled-input and expiry mutations target it. |

## Property classification (issue #2321)

TypeOK, ReadSnapshotConsistent and CallerOnlyAfterTerminals are state safety.
QuiesceNeverAdmitsDuringSaturation and NoDecisionUnderReadGate are action safety.
The three eventually properties are temporal liveness. ThreeLeaves retains
every safety and temporal check: measured cost does not justify omitting
liveness at that bound. LeaseLoss retains resolution and saga completion;
ReaderEventuallyAnswers is inapplicable after an environment-invalidated lease,
not silently weakened to acceptance.

Every mutation is an anchored action perturbation, not a transition splice.
TypeOKInvalidSample and TypeOKExpiryCorruptsLease perturb Sample and Expire.
ReadSnapshotConsistentMixedVersions, ReaderEventuallyAnswersPureRetry and
ReaderEventuallyResolvesNeverCaptures perturb ReaderOpportunity.
QuiesceNeverAdmitsDuringSaturationQuiesceSkipped and
NoDecisionUnderReadGateGateIgnored perturb WriterOpportunity.
CallerOnlyAfterTerminalsEarlyCompletion, CallerOnlyAfterTerminalsBeforeCheckpoint,
SagaCompletionVisibleDecideWithoutTerminal
and SagaCompletionVisibleNoCallerAcknowledgement perturb TerminalOpportunity.
The expiry experiment enables MayExpire only in its mutant arm; its clean
control retains the base configuration.

## Detectors

The reader harness calls the real LatticeGrain, registry and two real leaves.
Every optimistic fan-out races a new durable commit and an irreversible drain
on one leaf before its sibling reads. The fallback must actually refuse a new
decision, return a whole batch and release its hold. Coyote chooses successful
versus lapsed gate delivery using the same production harness. Caller tests
drive the real ExecuteAsync dispatch, separately hold a terminal RPC and the
completion storage write pending, then deliver them and observe the ORIGINAL
caller. The checkpoint cases cover successful durability and provider failure;
neither accepts an in-memory Completed phase as success. The Coyote companion varies duplicate
public deliveries and asserts both idempotence and caller completion.

These are cooperative message-boundary experiments, not rewritten-async
infinite fairness proofs. No test equates a phase counter with caller return.
### Actual source-negative experiments

Each experiment changed the real production source in this worktree, marked
SOURCE PERTURBATION, rebuilt the affected binary and ran the named detector.
Each was restored with a fresh timestamp and rebuilt before its clean control;
no preserved-timestamp copy, no mutant binary reuse and no changed test oracle.
All ten experiments failed actual assertions (fourteen failing test cases),
not compilation, TLC parsing or a test-runner timeout. Session evidence logs
are named below; the final named-method census also reruns every detector.

| Source perturbation | Actual detector failure | Rebuilt clean control |
| --- | --- | --- |
| Replace `LatticeGrain.GetManyAsyncCore` gated fallback with the historical retry-exhaustion exception. | Reader Coyote: `[ReaderEventuallyAnswers] writer rate alone cannot exhaust retries`; `source-retry-exhaustion.log`. | `saga-provisional-certification.log`, reader pass; later `control-reader.log`, 2/2. |
| Pass Stable unconditionally to `ReaderStabilityGate.Decide`. | Real two-leaf batch assertions and reader Coyote `[ReadSnapshotConsistent]`; `source-mixed-snapshot.log`, 2 failures. | `control-reader.log`, 2/2. |
| Remove `TxRegistryGrain.ThrowIfDecisionGated` guard. | Registry assertion expected retryable refusal, got none; reader Coyote fallback/refusal count; `source-gate-bypass.log`, 2 failures. | `control-gate.log`, 2/2. |
| Remove expiry comparison in `TxRegistryGrain.GetCaptureGateSnapshotAsync`. | Expired local snapshot assertion expected GateLapsed exception, got none; `source-expiry.log`. | `control-expiry.log`, 1/1. |
| Ignore invalid release coverage unless cancellation is also requested. | Real GetMany high-water-widening assertion expected typed outcome-unavailable, got success; `source-coverage-loss.log`. | `control-coverage-loss.log`, 1/1. |
| Discard rather than await `WriteSagaStateAsync("complete")`. | Both held-checkpoint cases: original caller IsCompleted=True before durability; `source-checkpoint-await.log`, 2 failures. | `control-checkpoint-await.log`, 2/2. |
| Omit commit `BroadcastTerminalsAsync`. | Caller Coyote `[CallerOnlyAfterTerminals]` catches false success without any terminal; `source-terminal-gap.log`. | `control-terminal-gap.log`, 1/1. |
| Replace ready post-decision continuation with an infinite pending delay. | Caller Coyote `[SagaCompletionVisible]` catches the original outstanding caller immediately; `source-lost-delivery.log`. | `control-lost-delivery.log`, 1/1. |
| Replace data quiesce outcome with Proceed. | BP Coyote scoped-wait assertion, plus real saturated-budget no-SetMany assertion receives a forbidden dispatch; `source-quiesce-bypass.log`, `source-saturation-admission.log`. | `control-quiesce.log`, 2/2. |
| Drop outer `.WaitAsync(deadline.Token)` around gated acquisition. | Original reader deadline assertion loses to its watchdog before late gate delivery; `source-reader-deadline.log`. | `control-reader-deadline.log`, 1/1, including late hold cleanup. |

The deliberately stranded continuation is a synthetic fault injection, not an
identified historical implementation of #3581. The detector fails immediately
on the original Task, not on a completion counter, an eventual runner timeout
or a rewritten task scheduler. The reader watchdog is only a test failsafe;
its production deadline is advanced by the manual clock and its negative
fails a named NUnit progress assertion, then releases the late acquisition.
All experiments restored production exactly; no production change is needed
for these already-landed guarantees.

### Model coverage corrections retained as evidence

The first model's fixed round let the mixed-snapshot mutant stay green
(`saga-tlc-first.log`, ReadSnapshotConsistentMixedVersions). That is a coverage
defect, not evidence for consistency. Removing the round allowed arbitrary
relative deliveries and made that same mutation fail its named invariant.
Review then removed a second artificial order: least-leaf-first terminal drain.
Every undrained leaf is now eligible, matching independent Task.WhenAll delivery.
Completion durability and caller acknowledgement are also separate steps, with
an additional early-before-checkpoint mutant. No fairness is needed among leaf
choices: every delivered terminal strictly shrinks the finite undrained set.
These are assurance corrections, not newly discovered production failures.

## Defect mutations whose fixes have landed

ReaderEventuallyAnswersPureRetry reproduces the retry-exhaustion mechanism in
closed [#4563](https://github.com/NSTA1/Orleans.Lattice/issues/4563), fixed by
[PR #4758](https://github.com/NSTA1/Orleans.Lattice/pull/4758). Its detector uses
the actual shipped fallback, not a second reader algorithm.

SagaCompletionVisibleDecideWithoutTerminal represents the decision-versus-caller
gap observed in closed [#3581](https://github.com/NSTA1/Orleans.Lattice/issues/3581).
That incident report lists several hypotheses, not a proved missing-terminal
root cause. This mutation tests the reported failure shape; it must not be
described as reproduction of an established root cause or an ACA throughput
certificate. BP's reviewed terminal-tail resumption boundary is retained.

## Replaced draft assertions

An unbounded fair writer rate says nothing about storage completion, gate lease
validity, topology stability or fair admission among competing readers. The
successful-answer property assumes available collaborators and a live gate.
The separate resolution property covers expiry and typed refusal. A producer
may offer work forever and no producer-fairness premise requires an empty
registry interval. The finite registry identity is paired with a crossing
witness so version wrap-around cannot manufacture a stable read.

Weak fairness is applied only to delivered continuation opportunities, in
arbitrary relative order: each delivery executes the production-selected
disjoint branch. No fairness is placed on Acquire,
Decide, Validate, Prepare or a choice between competing scheduling policies.
This is not a production round-robin promise or a bounded wall-clock result.
Independent arbitrary cached signal changes never block already-decided
terminals. Data dispatch is forbidden only by the consumed scoped Saturated
verdict: a wait that recovers before dispatch, including Throttled recovery,
can proceed. The full classifier, wait budget, routing refresh and terminal
relief contracts remain WalBackPressure's responsibility.

## Reviewed dependency boundary

These detectors consume the reviewed WalBackPressure bootstrap at
f62ba058f9dab86f587cc9a9dca98d1a1051de4a (PR #4997): terminal-only operations and
actual reminder-driven terminal tails bypass pacing, while resumed data stays
gated. The Saga member owns only its model/refinement and production detectors,
not that dependency's commits or the parent's shared index/counts edits.

## Bounds and measured cost

The base checks 3 state invariants and 5 properties over 2 leaves, 1 optimistic
retry and a finite lease: 2,451 distinct states / 17,838 generated. ThreeLeaves
raises leaves to 3 and retries to 3: 14,076 distinct / 106,179 generated, with
ALL base assertions including liveness. LeaseLoss enables optional arbitrary
lease loss: 3,585 distinct / 26,124 generated; successful answer is inapplicable
under that invalidation, while explicit resolution and caller completion remain.
The manifest has 6 Next actions, 11 paired mutations and 12 behaviour rows;
Stutter is the only non-behavioural action. No helper module is omitted.

Measured direct TLC wall time, including JVM startup: base 1.627 s, ThreeLeaves
5.907 s, LeaseLoss 1.980 s. Each used two workers on two visible processors,
configured Xmx1800m with ParallelGC (TLC reports 1600 MB usable heap and 64 MB
offheap; neither is a measured resident-memory claim). Java 17.0.20.1 and pinned
tla2tools.jar SHA256 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88.
The paired harness is limited with LATTICE_TLC_CONCURRENCY=1. Actual build
contention was observed with sibling builds: two source rebuilds exceeded
120 seconds while their test bodies took under a second. This is an observation,
not a reason for an exclusive global slot or a larger local cohort.

## Deliberate abstraction gaps

One reader with finite leaves, one reusable saga and fixed routing; no arbitrary
reader queue, cross-tree authority RPC, host shutdown, process crash, permanent provider fault
or caller cancellation. Local D0 ownership and all-registry coverage validation
are supplied by the real fallback and independently tested fan-out. The model
does not claim starvation-free admission among infinitely many competing
readers; production bounds each caller's wait and reserves a writer-open window.
It does not re-prove AtomicCommit safety, WAL storage capacity or classifier
recovery. Composition is by these explicit interfaces, not a cross-product of
the independent protocol state spaces. Finite-instance checking does not prove
arbitrary N, ops/s, or wall-clock latency.
