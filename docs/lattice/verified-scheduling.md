---
agent_spec: "docs/agents/invariants.yaml"
---

# Verified Scheduling and Progress

Protocol safety does not by itself establish progress. A WAL append can obey
every durability rule while its caller remains parked behind an admission gate;
a leaf can preserve every record while a timer occupies the grain turn needed
to read it. Scheduling assurance makes those resource and execution boundaries
explicit instead of assuming that a correct protocol action will run.

The scheduling specifications live together in
[`spec/scheduling/`](../../spec/scheduling/README.md). Their manifests and the
area README's checked Counts table are the authority for the bounded instances
and current census. The [formal coverage index](formal-coverage.md) places this
area alongside the existing protocol models; neither area replaces the other.

## The production boundary

Scheduling models distinguish a decision from the operation it enables.
Releasing a saturation waiter is not assigning it an admission-semaphore slot,
and admitting an append is not completing its storage flush. Likewise,
dispatching background work is not awaiting that work inside a non-reentrant
grain turn. A refinement mapping must preserve these distinctions.

The checked scheduling seams are:

| Module | Boundary represented |
| --- | --- |
| `TurnOccupancy` | A non-reentrant turn, queued calls, and the difference between awaiting an operation inside the turn and dispatching it outside the turn. |
| `WalAdmission` | Partition-scoped admission capacity, parked saturation waiters, paced recovery release, sampling and wait-budget outcomes. |
| `WalBackPressure` | Cached tree/partition classification, writer/saga/snapshot/receiver/view responses and signal-independent relief. |
| `ReplayPermits` | Physical permit conservation, memory withholding, class-sensitive queue admission, bounded warm drives and intentionally shared cold-owner lifetimes. |
| `ReplicationShipping` | FIFO physical-source windows, pump-local feedback, fail-closed receiver retention and conditional deferral progress. |
| `MaterialiserDrain` | Online-copy slots, leaf replay/checkpoint/pin progress, finite-budget WAL shutdown, cooperative view service and stable-identity live routing. |
| `SagaReadProgress` | Consistent multi-key decision cuts, bounded gated fallback, pressure-independent terminals and original caller acknowledgement after completion durability. |
| `TagIndexReconcile` | A single durable probe/repair sweep, successful baseline publication, per-phase tree work and timer/reminder recovery. |

`TurnOccupancy` uses the turn-holder, queued-call and awaited-completion
vocabulary of the existing
[`SplitLinkTurns` and `SplitGateCycles` models](../../spec/bplustree/README.md).
Those models describe split-specific dependency cycles; the separately
discovered scheduling module describes occupancy by a timer or an
environment-controlled await. Sharing a vocabulary does not make the two
checked state spaces equivalent.

The runtime contracts remain in
[WAL saturation back-pressure](wal-saturation-signal.md) and
[the WAL design](wal.md). In particular, the saturation queue's oldest-first
release contract must not be read as a global FIFO guarantee over every writer
or over the separate admission semaphore.

## Production detectors and the Coyote tier

A TLA+ mutation is evidence about the abstract specification, not about a C#
call site. Each behaviour-asserting refinement row therefore names production
symbols and a detector test. A corresponding production perturbation must make
that detector fail; merely resolving the symbol or passing the unmodified test
is not evidence that the test can observe the mapped behaviour.

Core scheduling detectors may live under
`test/lattice/BPlusTree/Coyote/` or in existing grain-fixture partials under
`test/lattice/BPlusTree/Grains/`. Systematic Coyote cases retain the `Coyote`
category and follow the existing
[Coyote tier](verified-atomic-commit.md#the-coyote-concurrency-tier), including
negative controls, and use the production decision seam rather than a second
implementation of the algorithm. Deterministic time-dependent checks use the
shared manual clock instead of sleeping until a wall-clock deadline.

The module refinement notes identify which production tests cover each
property, which historical defects a mutation reproduces, and what remains
outside the abstraction. A historical-defect experiment demonstrates detection
of that mechanism; it does not assert that every manifestation of the original
incident is reproduced or that an empirical throughput problem is resolved.

## TLA+ scope and progress assumptions

Ticks are abstract observations or execution rounds, not milliseconds. Weak
fairness says that a continuously enabled action eventually occurs. It supplies
no bound on how many other transitions can happen first. Strong fairness also
does not establish a bounded response time. A bounded progress claim must name
its counting unit and the additional premise that actually bounds service.

Environment progress assumptions, such as eventual storage completion and
continued sampling, are distinct from the scheduling decision being checked.
Adding fairness to the admission or release action under test would assume away
the starvation that the model is meant to detect. A production loop that performs
release as part of each sample should be represented as that coupled step, not
as an independently scheduled action made fair without justification.

A finite-backlog release claim also needs continued qualifying samples: a
renewed saturation episode can pause release. Progress from release to admission
needs available semaphore capacity, and progress from admission to completion
needs storage to respond. None of these steps implies that an unbounded offered
load will eventually leave the classifier permanently `Healthy`. Refinement
notes must state their finite-cohort and stable-environment premises separately
instead of hiding those premises in a generic fairness assumption.

Random jitter does not guarantee that two participants eventually choose
different phases on every possible execution. A nondeterministic model includes
the execution in which their choices keep matching. A deterministic
non-starvation guarantee needs a load-bearing arbitration mechanism; a claim
about random decorrelation needs an explicitly probabilistic argument instead.

All checked instances are finite. Larger-bound variants increase evidence at
the documented bounds, not a proof for arbitrary callers, partitions or leaves.
These models do not predict operations per second, latency percentiles, storage
performance or appropriate default settings. The benchmark rig and deployed
telemetry remain the evidence for those quantities.

## Tag-index reconciliation progress

The tag reconciler distinguishes an idle coordinator from probe and repair
phases. Probe-only sweeps do not perform repairs or bank a successful-repair
baseline. An active sweep keeps its progress across failures and overlapping
triggers; an on-demand request does not reset its cursor to start a fresh sweep.
Baseline publication requires complete successful repair RPCs and the durable
checkpoint, not merely a matching subject fingerprint or an attempted repair.

The chunk unit is a tree in a phase callback: a probe callback examines at most
`max(1, ChunkSize)` trees, while a repair callback processes one whole tree.
That tree can require multiple row scans and storage operations. A synchronous
`RunSweepAsync` drains multiple callbacks, so the chunk setting does not bound
its caller turn, total rows or elapsed time. Eventual repair assumes subjects
stop changing, runtime/reminder opportunities continue, and registry, storage
and checkpoint operations eventually succeed.
An unsuccessful keepalive registration must remain retryable; an in-progress
flag is not evidence that a timer or reminder is actually armed.

## WAL back-pressure progress

The classifier combines partition depth with tree-wide acute trip sums and
sustained input windows. It publishes the worst tree state and an ordered cause.
A recovery-only Throttled floor can legitimately carry cause None. A refusal
instead identifies its writer, saga or snapshot-open seam. Zero recovery
disables the floor; Infinite retains it. Writer, saga and view operations use
their physical source identities; snapshot-open and receiver lookups retain
the supplied logical identity, which may have no observed physical-copy signal.

Plain writers wait only on their own partition and re-read after wait expiry.
Throttled point/batch pacing does not imply saturation refusal. Sagas quiesce
captured partitions, fall back to the tree when that capture is unusable,
bound the effective wait by the dispatch budget and refresh changed routing
before redispatch. A refusal preserves resumable Execute progress rather
than compensating. Snapshot shedding, when enabled, is Saturated-only. Receivers
and physical projection/aggregation views preserve Healthy batches, scale
Throttled batches and drip-feed Saturated batches; receiver pauses are hints.

Flush, materialiser drain, GC trim, checkpoint, durable-pin publication,
saga terminals and terminal-tail resumption cannot depend on the signal they
help relieve. The writer classifies a whole terminal-only operation before
partition fan-out; a mixed-data batch is not relief. Bypassing signal admission
and pacing does not bypass capacity, durable storage, caller cancellation,
deadlines or shutdown. Terminal-tail resumption sends commit/abort without
new data; resumed ordinary data is still a quiesced producer. Materialiser
drain advances leaf replay/checkpoint work, not merely storage shutdown-drain.

Finite offered work, stopped acute inputs (including sustained flush latency)
and actual relief opportunities permit departure from Saturated. Reaching
Healthy additionally requires stopped throttling inputs and finite recovery.
A checkpoint or terminal cannot erase a sampled pin-write latency fault.
These are explicit conditional progress claims, not bounded latency or
deterministic fairness from jitter. See the
[refinement mapping](../../spec/scheduling/WalBackPressureRefinement.md)
for the shared-window episode abstraction and the real Coyote/detector boundary.

## Saga and reader progress

Bounded optimistic retries fall back to an unrenewed decision gate rather than
requiring an empty interval in a continuously offered writer stream. A valid
gate and available stable routing permit a consistent batch; expired lease
or lost coverage must fail explicitly. The model separates successful answers
under those premises from eventual resolution when the environment invalidates
the hold. It does not assert starvation-free admission among unbounded readers.

A durable decision is not caller success. Terminals can arrive in arbitrary
leaf order and do not depend on saturation. The completion checkpoint must then
be durable before the original caller is acknowledged. Real storage success
and failure controls reject dropping that await, not merely an incorrect
in-memory phase. See the
[refinement](../../spec/scheduling/SagaReadProgressRefinement.md) for the finite
instances, all retained temporal checks, source-negative certificates and
cooperative production Coyote boundary.

## Materialiser drain progress

Online snapshot shard-copy slots, admitted leaf replay and WAL-shard shutdown
are different drain units. MaxConcurrentDrains bounds the first;
BackgroundDrainLeavesPerPass bounds a copy pass. A WAL-shard shutdown drain
has its own finite WalDrainBudget and may give a pending follow-on batch a
fresh budget. That is not a deadline for every leaf teardown barrier.
InfiniteTimeSpan is an explicit opt-out.

Leaf replay respects the exclusive head. Checkpoint publication follows
consumption; a durable pin cannot exceed either the persisted checkpoint or
durable snapshot coverage. Admission completion and eventual storage success
are separate premises. A cold owner is not assigned a new residency deadline.

Views retain positive service under all supplied pressure arms. Offered load
is bounded between successful service opportunities, not sampler ticks.
Backlog rows are not the classifier's oldest-unread-age/window unit.
Rebuild/reconcile refresh a split source's live logical
topology without changing physical identity or restarting the cursor.
See the [refinement](../../spec/scheduling/MaterialiserDrainRefinement.md)
for the sum-of-scenarios boundary, real detectors and progress premises.

## Replay permit ownership and progress

Returning a caller wait, stopping a warm drive and releasing a physical replay
permit are separate events. A client deadline cannot abandon a shared cold
owner. Genuine cancellation of the owner's independent lifetime can end its
read-only provider awaits; permit return still follows owner-frame unwind,
including required prefix banking and durability writes. Detached provider
results are not consumed and late faults are observed.

Available, held and memory-withheld permits account for the physical ceiling.
Memory pressure keeps at least one permit circulating; restoration returns only
capacity actually withheld. Class priority uses observed depth and the floored
Bulk margin, not a contractual FIFO guarantee from the admission semaphore.

Conditional progress requires returned physical capacity, delivered runtime
opportunities and durable banking of an earned replay prefix. PartialPrefix
needs multiple successful slices to finish one coverage gap. The warm-drive
budget does not guarantee cold-owner termination, provider cooperation or
bounded elapsed time. The model deliberately retains permanently hung holders.

The [area description](../../spec/scheduling/README.md#replay-permits) separates
the full configurations, exact one-target model pairs and actual production
regressions from the repaired temporal-equivalence probe. Its Coyote case calls
the production memory/admission seams, rather than implementing a second ledger.

## Replication shipping and receiver progress

Shipping keeps launch-window capacity, ordered acknowledgement folding and
receiver retention separate. A rejected FIFO head does not grant cursor
progress over a later success. Cursor monotonicity is scoped to one physical
source; explicit rebind/reseed is a separate boundary.

The actual back-pressure consumer receives a logical-name classification.
Batch-only and pause feedback are optional observations, not a second
classifier. An observed reduction stops replenishment for this pump, including
held-terminal release; a later healthy acknowledgement cannot erase an active
pause or the pump-local stop. Already launched work remains in flight.

Receiver deferral is not an unconditional timeout-to-acknowledgement promise.
Eligible expiration needs another retry, real elapsed-time opportunities,
retention capacity and a stable receiver lifetime. An authoritative decided
prepare stays unacknowledged until application succeeds rather than being
poisoned to force progress. Full dead-letter, poison and quarantine storage
fail closed. Restarts preserve durable safety but restart volatile deferral
age; retries alone do not advance time. The retained retirement-marker bound
limits the no-reseed-cycle claim. No jitter-based fairness or wall-time bound
is asserted.

The [area description](../../spec/scheduling/README.md#replication-shipping)
separates the full state-space measurements, one-target pairs and real
production/Coyote detector evidence. The revised complete harness passed both
full configurations and all twenty-six pairs in 2745.4195361 monotonic seconds.
Its accepted launcher-artifact completion is not a recovered CLI terminal event,
loaded-DLL attestation or peak-RSS measurement. CI routing does not compose
those certificates. The matrix prices the five scheduling items from passing TRX
evidence in ci.yml run 40658688360 (head 92832b77): the dedicated TwoPartitions
item is 41.15 minutes (job 122717148551), Shipping base/pairs are 6.17 (job
122717148735), and FourRecords is 18.72 (job 122717148680). The passing 192/192
general item (job 122717148586) is split into 106/86-case groups priced at
21.03/23.04 minutes by four-worker LPT reconstruction of its case durations, not
measured work-item times. Four workers is a reconstruction assumption, not an
attested runner trace. The complete source run passed, including rebootstrap job
122717148595. Historical run 40588708711's rebootstrap job 122488444001 failed
under #5116; that failure remains recorded and contributes no timing. The
estimates are planning inputs, not runtime guarantees.

## Defects found during the bucket

The production review found a saturation waiter's cancellation registration
could be installed after completion, retaining its callback and references
(#4979). The fix gives completion and registration installation an explicit
ownership handshake and disposes outside the signal's locks.

The same bucket found that a tag-repair cursor advanced before fallible work
finished (#4976). The repair now advances only after success, restores
uncommitted checkpoint and terminal state after persistence failure, and emits
completion only after the terminal write succeeds. Incomplete required reads
could also certify an unchanged fingerprint and suppress later repair (#4977);
those reads now fail explicitly. A failed initial keepalive registration could
strand a durably in-progress sweep (#4985), so a later schedule retries
coordinator startup without resetting its progress. Separately, cancellation of
a tracked demand could abandon an already-active scheduled sweep before the
demand started its own work (#4992). Cancellation cleanup now requires ownership
of the new sweep, preserving the earlier sweep's phase, cursor and pending
baseline when ownership has not transferred. These production defects are
distinct from the already-fixed historical mechanisms retained as
[paired scheduling controls](formal-coverage.md#scheduling-foundation-and-historical-controls).

The back-pressure review found terminal-only commit/abort writes could wait
on saturation or Throttled pacing while their prepares pinned the work needed
for recovery (#4986). Four real point/batch terminal cases failed before the
operation-wide bypass; mixed-data, capacity/storage and shutdown controls retain
their contracts. Real reminder-driven terminal-tail controls separately reject
quiesce at resumption, while ordinary resumed data remains gated.

Two harness defects were repaired without weakening acceptance: ordered comment
lexing preserves live declarations after XML paths containing comment delimiters
(#4990), and the gap-citation gate reads the leading verdict rather than None
or Partial embedded in a resolving detector identifier (#4994). Both have
full-gate assertion-red/green controls. A missing Throttled view-model response
and insufficient terminal-resumption and quiesce-call-site detectors were
corrected with independent negative controls before the final certificate.

## Related

- [Formal coverage](formal-coverage.md) - the area census and acceptance gates.
- [Verified WAL concurrency](verified-wal.md) - durability and WAL decision cores.
- [WAL saturation signal](wal-saturation-signal.md) - classifier and consumer contracts.
- [WAL tuning](wal-tuning.md) - operational capacity and back-pressure sizing.
- [Scheduling specifications](../../spec/scheduling/README.md) - checked bounds,
  mutation experiments, refinement mappings and TLC execution.
