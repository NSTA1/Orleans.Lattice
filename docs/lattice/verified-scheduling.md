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
coordinator startup without resetting its progress. These production defects are
distinct from the already-fixed historical mechanisms retained as
[paired scheduling controls](formal-coverage.md#scheduling-foundation-and-historical-controls).

## Related

- [Formal coverage](formal-coverage.md) - the area census and acceptance gates.
- [Verified WAL concurrency](verified-wal.md) - durability and WAL decision cores.
- [WAL saturation signal](wal-saturation-signal.md) - classifier and consumer contracts.
- [WAL tuning](wal-tuning.md) - operational capacity and back-pressure sizing.
- [Scheduling specifications](../../spec/scheduling/README.md) - checked bounds,
  mutation experiments, refinement mappings and TLC execution.
