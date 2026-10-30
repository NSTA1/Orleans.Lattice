---
agent_spec: "docs/agents/api/lattice.json"
---

# Formal Coverage

Orleans.Lattice checks its most concurrency-sensitive protocols formally, not
only with prose and chaos tests. Epic #4430 extended that coverage from the
atomic-commit protocol to five more areas. The B+ tree topology census now also
checks cascading splits through a fan-out-three instance reaching its six-level
bound, and durable leaf-reclaim recovery through successor back-link repair.
This page indexes what each area checks, how much, which production defects the
checking found, and, just as prominently, what each area does **not** cover.

## How every area is checked

Each area follows the pattern the
[atomic-commit specification](verified-atomic-commit.md) set:

- **TLA+ modules checked by TLC in CI.** They live under
  [`spec/`](../../spec/README.md), one directory per area. Every pull request
  that runs the core `test/lattice` suite keeps a base-model smoke check. Exhaustive bases, variants and
  mutants run when the diff touches a TLC verdict input, on every release-line
  pull request, on integration-branch and release-line pushes, and nightly on
  `main`; outside release lines, the nightly run and a dispatch with receipts
  switched off, a module whose fingerprint already has a trusted verdict receipt
  is not rerun. The [Formal Verification Pipeline](formal-verification-pipeline.md)
  describes every lane.
- **A mutation for every property and every action.** Each is a deliberate
  defect that must make TLC report a violation, so no property holds
  vacuously and no protocol step goes unchecked.
- **A refinement note per module.** It maps every variable, action and property
  to the production code that plays its role, and to the detector tests meant to
  go red when that code deviates. Gates check that every named symbol exists and
  every cited detector is a test CI runs. Whether a detector actually goes red is
  re-checked mechanically only for rows that have a production perturbation,
  which the [weekly perturbation lane](formal-verification-pipeline.md#weekly-production-perturbations)
  applies.
- **Production detectors, with pure-core and Coyote tiers where applicable.**
  The established concurrency cores also run under systematic Coyote
  interleaving with negative controls. The view, anti-entropy and lifecycle
  additions check bounded TLA+ protocol abstractions mapped to production grain
  and integration tests; they do not claim a new pure-core or Coyote proof.

The structural Formal gates discover every module from disk and check its
manifest, mutation coverage and refinement mappings independently of whether
CI selects exhaustive TLC. The acceptance census (#4442) adds two more gates:

- every behaviour-asserting row of every refinement note must read `Yes`, with no
  `Partial`, `None`, gap or assumption-only verdict;
- no note may cite an open issue as the place a gap is tracked.

## Areas

Figures are derived from module manifests and refinement notes, and the build checks
every one of them: `FormalCoverageAreaCensusTests` re-derives each row from the
modules it counts, and requires every module under `spec/` to be counted exactly
once, either here or in the
[focused materialised-view companions](#focused-materialised-view-companions).

| Area | Modules | Mutations | Behaviour rows | Detector tests | Page |
|------|---------|-----------|----------------|----------------|------|
| Single-cluster atomic commit | 1 | 21 | 17 | 40 | [Verified Atomic-Commit](verified-atomic-commit.md) |
| WAL durability lifecycle | 2 | 61 | 57 | 140 | [Verified WAL](verified-wal.md#the-tla-specification-and-the-scope-of-the-assurance) |
| Shard ownership | 5 | 124 | 124 | 201 | [Verified Shard Ownership](verified-shard-ownership.md) |
| Cross-cluster atomic visibility | 1 | 66 | 43 | 150 | [Verified Atomic-Commit: the replicated half](verified-atomic-commit.md#scope-the-single-cluster-protocol-and-the-replicated-half-checked-separately) |
| Replication convergence | 4 | 84 | 64 | 195 | [Replication: formal verification](../lattice.replication/architecture.md#formal-verification) |
| Backup and restore | 5 | 71 | 78 | 154 | [Verified backup and restore](../lattice.backup/verified-backup.md) |
| B+ tree topology | 6 | 100 | 140 | 62 | [B+ tree topology](../../spec/bplustree/README.md) |
| Materialised-view tail and lifecycle | 1 | 22 | 27 | 30 | [Materialised views](../../spec/materialised-views/README.md) |
| Tree lifecycle composition | 2 | 46 | 66 | 48 | [Resize, reshard and shadow cutover](../../spec/tree-lifecycle/README.md) |
| Anti-entropy remediation | 1 | 32 | 39 | 59 | [Digest-driven repair](../../spec/anti-entropy/README.md) |
| Shared alias routing and revert | 2 | 25 | 40 | 22 | [Aliasing](../../spec/aliasing/README.md) |
| Tenant quotas | 3 | 29 | 26 | 32 | [Quota admission](../../spec/tenant-quotas/README.md) |
| Tenant rate budgets | 1 | 14 | 23 | 22 | [Rate-budget leases](../../spec/tenant-rate-budget/README.md) |
| Tenant isolation | 1 | 25 | 25 | 31 | [Isolation epochs and leases](../../spec/tenant-isolation/README.md) |
| Scheduling and progress | 10 | 196 | 244 | 119 | [Verified Scheduling and Progress](verified-scheduling.md) |
| Schema enforcement and versioning | 5 | 74 | 97 | 186 | [Schema enforcement and versioning](../../spec/schema/README.md) |

**Behaviour rows** are the rows of a refinement note's action and property tables
that assert a production behaviour. **Detector tests** are the distinct tests those
rows cite.

The scheduling census includes the GC core and clocked witness. In
[CI run 40588708711](https://github.com/NSTA1/Orleans.Lattice/actions/runs/40588708711),
all 221 scheduling cases passed on GC member head `9be5db45`: ten base
configurations, fifteen variants and 196 paired mutations. All four scheduling
TRXs report zero failed or unexecuted cases; their 221 unique actual names
match discovery from the ten exact-head manifests without omission or duplication.
This is an exact scheduling-only executed certificate, not overall CI success
or final bucket compatibility: the overall run failed outside those jobs.
The cases are not 221 JVM launches. Targeted clean controls check TypeOK plus
their single named target; matching-bound and deadlock controls can add launches.

## Scheduling foundation and historical controls

The scheduling foundation consists of two separately discovered modules, not an
unchecked helper plus one module. Its checked census is 23 mutations, 32
behaviour-asserting refinement rows and ten distinct production detector
members. `TypeOK` remains a paired model-domain check; it is not counted as a
production behaviour row.

| Module | Base states | Variant states | Mutations | Behaviour rows | Detector tests |
|--------|-------------|----------------|-----------|----------------|----------------|
| `TurnOccupancy` | 961 | 29,791 (`ThreeGrains`) | 7 | 11 | 2 |
| `WalAdmission` | 86,496 | 370,345 (`ThreeCallers`) | 16 | 21 | 8 |

The two bases, two variants and 23 paired mutation experiments passed as 27 TLC
harness cases in a measured two-CPU, one-TLC-at-a-time run of 2 minutes 22
seconds. The WAL base took 18 seconds and its variant 7 seconds. These are
observations of the checked instances, not CI deadlines or runtime latency
claims. Base and mutant WAL configurations use the same three-unit wait
deadlines; a later-tick recovery witness must remain possible in the clean
control instead of being erased by an earlier budget expiry.

The historical experiments below retain already-fixed mechanisms as negative
controls. They are not new discoveries of the original incidents and do not
claim to reproduce every manifestation of the original incidents.

| Module | Historical repair | Mutation / target | Checked boundary |
|--------|-------------------|-------------------|------------------|
| `TurnOccupancy` | #4949, fixed by #4950 | `NoAwaitedDriveAwaitDriveOnExclusiveTurn` / `NoAwaitedDrive` | Awaiting the background drive can occupy the non-reentrant turn; dispatch must return the turn while storage remains pending. |
| `WalAdmission` | #3402, fixed by #3404 | `PacedRecoveryReleaseRecoveryHerd` / `PacedRecoveryRelease` | A recovery sample caps saturation-gate releases; released callers still need partition capacity and storage progress. |
| `TagIndexReconcile` | Merged member PR #4978; #4976 remains open until integration | `RepairCursorOnlyAfterCompletionCursorFirst` and `RepairCursorOnlyAfterCompletionFailedCheckpoint` / `RepairCursorOnlyAfterCompletion` | A failed repair RPC or checkpoint cannot consume the current tree or certify durable progress. |

The classifier recovery window is tree-wide even though admission capacity and
waiter queues are partition-scoped. An idle sibling receives the Healthy floor
only when the raw tree verdict is Healthy. Independent acute inputs in the
bounded model are a conservative abstraction, not a claim that production
provider counters or recovery windows are partition-local. The full mappings
and explicit progress premises live in the
[scheduling refinement notes](../../spec/scheduling/README.md).

### Tag-index reconciliation scheduling

`TagIndexReconcile` adds 15 mutations, 19 behaviour-asserting rows and eight
distinct detector members to the scheduling foundation. Its manifest records
three invariants, six properties and twelve `Next` actions, including stutter.
The base has 2,784 states; the larger `ThreeTrees` safety instance has 11,930.
`TypeOK` remains paired domain safety, outside the production behaviour census.

The model separates an idle coordinator from probe and repair work, checks that
only complete successful repair and checkpoint publication bank a baseline,
and covers retrying the coordinator after failed keepalive registration.
Progress assumes finite subject changes, continued runtime/reminder
opportunities and eventually successful registry/storage/checkpoint operations.
A phase callback probes at most the configured
tree count, clamped to at least one, or repairs one whole tree. This is not a
bound on rows, storage time, caller-turn occupancy or a synchronous full sweep:
`RunSweepAsync` can drain several phase callbacks.

The real cursor/checkpoint and incomplete-read repairs in #4976/#4977 remain
part of the checked boundary. #4985 adds recovery after initial keepalive
registration failure. The exact action/property/mutation and detector mappings
live in [TagIndexReconcileRefinement](../../spec/scheduling/TagIndexReconcileRefinement.md).

### WAL back-pressure scheduling

`WalBackPressure` adds 34 mutations, 38 behaviour-asserting rows and 36
distinct detector methods. Five methods are shared with WalAdmission, so the
four-module scheduling aggregate has 49 distinct methods, not 54. TypeOK keeps
its independent paired experiment without becoming a production behaviour row.
The base has 135,464 states; Bursty, BurstyFinite, Sticky and SagaReroute have
262,606, 368,446, 658,316 and 1,593,944. Every variant retains safety;
the three temporal checks stay at the base bound.

The cached classifier sums acute trip deltas across the tree, applies cause
priority independently of refusal provenance, and floors a raw Healthy tree
during recovery. Physical writer/saga/view consumers differ from supplied
logical snapshot/receiver names. Healthy, Throttled and Saturated responses,
captured saga scope, expiry re-reads, routing refresh and seven signal-independent
relief actors have real production detectors. Coyote executes the actual
coordinator and writer decision seams, including reminder-driven terminal tails.
It does not enumerate CLR-thread schedules.

Terminal-only operations bypass signal admission and pacing, not downstream
capacity, storage acknowledgement, cancellation, deadlines or shutdown.
Ordinary resumed data remains quiesced. Conditional progress requires finite
offered work and stopped acute inputs; Healthy additionally needs stopped
throttling inputs and finite recovery. Neither fairness nor pacing is a
wall-time guarantee. The [area measurements and premises](../../spec/scheduling/README.md#wal-back-pressure)
distinguish the 328.81-second five-config measurement from the complete
39-case paired run's 729.46 seconds. Neither is the combined shard cost.

This member repairs the terminal dependency cycle (#4986) and the refinement
comment/verdict harness defects (#4990/#4994). Its source/model audit also
rejected an omitted Throttled view response before certification. The
[refinement history](../../spec/scheduling/WalBackPressureRefinement.md#defect-mutations-whose-fixes-have-landed)
records real assertion-red controls and restored source, not just model
counterexamples or resolvable test identifiers.

### Saga and reader scheduling

`SagaReadProgress` adds eleven paired mutations and twelve behaviour rows.
Its thirteen distinct production methods reuse two WalBackPressure methods,
so the five-module aggregate contains sixty distinct methods, not sixty-two.
The base, ThreeLeaves and LeaseLoss instances have 2,451, 14,076 and 3,585
states. The larger instance retains every base safety and temporal check;
lease loss retains explicit resolution and caller completion instead of
claiming a successful answer after invalidation.

The real LatticeGrain/registry/two-leaf harness retains #4758's shipped
fallback and rejects historical retry exhaustion (#4563), mixed reads and
lost coverage. Actual caller controls separately hold terminal delivery and
the completion checkpoint, observing success or the precise provider fault.
The original fixed delivery-round model let a mixed-snapshot mutant remain
green; removing that ordering made the same invariant fail. Review also removed
least-leaf-first terminals and separated durable checkpoint from caller return.
These are corrected assurance defects, not newly discovered production bugs.

See the [area boundary](../../spec/scheduling/README.md#saga-and-reader-progress)
and [refinement](../../spec/scheduling/SagaReadProgressRefinement.md) for all
ten source perturbations, fourteen assertion-red cases and restored controls.
The cooperative Coyote cases execute production delivery choices; they do not
prove CLR-thread fairness, arbitrary reader-queue admission or #3581's
historical deployment root cause.

### Materialiser drain scheduling

`MaterialiserDrain` adds twenty-one mutations and thirty-three
behaviour-asserting rows. Its sixteen detector methods share one existing
WalBackPressure detector and no additional Saga detector, so the six-module
aggregate has seventy-five distinct methods, not seventy-six.
The base and Larger instances contain 1,076 and
3,780 distinct states. The [area boundary](../../spec/scheduling/README.md#materialiser-drain)
separates online-copy concurrency, admitted leaf durability, finite-budget WAL
shutdown, service-opportunity backlog and stable-identity live routing.

The Coyote companion executes real WalLogSubscriber/ViewBackpressure cores,
not a duplicate scheduler. Grain controls cover checkpoint failure, pin
publication, exclusive-head replay, bounded copy passes and both rebuild and
reconcile entry points. The old slot-cap detector was false-green under a
real cap-plus-one perturbation; its strengthened first-wave assertion rejects
that same source change. The real online-slice entry-point detector also
rejects an increased LeafWalkBudget that standalone copy/resume tests missed
(#4999). Finite work and completing collaborators remain
premises, not conclusions supplied by fairness.

### Replay permit scheduling

`ReplayPermits` adds twenty-nine paired mutations and thirty-eight behaviour
rows. Its sixteen distinct detector methods do not overlap the prior six-module
set, so the seven-module aggregate is 133 mutations/172 rows/91 methods.
The three full configurations retain all temporal branches at 695064,
2762408 and 1038664 distinct states. The
[area description](../../spec/scheduling/README.md#replay-permits) states the
physical-owner, warm-drive and earned-prefix progress premises without imposing
an unconditional lifetime bound on shared cold work.

### Replication shipping scheduling

`ReplicationShipping` adds 26 one-target mutations and 33 behaviour rows.
Its 20 distinct detector names overlap the prior seven-module census by four,
including the Healthy receiver method whose source-negative evidence is new.
The prior eight-module union is 159 mutations/205 rows/107 methods, not a sum
of per-source-evidence ownership counts. The base and FourRecords full
configurations retain all four invariants and twelve properties at 527689 and
3306850 distinct states. The
[area description](../../spec/scheduling/README.md#replication-shipping) records
the finite FIFO window, shared classifier consumer, conditional deferral and
fail-closed storage assumptions.

### GC starvation-drive source census and evidence boundary

The GC core adds 23 mutations and 26 behaviour rows. The clocked witness adds
14 mutations and 13 behaviour rows. It also retains three model-to-model
refinement obligations outside the production-behaviour census. The core cites
fourteen distinct production detector members: twelve are new to the prior eight-module
union and two are shared. The witness cites eleven members of that same set,
so it adds no further distinct production detectors. The resulting source union
is 196 mutations, 244 behaviour rows and 119 detector members.

The authenticated CFF B2-B7 evidence covers six paired cases and eighteen
retained arms: twelve clean controls, five named action violations and one
ShareRespected invariant violation. Each pair includes its exact matching-bound
clean control, including the two-leaf, Ceiling-two reservation case. This is
historical six-pair evidence, not the complete GC selection. The later scheduling
jobs in run 40588708711 passed all 221 exact-manifest cases on `9be5db45`,
including the complete GC selection;
the overall run was red. Neither the six pairs alone nor those member-job
outcomes establish final bucket compatibility.
The [GC scope](verified-scheduling.md#gc-starvation-drive-progress) retains the
finite-load, capacity, runtime and timely-banking premises; jitter and fairness
do not establish bounded latency.

## Focused materialised-view companions

The tail row in the area census above is one lifecycle model, not a claim that
it checks every view interaction. Nine focused companions supply separate
replay, admission, physical identity, projection, joint-repair and
schema-independent numeric contracts. Together with the tail they have ten
modules, 186 mutations, 232 behaviour rows and 215 TLC harness cases, derived
from their checked manifests.

| Module | Base states | Variant states | Mutants | Harness cases | Production refinement |
|--------|-------------|----------------|---------|---------------|-----------------------|
| `MaterialisedViewFence` | 194 | 359 (`NewerOrdinary`) | 9 | 11 | Yes, bounded replay/rebind |
| `MaterialisedViewHorizon` | 16,792 | 29,932 (`FullScan`) | 23 | 25 | Yes, retained witness/horizon |
| `MaterialisedViewAggregation` | 24,072 | 7,176 (`Delete`) | 18 | 20 | Yes, bounded numeric/history seams |
| `MaterialisedViewRebind` | 21,086 | 8,143 (`NoCrash`) | 20 | 22 | Yes, captured physical identity |
| `MaterialisedViewReadyClosure` | 46,846 | 39,651 (`History`); 31,577 (`TwoConsumers`) | 12 | 15 | Yes, transitive local closure |
| `MaterialisedViewSourceAdmission` | 22,720 | 836 (`CrossTree`); 16,412 (`History`); 660 (`PendingSource`); 1,028 (`PendingOverlay`) | 21 | 26 | Yes, logical tuple/participant admission |
| `MaterialisedViewResolvedIdentity` | 196 | 52 (`SameLog`); 60 (`FailedCandidate`); 49 (`LostCandidate`) | 16 | 20 | Yes, actual-log cache ownership |
| `MaterialisedViewJointRepair` | 11,006 | 144 (`Legacy`); 4,572 (`SingleTarget`); 16,437 (`TwoFaults`) | 25 | 29 | Yes, conditional joint lifecycle |
| `MaterialisedViewNumeric` | 8,208 | 44,134 (`Swap`); 38,698 (`TwoFaults`) | 20 | 23 | Yes, schema-independent Count/Sum extraction |

**Yes is a bounded seam mapping, not a machine-checked implementation proof.**
The full refinement acceptance/resolver audit passes without waived Pending or
Partial rows. Each note identifies exact production detectors, paired negative
controls and the assumptions that remain outside its finite abstraction.

The fence checks an established two-batch prefix, source-HLC winners at different
WAL arrival offsets, unread replay after staging empties, fresh activation and
live retirement of every old fence component when rebinding to an empty source.
The horizon separates prepare partitions p0/p2 from terminal partitions p0/p1.
One retained positive batch-size witness suffices for regular orphan recovery,
but every distinct terminal and the consumed post-terminal horizon are required.
Terminal-only history cannot invent cardinality; losing every prepare witness
is outside convergence. Abandoned batches have their own terminal-triggered
rebuild path. Source decision, row installation and terminal delivery remain
distinct; the model relaxes the producer's stronger decision-before-any-terminal
order and does not certify fabricated terminals or whole-tree snapshot scans.
See the [replay/horizon scope](../../spec/materialised-views/README.md#focused-replay-and-horizon-companions).

The aggregation checks independent numeric contributions, retract/add arithmetic,
durable per-group publication obligations and ordinary/committed history revisions
after a fixed captured floor. Public Count/Sum and omission controls are separate
from local dependency closure; neither certifies every numeric projection or
cross-tree snapshot. Rebind separates capture, scan, validation and publication:
an alias move after validation may publish the coherent captured tuple and heal
later, never relabel the scan with a fresh alias. Actual rebuild/GC controls hold
both immutable bound and candidate floors through rejected and uncertain
publication responses, with controlled registry/transport collaborators rather
than a cluster-durability proof. The resolved-identity companion separately checks
two physical logs reusing a transaction ID, actual-tail cache ownership, both
cache structures, same-log rebuild and failed-before-commit versus committed-lost
responses. Rebind alone lacks that reuse dimension. See the
[aggregation/rebind scope](../../spec/materialised-views/README.md#focused-aggregation-and-physical-rebind-companions)
and [resolved identity](../../spec/view-resolved-identity/README.md).

ReadyClosure checks crossed independent clocks on a replacement-dependency ring,
whole transitive local publication, external eligibility distinct from consumed
heads, history and lost-response replay. SourceAdmission distinguishes coherent
HLC/bytes/tombstone identity from byte equality, source-apply lag, dropped
ordinary progress from applied replacement, and current-state from accumulative
history. Its pending overlay resolves the bounded captured decision and newer
undrained prepare, not just the older materialised row. Actual maintainer caller
controls independently reject local source-admission and expected-peer bypasses.
Post-observation head controls change a head inside the real source read, so a
premature sample cannot pass merely because the head happened not to move.
Neither model promises source-wide transaction-atomic capture or simultaneous
independent consumers. See the [staging composition contract](../../spec/view-staging/README.md).

JointRepair preserves original intent after response loss, freezes fresh
contributions/physical targets, fences ordinary publication until mandatory
repair completes, and orders durable checkpoint retirement before acknowledgements
and cleanup. Actual fresh-capture controls require a six-key transitive closure,
current source HLC witnesses and consumed post-observation heads. Separate real
paired-view detectors hold publication boundaries, change source after freeze,
lose completion/ack responses and reactivate maintainers; independent view opens
can still straddle a valid joint commit. Backend atomicity is a separate rely.
Legacy original-contract completion expires automatically without fabricated
fresh certification. Rolling patch onboarding pauses new RPCs/holds until all
non-dead members advertise capability, then resumes automatically; public
maintenance observes the same admission fence. No manual migration, unsupported
old-binary rejoin or downgrade is assumed safe.
See [joint repair and automatic upgrade](../../spec/view-joint-repair/README.md).

Recovery claims assume finite faults, eventual peer/source recovery, fair
maintenance and sufficient staging capacity. Permanent unavailability stays
fail-closed; arbitrary retention gaps, complete projection encodings, join/view
DAGs and a shared multi-view snapshot cursor are not checked. Composing named
seams is not an exhaustive Cartesian product of every feature.

Every companion has a dedicated deterministic TLC shard. Existing independent
one-JVM/two-visible-CPU measurements, including startup and distinct paired
controls, price Fence at 48.434 seconds, Horizon at 196.4, Aggregation at 95.417,
Rebind at 117.545, ReadyClosure at 94.200, expanded SourceAdmission at 239.838,
ResolvedIdentity at 61.385 and JointRepair at 97.221. A later isolated Horizon
run passed the same 25 cases in 113.521 seconds; the planner retains the earlier
conservative measured weight rather than claiming a performance improvement.
Heap ceilings were 2 GiB for Fence/Horizon and 1,800 MiB for the other companions;
peak live heap was not measured. These are local observations, not predicted CI
deadlines. Exhaustive TLC is validated in CI's sharded runs; local iteration uses
the structural and production-detector scopes rather than serially repeating it.

## What each area covers, and what it does not

Coverage of one area must not be read as coverage of another. Each area's page,
and each module's refinement note under its section on deliberate abstraction
gaps, states its full scope.

- **WAL durability lifecycle** ([`spec/wal/`](../../spec/wal/README.md)).
  - **Covers:** a leaf's lifecycle under crash-anywhere recovery, across one WAL
    partition shared by two leaves, including snapshot and row loss and a purge;
    and a WAL shard move with a durable fence, a shard crash and the loss of its
    coordinator.
  - **Does not cover:** more than two partitions; splits, resharding and saga
    state; replication consumers, beyond their effect on the trim floor.
- **Shard ownership** ([`spec/shard-ownership/`](../../spec/shard-ownership/README.md)).
  - **Covers:** who serves a key across an adaptive split, an online shard
    fold, an online reshard, an online resize and its undo, with stale routers
    and an atomic write bound to one physical copy; a fold back onto the shard a
    split took the slot from, with the survivor's sealed rows, their
    last-writer-wins import, reclaim, cancel and retirement; the transaction
    registry's retention, late forwarded prepares and leaf reactivation;
    CRDT-mode keys across the same moves; and an atomic write bound across a
    shadow-cutover restore.
  - **Does not cover:** other alias moves, a fold back whose stamps disagree
    with real time, and any cross-cluster replication of ownership moves.
- **B+ tree topology** ([`spec/bplustree/`](../../spec/bplustree/README.md)).
  - **Covers:** a donor-to-sibling split and parent-link crash window; right-edge
    parent splits from three to six levels in the checked instance; interleaved
    writes through a bounded leaf split and fold; sibling-chain and parent-route
    publication; and durable retirement, route removal, successor back-link
    repair, clearing, and recovery for a reclaimed leaf; and the grain-turn
    wait-for graph of a split linked from inside the donor's capture turn,
    including a parent overflow whose seeding calls back into that donor, the
    leaf's persist-write gate, and its clear latch. Range ownership during
    adaptive shard split, online reshard and resize is already covered by the
    `ShardOwnership` models above.
  - **Does not cover:** an unbounded-height proof (the level-indexed model is
    checked only through six levels), simultaneous cascading split propagation,
    the orphan audit/repair algorithm itself, or the full reshard/resize protocol
    in this module. See the refinement notes.
- **Cross-cluster atomic visibility** ([`spec/atomic-commit/`](../../spec/atomic-commit/README.md),
  module `AtomicCommitCrossCluster`).
  - **Covers:** the receiver side of an atomic write replicated to a peer: the
    per-source-shard terminal tally, the cross-tree receiver barrier, every way
    production loses a record to a peer, and a receiver's bootstrap and
    cross-tree import.
  - **Does not cover:** the single-cluster protocol, which the `AtomicCommit`
    module covers, separately and from before this epic. Keys a peer's key
    filter excludes are outside the all-or-nothing guarantee.
- **Replication convergence** ([`spec/replication/`](../../spec/replication/README.md)).
  - **Covers:** plain replication over a lossy, duplicating, reordering transport:
    the cycle-break, duplicate suppression, the bootstrap handoff, causal
    dependencies and their low watermark, and an in-place re-bootstrap after
    the source reaped a delete, including the bootstrap drop floor.
  - **Does not cover:** atomic-write sagas carried over replication (the
    cross-cluster area covers them), anti-entropy repair, and more than one WAL
    partition per cluster.
- **Backup and restore** ([`spec/backup/`](../../spec/backup/README.md)).
  - **Covers:** capture, incremental chains, provenance, a coordinated restore
    across regions, and a local cutover and its revert, all against in-flight
    atomic writes.
  - **Does not cover:** in-place and cold restores, reader atomicity across a
    backup set's members during the alias swap, and a replication batch delayed
    across a whole restore cutover.

- **Materialised-view tail and lifecycle** ([`spec/materialised-views/`](../../spec/materialised-views/README.md)).
  - **Covers:** physical WAL binding, checkpoint and published read-position
    entitlement, cursor and retention trims, fall-off and lag rebuild, finite
    crash replay, shadow-generation publication, reclaim and decommission,
    and source identity cutover followed by rebind.
  - **Does not cover:** atomic/cross-tree staging, aggregation membership,
    history shaping, per-partition terminal recovery, or reader-handle reclaim
    grace. Source swaps are environment steps; lifecycle internals are checked
    by the lifecycle and alias companions. Replay can transiently regress a
    value: this model checks prefix convergence, not monotonic reads across a
    replay. Its seven invariants and five temporal properties are not a claim
    of read-atomic safety for the separate staging protocol.

- **Tree lifecycle composition** ([`spec/tree-lifecycle/`](../../spec/tree-lifecycle/README.md)).
  - **Covers:** sparse resize copy/fence/swap/undo; grow and shrink reshard;
    local shadow restore/revert overtaking a physically bound migration; alias
    reservations, deletion exclusion and completion-before-release recovery;
    publish-before-read resize versus split/fold or legacy bootstrap admission;
    once-per-operation undo clearance when replication becomes enabled; and
    refusal of old undo after a later restore or administrative alias cutover.
  - **Does not cover:** arbitrary-await crash safety, a cutover racing every
    compensation await of a still-undoable resize, snapshot transport, or the
    data-plane rows and saga internals already modelled by the ownership, WAL
    and backup companions.
    An undo request can overtake already-admitted work; its admission interlock
    prevents new work, not retroactive cancellation.

- **Anti-entropy remediation** ([`spec/anti-entropy/`](../../spec/anti-entropy/README.md)).
  - **Covers:** digest comparison and Merkle localisation; opt-in, traffic-window
    and breaker gates; retained-WAL targeted replay and snapshot fallback;
    origin suppression, receiver dedup and LWW joins; dropped Sets and Deletes, foreign-origin
    rows, trim/cursor blind spots, finite transport faults and activation restart.
    Topology changes invalidate localisation through a physical generation.
  - **Does not cover:** full topology data movement, bounded-cache eviction,
    unbounded clocks or transport faults, or repairing lost deletes after their
    tombstones have been reaped. Retained snapshot tombstones repair trimmed-WAL
    and below-cursor lost deletes; reaping is outside the model. The Alias variant
    covers one physical swap, not rollback restores. See the refinement note.

- **Shared alias routing and revert** ([`spec/aliasing/`](../../spec/aliasing/README.md)).
  - **Covers:** two logical aliases sharing physical copies; intent-before-fence
    and fence-before-publication ordering; atomic alias/map publication;
    logical-alias-specific fence ownership; finite crash/retry and stale durable
    administrative recovery after ABA; restore revert's early refusal and
    expected-physical fence; deletion admission; and split/fold physical binding
    across repeated alias publications.
  - **Does not cover:** unbounded redirect chains, physical purge of shared
    copies, authorization, or an epoch-fenced restore revert. A revert admission
    read is not a lock: a superseding move after it can leave lineage/reservation
    changed on later refusal, and a return to the same shadow passes the physical
    check. No completion guarantee is claimed for permanently superseded work.
    Physical WAL consumers, atomic visibility and data movement retain the
    view, ownership, anti-entropy and lifecycle companion models' guarantees.

- **Scheduling and progress** ([`spec/scheduling/`](../../spec/scheduling/README.md)).
  - **Covers:** non-reentrant turn occupancy, partition admission capacity,
    paced gate-release bursts, queue ordering and wait-budget outcomes in the
    checked instances, with explicit environment-progress premises. Gate release
    is distinct from semaphore admission and storage completion.
  - **Does not cover:** throughput numbers, latency percentiles, default tuning,
    arbitrary caller counts or a wall-clock service bound. Fairness does not
    imply bounded latency, and unbounded offered load need not become permanently
    Healthy. See [Verified Scheduling and Progress](verified-scheduling.md).

Across every area, the instances are bounded: each module checks a small number
of copies, shards, keys and faults exhaustively, and says which. A clean run is
a statement about the design over that instance. The integration and
[chaos tests](chaos-tests.md) remain the evidence for the deployed system.

The single-cluster `AtomicCommit` module predates this epic; its row in the
areas table counts it, and [Verified Atomic-Commit](verified-atomic-commit.md)
describes it. Two other verified pages,
[Verified Atomic Action](verified-atomic-action.md) and
[Verified Distributed Lock](verified-lock.md), rest on verified cores and Coyote
models rather than a TLA+ module, so they are outside this census.

## Defects the coverage found and fixed

The runtime defects below were found by building or reviewing an area's specification,
reproduced against production, and fixed; each fix is pinned by detectors, and
the mutation that reproduced production stays as a regression check. Each
area's list is the authoritative one. The separately listed interrupted-shadow detector correction is a test flaw, not a runtime defect:

| Area | Defects | Full list |
|------|---------|-----------|
| WAL durability lifecycle | #4450, #4451, #4456, #4467, #4523, #4525, #4621, #4622, #4634, #4641, #4654, #4669, #4699, #4700 | [Verified WAL](verified-wal.md#the-tla-specification-and-the-scope-of-the-assurance) |
| Shard ownership | #4452, #4453, #4454, #4455, #4473, #4474, #4475, #4503, #4522, #4545, #4564, #4611, #4613, #4618, #4619, #4689 | [`spec/shard-ownership/`](../../spec/shard-ownership/README.md#defects-this-area-found) |
| Cross-cluster atomic visibility | #4480, #4481, #4482, #4508, #4511, #4526, #4533, #4534, #4591, #4627, #4664, #4683, #4684, #4685 | [Verified Atomic-Commit](verified-atomic-commit.md#scope-the-single-cluster-protocol-and-the-replicated-half-checked-separately) |
| Replication convergence | #4463, #4464, #4465, #4504, #4537, #4549, #4585, #4586, #4587, #4603, #4604, #4614, #4615, #4673, #4707 | [`spec/replication/`](../../spec/replication/README.md#production-defects-this-module-found) |
| Backup and restore | #4485, #4490, #4589, #4593, #4686 | [`spec/backup/`](../../spec/backup/README.md#defects-the-modules-found) |
| B+ tree topology | #4795, #4815, #4818, #4914 | [`spec/bplustree/`](../../spec/bplustree/README.md#defects-this-specification-found) |
| Materialised-view tail and lifecycle | #4853, #4856, #4857 | [`spec/materialised-views/`](../../spec/materialised-views/README.md#defects-found) |
| Materialised-view physical rebuild and resolved identity | #4891, #4895, #4905 | [Physical-rebind companion](../../spec/materialised-views/README.md#focused-aggregation-and-physical-rebind-companions) and [resolved identity](../../spec/view-resolved-identity/README.md) |
| Materialised-view staging, replay and source admission | #4864, #4865, #4866, #4867, #4868, #4869, #4870, #4871, #4872, #4877, #4878, #4879, #4880, #4881, #4882, #4883, #4884, #4885, #4886, #4890, #4900 | [Tail and focused companions](../../spec/materialised-views/README.md), [staging](../../spec/view-staging/README.md) |
| Materialised-view joint repair and automatic upgrade | #4876, #4897, #4898, #4899, #4902 | [Joint repair](../../spec/view-joint-repair/README.md) |
| Interrupted-shadow regression detector | #4906 | [Exact physical generation control](../../spec/materialised-views/RefinementHorizon.md) |
| Tree lifecycle composition | #4858 | [`spec/tree-lifecycle/`](../../spec/tree-lifecycle/README.md#defect-found) |
| Anti-entropy remediation | #4859, #4862, #4863 | [`spec/anti-entropy/`](../../spec/anti-entropy/README.md) |
| Shared alias routing and revert | #4860 | [`spec/aliasing/`](../../spec/aliasing/README.md#defect-found) |
| Scheduling and progress | #4976, #4977, #4979, #4985, #4992 | [Scheduling defects](verified-scheduling.md#defects-found-during-the-bucket) |

## CI partition and non-vacuity

Every TLC case carries a `TlcModule_<Module>` category, and `TlcCiShard` gives
grouped cases a `TlcShard*` category; `test-shards.json` partitions those across
deterministic TLC shards, plus a catch-all for every ungrouped case, and their
measured costs feed the matrix planner. Structural discovery, mutation and refinement
cases carry no TLC category, so they run in the content gates even when policy
skips exhaustive model checking; the #4852 regression stops a shard category
from moving them into skipped work, and a toolchain-free partition check proves
every TLC case is selected by exactly one TLC shard. The
[Formal Verification Pipeline](formal-verification-pipeline.md) explains the
routing, the TLC input classes, verdict receipts and the nightly and weekly
lanes. The scheduling shards' measured and reconstructed prices, with their run
provenance, are recorded in the
[scheduling area README](../../spec/scheduling/README.md) and
[Verified Scheduling and Progress](verified-scheduling.md).

## Related

- [Formal Verification Pipeline](formal-verification-pipeline.md) - which CI lane
  runs which checks, TLC input classes, verdict receipts, and the refinement
  gates, with diagrams.
- [`spec/README.md`](../../spec/README.md) - the specification index, the module
  layout and the CI decision.
- [Chaos Tests](chaos-tests.md) - the integration suite that drives a live
  cluster under concurrent load and faults.
