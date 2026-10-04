# Cross-cluster specification mutations

Each `.mutation` file here is a deliberate defect in
[`../AtomicCommitCrossCluster.tla`](../AtomicCommitCrossCluster.tla), paired with
exactly one property it must make TLC report. The file format, the two-arm
experiment, the generated cfg and the `PERTURBS:` / `DEADLOCK:` headers are the
same as for the single-cluster module and are described once, in
[`../mutations/README.md`](../mutations/README.md). This file covers only what is
particular to this catalogue.

The mutant is written beside a copy of `AtomicCommit.tla`, which the module
instances, so a mutation can only edit `AtomicCommitCrossCluster.tla`. The origin
actions here wrap `AtomicCommit`'s; a mutation that perturbs one edits what that
step writes for replication, never `AtomicCommit`'s own protocol step.

## Inventory

Every property is paired and every protocol action is perturbed; the module's
[counts table](../README.md#counts) states how many of each. No mutation here
adds an action: every one changes something the modelled protocol, its
transport assumption or its read view already does.

| Mutation | Property it makes fire | Class | Perturbs | The defect it models |
| --- | --- | --- | --- | --- |
| `TypeOkTallyExpectedRunaway` | `TypeOK` | Invariant | `DeliverTerminal` | the receiver tally sums terminal counts instead of merging them upward |
| `RAllOrNothingLegacyNoTallyMultiShard` | `RAllOrNothing` | Invariant | `OriginBroadcast` | terminals carry no touched-shard count, so a multi-shard saga is marked on its first terminal |
| `RAllOrNothingMarksOnFirstTerminal` | `RAllOrNothing` | Invariant | `DeliverTerminal` | the receiver tally is final on any arrival |
| `RAllOrNothingDecisionShipsUncountedTerminal` | `RAllOrNothing` | Invariant | `OriginDecide` | the decision step ships an unstamped terminal ahead of the broadcast |
| `RAllOrNothingBarrierDecidesOnFirstTree` | `RAllOrNothing` | Invariant | `ReceiverNotify` | the cross-tree barrier decides on its first arrival |
| `RAllOrNothingNotifyBeforeRegister` | `RAllOrNothing` | Invariant | `DeliverTerminal`, `ReceiverRegister`, `ReceiverNotify` | the barrier is notified before the delegation is registered |
| `RAllOrNothingDialFailureDropsDelegation` | `RAllOrNothing` | Invariant | `DialFault` | a failed dial forgets the delegation, so the tree reads InFlight |
| `RAllOrNothingSnapshotReadsUnresolvableAsInFlight` | `RAllOrNothing` | Invariant | - (read view) | an undiallable delegation reads InFlight, as the snapshot read paths answer it (#4448) |
| `RAllOrNothingTerminalOvertakesPrepare` | `RAllOrNothing` | Invariant | `DeliverTerminal` | a terminal is delivered before its shard's prepare, which is then refused (#4480) |
| `RAllOrNothingExportOverStrandedPrepare` | `RAllOrNothing` | Invariant | `OriginForget` | the origin retires a row over a resident bucket and a bootstrapping receiver is exported a split saga (#4481) |
| `RStrictIsolationTerminalOutcomeIgnored` | `RStrictIsolation` | Invariant | `DeliverTerminal` | the receiver records every terminal as a commit |
| `RLinearizedTerminalsFanOutAppliesCommit` | `RLinearizedTerminals` | Invariant | `ReceiverFanOut` | the fan-out tells every leaf "commit" whatever was recorded |
| `DelegationsDisjointRegistryAdmitsForeignClaim` | `DelegationsDisjoint` | Invariant | `ForeignOriginClaim` | the registry has no coexistence check |
| `RMonotonicVisibilityFanOutDiscardsCommittedBucket` | `RMonotonicVisibility` | Temporal | `ReceiverFanOut` | the fan-out consumes a committed bucket without materialising it |
| `RCommittedEventuallyVisiblePrepareNotShipped` | `RCommittedEventuallyVisible` | Temporal | `OriginPrepare` | one participant's prepare is never replicated |
| `RCommittedEventuallyVisibleFinalizeSkipsFanOut` | `RCommittedEventuallyVisible` | Temporal | `ReceiverFinalize` | a finalised tree marks its registry but never fans out |
| `RNoStrandedPrepareShipperDropsTerminal` | `RNoStrandedPrepare` | Temporal | `OriginBroadcast` | one source shard's terminal is never replicated (the #2324 class) |
| `RNoStrandedPrepareLatePrepareStaged` | `RNoStrandedPrepare` | Temporal | `DeliverPrepare` | a duplicate prepare trailing its terminal is staged |
| `RNoStrandedPrepareBootstrapReshipsPreCut` | `RNoStrandedPrepare` | Temporal | `Bootstrap` | retained pre-cut saga records are shipped again after a bootstrap (#4482) |

Every mutant is deadlock-free: run with a cfg naming only `TypeOK` and deadlock
checking on, each reports no error (`TypeOkTallyExpectedRunaway` reports its own
target first). None therefore needs `DEADLOCK: off`.

## Liveness fails on protocol defects, under the module's own fairness

All three temporal properties are paired with mutations that leave `Spec`'s
fairness untouched and change a protocol step instead (issue #2321's
requirement): a prepare or a terminal the origin never replicates, a fan-out
that never runs or consumes a bucket without draining it, a late prepare staged,
and a bootstrap that re-ships pre-cut records. The transport stays fair about
every record it is given; the defect is always in what it is given or in what
the receiver does with it.

## Property classification

Issue #2321's question for each property is why it holds on the base, and when
the answer is that no action produces the violating cell, whether production can
produce it. The classification below is a production question, answered by
reading the code the refinement note maps.

| Property | Why it holds on the base | Cells the base cannot reach |
| --- | --- | --- |
| `RAllOrNothing` | The tally, the barrier, the register-before-notify order and the Indeterminate dial answer; mutations of each fire it. | A terminal overtaking its prepare, an unstamped multi-shard terminal, an undiallable delegation read through a snapshot, and a bootstrap over a stranded origin prepare. All four are **blindly inexpressible**: production reaches them (#4480, #4480's unstamped-terminal route, #4448, #4481), and each is kept as a standing mutation and a gap row rather than as an action of the base. |
| `RStrictIsolation` | The receiver records the outcome its terminals carry, which is the origin's. | None. |
| `RLinearizedTerminals` | The receiver marks before it fans out, and the fan-out carries the recorded outcome. | None. |
| `DelegationsDisjoint` | The registry's coexistence check on the foreign claim. | None: the claim is enabled for the whole window the authoring row exists. |
| `RMonotonicVisibility` | The fan-out drains a committed bucket into the projection. | A late orphan on a reactivated receiver leaf, which the module does not model (#4445's mechanism): **blindly inexpressible**, recorded as an abstraction gap. |
| `RCommittedEventuallyVisible` | At-least-once delivery, the tally, the barrier and the fan-out. | None. Stated over materialisation so that a dial fault lasting forever, which production also allows, does not make it unfalsifiable-by-construction. |
| `RNoStrandedPrepare` | Late prepares are refused, and the handoff after a bootstrap is exactly-once. | A retained pre-cut prepare re-shipped after its terminal was trimmed: **blindly inexpressible** (#4482). |

**Bounded-out.** The instance has two source shards (or two trees) and one
touched-shard count per saga, so the tally's upward merge of a raised count - a
late-pass shard's terminal carrying a larger count than earlier ones - is never
reached. It is reachable in the general protocol and lies outside this instance;
`TerminalArrivalTallyTests` pins the merge rule and the Coyote model
`CrossClusterReceiverTallyModel` drives a three-shard tally.
