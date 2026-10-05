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
| `RAllOrNothingSettleKeyByKey` | `RAllOrNothing` | Invariant | - (export rows) | under the receiver settle (#4510), a decided saga's undrained buckets are exported without committed rows, so re-shipped prepares settle it one key at a time |
| `RAllOrNothingNotifyBeforeRegister` | `RAllOrNothing` | Invariant | `DeliverTerminal`, `ReceiverRegister`, `ReceiverNotify` | the barrier is notified before the delegation is registered |
| `RAllOrNothingDialFailureDropsDelegation` | `RAllOrNothing` | Invariant | `DialFault` | a failed dial forgets the delegation, so the tree reads InFlight |
| `RAllOrNothingSnapshotReadsUnresolvableAsInFlight` | `RAllOrNothing` | Invariant | - (read view) | an undiallable delegation reads InFlight, as the snapshot read paths answered it before #4461 (regression check for #4448) |
| `RAllOrNothingPrepareAckedUnapplied` | `RAllOrNothing` | Invariant | `DeliverPrepare` | a prepare is acknowledged without being applied, so the hold releases its saga's terminals over a key with no bucket (the lost-record cell of #4591) |
| `RAllOrNothingTerminalOvertakesPrepare` | `RAllOrNothing` | Invariant | `DeliverTerminal` | a terminal is delivered before its shard's prepare, which is then refused (#4480, before the shipper's terminal hold) |
| `RAllOrNothingExportOverStrandedPrepare` | `RAllOrNothing` | Invariant | `OriginForget` | the origin retires a row over a resident bucket and exports the aged-out row as Indeterminate, as before #4501, so a bootstrapping receiver is exported a split saga (regression check for #4481) |
| `RStrictIsolationTerminalOutcomeIgnored` | `RStrictIsolation` | Invariant | `DeliverTerminal` | the receiver records an undecided saga's terminals as a commit |
| `RLinearizedTerminalsFanOutAppliesCommit` | `RLinearizedTerminals` | Invariant | `ReceiverFanOut` | the fan-out tells every leaf "commit" whatever was recorded |
| `DelegationsDisjointRegistryAdmitsForeignClaim` | `DelegationsDisjoint` | Invariant | `ForeignOriginClaim` | the registry has no coexistence check |
| `RMonotonicVisibilityFanOutDiscardsCommittedBucket` | `RMonotonicVisibility` | Temporal | `ReceiverFanOut` | the fan-out consumes a committed bucket without materialising it |
| `RCommittedEventuallyVisiblePrepareNotShipped` | `RCommittedEventuallyVisible` | Temporal | `OriginPrepare` | one participant's prepare is never replicated |
| `RCommittedEventuallyVisibleFinalizeSkipsFanOut` | `RCommittedEventuallyVisible` | Temporal | `ReceiverFinalize` | a finalised tree marks its registry but never fans out |
| `RNoStrandedPrepareShipperDropsTerminal` | `RNoStrandedPrepare` | Temporal | `OriginBroadcast` | one source shard's terminal is never replicated (the #2324 class) |
| `RNoStrandedPrepareLatePrepareStaged` | `RNoStrandedPrepare` | Temporal | `DeliverPrepare` | a duplicate prepare trailing its terminal is staged, with neither the settle nor the late-prepare refusal in front of it |
| `RNoStrandedPrepareBootstrapReshipsPreCut` | `RNoStrandedPrepare` | Temporal | `Bootstrap`, `DeliverPrepare` | re-shipped pre-cut prepares are staged with no decision row to settle them, as before #4510 (regression check for #4482) |
| `RNoStrandedPrepareDedupeOverPurgedDecision` | `RNoStrandedPrepare` | Temporal | `OriginPurge` | the origin purges a forgotten saga's decision while a prepare of it is still retained, as before #4553, and no replay filter withholds the re-shipped prepare, which has nothing to settle against (regression check for #4508; the guard alone is covered by the filter) |
| `RNoStrandedPrepareHoldWaitsOnUnshippedPrepare` | `RNoStrandedPrepare` | Temporal (`DEADLOCK: off`) | `OriginPrepare`, `DeliverTerminal` | a terminal waits on every prepare the saga wrote, one of which is never shipped, so it is never released |
| `RAllOrNothingGapStaysOnLog` | `RAllOrNothing` | Invariant | `ShipperGap` | a shipper that loses a record to a trim or an encode failure stays on the log (before #4577 and #4651) |
| `RAllOrNothingDetachStaysOnLog` | `RAllOrNothing` | Invariant | `Detach` | a removed peer's shipper detaches without taking the peer off the log (before #4652) |
| `RNoStrandedPrepareReaddKeepsDetachedDrain` | `RNoStrandedPrepare` | Temporal | `Readd` | a re-added peer's re-seed is settled by an export drained while its shipper was detached (before #4652) |
| `RAllOrNothingPoisonedTerminalApplied` | `RAllOrNothing` | Invariant | `DeliverTerminal` | the receiver applies a terminal of a saga it poisoned (before #4633) |
| `RAllOrNothingPoisonSettleDiscardsCarried` | `RAllOrNothing` | Invariant | `PoisonReseed` | the poison re-seed discards a saga the export carried as prepared rows (before #4633) |
| `RNoStrandedPrepareReseedKeepsStaleBucket` | `RNoStrandedPrepare` | Temporal | `ReseedDrain` | the re-seed drain leaves a purged saga's leftover bucket (before #4631) |
| `RAllOrNothingReseedClearsCarriedBucket` | `RAllOrNothing` | Invariant | `ReseedDrain` | the re-seed clear discards a carried saga's buckets too (before #4631) |
| `RNoStrandedPrepareDecisionRowNotFannedOut` | `RNoStrandedPrepare` | Temporal | `ReseedDrain` | a decision row is recorded but its saga's leftover buckets are not drained by it (before #4631) |
| `RNoStrandedPrepareReplayVerdictReadsDecisionOnly` | `RNoStrandedPrepare` | Temporal | `ReplayWithhold` | the replay filter reads the decision alone, so an undecided saga reads as purged (before #4533's participants-first read) |
| `RNoStrandedPrepareReplayFilterOff` | `RNoStrandedPrepare` | Temporal | `ReplayWithhold` | no replay filter, so a purged saga's retained prepare is re-shipped (before #4533) |
| `RNoStrandedPrepareDrainUnderPreHoldSiloSettles` | `RNoStrandedPrepare` | Temporal | `ReseedDrain` | an export drained while a silo predates the purge hold settles the re-seed (#4664, before #4666) |
| `RNoStrandedPrepareWithheldRecordsNotRetained` | `RNoStrandedPrepare` | Temporal | `ReseedRewind` | the records withheld while the peer was off the log are gone by the rewind (before `ReseedRetainFrom`) |
| `RAllOrNothingExportDecisionReadStale` | `RAllOrNothing` | Invariant | `ReseedDrain` | the re-seed export's decision read predates its row passes (#4627, before its late-decision pass) |
| `RNoStrandedPrepareBootstrapUnderPreHoldSilo` | `RNoStrandedPrepare` | Temporal | `Bootstrap` | a bootstrap's replay starts while a silo predates the purge hold (before #4533's wait) |
| `RNoStrandedPreparePurgeIgnoresHold` | `RNoStrandedPrepare` | Temporal | `OriginPurge` | the registry purges while a re-seed or its replay holds purges (before #4533 and #4534-B) |
| `RAllOrNothingReceiverParksWithoutPoison` | `RAllOrNothing` | Invariant | `ReceiverPoison` | the receiver parks and acknowledges a prepare it gave up on without poisoning its saga (regression check for #4591, fixed by #4633) |
| `RNoStrandedPrepareFilterClearsBeforeHorizon` | `RNoStrandedPrepare` | Temporal | `FilterClear` | the replay filter clears before every cursor has passed its horizon (before #4533) |
| `RCommittedEventuallyVisibleUpgradeNeverSeen` | `RCommittedEventuallyVisible` | Temporal | `UpgradeDone` | the shipper never sees every silo honour the purge hold, so a joining receiver waits for ever |

Each loss-path mutation declares `BOUNDS:` to enable its loss path on one slice
of the instance (`LossPath`, `JoinStart`, `SagaOutcome`): the base cfg enables
none, so the control arm checks the target on the instance with no loss path,
and the variant configurations check every property under each loss path with
its fix.

Every mutant but one is deadlock-free: run with a cfg naming only `TypeOK` and
deadlock checking on, each reports no error (`TypeOkTallyExpectedRunaway`
reports its own target first), so none races its target against a deadlock
under TLC's parallel search. `RNoStrandedPrepareHoldWaitsOnUnshippedPrepare` is
the exception by construction - its defect is a terminal that can never be
delivered, which leaves no enabled action - so it declares `DEADLOCK: off`, and
the harness's third arm confirms the deadlock is real.

These mutations are regression checks for defects this module found or
reproduced and that are now fixed: `RAllOrNothingTerminalOvertakesPrepare`
(#4480), `RAllOrNothingSnapshotReadsUnresolvableAsInFlight` (#4448),
`RAllOrNothingExportOverStrandedPrepare` (#4481),
`RNoStrandedPrepareBootstrapReshipsPreCut` (#4482) and
`RNoStrandedPrepareDedupeOverPurgedDecision` (#4508), and every loss-path
mutation above, one per fix (#4577, #4651, #4652, #4633, #4631, #4627, #4533,
#4534-B and #4666). Each restores what production did before the fix, and the
refinement note cites the detectors that go red when production does it again.

Every safety property is a state invariant, so TLC checks it in every
reachable state: a receiver reader observing between any two steps - between
two prepare arrivals of one saga, say - needs no reader action to be modelled.
`RAllOrNothingSettleKeyByKey` is the witness: its counterexample is a reader
between two re-shipped prepares of a saga the receiver already holds as
committed.

The ordering a terminal waits for in the base is stated over the prepares still
**outstanding**, never over every prepare the saga wrote, so a prepare that has
left the outbox cannot hold its terminal back for ever;
`RNoStrandedPrepareHoldWaitsOnUnshippedPrepare` is the wedge that waiting on
every prepare would cause. That release is safe because every way a prepare
leaves the outbox unapplied - a shipper gap, a detach, a receiver poison - takes
the peer off the log or poisons the saga first, so its terminal is withheld until
a re-seed restores the saga. Where neither happens, releasing the terminal splits
the receiver, which `RAllOrNothingPrepareAckedUnapplied`, `RAllOrNothingGapStaysOnLog`
and `RAllOrNothingDetachStaysOnLog` show.

## Liveness fails on protocol defects, under the module's own fairness

All three temporal properties are paired with mutations that leave `Spec`'s
fairness untouched and change a protocol step instead (issue #2321's
requirement): a prepare or a terminal the origin never replicates, a fan-out
that never runs or consumes a bucket without draining it, a late prepare staged,
a re-shipped pre-cut prepare with nothing to settle it against, a decision
purged while a prepare of its saga can still be re-shipped, and each loss path's
re-seed missing a step: a stale bucket left, a decision row not fanned out, a
replay that withholds a live saga or re-ships a purged one, withheld records
lost, a purge the holds do not stop, and an export that settles a re-seed it
cannot vouch for. The transport stays fair about
every record it is given; the defect is always in what it is given or in what
the receiver does with it.

## Property classification

Issue #2321's question for each property is why it holds on the base, and when
the answer is that no action produces the violating cell, whether production can
produce it. The classification below is a production question, answered by
reading the code the refinement note maps.

| Property | Why it holds on the base | Cells the base cannot reach |
| --- | --- | --- |
| `RAllOrNothing` | The tally, the barrier, the register-before-notify order, the Indeterminate dial answer, and every loss path's fix; mutations of each fire it. | A terminal overtaking its prepare, an unstamped multi-shard terminal, an undiallable delegation read through a snapshot, and a bootstrap over a stranded origin prepare, all **faithfully inexpressible**: the shipper holds every terminal until its saga's prepares are acked (#4480), the snapshot read paths answer Indeterminate (#4448, fixed by #4461), and a stored aged-out row exports its recorded verdict (#4481, fixed by #4501); a stranded bucket whose row is already purged exports the split the origin itself serves (#2318's premise). A prepare lost to a peer is reached in the variant configurations, with the fix that withholds its terminal. |
| `RStrictIsolation` | The receiver records the outcome its terminals carry, which is the origin's. | None. |
| `RLinearizedTerminals` | The receiver marks before it fans out, and the fan-out carries the recorded outcome. | None. |
| `DelegationsDisjoint` | The registry's coexistence check on the foreign claim. | None: the claim is enabled for the whole window the authoring row exists. |
| `RMonotonicVisibility` | The fan-out drains a committed bucket into the projection. | A late orphan on a reactivated receiver leaf, which the module does not model: **faithfully inexpressible** on the replication path, where the settle (#4510) and, since #4461, the leaf's registry-consulting late-prepare refusal (#4445's fix) both stand in front of it; recorded as an abstraction gap. |
| `RCommittedEventuallyVisible` | At-least-once delivery, the tally, the barrier, the fan-out, and each loss path's re-seed. | None: every record production can lose is reached in the variant configurations, and the re-seed brings its saga back. Stated over materialisation so that a dial fault lasting forever, which production also allows, does not make it unfalsifiable-by-construction. |
| `RNoStrandedPrepare` | Late prepares are refused, a re-shipped pre-cut prepare is settled against the exported decision (#4510), the replay filter withholds a purged saga whole (#4533), and each loss path's re-seed drains or discards every leftover bucket. | A retained pre-cut prepare re-shipped after its terminal was trimmed: **faithfully inexpressible** since #4510 and #4553 (#4482, #4508). |

**Bounded-out.** The instance has two source shards (or two trees) and one
touched-shard count per saga, so the tally's upward merge of a raised count - a
late-pass shard's terminal carrying a larger count than earlier ones - is never
reached. It is reachable in the general protocol and lies outside this instance;
`TerminalArrivalTallyTests` pins the merge rule. The Coyote model
`CrossClusterReceiverTallyModel` does not exercise it either: it stamps every
terminal with the same count, so its three-shard tally never raises one.
