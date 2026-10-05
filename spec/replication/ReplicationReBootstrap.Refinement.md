# Refinement note: the re-bootstrap module to code

This note maps [`ReplicationReBootstrap.tla`](ReplicationReBootstrap.tla), a
focused companion of [`Replication.tla`](Replication.tla), to the code. It
follows the conventions of [`Refinement.md`](Refinement.md), which maps the main
module and explains the Detector column.

The module answers the one question the main module bounds out by not
modelling tombstone garbage collection. A receiver falls off the source's log
past a delete, and the source then reaps that delete's tombstone. Can an
in-place re-bootstrap still bring the receiver to the source's value? The
export carries every tombstone the source still holds (#4504, fixed by #4544),
but not a reaped one. The module models the intended design of #4537, a
receiver-side reconcile in the drain, and the mutation
`EventualConvergenceReapedDeleteNotReconciled` reproduces current production,
which has none.

The design is not yet built, so most rows below cite #4537. Read them as "the
design is checked; production does not implement it yet". A key the receiver
holds under another origin cannot be reconciled at all; that residual is the
`Residual` predicate and #4549.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `authored` | History of every write | Not a production variable: the set the properties quantify over. |
| `wal[x]` | A cluster's own writes | The per-tree WAL a shipper drains. |
| `cursor[e]` | A shipper's acknowledged position | `ReplicationShipperState.PartitionCursors`. |
| `reg[x]` | A replica's value of the key | The leaf's `LwwValue`. `Absent` is a key with no entry, which is what a reaped tombstone leaves. `fab` marks a tombstone the reconcile fabricated, for the safety property only. |
| `clk[x]` | A leaf's clock | The leaf HLC. A reap does not lower it. |
| `reaped` | Highest reaped tombstone HLC | Not a production variable: it states that a write authored after a reap stamps above it. The reap guard that makes that true is #4615's, together with the in-flight half (`Reap`). |
| `fellOff` | A re-seed of the receiver is owed | `ReplicationShipperState.ReseedRequiredEpoch`, which the shipper persists when a shipping read finds the source trimmed past its cursor (since #4599, the fix for #4587), and which every push carries until a full bootstrap from a later export clears it. |
| `booted` | The requested re-bootstrap completed | The terminal phase of `LatticeBootstrapCoordinatorGrain`. |
| `topo` | The source's shard-map version | The shard map's version. The export does not carry it yet (#4537). |
| `gen` | The source's tree generation | Restore epoch and physical tree identity. The export does not carry it, and the receiver records no aligned generation yet (#4537). |
| `restored` | The source lost the key without a delete | Not a production variable: it scopes the liveness property. |
| `ex` | The export in progress | The coordinator's drain over `LatticeSnapshotProvider.ExportAsync`, with the reconcile's pre-capture and carried-key bookkeeping (#4537). |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(o, h, d)` | A cluster commits a write, or the source deletes the key | A leaf commit or `DeleteAsync`. The HLC is above the leaf clock, which the merge path advances past every merged timestamp, and above every reaped tombstone: the grace period is assumed longer than clock skew. | Yes: `BPlusLeafGrainTests.MergeMany_advances_local_clock_past_incoming_max`. |
| `Deliver(e)` | A shipper delivers its next entry | The ship and apply path, which `Replication.tla` checks in full. The receiver merges by `LwwValue.Merge`: the HLC, then a tombstone wins the tie, then the value. Delivery here is FIFO and exactly once. | Yes: `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break` and `LwwValueMergeCrossSiteConvergenceTests.Merge_converges_across_sites_when_only_one_side_authored_locally`. |
| `Trim` | The source trims its log past the receiver, and its shipper requests a re-seed | A `WalRetention` trim past the receiver's cursor. The shipper's next read finds the page starting above the requested sequence (`ReplicationShipperGrain.TryRefillPartitionAsync`) and `MarkReseedRequiredAsync` records the export epoch; `ReplicationReseedResponder` starts a full bootstrap from the sender unless one from an export opened after it has completed (since #4599, the fix for #4587). Before #4599 the only probe, `LatticeFallOffLogDetector.CheckAndTriggerAsync`, compared the receiver's own WAL and never saw a source trim (`EventualConvergenceFallOffUndetected`); it remains a local seal guard. With auto-bootstrap disabled the bootstrap is the operator's, and the link reports Stalled until it runs. | Yes: `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`, `CrossClusterAtomicVisibilityTests.Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has` and `ReplicationReseedResponderTests.Starts_a_bootstrap_when_none_has_completed_past_the_requested_epoch`. |
| `Reap` | The source garbage-collects a tombstone | `BPlusLeafGrain.CompactTombstonesAsync` past `TombstoneGracePeriod`. The guard, that no write the tombstone beats is still on its way to the source, is the design #4615 needs: **production reaps on the wall clock alone**, so a write delayed past the grace period (a partition longer than it, or a link stalled on a dead-letter backpressure or a re-seed) resurrects the key, which is `EventualConvergenceReapInsideGrace`. | Partial (#4615): no production mechanism enforces the guard. The wall-clock window itself is covered by `BPlusLeafGrainTests.CompactTombstones_does_not_block_future_passes_when_tombstones_remain_in_grace` and `BPlusLeafGrainTests.CompactTombstones_in_grace_tombstones_still_suppress_the_completeness_stamp`. |
| `Restore` | The source loses the key without a delete | A restore or revert, a purge and recreate, or an alias rebind to another physical tree. The generation change it makes is the input the reconcile's generation gate needs, which nothing records yet (#4537). | Partial (#4537): the generation gate is not built. That a restore removes keys without a delete is covered by `LatticeBackupRestoreIntegrationTests.RestoreAsync_shadow_cutover_swaps_alias_then_revert_restores_prior_tree`. |
| `Reshard` | The source's shard map changes | A split, reshard or resize. A scan in progress may pass a key that was present throughout; the reconcile's topology gate, which needs the shard-map version at export open and close, is not built (#4537). | Partial (#4537): the topology gate is not built. That the shard map's version advances is covered by `LatticeRegistryGrainTests.SetShardMapAsync_increments_version_on_each_persist`. |
| `BeginExport(kind)` | An export opens and the receiver pre-captures | `LatticeBootstrapCoordinatorGrain` opening `LatticeSnapshotProvider.ExportAsync`, full for a fall-off re-bootstrap, range-scoped for the anti-entropy bootstrap fallback. The pre-capture of live source-origin entries is the design (#4537). | Partial (#4537): the pre-capture is not built. The drain stamps every row with the source as its origin, which is what makes origin = source a proof that the source held the value: `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_routes_snapshot_drain_through_IReplicationApplier`. |
| `ExportRow` | The scan reaches the key | The export's committed-projection pass, which also carries a retained tombstone as a committed tombstone row (since #4544, the fix for #4504), and the drain applying it. | Yes: `InPlaceReBootstrapDeleteIntegrationTests.Re_bootstrap_over_a_populated_receiver_deletes_a_key_the_source_deleted_while_the_receiver_was_behind`, `LatticeSnapshotProviderTests.ExportAsync_ships_a_tombstoned_entry_as_a_committed_tombstone_row`, `BootstrapAtomicVisibilityTests.Re_bootstrap_over_a_populated_receiver_deletes_a_key_a_committed_saga_deleted_before_its_terminal_drained` and `LeafSnapshotProviderTests.StreamAsync_projects_a_committed_tombstone_as_a_committed_delete`. |
| `EndExport` | The drain completes and reconciles | The reconcile in the drain: a delete attributed to the source at the captured HLC, under the scope, topology and generation gates, with the re-bootstrap re-requested when a topology change skipped it. None of it is built; current production applies only the rows carried (`EventualConvergenceReapedDeleteNotReconciled`). | Partial (#4537): the reconcile is not built. The drain's pin and handoff are covered by `LatticeBootstrapCoordinatorGrainTests.ProcessNextPhase_pin_seals_source_coordinate_and_preserves_other_origins`. |
| `Stutter` | Natural termination | Not a protocol step; a stuttering successor at full quiescence so TLC does not report ordinary termination as a deadlock. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `ReconcileDeletesOnlyDeleted` | A tombstone the reconcile fabricates is dominated by a delete the source really authored, so absence from an export for any other reason - outside the scope, passed over during a reshard, lost to a restore, purge or rebind, or a write the source never received - is never turned into a delete. | Partial (#4537): the reconcile is not built. Its dominance argument rests on a tombstone winning the HLC tie: `LwwValueMergeConvergenceTests.Merge_still_prefers_the_tombstone_over_the_value_tie_break`. |
| `EventualConvergence` | Once writing stops, both replicas hold the value of every write, the deletes included, even when the source reaped a delete the receiver missed - except in the `Residual` and after the source lost the key without a delete. | Partial (#4537, #4549, #4615): no test covers a reaped delete, the residual is #4549, and the reap guard is #4615. The merge converges and a trim past the receiver is repaired by a re-seed: `LwwValueMergeCrossSiteConvergenceTests.Merge_converges_across_sites_when_only_one_side_authored_locally` and `SourceWalTrimFallOffIntegrationTests.Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Territory owned by other open issues

| Issue | What it owns | Reproducing mutation |
|-------|--------------|----------------------|
| #4537 | The receiver-side reconcile of a reaped delete and its gates. | `EventualConvergenceReapedDeleteNotReconciled` |
| #4549 | A reaped delete of a key the receiver holds under another origin, which the reconcile cannot prove the source held. | None: the module states it as `Residual` and excuses it. |
| #4615 | A tombstone reaped on the wall clock alone, while a write it beats is still on its way, which then resurrects the key. | `EventualConvergenceReapInsideGrace` |

When a fix lands, its row here and the matching gap text in the tables above
are removed, and the rows' detectors are proven red against the mutation's
production shape.

## Property classification

| Property | Why it holds on the base | Class | Firing mutations |
|----------|--------------------------|-------|------------------|
| TypeOK | Every action keeps its variables in domain. | Faithfully inexpressible | `TypeOkReBootstrapHlcRunaway` |
| ReconcileDeletesOnlyDeleted | Only a source-origin value is pre-captured, and the reconcile is gated on scope, an unchanged shard map and the aligned tree generation. | Faithful for the intended design; blind for production until #4537 lands | `ReconcileDeletesOnlyDeletedOutOfScope`, `ReconcileDeletesOnlyDeletedGenerationAtExportOnly`, `ReconcileDeletesOnlyDeletedReshardUnseen`, `ReconcileDeletesOnlyDeletedRestoreUnseen`, `ReconcileDeletesOnlyDeletedAnyOrigin` |
| EventualConvergence | Merges are confluent, a fall-off requests a re-bootstrap, as production's has since #4599, the export carries retained tombstones, the reconcile covers reaped ones, a skipped reconcile is retried, and a tombstone is reaped only once no write it beats is on its way. | Faithful for the intended design; blind for production until #4537 and #4615 land | `EventualConvergenceReapedDeleteNotReconciled`, `EventualConvergenceReBootstrapSkipsTombstones`, `EventualConvergenceReapInsideGrace`, `EventualConvergenceDeliverOverwrites`, `EventualConvergenceFallOffUndetected`, `EventualConvergenceReshardSkipNotRetried` |

No mutation adds an action, and every `EventualConvergence` mutation leaves the
fairness intact.

Writing the module changed the design #4537 first proposed in three ways.

- **The fabricated delete is at the captured HLC t, not `succ(t)`.** A
  tombstone wins the HLC tie, so it still removes the captured value, and it
  is never above the source's delete. `succ(t)` exceeded the delete as soon as
  the model admitted a value that ties with it.
- **A reconcile a reshard skipped must be retried.** Otherwise the reaped
  delete is never applied (`EventualConvergenceReshardSkipNotRetried`).
- **The generation gate compares with the generation the receiver's copy is
  aligned with, not only export open with close.** A restore before the
  export opens is otherwise invisible
  (`ReconcileDeletesOnlyDeletedGenerationAtExportOnly`).

The proposed check that the receiver's entry is unchanged since the
pre-capture is absent from the model, which runs the incremental stream during
the export and holds without it.

## Deliberate abstraction gaps

- Everything [`Replication.tla`](Replication.tla) covers - loss, duplication
  and reordering, the cycle-break, the identity cache, the causal buffer,
  dead-lettering and restarts - is out of scope here; see
  [`Refinement.md`](Refinement.md#deliberate-abstraction-gaps).
- **One key, two clusters.** The receiver's own write stands for every
  non-source origin; a third cluster is not modelled, and #4549 covers both.
- **CRDT trees.** The reconcile is last-writer-wins only. The model has no
  CRDT delete, so it neither needs nor checks that gate; the reason for it is
  that a CRDT fold can carry a receiver-local HLC, which breaks the
  dominance argument.
- **The reap guard** is the design #4615 asks for, not production: no write
  a tombstone beats is still on its way when it is reaped, and a write
  authored after a reap stamps above it. Production's wall-clock grace period
  stands for both and enforces neither (#4615).
- **Coordinated restore** is not modelled; a restore here is unilateral, and
  the liveness property excuses its effect. A coordinated-restore cutover is
  where the receiver re-records its aligned generation.
- **Receiver reaps** are not modelled, so a receiver tombstone is never
  garbage-collected.
