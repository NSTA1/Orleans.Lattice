# Refinement note: ShardOwnershipCrdt to production

This note maps the TLA+ specification in
[`ShardOwnershipCrdt.tla`](ShardOwnershipCrdt.tla) to Orleans.Lattice as it
exists in code. The module is the third of this area's modules, beside
`ShardOwnership` ([`Refinement.md`](Refinement.md)) and
`ShardOwnershipRetention` ([`RefinementRetention.md`](RefinementRetention.md)).

It is a **documented mapping, not a machine-checked refinement proof**. The
Detector column names the production test that goes red if the behaviour a row
abstracts regresses, and every named detector was shown red against a
production perturbation of the seam it covers; the log is in the pull request
that added the module.

## What this module is for

The other two modules model last-writer-wins keys, where every path that brings
two copies of a key together may keep the newer one. A CRDT-mode key breaks that
assumption: two copies can each hold a contribution the other lacks, so keeping
either copy whole loses whatever the other alone held, whatever their stamps.
This module checks that every path that brings two copies of a CRDT key
together joins them:

- the terminal's drain, which folds the staged delta, and its backstop, which
  applies the staged state to a bucket a leaf split stranded or to a leaf with
  no bucket (#4611);
- a shard split's forward of a CRDT write and its drains, which import the
  source's row as a cross-shard migration (#4613);
- an online resize's mirror of a CRDT write and its snapshot drain (#4618).

The key's value is a grow-only set of contributions, which is enough to tell a
join from an overwrite: the saga stages `"a"`, a non-atomic CRDT write adds
`"b"`. The saga's staged state is the owner's row at staging time with `"a"`
added, which is what a CRDT accessor's `Stage` method mints
(`LatticeStagedCrdtWrite.Value`).

## The base module models the intended design where production has an open defect

No defect in this module's territory is open. Each was fixed before the module
was added, and its reproduction stays as a standing regression check:

- **The terminal's backstop** (#4611, fixed by #4617). A backstop on a CRDT-mode
  tree joins the staged state into the row, through the tree's registered
  `CrdtShape`, and stamps the join above the row: the base's `Terminal`.
  `NoLostContributionBackstopInstallsLastWriterWins` reproduces production
  before the fix.
- **A split's CRDT forward and imports** (#4613, fixed by #4626). The source
  forwards a typed CRDT write to the split destination, and a cross-shard
  migration import of a CRDT row is joined, never dropped over the
  destination's own row: the base's `WriteB`, `Copy` and `Commit`.
  `NoLostContributionSplitImportDropsOverOwnRow` reproduces production before
  the fix.
- **A resize's CRDT mirror and drain** (#4618, fixed by #4665). The resize
  mirror forwards a typed CRDT write to the resized copy, and both the mirror's
  and the snapshot drain's merges join a CRDT row: the base's `WriteB` and
  `Copy`. `NoLostContributionResizeWriteNotMirrored` and
  `NoLostContributionResizeDrainLastWriterWins` reproduce production before the
  fix, one half each.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `row[l]` | The contributions the key's committed row holds at a location | The leaf projection row for the key at the migration's source (`"src"`) or destination (`"dst"`, the split target shard or the resized copy), decoded through the tree's `CrdtShape`. |
| `stamp[l]` | The row's HLC stamp | `LwwValue.Timestamp` of that row; `0` stands for no row. |
| `clk[l]` | The leaf clock | `LeafNodeState.Clock` of the leaf holding the row. |
| `migd[l]` | The row was last written by a cross-shard migration import | `LwwValue.IsMigrated`, which `BPlusLeafGrain.MergeEntriesAsync` sets on the cross-shard migration callsite. |
| `owner` | The location that owns the key | The registry's routing for the key: the source until the split's slot reassignment or the resize's alias swap, the destination after. |
| `leaf` | The source leaf that declares the key | The leaf whose span covers the key; `"L2"` after a leaf split moved the key's row to a new sibling. |
| `sg` | The saga's phase | `AtomicWriteState.Phase` of the cross-tree or single-tree atomic write. |
| `staged` | The saga's staged merged state | `LatticeStagedCrdtWrite.Value`, carried in `AtomicWriteState.Entries` beside its per-entry delta (`AtomicWriteState.EntryDeltas`). |
| `pend[l]` | The saga's prepared bucket for the key at a location | The leaf's pending bucket (`BPlusLeafGrain.PendingTx`) with its CRDT delta side-map entry. |
| `bleaf` | The source leaf the bucket was prepared on | The leaf that took the prepare; a leaf split leaves the bucket on it while moving the key's row. |
| `told` | Locations the terminal broadcast has visited | `AtomicWriteState`'s delivered-shard bookkeeping in `AtomicWriteGrain.BroadcastTerminalsAsync`. |
| `mg` | The migration's phase | `TreeShardSplitGrain`'s phase for a split; `TreeResizeGrain`'s for a resize. |
| `kind` | Which migration | A shard split (`TreeShardSplitGrain`) or an online resize (`TreeResizeGrain` with `TreeSnapshotGrain`). |
| `aAck`, `bAck` | Acknowledgement to a writer | The saga's caller returning, and `ILattice.ApplyCrdtDeltaAsync` returning. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Stage` | The caller stages a CRDT mutation and the saga prepares it | A CRDT accessor's `Stage` method minting `LatticeStagedCrdtWrite`, added through `LatticeAtomicWriteBuilder.Set`, and the saga's prepare bucketing it with its delta (`BPlusLeafGrain.AddPreparedMutation`). In the window the prepare is forwarded with it: the split's hot-path forward, or the resize mirror's prepare. | Yes: `SplitCrdtImportJoinIntegrationTests.An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split` and `CrdtBackstopJoinIntegrationTests.A_stranded_or_set_add_keeps_an_add_acknowledged_after_its_leaf_split`. |
| `Decide` | The saga records its commit decision | `AtomicWriteGrain` recording the decision in the transaction registry. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard`. |
| `Terminal(l)` | One location of the terminal broadcast | The leaf's `ApplyTxTerminalAsync`: a bucket on the declaring leaf drains through `BPlusLeafGrain.FoldPreparedCrdtDelta`; otherwise the backstop applies the staged state, joined into the row when the tree's merge mode is a CRDT mode, both for a bucket a leaf split stranded (forwarded to the declaring sibling) and for the coordinator's committed values on a leaf with no bucket (#4611). | Yes: `CrdtBackstopJoinIntegrationTests.A_stranded_or_set_add_keeps_an_add_acknowledged_after_its_leaf_split`, `CrdtBackstopJoinIntegrationTests.A_stranded_g_counter_increment_keeps_an_increment_acknowledged_after_its_leaf_split`, `BPlusLeafGrainTests.A_committed_values_backstop_joins_an_or_set_state_into_the_row`, `BPlusLeafGrainTests.A_committed_values_backstop_joins_a_g_counter_state_into_the_row` and `BPlusLeafGrainTests.A_stranded_crdt_prepare_forwarded_to_its_declaring_sibling_is_joined_there`. |
| `Complete` | The broadcast has visited every target and the caller is acknowledged | `AtomicWriteGrain.BroadcastTerminalsAsync` returning once every touched shard, the split's closure and the resize mirror's target have the terminal. | Yes: `CompensationContinuousReaderTests.Successful_saga_broadcasts_TxCommit_to_every_touched_shard` and `ResizeMirrorCrdtAndBulkIntegrationTests.A_saga_prepared_CRDT_delta_and_a_non_atomic_increment_both_reach_the_destination`. |
| `WriteB` | A non-atomic typed CRDT write | `ShardRootGrain.ApplyCrdtDeltaAsync` folding the delta on the owner's leaf. In the window the source forwards the post-fold row: a split through `ShardRootGrain.ForwardLocalCrdtWriteToShadowIfNeededAsync` as a migration import (#4613), a resize through the mirror into the resized copy under `LatticeCrdtJoinMergeContext` (#4618). The split's forward is not load-bearing for `NoLostContribution`: the final drain runs under the source's freeze, after which the source refuses the key, and joins every row the source holds, so it carries any write the forward would have. The model agrees (removing the split's forward leaves it clean), and so does production (removing it leaves the split detectors green, while disabling the import's join turns them red). | Yes: `SplitCrdtImportJoinIntegrationTests.A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split`, `ResizeMirrorCrdtAndBulkIntegrationTests.A_typed_CRDT_delta_applied_after_the_drain_passed_its_key_reaches_the_destination` and `ResizeMirrorCrdtAndBulkIntegrationTests.A_CRDT_delta_batch_applied_after_the_drain_passed_its_keys_reaches_the_destination`. |
| `LeafSplit` | A leaf split narrows the source leaf that declares the key | `BPlusLeafGrain`'s leaf split: the key's row moves to the new sibling and a prepared bucket stays on the donor, stranded, until the terminal re-routes it. **Environment action:** a leaf splits whenever it fills. | Yes: `CrdtBackstopJoinIntegrationTests.A_stranded_or_set_add_keeps_an_add_acknowledged_after_its_leaf_split` and `CrdtBackstopJoinIntegrationTests.A_stranded_g_counter_increment_keeps_an_increment_acknowledged_after_its_leaf_split`. |
| `Begin(k)` | A shard split or an online resize opens its window | `TreeShardSplitGrain` entering its shadow-write phase, or `TreeSnapshotGrain`'s online snapshot installing the shadow forward on the source copy. **Environment action:** either may begin at any time. | Yes: `TreeShardSplitGrainTests.ProcessNextPhase_drives_the_shadow_write_phase_through_the_full_split_pass` and `TreeSnapshotGrainTests.BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count`. |
| `Copy` | The drain passes the key and the in-flight bucket is replayed | The split's background drain (`TreeShardSplitGrain` through `ForwardMovedSlotEntriesAsync`, a cross-shard migration import), or the resize's snapshot drain (`TreeSnapshotGrain` merging under `LatticeCrdtJoinMergeContext`), each joining a CRDT row in `BPlusLeafGrain.MergeIntoStateAsync`; and `PreparedBucketSweep` replaying the source's bucket. | Yes: `SplitCrdtImportJoinIntegrationTests.An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split`, `SplitCrdtImportJoinIntegrationTests.An_OR_set_keeps_both_adds_across_a_split_with_the_destination_fold_ahead`, `TreeSnapshotGrainTests.Copy_merges_an_online_drain_batch_under_the_crdt_join_scope` (the snapshot drain's join scope, the only guard for a write the source took before the window opened, which the mirror never saw), `ResizeMirrorCrdtAndBulkIntegrationTests.An_OR_set_add_mirrored_below_the_destinations_fold_is_joined_not_dropped` and `ResizeMirrorCrdtAndBulkIntegrationTests.A_saga_prepared_CRDT_delta_and_a_non_atomic_increment_both_reach_the_destination`. |
| `Commit` | The migration makes the destination the owner | A split's final authoritative drain under the source's freeze, then the slot reassignment; a resize's alias swap. | Yes: `TreeShardSplitGrainTests.Swap_runs_final_drain_after_reject_and_before_shard_map_flip` and `SplitCrdtImportJoinIntegrationTests.A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split`. |
| `Stutter` | Quiescence | Not a protocol step: a stuttering successor once nothing is in flight. | Not applicable: not a protocol step, so there is no production behaviour to detect. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|----------------------------------|----------|
| `NoLostContribution` | The owner's copy of a CRDT key holds every contribution acknowledged to a writer, across a leaf split, a shard split and an online resize. | Yes: `SplitCrdtImportJoinIntegrationTests.A_G_counter_keeps_the_saga_and_the_non_atomic_increment_across_a_split`, `SplitCrdtImportJoinIntegrationTests.An_OR_set_keeps_the_saga_and_the_non_atomic_add_across_a_split`, `CrdtBackstopJoinIntegrationTests.A_stranded_or_set_add_keeps_an_add_acknowledged_after_its_leaf_split` and `ResizeMirrorCrdtAndBulkIntegrationTests.An_OR_set_add_mirrored_below_the_destinations_fold_is_joined_not_dropped`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Constrains the model's variables to their declared domains; there is no production counterpart or behavioural detector to map. |

## Over-approximation arguments

Each environment action is argued in its row. These deliberate
under-approximations are stated so that they are not mistaken for coverage:

- **One key, two contributions.** A second CRDT key, a second saga or a second
  non-atomic write adds no new way for two copies of one key to meet.
- **The drain imports the whole row.** Production's drains page through moved
  slots; for one key a page is the row.
- **No abort.** An aborted saga's bucket is discarded and contributes nothing,
  so it cannot lose a contribution; `ShardOwnership` covers the abort.

## Deliberate abstraction gaps

- **Same-replica counters in one cluster.** A G-counter increment staged and
  one applied directly by the same replica id in the same cluster resolve to
  the larger count, not their sum: the documented single-cluster
  concurrent-writer caveat on `LatticeStagedCrdtWrite`. The drain loses it as
  much as any other path, so it is not a merge defect; the module's two
  contributions are distinct, as an OR-Set's dots or two replicas' counts are.
- **Last-writer-wins keys.** Every non-CRDT path is `ShardOwnership`'s, which a
  merge outside the join scope keeps
  (`ResizeMirrorCrdtAndBulkIntegrationTests.A_merge_outside_the_mirror_keeps_last_writer_wins_for_a_CRDT_row`).
- **Time.** No timers, retention windows or deadlines.

## Territory owned by other open issues

No open issue currently owns a claim here. The section stays, empty of owners,
so a later census can see the question was asked; re-populate it when an open
issue next takes ownership of a claim made here.
