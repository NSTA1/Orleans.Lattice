# Refinement note: B+ tree topology under writes and recovery

This bounded model complements [`SplitLink.tla`](SplitLink.tla). It captures
concurrent writes during leaf split and fold, the persisted ordering of the
sibling chain and parent route, and the durable recovery obligation when the
root loses its in-memory split result. It abstracts key payloads to presence;
the existing CRDT ownership models cover payload joins during shard moves.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `donorRows`, `siblingRows`, `acked` | Key presence during writes and topology transfer | Leaf row maps and completed `SetAsync` operations. |
| `phase` | Split/fold protocol stage | Split intent and completion in `BPlusLeafGrain.Split.cs`; leaf-fold lifecycle in `ShardRootGrain.LeafReclaim.cs`. |
| `born` | Sibling durable state exists | `InitializeSiblingAsync` persists sibling metadata before linking. |
| `donorNext`, `siblingPrev`, `successorPrev` | Both directions of the sibling chain | Donor `NextSibling`, new sibling `PrevSibling`/`NextSibling`, and the old successor's repaired `PrevSibling`. |
| `linked`, `childParent` | Parent routing and child-side link are published | The parent's child list and the durable pending child-link protocol. This is a routing abstraction, not a literal leaf parent-pointer field. |
| `pending`, `marker`, `held` | Durable and volatile split-link evidence | `PendingChildLink`, `UnlinkedSplitSiblingId`/`UnlinkedSplitKey`, and the returned `SplitResult`. |
| `crashes` | Bounded root activation loss | Root state survives; volatile split result does not. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write(k)` | Acknowledged key write, routed by the published boundary | `BPlusLeafGrain.SetAsync` and concurrent root writes | Yes: `ConcurrentSplitLinkageIntegrationTests.Interleaved_SetAsync_and_SetManyAsync_leave_no_orphaned_leaves_or_lost_keys`. |
| `SplitIntent` | Persist split intent before moving rows | `BPlusLeafGrain.SplitAsync` | Yes: `BPlusLeafGrainTests.SetAsync_returns_split_result_when_state_has_in_progress_split`. |
| `Birth` | Persist the new sibling before linking it | `BPlusLeafGrain.CompleteSplitAsync` and `InitializeSiblingAsync` | Yes: `BPlusLeafGrainTests.Split_stamps_sibling_key_range_with_split_key_and_inherited_high`. |
| `ChainDonor` | Publish donor's forward pointer | `BPlusLeafGrain.CompleteSplitAsync` | Yes: `BPlusLeafGrainTests.Split_recovery_sets_new_sibling_PrevSibling_to_this_leaf`. |
| `ChainSuccessor` | Repair the old successor's reverse pointer | `BPlusLeafGrain.CompleteSplitAsync` | Yes: `BPlusLeafGrainTests.Split_recovery_updates_old_next_PrevSibling_to_new_sibling`. |
| `Move(k)` | Transfer a split-range key | Bounded idempotent transfer and donor removal | Yes: `BPlusLeafGrainTests.Recovery_trims_entries_from_original_leaf` and `BPlusLeafGrainTests.Recovery_preserves_tombstones_in_right_half`. |
| `FinishSplit` | Complete transfer while retaining a durable link marker | `CompleteSplitAsync` | Yes: `BPlusLeafGrainTests.Split_flushes_source_state_before_returning_SplitResult`. |
| `RecordPending` | Persist parent link obligation | `ShardRootGrain.RecordPendingChildLinksAsync` | Yes: `ShardRootGrainSplitLinkTests.The_link_intent_is_persisted_before_the_parent_is_asked_to_accept`. |
| `Link` | Publish the child into parent routing | `ShardRootGrain.LinkSplitLockedAsync` | Yes: `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds`. |
| `Crash` | Lose the volatile result only | Root activation recovery | Yes: `ShardRootGrainSplitLinkTests.A_link_that_faults_stays_recorded_and_the_next_operation_redelivers_it`. |
| `MergeBegin` | Begin folding a routed sibling into its predecessor | Empty-leaf reclaim; a candidate is latched before unlink | Yes: `ShardRootGrainLeafReclaimResilienceTests.A_completed_split_does_not_suppress_reclaim_forever`. |
| `Merge(k)` | Move a sibling key to the surviving leaf | Idempotent row merge during topology consolidation | Yes: `BPlusLeafGrainTests.MergeEntries_is_idempotent` and `BPlusLeafGrainTests.MergeEntries_keeps_newer_local_value`. |
| `MergeFinish` | Remove the empty sibling from routing and the chain | `ShardRootGrain.TryReclaimLeafAsync` | Yes: `ShardRootGrainOrphanRepairTests.A_verified_orphan_is_unspliced_and_its_pin_retired`. |
| `Stutter` | Terminal self-loop | No production step | Not applicable. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|---------------------------------|----------|
| `TypeOK` | All model state remains in its finite domains. | Yes: `BPlusLeafGrainTests.Recovery_reuses_persisted_sibling_id`. |
| `NoKeyLost` | Acknowledged keys remain on at least one live leaf through transfer. | Yes: `ConcurrentSplitLinkageIntegrationTests.Concurrent_SetAsync_leaves_no_orphaned_leaves_or_lost_keys`. |
| `ParentSiblingConsistency` | Parent routing never exposes an unborn leaf or a partly-written sibling chain. | Yes: `BPlusLeafGrainTests.Split_recovery_updates_old_next_PrevSibling_to_new_sibling` and `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds`. |
| `OrphanRecoverable` | A born, unlinked sibling is still in an active split or carries durable recovery evidence. | Yes: `BPlusLeafGrainTests.Set_after_a_lost_split_result_resurfaces_the_unlinked_sibling`. |
| `PendingOnlyForCompleteSplit` | A root pending-link record cannot name a split that did not finish. | Yes: `ShardRootGrainSplitLinkTests.The_link_intent_is_persisted_before_the_parent_is_asked_to_accept`. |
| `SplitRequiresData` | A split starts only for a non-empty donor. | Yes: `BPlusLeafGrainTests.SetAsync_returns_split_result_when_state_has_in_progress_split`. |
| `BornImpliesSplit` | A sibling exists only as part of a split or its completed topology. | Yes: `BPlusLeafGrainTests.Recovery_reuses_persisted_sibling_id`. |
| `StableNoDuplicate` | A settled split or fold leaves each key on one leaf. | Yes: `BPlusLeafGrainTests.Recovery_trims_entries_from_original_leaf` and `BPlusLeafGrainTests.MergeEntries_is_idempotent`. |

## Bounds and complementary coverage

This model uses a fixed two-leaf split/fold and three keys. Each `Move` and
`Merge` is one key-level transfer; writes can interleave between transfers.
The abstraction checks key presence, not value stamps, tombstones or multi-level
parent splits. The existing `ShardOwnership` models already cover the range
ownership map through adaptive shard split, online reshard and online resize,
including the `UniqueOwner`, `NoKeyLost`, `ReshardCompletes`,
`ResizeCompletes` and `RoutingConverges` properties. Their CRDT companion
checks that moved contributions join rather than overwrite. This module does
not duplicate those larger state spaces.
