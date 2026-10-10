# Refinement note: bounded cascading B+ tree splits

`BPlusCascade` extends the B+ tree topology models with a bounded hierarchy
whose rightmost leaf split can overflow its parent, continue through multiple
internal levels, and promote a new root. It abstracts one serialized structural
operation at a time; ordinary writes can interleave while the parent split is
being propagated.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `active`, `kind`, `parent`, `children`, `nodeKeys`, `nodeLevel` | Finite node identities, kinds, parent/child edges, ordered key summaries, and levels from the current root. | `InternalNodeState.Children`, `InternalNodeState.ParentId`, `InternalNodeState.ChildrenAreLeaves`, `LeafNodeState.ParentId`, and the split transitions in `BPlusInternalGrain`. Leaf values, tombstones, and HLC ordering are abstracted. |
| `previousLeaf`, `nextLeaf`, `firstLeaf`, `rightmost` | The ordered doubly-linked leaf chain and the ends used by the append-only workload. | `LeafNodeState.PrevSibling`, `LeafNodeState.NextSibling`, `BPlusLeafGrain.SetPrevSiblingAsync`, and `BPlusLeafGrain.SetNextSiblingAsync`. The model appends only at the right edge. |
| `root`, `treeHeight` | The unique root and the derived number of node levels. | `ShardRootState.RootNodeId`, `ShardRootState.RootIsLeaf`, `InternalNodeState.ChildrenAreLeaves`, and `ShardRootGrain.PromoteRootLockedAsync`. Production derives height from node links; it has no persisted height counter or maximum-height setting. |
| `acked` | The set of successfully acknowledged keys that must remain in exactly one leaf. | `BPlusLeafGrain.SetAsync` and the shard-root write path. Values, deletes, tombstones, and duplicate-key update semantics are abstracted. |
| `pending` | The current durable phase: leaf birth/link, internal split/link, or root promotion. | `LeafNodeState.SplitState`, `InternalNodeState.SplitState`, `InternalNodeState.SplitRightChildren`, `ShardRootState.PendingChildLinks`, and `ShardRootState.PendingPromotion`. Distributed intents are combined into one abstract obligation. |
| `ready`, `crashes` | Whether the activation can advance and the finite activation-loss budget. | Grain deactivation/reactivation while persisted split state and root link intent remain durable. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Write` | Acknowledge the next ordered key; on leaf overflow, record the pending leaf-birth phase. | `BPlusLeafGrain.SetAsync` and the shard-root write path. | Yes: `ConcurrentSplitLinkageIntegrationTests.Sequential_splits_reach_at_least_six_tree_levels`. |
| `BirthLeaf` | Divide the rightmost donor's keys, allocate its sibling, and publish the forward/backward leaf links. | `BPlusLeafGrain.SplitAsync` and `BPlusLeafGrain.CompleteSplitAsync`. | Yes: `BPlusLeafGrainTests.Split_stamps_sibling_key_range_with_split_key_and_inherited_high`. |
| `LinkLeaf` | Add the born sibling to its parent and discharge or advance the parent overflow. | `ShardRootGrain.LinkSplitLockedAsync` and `BPlusInternalGrain.AcceptSplitAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds`. |
| `SplitInternal` | Divide an over-capacity internal node into ordered left and right child groups. | `BPlusInternalGrain.SplitAsync` and `BPlusInternalGrain.CompleteSplitAsync`. | Yes: `BPlusInternalGrainTests.AcceptSplit_inserts_new_separator` and `BPlusInternalGrainTests.AcceptSplit_recovers_in_progress_split_and_forwards_promotion_to_sibling`. |
| `LinkInternal` | Publish the internal sibling's promoted separator at the next parent, possibly starting another split. | `ShardRootGrain.LinkSplitLockedAsync` and `BPlusInternalGrain.AcceptSplitAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `PromoteRoot` | Wrap the two split root halves in a fresh root and increase the derived height. | `ShardRootGrain.PromoteRootLockedAsync`. | Yes: `ShardRootGrainSplitLinkTests.A_root_that_divides_on_accept_is_wrapped_under_a_new_root` and `ConcurrentSplitLinkageIntegrationTests.Sequential_splits_reach_at_least_six_tree_levels`. |
| `Crash` | Lose activation-local progress while preserving durable split or parent-link intent. | Grain activation loss; persisted state is replayed by the existing activation and recovery paths. | Yes: `BPlusInternalGrainTests.AcceptSplit_recovers_in_progress_split_and_forwards_promotion_to_sibling`. |
| `Recover` | Resume the pending internal split or parent-link phase after activation recovery. | `BPlusInternalGrain.AcceptSplitCoreAsync` and `ShardRootGrain.ResumePendingChildLinksAsync`. | Yes: `BPlusInternalGrainTests.Recovery_reuses_persisted_sibling_id` and `ShardRootGrainSplitLinkTests.A_link_that_faults_stays_recorded_and_the_next_operation_redelivers_it`. |
| `Stutter` | Terminal self-loop. | No production step. | Not applicable. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
|---------------|--------------------------------|----------|
| `TypeOK` | Persisted split phases and child layouts remain in legal state domains. | Yes: `BPlusInternalGrainTests.Initialize_creates_two_children` and `BPlusInternalGrainTests.Recovery_clears_split_right_children`. |
| `NoKeyLostOrDuplicated` | Every acknowledged key is owned by exactly one live leaf. | Yes: `ConcurrentSplitLinkageIntegrationTests.Concurrent_SetAsync_leaves_no_orphaned_leaves_or_lost_keys`. |
| `SortedLeafChain` | The append-only leaf chain contains every leaf once, has reciprocal links, and orders disjoint leaf ranges. | Yes: `BPlusLeafGrainTests.Split_recovery_sets_new_sibling_PrevSibling_to_this_leaf`, `BPlusLeafGrainTests.Split_recovery_sets_new_sibling_NextSibling_to_old_next`, and `ConcurrentSplitLinkageIntegrationTests.Concurrent_SetAsync_leaves_no_orphaned_leaves_or_lost_keys`. |
| `ParentChildAccounting` | Every settled child has exactly one matching parent edge; a detached split sibling is explained by the pending link. | Yes: `ShardRootGrainSplitLinkTests.A_leaf_split_is_linked_under_the_parent_a_fresh_descent_finds` and `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `SeparatorRanges` | Each child separator is the lower bound of its subtree and routes keys to the owning child. | Yes: `BPlusInternalGrainTests.AcceptSplit_maintains_sort_order` and `BPlusInternalGrainTests.Route_with_multiple_separators_picks_correct_child`. |
| `FanoutBound` | Settled leaves and internal nodes respect configured fan-out; only the currently splitting node may temporarily overflow. | Yes: `BPlusInternalGrainTests.AcceptSplit_returns_null_when_under_capacity` and `ShardRootGrainSplitLinkTests.A_parent_that_divides_on_accept_has_its_division_linked_a_level_up`. |
| `RootHasOneRoot` | The root is parentless and promotion retains both halves under the one new root. | Yes: `ShardRootGrainSplitLinkTests.A_root_that_divides_on_accept_is_wrapped_under_a_new_root`. |
| `HeightBound` | The TLC instance never exceeds its configured `MaxHeight`; this is a finite model bound, not a production maximum. | Yes: `ConcurrentSplitLinkageIntegrationTests.Sequential_splits_reach_at_least_six_tree_levels`. |
| `RecoverablePending` | Every interrupted split phase retains enough durable evidence to finish or deliver the parent link. | Yes: `BPlusInternalGrainTests.AcceptSplit_recovers_in_progress_split_and_forwards_promotion_to_sibling` and `ShardRootGrainSplitLinkTests.The_link_intent_is_persisted_before_the_parent_is_asked_to_accept`. |
| `RoutedKeysOwned` | In settled states, separator descent routes each acknowledged key to its unique owning leaf. | Yes: `BPlusInternalGrainTests.Route_with_multiple_separators_picks_correct_child` and `ConcurrentSplitLinkageIntegrationTests.Concurrent_SetAsync_leaves_no_orphaned_leaves_or_lost_keys`. |
| `ReachMaxHeight` | Under the model's weak fairness assumptions, cascading right-edge splits can reach the six-level instance bound. | Yes: `ConcurrentSplitLinkageIntegrationTests.Sequential_splits_reach_at_least_six_tree_levels`. |

## Bounds and abstraction gaps

The checked configuration begins at height three with four leaves and fan-out
three. It uses keys `1..184`, at most sixty-four leaves, 128 node identities,
one activation loss, and `MaxHeight = 6`. TLC 1.7.4 exhaustively checks 1,271
distinct states for this configuration. The level-indexed transitions are
generic over the modeled levels, and an integration regression drives a real
fan-out-three shard to at least six levels. Neither finite check is an
unbounded-height proof or a production maximum-height guarantee.

One structural split is pending at a time in this abstraction; ordered writes
can interleave between its durable protocol phases, but two simultaneous
cascades are not modeled here. `BPlusTopology` and the concurrent split
integration tests cover their separate bounded interleavings. The cascade also
abstracts values, deletions, arbitrary split pivots, non-rightmost splits, and
the full leaf-transfer protocol.

Internal-node redistribution/merge and root contraction are deliberate
non-goals: production leaf reclaim removes an eligible child route but does not
rebalance internal nodes or contract the root. The model also does not cover the
orphan audit/repair algorithm. These are not implied behaviors; see the related
production boundaries in the B+ tree module inventory.