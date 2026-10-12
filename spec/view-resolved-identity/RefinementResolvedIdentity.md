# Resolved physical identity refinement

This finite companion covers #4895, but is not a machine-checked refinement of
the implementation. The accepted correction scopes resolved identifiers and
eviction order to the physical WAL actually tailed. Actual drain/rebuild detectors
discriminate reused identifiers, same-log duplicate retention and both uncertain
publication responses in one activation, without clearing caches by restarting.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `tail`, `owner` | Actual subscribed WAL identity and volatile cache owner | `ViewMaintainerGrain.DrainReadCheckpoints`; the physical-owner guard runs here before subscribing, not during candidate capture. |
| `resolved`, `order` | Same-activation suppression set and eviction queue | `ViewMaintainerGrain.MarkResolved`; one ID represents both structures, not capacity eviction. |
| `prepared`, `terminal`, `stagedOwner` | Current physical log's bounded staging | `ViewMaintainerGrain.HandleStagingEntry`, `ViewMaintainerGrain.IsComplete`. |
| `read`, `heads`, `offsets` | Physical progress, captured source bounds and durable replay entitlement | `ViewMaintainerGrain.DrainReadCheckpoints`, `ViewCheckpointState.AppliedOffsets`; physical-tagged offsets are abstract, not a persisted schema change. |
| `projections` | Projection invocations for each distinct physical transaction | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`; backend idempotent effects are not expanded. |
| `liveBinding`, `durableBinding` | Mutated checkpoint object versus committed storage | `ViewMaintainerGrain.RebuildAsync`; a failed write may change only the live object. |
| `candidate`, `capturedHead`, `outcome`, `phase` | One candidate episode and distinct publication responses | `ViewMaintainerGrain.RebuildAsync`; finite sequencing devices, not new persisted members. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `StartRebuild` | Choose same or new physical candidate after original resolution | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_same_physical_log_rebuild_keeps_resolved_duplicate_suppression`. |
| `Capture` | Capture candidate heads without adopting or clearing the tail's cache | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_same_physical_log_rebuild_keeps_resolved_duplicate_suppression`. |
| `Deliver` | Late original duplicates and reused new-log T arrive after capture | `ViewMaintainerGrain.HandleStagingEntry`; an explicit source environment step. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |
| `PublishCheckpoint` | Candidate mutates live checkpoint; commit and response are distinct | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_failed_candidate_publication_then_original_log_retry_keeps_resolved_ownership`. |
| `RetryCheckpoint` | Fair persistence retry after an uncommitted write | `ViewMaintainerGrain.RebuildAsync`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_failed_candidate_publication_then_original_log_retry_keeps_resolved_ownership`. |
| `EnterTail` | Actual physical tail transition adopts owner and clears both caches only if changed | `ViewMaintainerGrain.DrainReadCheckpoints`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |
| `Read` | Stage new-owner entries; suppress already resolved current-owner duplicates | `ViewMaintainerGrain.HandleStagingEntry`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal`. |
| `Flush` | Invoke projection once, then resolve the identity for the current owner | `ViewMaintainerGrain.FlushCompletedFilterBatchesAsync`, `ViewMaintainerGrain.MarkResolved`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |
| `Checkpoint` | Durable progress cannot silently skip a physically distinct reused identity | `ViewMaintainerGrain.ApplyCheckpointHoldBack`. | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |
| `Stutter` | No protocol step | Not applicable. | Not applicable. |

## Property mapping

| Spec property | Code-level property | Detector |
|---------------|---------------------|----------|
| `CacheOwned` | Cache owner is the actual subscriber tail, never a candidate or mutable checkpoint | Yes: `ViewMaintainerPhysicalStagingTests.Drain_failed_candidate_publication_then_original_log_retry_keeps_resolved_ownership`. |
| `QueueSound` | Physical transition clears both the resolved set and eviction order | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_resolved_eviction_queue_does_not_evict_new_log_duplicate_ownership` reaches the actual production capacity after physical rebind. `ViewMaintainerPhysicalStagingTests.Drain_resolved_queue_detector_rejects_retained_old_queue_only_control` retains only the retired queue after actual new-tail selection; its old entry wrongly evicts the newly owned reused ID and the public projection exposes a duplicate. |
| `StageOwned` | Buffered prepares and terminals name the subscribed physical log | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_flush_old_prepares_under_a_new_log_terminal`. |
| `ProjectionOnce` | Same-owner late duplicates do not re-invoke projection in the same activation | Yes: `ViewMaintainerPhysicalStagingTests.Drain_same_physical_log_rebuild_keeps_resolved_duplicate_suppression`. |
| `CheckpointSafe` | Old-log resolution cannot authorize checkpointing over a different new-log transaction reusing T | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |
| `ResolvedRetained` | Candidate/rebuild/failure cannot clear suppression while the actual tail remains unchanged | Yes: `ViewMaintainerPhysicalStagingTests.Drain_failed_candidate_publication_then_original_log_retry_keeps_resolved_ownership`. |
| `Converges` | Fair retry, source delivery and tail processing finish the new physical transaction | Yes: `ViewMaintainerPhysicalStagingTests.Drain_rebind_does_not_reuse_old_log_resolved_transaction_ids`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite model-domain check, not production behaviour. |

## Composition

One activation and one logical transaction ID suffice to distinguish the cache
ownership bug. Actual old resolution is retained through candidate capture and
publication uncertainty. A new tail has a new ownership epoch even when its
logical TxId matches the old one. Same physical identity does not establish a new
epoch merely because a rebuild ran.

SourceAdmission/ReadyClosure fix a physical identity while modelling revision
admission and atomic winner cuts; they rely on this identity separation, but do
not prove it. Rebind supplies checkpoint/generation/hold transitions without a
resolved-cache dimension. Crash replay and bounded cache eviction remain outside
this finite episode and require their own detectors.
