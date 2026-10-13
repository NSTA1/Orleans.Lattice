# AliasRouting refinement

## Variable mapping

| Spec variable | Role | Code counterpart |
|---------------|------|------------------|
| `alias`, `map`, `version` | Atomic registry routing row and monotonically changed map revision | `TreeRegistryEntry.PhysicalTreeId`, `ShardMap.Version`, `LatticeRegistryGrain.ExecuteAliasMoveAsync`. |
| `phase`, `source`, `destination`, `expected`, `token` | Durable move intent, captured predecessor and owning operation | `AliasRoutingMoveState`, `LatticeRegistryGrain.ExecuteAliasMoveAsync`. |
| `redirect`, `owner` | Per-logical-alias retained-copy fences and ownership | `ShardRootGrain.MarkRetainedRedirectAsync`, `ShardRootGrain.ClearRetainedRedirectIfOwnedAsync`. |
| `route` | Arbitrarily stale physical binding in a routing activation | `LatticeGrain.GetRoutingAsync`. |
| `moves`, `running`, `faults`, `overwritten` | Finite environment budgets, interruption and safety observer | Modelling devices. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Begin` | Persist intent before touching source shards | `LatticeRegistryGrain.MoveAliasRoutingAsync`. | Yes: `LatticeRegistryGrainTests.SetAliasAsync_intent_persist_failure_installs_no_source_fences`. |
| `Arm` | Wait for all source fences before publication | `LatticeRegistryGrain.ExecuteAliasMoveAsync`. | Yes: `LatticeRegistryGrainTests.SetAliasAsync_waits_for_every_source_fence_before_publishing`. |
| `Publish` | Publish matching identity/map and increment revision | `LatticeRegistryGrain.ExecuteAliasMoveAsync`. | Yes: `BareAliasRoutingIntegrationTests.SetAliasAsync_repeated_moves_and_remove_restore_the_matching_map`, `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `Supersede` | A competing writer owns a newer registry row | `LatticeRegistryGrain.ExecuteAliasMoveAsync`. | Yes: `LatticeRegistryGrainTests.SetAliasAsync_ambiguous_ack_cannot_overwrite_a_newer_alias_publication`. |
| `Abandon` | Preserve a superseding row and clear only owned source fences | `LatticeRegistryGrain.RollBackSourceFencesAsync`. | Yes: `LatticeRegistryGrainTests.SetAliasAsync_ambiguous_ack_cannot_overwrite_a_newer_alias_publication`, `LatticeRegistryGrainTests.SetAliasAsync_failure_rolls_back_routing_and_partial_fences_then_retry_succeeds`. |
| `Release` | Release only this logical alias on its destination | `AliasCutoverShardMaps.ReleaseRedirectsAsync`. | Yes: `BareAliasRoutingIntegrationTests.Moving_one_of_two_aliases_does_not_erase_the_other_alias_redirect`. |
| `Complete` | Retire durable intent only after destination release | `LatticeRegistryGrain.CompletePublishedAliasMoveAsync`. | Yes: `LatticeRegistryGrainTests.ReceiveReminder_rehydrates_and_finishes_committed_move_without_caller_retry`. |
| `Refresh` | Refresh cached physical routing from the committed row | `LatticeGrain.GetRoutingAsync`. | Yes: `BareAliasRoutingIntegrationTests.SetAliasAsync_moves_every_warmed_router_and_never_writes_the_retained_copy`. |
| `Crash` | Preserve durable intent during finite interruption | `AliasRoutingMoveState`. | Yes: `LatticeRegistryGrainTests.ReceiveReminder_rehydrates_prepublication_intent_and_moves_forward_without_caller_retry`. |
| `Retry` | Reminder resumes without caller retry | `LatticeRegistryGrain.ReceiveReminder`. | Yes: `LatticeRegistryGrainTests.ReceiveReminder_rehydrates_and_finishes_committed_move_without_caller_retry`. |
| `Stutter` | Environment inactivity | Modelling device. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|---------------------|----------|
| `CoherentRow` | Physical identity and map are published together | Yes: `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `NoStalePublication` | Recovery never overwrites a newer publication, even when physical identity returns | Yes: `LatticeRegistryGrainTests.SetAliasAsync_ambiguous_ack_cannot_overwrite_a_newer_alias_publication`, `LatticeRegistryGrainTests.ReceiveReminder_refuses_ABA_predecessor_with_newer_operation_or_map_version`. |
| `FinishedCopyUnfenced` | Returned move serves its destination directly | Yes: `BareAliasRoutingIntegrationTests.SetAliasAsync_repeated_moves_and_remove_restore_the_matching_map`. |
| `FenceBeforePublication` | Source fences precede alias publication | Yes: `LatticeRegistryGrainTests.SetAliasAsync_waits_for_every_source_fence_before_publishing`. |
| `RevisionAdvances` | Every effective move invalidates map-version caches | Yes: `AliasSwapRoutingAtomicityIntegrationTests.SwapAliasAsync_writes_the_alias_and_the_map_in_one_row`. |
| `IndependentAliases` | One logical alias does not clear another's fences | Yes: `BareAliasRoutingIntegrationTests.Moving_one_of_two_aliases_does_not_erase_the_other_alias_redirect`. |
| `RefreshedBinding` | A refreshed router binds to the current physical copy | Yes: `BareAliasRoutingIntegrationTests.SetAliasAsync_moves_every_warmed_router_and_never_writes_the_retained_copy`. |
| `DurableMove` | Recovery retains predecessor, phase and operation identity | Yes: `LatticeRegistryGrainTests.ReceiveReminder_rehydrates_prepublication_intent_and_moves_forward_without_caller_retry`. |
| `MovesSettle` | Finite faults and retries eventually settle durable work | Yes: `LatticeRegistryGrainTests.ReceiveReminder_rehydrates_and_finishes_committed_move_without_caller_retry`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Model-domain constraint, not a production behavior. |

## Limits

Registry serialization is abstracted into a single publication action.
Superseding writers are environmental steps; their own copy preparation belongs
to companion cutover models. Model token/revision bounds are finite, not
production counters. Phase-boundary faults do not enumerate every storage await.
The routing cache is a binding, not a full data-plane request interpreter.
Production detectors exercise the actual router, registry and shard boundaries.
