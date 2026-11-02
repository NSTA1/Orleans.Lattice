# Refinement note: AntiEntropy to production

This is a documented correspondence, not a machine-checked refinement proof.
The source is `src/lattice.replication/`: the digest-probe grain, its
comparer, localiser, remediation guard, leaf re-replayer and bootstrap
fallback planner, and the receiver applier and high-water-mark grain. The
detector fixtures exercise those real components through their seams.

Internal-node resolution reads the projection digest and then a defensive routing
snapshot through one `GetSubtreeDigestAndRoutingAsync` call, instead of two
external dispatches. The option gate, digest calculation, child order and live
physical-tree resolution are unchanged; no topology mutation, child publication
or repair step is elided. This is a read fusion, not a new persisted snapshot
or a claim of cross-node atomicity. `BPlusInternalGrainTests` checks both returned
projections and `GrainMerkleWalkLocalTreeTests` pins the one-call adapter budget.

## Variable mapping

| Spec variable | Role | Code counterpart |
|---------------|------|------------------|
| `aStore`, `bStore` | Each cluster's committed projection of a key: timestamp, tombstone flag and origin stamp | The leaf rows the fallback reads through `BootstrapFallbackPlanner.PlanAsync` and the receiver writes through `ReplicationApplier.ApplyAsync`. |
| `inflight` | Replication and repair entries in transit, tagged ship or repair | Batches shipped by the replicator and the re-ship sink, received by `ReplicationApplier.ApplyBatchAsync`. |
| `applied` | The receiver's identity dedup record | `ReplicationApplier.AppliedIdentity`, consulted by `ReplicationApplier.ApplyAsync`. Production bounds the record; the model does not. |
| `hwm` | B's per-origin high-water-mark for A, which is A's re-replay cursor | `ReplicationHighWaterMarkGrain.TryAdvanceAsync`, read by `ReplicationDigestProbeGrain.TryReReplayLeavesAsync`. |
| `wal`, `walFloor` | A's retained A-origin WAL and its trim floor | The retained entries and oldest retained offset consulted by `LeafReReplayer.ReplayAsync`. |
| `journal` | Ghost record of every A-origin entry ever journalled | Modelling device for the WAL completeness claim; no production field. |
| `clock` | Globally unique hybrid logical clock stamps | The HLC stamped on every write; bounded here. |
| `phase`, `loc` | The probe pass in flight and the ranges the walk localised | `ReplicationDigestProbeGrain.ProcessNextPhaseAsync` and the ranges returned by `MerkleWalkLocaliser.WalkAsync`. |
| `epoch`, `locEpoch` | The topology generation and the generation a walk resolved | `ReplicationDigestProbeGrain.EnsurePhysicalTreeIdAsync` resolves the physical tree each pass; a reshard or resize changes the shard map, and an alias swap changes the physical tree. |
| `swapEpoch`, `swapTs` | The generation an alias swap installed, and the clock at the swap | The alias resolve `ReplicationDigestProbeGrain.EnsurePhysicalTreeIdAsync` performs; a shadow cutover, restore or explicit alias change binds the logical name to a new physical tree with its own WAL. |
| `retired`, `retFloor` | The retired physical tree's frozen WAL and its floor | The WAL keyed by the old physical id; `WalGrainReReplaySource` reads the WAL of the physical id its caller passes. |
| `latch` | Projection-digest maintenance latched off for the tree | `ILatticeRegistry.LatchProjectionDigestPermanentlyDisabledAsync`; the probe grain skips a latched tree. |
| `optOut` | The AutoRemediateOnDigestMismatch master switch is off | `ReplicationDigestProbeGrain.SkipRemediation` records the opt-out skip. |
| `failures`, `open`, `cooled` | Consecutive failures, breaker state and elapsed cooldown for a tree and peer | `RemediationGuard.RecordFailure`, `RemediationGuard.RecordSuccess` and `RemediationGuard.IsCircuitBlocking`. |
| `consumed` | Entries shipped in the current budget window | `RemediationGuard.RecordEntriesShipped` and `RemediationGuard.TryBeginRemediation`. |
| `env` | The exploration bound on environment events | Modelling device; no production field. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `AWrite(k,d)` | A local Set or Delete is journalled stamped with A's origin and shipped | Local writes enter the WAL that `LeafReReplayer.ReplayAsync` later reads, and the ordinary shipper sends them to `ReplicationApplier.ApplyAsync`. | Yes: `ReplicationApplierTests.ApplyAsync_advances_hwm_after_successful_apply`, `LeafReReplayerTests.ReplayAsync_reships_in_range_entries_above_cursor` and `BPlusLeafGrainTests.DeleteAsync_restamps_above_a_refused_clock_floor_and_commits_once`. |
| `BWrite(k)` | A peer write replicates back to A, so A holds foreign-origin rows | The peer's applier path, `ReplicationApplier.ApplyAsync`. | Yes: `AntiEntropyMultiSiteDriftChaosTests.Partition_then_heal_diverges_both_directions_and_reconciles_under_last_writer_wins` and `LeafReReplayerTests.ReplayAsync_excludes_foreign_origin_entries`. |
| `Reflect(k)` | An own-origin row is echoed back to its origin | Dropped by the own-origin check in `ReplicationApplier.ApplyAsync`. | Yes: `ReplicationApplierTests.ApplyAsync_skips_local_origin_entries_as_no_op` and `ReplicationApplierTests.ApplyAsync_does_not_cache_local_origin_entries`. |
| `Drop(m)` | Ordinary shipping loses an A-origin Set or Delete (drift) | Environment fault; the dedup must not have recorded it, so `ReplicationApplier.ApplyAsync` still applies its re-ship. | Yes: `ReplicationApplierTests.ApplyAsync_rolls_back_cache_when_apply_grain_throws_so_retry_can_apply` and `AntiEntropyMultiSiteDriftChaosTests.Partition_then_heal_diverges_both_directions_and_reconciles_under_last_writer_wins`. |
| `DeliverB(m)` | Own-origin drop, identity dedup, last-writer-wins apply, cursor advance | `ReplicationApplier.ApplyAsync` and `ReplicationHighWaterMarkGrain.TryAdvanceAsync`. | Yes: `ReplicationApplierTests.ApplyAsync_dedupes_duplicate_identity_tuple_without_invoking_apply_grain`, `ReplicationApplierTests.ApplyAsync_advances_hwm_after_successful_apply` and `ReplicationHighWaterMarkGrainTests.TryAdvanceAsync_returns_false_when_candidate_is_less_than_current`. |
| `DeliverA(m)` | A applies a peer write under last-writer-wins | `ReplicationApplier.ApplyAsync`. | Yes: `CompressedReplicationApplyIntegrationTests.ApplySetAsync_older_source_hlc_does_not_overwrite_newer_local_value`. |
| `Trim` | WAL garbage collection past a floor | The trimmed log `LeafReReplayer.ReplayAsync` classifies against the cursor. | Yes: `LeafReReplayerTests.ReplayAsync_wal_trimmed_past_cursor_skips_with_operator_alert` and `LeafReReplayerTests.ReplayAsync_trimmed_but_cursor_above_oldest_still_repairs`. |
| `ProbeCompare` | A scheduled digest comparison records a match or a mismatch | `ReplicationDigestProbeGrain.ProcessNextPhaseAsync` and `DigestProbeComparer.Compare`. | Yes: `DigestProbeComparerTests.Compare_returns_mismatch_when_versions_match_but_hashes_differ`, `DigestProbeComparerTests.Compare_returns_match_when_versions_and_hashes_equal`, `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_records_mismatch_when_digests_differ_same_version` and `BPlusLeafGrainTests.Digest_is_independent_of_the_origin_stamp`, which pins the content-only per-entry contribution `SameValue` assumes. |
| `ProbeUnavailable` | The peer is unavailable, the versions skew or the digest is not maintained: no comparison | `DigestProbeComparer.Compare` and `ReplicationDigestProbeGrain.ProcessNextPhaseAsync`. | Yes: `DigestProbeComparerTests.Compare_returns_remote_unavailable_when_digest_not_available`, `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_records_version_skew_and_no_mismatch` and `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_skips_when_maintain_projection_digest_is_false`. |
| `Localise` | The Merkle walk descends to the diverging leaves of a freshly resolved physical tree | `ReplicationDigestProbeGrain.TryLocaliseDriftAsync` and `MerkleWalkLocaliser.WalkAsync`. | Yes: `MerkleWalkLocaliserTests.WalkAsync_descends_to_diverging_leaf_and_prunes_matching_sibling` and `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_localises_against_a_freshly_resolved_physical_tree`. |
| `LocaliseAbort` | The walk aborts or localises nothing; no remediation | `MerkleWalkLocaliser.WalkAsync` and `ReplicationDigestProbeGrain.TryLocaliseDriftAsync`. | Yes: `MerkleWalkLocaliserTests.WalkAsync_aborts_on_depth_cap`, `MerkleWalkLocaliserTests.WalkAsync_aborts_on_remote_unavailable`, `MerkleWalkLocaliserTests.WalkAsync_matching_root_localises_nothing` and `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_swallows_a_merkle_walk_localisation_fault`. |
| `RemediationGated` | Opt-out, a cooling breaker or an exhausted budget skips the pass with zero traffic | `ReplicationDigestProbeGrain.TryRemediateAsync`, `ReplicationDigestProbeGrain.SkipRemediation`, `RemediationGuard.IsCircuitBlocking` and `RemediationGuard.TryBeginRemediation`. | Yes: `ReplicationDigestProbeGrainTests.Remediation_is_skipped_with_opt_out_when_auto_remediate_is_off`, `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_circuit_open_after_the_first_shards_reship_is_rejected` and `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_budget_exhausted_after_the_first_shard_spends_the_window`. |
| `ReReplay` | Re-ship the retained A-origin entries of the localised ranges above the cursor; a shipped pass is a success | `ReplicationDigestProbeGrain.TryReReplayLeavesAsync`, `LeafReReplayer.ReplayAsync` and `RemediationGuard.RecordSuccess`. | Yes: `LeafReReplayerTests.ReplayAsync_reships_in_range_entries_above_cursor`, `LeafReReplayerTests.ReplayAsync_excludes_foreign_origin_entries`, `LeafReReplayerTests.ReplayAsync_reships_a_delete_entry_as_a_delete`, `ReplicationDigestProbeGrainTests.Re_replay_that_closes_the_gap_does_not_trigger_the_bootstrap_fallback` and `ReplicationDigestProbeGrainTests.Re_replay_after_an_alias_swap_reads_the_live_physical_wal_not_the_retired_one`. |
| `Fallback` | After a wal_trimmed or range_empty skip, re-ship the committed rows of the localised ranges from the live projection, a tombstone as a delete | `ReplicationDigestProbeGrain.TryBootstrapFallbackAsync` and `BootstrapFallbackPlanner.PlanAsync`. | Yes: `BootstrapFallbackPlannerTests.PlanAsync_reships_in_range_committed_entries`, `BootstrapFallbackPlannerTests.PlanAsync_skips_prepared_rows_and_reships_tombstones_as_deletes`, `BootstrapFallbackPlannerTests.PlanAsync_reships_a_tombstone_as_a_delete_carrying_its_own_clock`, `ReplicationDigestProbeGrainTests.Below_cursor_lost_delete_is_reshipped_as_a_delete_by_the_bootstrap_fallback`, `BootstrapFallbackPlannerTests.PlanAsync_reships_set_records_stamped_with_origin` and `ReplicationDigestProbeGrainTests.Below_cursor_orphan_divergence_triggers_the_bootstrap_fallback`. |
| `ShipRejected` | The peer rejects a non-empty re-ship, or the re-ship or fallback faults; a failure is recorded and the breaker opens at the threshold | `RemediationGuard.RecordFailure`, reached from `ReplicationDigestProbeGrain.TryRemediateAsync`. | Yes: `LeafReReplayerTests.ReplayAsync_ack_rejected_reports_zero_shipped`, `BootstrapFallbackPlannerTests.PlanAsync_ack_rejected_reports_zero_shipped`, `RemediationGuardTests.RecordFailure_with_threshold_one_opens_on_first_failure`, `RemediationGuardTests.RecordFailure_during_half_open_refreshes_cooldown`, `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_circuit_open_after_the_first_shards_reship_throws` and `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_circuit_open_after_the_first_shards_fallback_faults`. |
| `Cooldown` | The breaker's cooldown elapses and the next pass is a half-open trial | `RemediationGuard.IsCircuitBlocking`. | Yes: `RemediationGuardTests.IsCircuitBlocking_is_true_while_cooling_and_false_after_cooldown`. |
| `WindowRoll` | The traffic-budget window rolls over | `RemediationGuard.TryBeginRemediation`. | Yes: `RemediationGuardTests.TryBeginRemediation_allows_again_after_window_rolls_over`. |
| `Restart` | The probe grain re-activates, losing in-memory guard accounting and the pass in flight | `ReplicationDigestProbeGrain.EnsureActiveAsync` creates a fresh `RemediationGuard`; the keepalive reminder re-arms the probe. | Yes: `ReplicationDigestProbeGrainTests.EnsureActiveAsync_registers_the_keepalive_reminder_and_starts_the_phase_timer` and `ReplicationDigestProbeGrainTests.ReceiveReminder_rearms_the_phase_timer_because_the_probe_is_always_in_progress`. |
| `Reshard` | A reshard or resize changes the shard map within the same physical tree | `ReplicationDigestProbeGrain.EnsurePhysicalTreeIdAsync` and the live shard map read by `ReplicationDigestProbeGrain.ProcessNextPhaseAsync`. | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_localises_against_a_freshly_resolved_physical_tree` and `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_probes_the_shards_of_the_live_map_not_the_pinned_count`. |
| `AliasSwap` | A shadow cutover, restore or alias change binds the logical tree to a new physical tree with an empty WAL, retiring the old WAL, and advances the generation a resolve observes | `ReplicationDigestProbeGrain.EnsurePhysicalTreeIdAsync`, threaded through `ReplicationDigestProbeGrain.TryRemediateAsync` into `WalGrainReReplaySource`. | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_localises_against_a_freshly_resolved_physical_tree` and `ReplicationDigestProbeGrainTests.Re_replay_after_an_alias_swap_reads_the_live_physical_wal_not_the_retired_one`. |
| `LatchOff` | A non-domain digest fault permanently latches projection-digest maintenance off | `ILatticeRegistry.LatchProjectionDigestPermanentlyDisabledAsync`, honoured by `ReplicationDigestProbeGrain.ProcessNextPhaseAsync`. | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_permanently_skips_on_latched_registry`, `LatticeOptionsResolverTests.Latch_supersedes_silo_option_true` and `BPlusLeafGrainTests.First_mutation_with_digest_disabled_stamps_the_latch_once`. |
| `Stutter` | Permit arbitrary inactivity, including permanent idleness after exploration bounds | Modelling device, no production counterpart. | Not applicable. |

## Property mapping

| Spec property | Code-level claim | Detector |
|---------------|------------------|----------|
| `NoOwnOriginApplied` | The receiver never applies or records an entry stamped with its own origin | Yes: `ReplicationApplierTests.ApplyAsync_skips_local_origin_entries_as_no_op` and `ReplicationApplierTests.ApplyAsync_does_not_cache_local_origin_entries`. |
| `DedupSound` | An identity is recorded only once its apply succeeded, so a re-ship of a lost entry is not suppressed | Yes: `ReplicationApplierTests.ApplyAsync_rolls_back_cache_when_apply_grain_throws_so_retry_can_apply` and `ReplicationApplierTests.ApplyAsync_dedupes_duplicate_identity_tuple_without_invoking_apply_grain`. |
| `WalComplete` | The live physical tree's retained log is the journal above both its floor and the last alias swap, so a cursor below the floor is classified wal_trimmed | Yes: `LeafReReplayerTests.ReplayAsync_wal_trimmed_past_cursor_skips_with_operator_alert` and `LeafReReplayerTests.ReplayAsync_trimmed_but_cursor_above_oldest_still_repairs`. |
| `HwmMonotone` | The per-origin high-water-mark never decreases | Yes: `ReplicationHighWaterMarkGrainTests.TryAdvanceAsync_returns_false_when_candidate_is_less_than_current` and `ReplicationHighWaterMarkGrainTests.TryAdvanceAsync_is_monotonic_across_a_burst`. |
| `LwwNoRegress` | Neither projection is overwritten by an older timestamp | Yes: `CompressedReplicationApplyIntegrationTests.ApplySetAsync_older_source_hlc_does_not_overwrite_newer_local_value`. |
| `ProbeReadOnly` | The digest probe and the Merkle walk change neither data, cursor nor guard accounting | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_does_not_walk_on_mismatch_when_merkle_walk_disabled`, `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_reads_local_digest_under_a_system_origin_scope` and `MerkleWalkLocaliserTests.WalkAsync_descends_to_diverging_leaf_and_prunes_matching_sibling`. |
| `RepairOnlyLocalised` | Repair traffic covers only the localised ranges of a localised pass | Yes: `BootstrapFallbackPlannerTests.PlanAsync_reships_in_range_committed_entries`, `BootstrapFallbackPlannerTests.PlanAsync_empty_ranges_skips_range_empty_without_triggering` and `LeafReReplayerTests.ReplayAsync_empty_ranges_skips_range_empty`. |
| `OptOutSilent` | The opt-out emits no repair traffic | Yes: `ReplicationDigestProbeGrainTests.Remediation_is_skipped_with_opt_out_when_auto_remediate_is_off`. |
| `BreakerBlocks` | An open breaker within its cooldown emits no repair traffic from either stage | Yes: `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_circuit_open_after_the_first_shards_reship_is_rejected` and `RemediationGuardTests.IsCircuitBlocking_is_true_while_cooling_and_false_after_cooldown`. |
| `BudgetGate` | A pass begins only inside an unexhausted budget window | Yes: `RemediationGuardTests.TryBeginRemediation_blocks_once_budget_is_consumed_within_window` and `ReplicationDigestProbeGrainTests.Second_shard_is_skipped_with_budget_exhausted_after_the_first_shard_spends_the_window`. |
| `ReplayAboveCursor` | Re-replay ships only A-origin entries above the peer's cursor that are retained in the WAL of the physical tree the walk resolved | Yes: `LeafReReplayerTests.ReplayAsync_reships_in_range_entries_above_cursor`, `LeafReReplayerTests.ReplayAsync_excludes_foreign_origin_entries` and `ReplicationDigestProbeGrainTests.Re_replay_after_an_alias_swap_reads_the_live_physical_wal_not_the_retired_one`. |
| `FallbackOnlyOnSkip` | The fallback runs only after a wal_trimmed or range_empty re-replay skip | Yes: `ReplicationDigestProbeGrainTests.Re_replay_that_closes_the_gap_does_not_trigger_the_bootstrap_fallback` and `ReplicationDigestProbeGrainTests.Below_cursor_orphan_divergence_triggers_the_bootstrap_fallback`. |
| `FallbackCommittedOnly` | The fallback ships committed rows only, at their own timestamp, a tombstone as a delete | Yes: `BootstrapFallbackPlannerTests.PlanAsync_skips_prepared_rows_and_reships_tombstones_as_deletes`, `BootstrapFallbackPlannerTests.PlanAsync_reships_a_tombstone_as_a_delete_carrying_its_own_clock` and `BootstrapFallbackPlannerTests.PlanAsync_reships_set_records_stamped_with_origin`. |
| `LatchPermanent` | A latched tree stays latched; a domain fault does not latch | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_permanently_skips_on_latched_registry` and `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_domain_digest_fault_does_not_permanently_disable_probing`. |
| `BreakerOpensAtThreshold` | Reaching the failure threshold opens the breaker | Yes: `RemediationGuardTests.RecordFailure_opens_circuit_at_threshold` and `RemediationGuardTests.RecordFailure_with_threshold_one_opens_on_first_failure`. |
| `SwapObserved` | An alias swap advances the topology generation a routing resolve observes, so a later walk resolves the new physical tree | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_localises_against_a_freshly_resolved_physical_tree`. |
| `LocaliseFresh` | A walk localises against the physical tree resolved on its own pass | Yes: `ReplicationDigestProbeGrainTests.ProcessNextPhaseAsync_localises_against_a_freshly_resolved_physical_tree`. |
| `Converges` | Under cadence, delivery, cooldown and window fairness, divergence is repaired unless latched or opted out | Yes: `AntiEntropyMultiSiteDriftChaosTests.Partition_then_heal_diverges_both_directions_and_reconciles_under_last_writer_wins` and `ReplicationDigestProbeGrainTests.Below_cursor_orphan_divergence_triggers_the_bootstrap_fallback`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-state exploration domains, including clock, topology-generation and environment bounds, not a production behaviour. |

## Scope and fairness

- One tree, one remediation direction and one peer. Guard accounting is keyed
  per tree and peer in production, so other peers do not interact with it;
  the per-peer accounting isolation test of the guard covers that
  isolation outside the model.
- `SF_vars(ProbeCompare /\ phase' = "mismatch")` and `SF_vars(Localise)`
  stand for the jittered probe cadence: a mismatch or a walk that keeps
  becoming possible eventually happens. Cadence and jitter values are not
  modelled. `WF_vars(Remediation)`, `WF_vars(Cooldown)` and
  `WF_vars(WindowRoll)` stand for a localised pass being resolved, the
  cooldown elapsing and the budget window rolling. Delivery is weakly fair per
  message, so ordinary delivery is eventually reliable once the bounded drops
  are spent.
- The projection digest folds each cluster's checkpoint offsets, so equal
  content may mismatch and the walk may over-localise; the model permits both.
- `SameValue` compares a row's committed value and ignores its origin stamp,
  because the digest's per-entry contribution does. An origin is
  observer-relative: the author stores its own write with no origin and every
  peer stores the author's id, so a contribution that hashed it would make
  every authored row mismatch between two converged clusters. Production's
  contribution hashed the origin until contribution version 1. The model
  cannot express that defect as a violation: `ProbeCompare` already allows a
  mismatch on equal content, and a spurious mismatch costs walks and
  re-replays, not safety. The detector named on the `ProbeCompare` row pins the
  content-only contribution instead.
- `Drop` loses Sets and Deletes alike. The fallback re-ships a committed
  tombstone as a delete carrying its own clock, so a lost delete whose WAL
  entry was trimmed is still repaired. A tombstone reaped after the tombstone
  grace period leaves no row to re-ship and is outside the model.
- Aliasing is bounded by `MaxSwap`. The base configuration sets it to 0; the
  `Alias` variant allows one swap over a single key. The live projection is
  carried across the swap, and a restore that rolls the projection back is
  outside the model. A walk localised before the swap reads the retired log
  of the generation it resolved, which the model permits.
- Re-ship entry and byte caps, atomic-batch grouping and prepared rows are
  not modelled; the cap and batch detectors cover them outside the model.
- A reshard or resize is a generation counter. Its data movement and the
  bootstrap admission floor are outside this module.
- The receiver dedup is an unbounded set here; production bounds it and falls
  back on the origin low watermark.

## Verification evidence

The final local TLC run checked the complete base graph (`MaxSwap = 0`) with
3,681,022 generated and 475,438 distinct states, depth 32, and no violations.
The `Alias` variant (one key, `MaxSwap = 1`) checked 1,467,371 generated and
200,460 distinct states, depth 32, with no violations. The two alias mutants
raise `MaxSwap` to 1 through their `BOUNDS` line; every other mutant runs at
the base bounds. Each anchored mutant was generated from that same
source, selected only its target property, and reported that target without a
deadlock. No mutant changes fairness or disables the deadlock check.
The temporal witnesses are scheduler-dependent, so their early-found state
counts are not a stable model census.

| Mutation | Target | Witness-run distinct states |
|----------|--------|-----------------------------|
| AbortRollsBudgetWindow | ProbeReadOnly | 12,729 |
| CooldownNeverHalfOpens | Converges | 235,314 |
| DedupRecordsDroppedEntry | DedupSound | 29 |
| DeleteReplayedAsSet | ReplayAboveCursor | 1,895 |
| DigestIgnoresDivergence | Converges | 7,552 |
| EchoRecordedAsApplied | NoOwnOriginApplied | 547 |
| EpochOverflow | TypeOK | 11 |
| FailureKeepsBreakerClosed | BreakerOpensAtThreshold | 1,364 |
| FallbackDropsTombstones | Converges | 222,558 |
| FallbackIgnoresBreaker | BreakerBlocks | 47,908 |
| FallbackPreemptsReplay | FallbackOnlyOnSkip | 1,363 |
| FallbackShipsWholeTree | RepairOnlyLocalised | 2,619 |
| FallbackTombstoneAsSet | FallbackCommittedOnly | 6,018 |
| GatedPassShips | BreakerBlocks | 27,844 |
| HwmOverwrite | HwmMonotone | 1,129 |
| LatchToggles | LatchPermanent | 109 |
| LocalDeleteLoggedAsSet | WalComplete | 5 |
| LwwIgnoresTimestamp | LwwNoRegress | 497 |
| OptOutReplays | OptOutSilent | 2,938 |
| OwnOriginApplied | NoOwnOriginApplied | 2,360 |
| PeerWriteNotShipped | Converges | 12,563 |
| ProbeAdvancesCursor | ProbeReadOnly | 32 |
| ReplayBelowCursor | ReplayAboveCursor | 11,723 |
| ReplayIgnoresBudget | BudgetGate | 48,181 |
| ReplayReadsRetiredWal | ReplayAboveCursor | 7,432 |
| RestartClearsLatch | LatchPermanent | 109 |
| StaleTopologyWalk | LocaliseFresh | 640 |
| SwapInvisibleToResolve | SwapObserved | 12 |
| TrimLeavesFloor | WalComplete | 31 |
| UnavailableClosesBreaker | ProbeReadOnly | 4,850 |
| WalkDropsLeaf | Converges | 16,728 |
| WindowNeverRolls | Converges | 198,510 |

Every cited fixture is a direct production-seam detector; TLC mutation kills
establish model non-vacuity, not a mechanical C# refinement proof. Building this module found three production defects (#4859, #4862 and #4863), each fixed with a
regression test that fails without the fix: a re-ship whose transport threw
was counted as a successful pass (`ShipRejected`), the bootstrap fallback
skipped tombstones so a lost delete whose WAL entry was trimmed or below the
cursor was never repaired (`Fallback`, `Converges`), and re-replay read the
logical tree's WAL rather than the live physical tree's after an alias swap
(`ReReplay`, `SwapObserved`).