# Refinement note: RestorePeerAuthority

This maps the authority a coordinated-restore participant acts under (issue
#6192) onto its production seams: the system-origin elevation a remote peer's
saga runs under, the own-saga exception for a decided commit or abort, and the
fenced cutover. The coordinator a step names reaches the participant through
the replication gRPC host's admitted-peer seam, which this module takes as an
abstract predicate with the contract `ReplicationOriginBinding` checks (see
[RefinementOriginBinding.md](RefinementOriginBinding.md)): a control verb is
admitted only for the credential-bound origin of a current peer, with any
body-declared coordinator equal to it, and the request's coordinator is
overwritten with that origin.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `wired` | Whether the membership, options and topology seams are wired | The optional `IReplicatedTreeMembership`, `IOptionsMonitor<LatticeReplicationOptions>` and `IReplicationTopology` constructor parameters of `RestoreParticipant` |
| `enrolled` | The trees this cluster replicates | `IReplicatedTreeMembership.IsReplicated` |
| `topology` | This cluster's current replication peers | `IReplicationTopology.CurrentPeers` |
| `decision` | The durable decision this cluster's own coordinator recorded for the saga | `ICrossClusterSagaCoordinatorGrain.GetStatusAsync`, read by `RestoreParticipant.ReadLocalDecisionAsync` |
| `prepCoord` | The coordinator the participant grain persisted at prepare | `CrossClusterSagaParticipantGrain` state `CoordinatorClusterId` |
| `cache` | The trees whose shadows a successful build cached | `RestoreParticipant._built`, `RestoreParticipant._builtSets` |
| `fence` | The saga write fence: engaged (local writes blocked), unblocked (shipping paused) or lifted | `ISagaWriteFenceGrain` |
| `obs` | What the last step did and the authority it ran under | No production state: the step's outcome, which the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Prepare` | A participant receives a prepare: the hosting gate, then elevation, the admission probe and the captured-source confinement, then the unfenced shadow build of every hosted tree | `RestoreParticipant.PrepareAsync`, `RestoreParticipant.PrepareSetAsync`, `RestoreParticipant.IsHostedTarget`, `RestoreParticipant.FilterHostedMembers`, `RestoreParticipant.HasPeerAuthority`, `RestoreParticipant.SourcesEnrolled`, `RestoreParticipant.PeerRefusal` | Yes: `RestoreParticipantTests.PrepareAsync_from_a_current_peer_runs_probe_and_build_with_system_origin`, `RestoreParticipantTests.PrepareAsync_from_a_peer_for_an_unreplicated_target_is_refused_before_any_engine_call`, `RestoreParticipantTests.PrepareAsync_from_a_peer_with_a_report_naming_no_source_votes_abort` and `RestoreParticipantTests.PrepareAsync_from_a_peer_answers_a_failed_probe_and_an_unreplicated_source_identically`. |
| `Commit` | A participant receives a commit: the hosting gate, the step's authority, a rebuild when the cache was lost, then the fenced cutover of the trees still hosted, which unblocks local writes before a failure propagates | `RestoreParticipant.CommitAsync`, `RestoreParticipant.CommitSetAsync`, `RestoreParticipant.SkipMembersNoLongerHostedAsync`, `RestoreParticipant.ResolveStepAuthorityAsync`, `RestoreParticipant.SourcesStillEnrolledAsync`, `RestoreParticipant.CutOverInsideFenceAsync` | Yes: `RestoreParticipantTests.CommitAsync_from_a_current_peer_cuts_over_with_system_origin`, `RestoreParticipantTests.CommitAsync_from_a_peer_that_must_rebuild_refuses_a_source_no_longer_replicated`, `RestoreParticipantTests.CommitAsync_naming_this_cluster_while_its_coordinator_is_undecided_is_refused_before_any_cutover`, `RestoreParticipantTests.CommitAsync_whose_cutover_fails_unblocks_local_writes_and_stays_retryable` and `RestoreParticipantTests.CommitSetAsync_from_a_peer_swaps_only_members_still_replicated_and_collects_the_rest`. |
| `Abort` | A participant receives an abort from the coordinator, or from its own grain after a fence expiry: the hosting gate, the step's authority, then the compensation | `RestoreParticipant.AbortAsync`, `RestoreParticipant.ResolveStepAuthorityAsync` | Yes: `RestoreParticipantTests.AbortAsync_from_a_current_peer_compensates_with_system_origin`, `RestoreParticipantTests.AbortAsync_targetTreeNotReplicated_doesNotRevertTheAlias` and `RestoreParticipantTests.AbortAsync_this_clusters_coordinator_has_not_decided_runs_as_the_ambient_caller`. |
| `Decide` | Environment: this cluster's own coordinator durably decides its saga | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `LoseCache` | Environment: a reactivation loses the in-memory shadow cache | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `FenceExpires` | Environment: the fence grain self-lifts on its bounded deadline | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `SetEnrollment` | Environment: an operator enrolls or disables a tree here | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `SetTopology` | Environment: a peer joins or leaves the replication topology | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `PeerAuthorityBounded` | A restore step enters system origin for peer authority only when every seam is wired, the credential-bound coordinator is a remote current peer, every tree it restores or swaps is replicated here and is swapped only inside the write fence, and a build replays only the target or replicated trees; an empty captured-source list refuses; and an elevated prepare answers a failed probe and an unreplicated source identically | Yes: `RestoreParticipantTests.PrepareAsync_from_this_clusters_own_coordinator_runs_as_the_ambient_caller`, `RestoreParticipantTests.PrepareAsync_from_a_cluster_that_is_not_a_current_peer_runs_as_the_ambient_caller`, `RestoreParticipantTests.PrepareAsync_without_the_topology_seam_runs_as_the_ambient_caller`, `RestoreParticipantTests.PrepareAsync_from_a_peer_replaying_a_backup_of_an_unreplicated_tree_votes_abort_without_building`, `RestoreParticipantTests.PrepareAsync_from_a_peer_with_a_report_naming_no_source_votes_abort`, `RestoreParticipantTests.PrepareAsync_from_a_peer_answers_a_failed_probe_and_an_unreplicated_source_identically`, `RestoreParticipantTests.CommitSetAsync_from_a_peer_swaps_only_members_still_replicated_and_collects_the_rest` and `LatticeSagaGrpcServiceTests.The_handler_sees_the_stamped_origin_as_the_coordinator_never_a_body_value`. |
| `OwnSagaCommitNeedsDurableDecision` | A commit naming this cluster's own saga without a readable durable Commit decision is refused before any fence or swap; an abort without a durable Abort decision runs as the ambient caller; the prepare's authority is borrowed only for the shadow this participant built | Yes: `RestoreParticipantTests.CommitAsync_naming_this_cluster_while_its_coordinator_is_undecided_is_refused_before_any_cutover`, `RestoreParticipantTests.CommitAsync_whose_coordinator_decision_cannot_be_read_is_refused`, `RestoreParticipantTests.CommitAsync_of_a_saga_this_clusters_coordinator_decided_to_abort_is_refused`, `RestoreParticipantTests.CommitAsync_of_a_decided_saga_that_must_rebuild_runs_as_the_ambient_caller`, `RestoreParticipantTests.AbortAsync_this_clusters_coordinator_has_not_decided_runs_as_the_ambient_caller` and `RestoreParticipantTests.AbortAsync_whose_coordinator_decision_cannot_be_read_runs_as_the_ambient_caller`. |
| `NoStrandedFence` | A cutover that fails unblocks local writes before the failure propagates and keeps the participant's shadow, so a redelivered decision can finish | Yes: `RestoreParticipantTests.CommitAsync_whose_cutover_fails_unblocks_local_writes_and_stays_retryable`, `RestoreParticipantTests.CommitSetAsync_whose_cutover_fails_unblocks_local_writes` and `RestoreParticipantTests.CommitAsync_whose_cutover_fails_still_surfaces_the_failure_when_the_unblock_fails`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **One backup per saga.** The model's restored trees, one for a single-tree restore and several for a set, all replay the same backup; a set restores each member from its own backup, through the same per-member probe and confinement.
  Production symbols: `RestoreParticipant.PrepareSetAsync`, `BackupSetMember`.
- **The coordinator hosts its own set's members.** On the coordinator cluster every member of its own set saga is hosted, including local-only members; the model filters an own-saga set by enrollment too, which only narrows what an own-saga step touches, and peer authority never holds for an own saga.
  Production symbols: `RestoreParticipant.FilterHostedMembers`.
- **Request identity binding.** The participant grain refuses a commit, abort or status read whose coordinator, target, backup or set id differs from the ones it persisted at prepare. The model admits any admitted coordinator at a commit or abort and binds only the target to the cached shadow, which is weaker, so the properties hold a fortiori.
  Production symbols: `CrossClusterSagaParticipantGrain.EnsureRequestIdentity`.
- **One seam flag.** The model has one `wired` flag for the membership, options and topology seams. Without the options seam the participant cannot recognise its own saga, so an own-saga step runs as the ambient caller and is never refused; with the membership seam missing the hosting gate is a no-op. Either way no step is elevated.
  Production symbols: `RestoreParticipant.HasPeerAuthority`, `RestoreParticipant.IsHostedTarget`, `RestoreParticipant.ResolveStepAuthorityAsync`.
- **What the ambient caller may do.** A step that runs as the ambient caller is authorized by the backup engine against that caller; the model records the authority and does not model the engine's grant check.
  Production symbols: `ILatticeCoordinatedRestoreEngine`, `LatticeAccessGateContext.EnterSystemOrigin`.
- **Capacity and validation refusals.** The capacity check and the pre-apply validation run only after the captured sources are confined, so they answer only for a backup the peer may restore; the model does not model them.
  Production symbols: `IRestoreCapacityProbe.CanHostAsync`, `LatticeRestoreValidationException`.
