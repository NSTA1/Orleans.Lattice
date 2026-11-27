# Refinement note: ReplicationAdmission

This maps the replication receiver's applier gate (issue #6053) onto its
production seams: the enrollment and merge-mode gate every receive path funnels
through, and the inbound contact it records. The callers that reach it are
admitted by `ReplicationOriginBinding` (see
[RefinementOriginBinding.md](RefinementOriginBinding.md)). The merge mode is an
open set of write classes: the module declares `Admits(mode, class)`
abstractly and the cfg binds it to `ModeAdmits`, under which a mode admits
exactly its own class, as `ReplicationInboundAdmission.IsAdmitted` does today.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `enrolled` | The receiver's per-tree enrollment and locally resolved merge mode | `ILatticeReplicationContext.ResolveMergeMode`, read through `ReplicationInboundAdmission.ResolveLocalMergeMode` |
| `dlq` | The trees with a dead-letter activation | `IReplicationDeadLetterGrain`, keyed by tree id |
| `contact` | The inbound per-peer contact rows | `ReplicationPeerStats` inbound rows, capped by `ReplicationPeerStats.DefaultMaxInboundRows` |
| `obs` | What the last run did | No production state: the outcome of a run, which the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `ApplyRun` | A run reaches the applier from a push or a replay; it is dropped, dead-lettered or applied against the local enrollment, and an admitted run records inbound contact | `ReplicationApplier.ApplyAsync`, `ReplicationApplier.ApplyBatchAsync`, `ReplicationInboundContact.Record` | Yes: `ReplicationApplierTests.ApplyAsync_rejects_entry_for_tree_not_enrolled_here`, `ReplicationApplierTests.ApplyAsync_dead_letters_entry_whose_mode_disagrees_with_local_mode`, `ReplicationApplierTests.ApplyBatchAsync_dead_letters_entries_smuggled_behind_a_conforming_first_entry` and `DeadLetterTrackingReplicationApplierTests.ApplyBatchAsync_resolves_enrollment_through_the_replication_context`. |
| `SetEnrollment` | Environment: an operator enrolls, re-modes or disables a tree | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `NonEnrolledDropped` | A run for a tree not enrolled here, or with no enrollment source, is dropped and never dead-lettered, on every receive path | Yes: `ReplicationApplierTests.ApplyAsync_rejects_entry_for_tree_not_enrolled_here`, `ReplicationApplierTests.ApplyBatchAsync_rejects_multi_entry_run_for_non_enrolled_tree` and `ReplicationApplierTests.ApplyAsync_drops_entry_when_no_enrollment_source_is_configured`. |
| `ModeReResolvedLocally` | An enrolled tree's run applies only under the locally resolved mode; a wire-mode disagreement is dead-lettered | Yes: `ReplicationApplierTests.ApplyAsync_dead_letters_entry_whose_mode_disagrees_with_local_mode`, `ReplicationApplierTests.ApplyBatchAsync_dead_letters_multi_entry_run_whose_mode_disagrees` and `ReplicationApplierTests.ApplyBatchAsync_dead_letters_entries_smuggled_behind_a_conforming_first_entry`. |
| `ContactOnlyWhenAdmitted` | Inbound contact is recorded only for an admitted run, and new inbound rows past the cap are not recorded | Yes: `ReplicationApplierTests.ApplyBatchAsync_does_not_record_inbound_for_a_run_on_a_tree_not_enrolled_here`, `ReplicationApplierTests.ApplyBatchAsync_does_not_record_inbound_for_a_run_whose_wire_mode_the_gate_rejects` and `ReplicationPeerStatsInboundBoundTests.New_inbound_pairs_beyond_the_cap_are_not_recorded`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **The gates after enrollment.** The tenant-isolation and source-lineage gates that follow the enrollment gate are not modelled; each only narrows what the enrollment gate admits.
  Production symbols: `IReplicationTenantIsolationGate`, `ReplicationSourceLineageGate`.
- **A full dead-letter queue defers.** A mode disagreement whose dead-letter enqueue finds the queue full is deferred rather than acknowledged; the model's dead-letter always succeeds.
  Production symbols: `ReplicationDeadLetterQueueFullException`.
- **Own-origin runs.** A run whose origin is this cluster's own id is classified as a duplicate after the enrollment gate and records no contact; the model's runs never carry the receiver's own origin.
  Production symbols: `ReplicationReceiveDedup.IsOwnOrigin`.
