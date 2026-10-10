# Refinement note: TenantFootprint

This is a documented mapping, not a machine-checked refinement proof. The
bounded module represents one generic non-negative resource dimension; the
separate evaluator module checks all dimensions and deterministic precedence.
Metering and usage replication are explicitly assumed available and fair.
The accounting boundary starts with available resident tree reports: `Commit`
and `ApplyReplication` denote local/inbound increases supplied in those reports,
not a verification of the core commit-to-report implementation. The detectors
execute the production meter, publisher, record merge, compiler and admission.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `scope` | Local versus global enforcement | `TenantEnforcementScope` and `TenantUsageView.UsageFor` |
| `band`, `age` | Significance threshold and age since successful publication | `UsagePublishHysteresis.ShouldPublish` and `TenantUsagePublisher.RollUpAndPublishAsync` |
| `phase` | Concurrent admitted requests whose resident reports have changed | `LatticeTenantAdmissionController.IsAdmittedAsync` and `TenantUsageMeteringService.MeterOnceAsync` |
| `live`, `inbound` | Available resident reports, including inbound replica storage | `TenantUsageMeteringService.MeterOnceAsync` reads stored usage, independent of author |
| `sample` | A delayed local roll-up | `LocalUsageSample.RollUp` |
| `published`, `slots` | Per-cluster stamped samples, merged at each replica | `TenantUsagePublisher.RollUpAndPublishAsync` and `TenantUsageRecord.MergeFrom` |
| `decision` | Result and selected-sample witness of the latest admission | `CompiledTenantUsage.Compile`, `TenantQuotaEvaluator.Admit` |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Admit` | Check sampled usage before allowing a local request | `LatticeTenantAdmissionController.IsAdmittedAsync` | Yes: `LatticeTenantAdmissionControllerTests.GlobalConverged_refuses_when_the_global_fold_exceeds_quota` and `LatticeTenantAdmissionControllerTests.IsAdmittedAsync_admits_a_tenant_with_no_view_failing_open`. |
| `Commit` | An available report increases after a locally admitted write | `TenantUsageMeteringService.MeterOnceAsync` samples the supplied resident storage | Yes: `TenantUsageMeteringServiceTests.MeterOnceAsync_changed_resident_reports_publish_merge_and_refuse_later_admission` changes the report then executes production metering, publication, merge, compilation and refusal. |
| `ApplyReplication` | An inbound increase in an available report is included without client admission | `ReplicationApplier.ApplyAsync` and `TenantUsageMeteringService.MeterOnceAsync` | Yes: `ReplicationApplierTests.ApplyAsync_bypasses_quota_but_not_isolation` detects an added client quota gate; `TenantUsageMeteringServiceTests.A_metering_cycle_counts_data_held_by_a_backfilled_tenant_tree` detects omitted resident report accounting. |
| `Sample` | Roll up actual tree reports | `LocalUsageSample.RollUp` | Yes: `LocalUsageSampleTests.RollUp_sums_per_tree_dimensions_and_counts_the_trees` and `TenantUsageOverflowTests.RollUp_overflow_saturates_instead_of_publishing_negative_usage`. |
| `Publish` | Publish this cluster's positive sample | `TenantUsagePublisher.RollUpAndPublishAsync` | Yes: `TenantUsagePublisherTests.RollUpAndPublishAsync_publishes_the_rolled_up_slot_for_this_cluster` and `TenantUsagePublisherTests.RollUpAndPublishAsync_publishes_a_movement_that_clears_the_band`. |
| `DeliverUsage` | Deliver arbitrary old/new stamped usage without regression | `TenantUsageRecord.MergeFrom` | Yes: `TenantUsageRecordTests.Merge_of_the_same_cluster_slot_keeps_the_superseding_stamp`. |
| `Probe` | A later local admission evaluates the latest compiled slots | `LatticeTenantAdmissionController.IsAdmittedAsync` | Yes: `LatticeTenantAdmissionControllerTests.IsAdmittedAsync_concurrent_cold_admissions_refuse_after_slots_converge`. |
| `ClockTick` | Supplied cadence-clock time advances the refresh age | `TenantUsagePublisher.RollUpAndPublishAsync` reads the supplied HLC wall ticks | Yes: `TenantUsagePublisherRefreshBoundaryTests.RollUpAndPublishAsync_refreshes_changed_sample_at_five_minutes_not_before` detects omitted age advancement or an off-by-one refresh boundary. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `AdmissionTruth` | Allow exactly when selected sampled usage is at or below cap | Yes: `TenantQuotaEvaluatorTests.Usage_at_the_ceiling_admits` and `TenantQuotaEvaluatorTests.Usage_over_the_bytes_ceiling_refuses_with_the_tenant_and_dimension`. |
| `ScopeCorrect` | Select the proper local slot or saturated global fold | Yes: `CompiledTenantUsageTests.UsageFor_selects_the_global_fold_or_the_local_slot_by_scope` and `TenantUsageOverflowTests.Fold_overflow_cannot_reopen_a_tenants_footprint_admission`. The overflow detector was measured failing on the original Add/RollUp implementation. |
| `AccountingCorrect` | Every available resident report contributes, whatever its origin | Yes: `TenantUsageMeteringServiceTests.MeterOnceAsync_changed_resident_reports_publish_merge_and_refuse_later_admission` and `TenantUsageMeteringServiceTests.A_metering_cycle_counts_data_held_by_a_backfilled_tenant_tree` assert exact reported totals after production metering and publication. |
| `SampleSound` | Roll-up and publication do not invent stored usage | Yes: `LocalUsageSampleTests.RollUp_sums_per_tree_dimensions_and_counts_the_trees` and `TenantUsagePublisherTests.RollUpAndPublishAsync_publishes_the_rolled_up_slot_for_this_cluster`. |
| `SlotMonotonic` | Delayed older stamps never replace newer slots | Yes: `TenantUsageRecordTests.Merge_of_the_same_cluster_slot_keeps_the_superseding_stamp`. Usage itself may decrease after deletion in production; this monotonic bounded model has only additive writes, so stamp order and usage order coincide. |
| `UsageConverges` | With supplied fair metering/delivery, current resident reports become current slots | Yes: `TenantUsageMeteringServiceTests.MeterOnceAsync_changed_resident_reports_publish_merge_and_refuse_later_admission` asserts changed report-to-slot convergence; `TenantUsageRecordTests.Merge_of_the_same_cluster_slot_keeps_the_superseding_stamp` detects reversed delivery ordering. |
| `EventualRefusal` | With supplied fair metering/delivery, stable over-quota reports cause later refusal even below hysteresis | Yes: `TenantUsagePublisherFreshnessTests.RollUpAndPublishAsync_stable_sub_threshold_crossing_eventually_refuses_admission` was measured RED in all four dimensions before #4805; `LatticeTenantAdmissionControllerTests.IsAdmittedAsync_concurrent_cold_admissions_refuse_after_slots_converge` checks cross-cluster scope selection. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Model-domain predicate, not a production behavioural guarantee. |

## Measured production detector evidence

The coordinating parent ran isolated source perturbations with exact byte
snapshots restored after each arm. Omitting the real publisher store call made
`MeterOnceAsync_changed_resident_reports_publish_merge_and_refuse_later_admission`
fail with expected slot bytes 100, actual 0. Replacing the meter's resident byte
projection with an empty byte count made that detector and
`A_metering_cycle_counts_data_held_by_a_backfilled_tenant_tree` fail with expected
bytes 100 and 400 respectively, actual 0. Omitting the real controller's
`TenantQuotaEvaluator.Admit` call made the changed-report detector fail because
the expected `LatticeQuotaExceededException` was absent. After verified byte
restoration, the rebuilt clean control passed both metering detectors.

These observations establish sensitivity to dropped publication, dropped
available-report accounting and omitted later refusal. They do not establish
core commit-to-report accuracy or eventual network delivery.

## Classification and limits

Every behavioural action and property has a protocol mutation. Temporal mutants
alter real publication/delivery/admission steps rather than weakening the
property. No mutation adds a spurious protocol action. Repeated admission
probes keep base and mutants deadlock-free without disabling deadlock checks.
The environment supplies available report reads, successful store writes and
fair latest delivery. The tests do not establish storage-report accuracy from
core commits, timer/network availability or a disconnected cluster's progress.
Those are outside this accounting/admission refinement boundary. The model
checks zero and nonzero hysteresis with bounded changed-sample refresh (#4805),
not the former zero-hysteresis-only assumption.
