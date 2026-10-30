# Refinement note: schema compliance scans

## Model scope in one paragraph

One tracked compliance scan of one governed tree. The scan reads the tree's
cached policy once; an ungoverned tree succeeds at once with an ungoverned
report (no policy, every count zero) and is neither counted nor scanned. A
governed scan counts the tree's live entries, then reads every key in order
through the routing-aware enumeration and judges each live value by the policy
it read at the start, tallying it compliant or under the reason of the first
rule it fails. A stored value the read path cannot decode to the tree's target
shape (no upcast hop, or stamped newer than the target) is counted
non-compliant under enforcement's decode reason, and the scan goes on (#5253).
The tree stays writable and the policy can change: the scan is not a snapshot,
so only keys untouched for the whole scan are promised, each judged value is a
value its key held during the scan, and a total the scan passes turns unknown.
The report keeps the policy's rule count apart from the number of distinct
failure reasons (#4966). Cancellation and a lost runner end the scan with no
report.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `value`, `writes`, `touched` | The tree's stored values, environment writes, and the ghost set of keys written after the scan started. | `ILattice.SetAsync`, `ILattice.DeleteAsync`. |
| `policy`, `policyChanges` | The tree's current policy and environment changes to it. | `ILatticeSchemaPolicyProvider.GetCompiledPolicyAsync`, `LatticeSchemaAdmin.SetPolicyAsync`. |
| `phase`, `lost` | The scan's tracked phase and whether its runner was lost. | `SchemaComplianceScanOperation.CountingPhase`, `SchemaComplianceScanOperation.ScanningPhase`, `LatticeOperationRecord`. |
| `scanPolicy` | The compiled policy the scan read at its start. | `LatticeSchemaComplianceAdmin.ScanComplianceAsync`. |
| `cursor`, `seen`, `lastValue`, `lastVerdict` | The ordered enumeration's position, and ghosts of the keys judged and the last judgement. | `LatticeSchemaComplianceAdmin.ScanComplianceAsync`, `CompiledSchemaPolicy.Validate`. |
| `total`, `totalKnown` | The live-entry count taken before the scan, and whether it still reads as known. | `ILattice.CountAsync`, `LatticeSchemaComplianceAdmin.ScanComplianceAsync`. |
| `compliant`, `nonCompliant`, `reasonCount` | The running tallies and the failure-reason breakdown. | `LatticeSchemaComplianceReport.CompliantCount`, `LatticeSchemaComplianceReport.NonCompliantCount`, `LatticeSchemaComplianceRuleCount`. |
| `hasReport`, `repHasPolicy`, `repRuleCount`, `repReasons` | The published report: whether the tree was governed, the policy's rule count and the failure-reason count. | `LatticeSchemaComplianceReport.HasPolicy`, `LatticeSchemaComplianceReport.RuleCount`, `SchemaComplianceScanResults.ToResultMap`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Start` | The scan reads the cached policy once; an ungoverned tree succeeds at once with an ungoverned report. | `LatticeSchemaComplianceAdmin.ScanComplianceAsync`, `LatticeSchemaComplianceReport.Ungoverned`, `LatticeSchemaComplianceOperations.StartComplianceScanAsync`. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_ungoverned_tree_returns_an_ungoverned_report_without_scanning`, `LatticeSchemaComplianceAdminProgressTests.A_tracked_scan_of_an_ungoverned_tree_neither_counts_nor_scans`, `LatticeSchemaComplianceOperationsTests.A_started_scan_returns_a_handle_and_polls_to_the_report`. |
| `Count` | The tracked scan counts the live entries before it scans. | `ILattice.CountAsync`, `LatticeSchemaComplianceAdmin.ScanComplianceAsync`. | Yes: `LatticeSchemaComplianceAdminProgressTests.A_tracked_scan_counts_first_then_reports_entries_scanned_against_the_count`. |
| `Step` | The ordered scan judges the next live value by the start policy and tallies it, an undecodable value under enforcement's decode reason. | `LatticeSchemaComplianceAdmin.ScanComplianceAsync`, `CompiledSchemaPolicy.Validate`, `SchemaUndecodableValue.Reason`. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_mixed_values_group_non_compliant_by_failure_reason`, `LatticeSchemaComplianceAdminTests.ScanCompliance_all_values_compliant_reports_no_breakdown`, `LatticeSchemaComplianceAdminUndecodableTests.ScanCompliance_after_a_decode_fault_judges_the_rest_from_the_stored_form`, `SchemaComplianceUndecodableValueIntegrationTests.ScanComplianceAsync_counts_undecodable_values_as_non_compliant_and_completes`. |
| `Finish` | The pass ends and the report carries the start policy's rule count and the failure-reason count. | `LatticeSchemaComplianceAdmin.ScanComplianceAsync`, `SchemaComplianceScanResults.ToResultMap`. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_rule_count_matches_policy_not_failure_population`, `SchemaComplianceScanResultsTests.A_report_round_trips_through_the_result_map`. |
| `Cancel` | A cancel request stops the running scan with no report. | `LatticeSchemaComplianceOperations.CancelOperationAsync`, `LatticeSchemaComplianceAdmin.ScanComplianceAsync`. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_cancelled_token_stops_the_scan`, `LatticeSchemaComplianceOperationsTests.Cancelling_a_running_scan_stops_it_and_reads_as_cancelled`. |
| `Lose` | The runner's silo is lost; the record reads failed with no report. | `LatticeOperationGrain.GetAsync`, `ILatticeOperationSiloLiveness.IsDead`. | Yes: `LatticeOperationGrainTests.A_running_operation_whose_runner_silo_is_dead_reads_as_failed`, `LatticeSchemaComplianceOperationsTests.A_failed_scan_reads_as_failed_with_the_engine_reason`. |
| `Write(k,v)` | Environment: a write or delete to a key while the scan runs or after it. | `ILattice.SetAsync`, `ILattice.DeleteAsync`. | Yes: `LatticeSchemaComplianceAdminProgressTests.A_count_that_falls_behind_the_scan_turns_the_total_unknown_rather_than_below_the_scanned_entries`. |
| `ChangePolicy(p)` | Environment: the tree's policy changes while the scan runs. | `LatticeSchemaAdmin.SetPolicyAsync`, `LatticeSchemaAdmin.ClearPolicyAsync`. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_judges_every_value_with_the_policy_read_at_start`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `CountsAddUp` | Every judged value is counted exactly once, as compliant or not. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_mixed_values_group_non_compliant_by_failure_reason`, `LatticeSchemaComplianceAdminTests.ScanCompliance_all_values_compliant_reports_no_breakdown`. |
| `ReasonsSumToNonCompliant` | The failure-reason breakdown accounts for every non-compliant value. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_mixed_values_group_non_compliant_by_failure_reason`. |
| `RuleCountIsPolicyRules` | A governed report's rule count is the policy's rule count, also on a fully compliant tree (#4966). | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_rule_count_matches_policy_not_failure_population`. |
| `FailureReasonCountIsBreakdown` | The failure-reason count sizes the breakdown, not the policy. | Yes: `SchemaComplianceScanResultsTests.A_report_round_trips_through_the_result_map`, `SchemaComplianceScanResultsTests.TryReadReport_invalid_breakdown_count_is_rejected_without_allocating`. |
| `UngovernedReportEmpty` | An ungoverned tree's report has no policy and every count zero. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_ungoverned_tree_returns_an_ungoverned_report_without_scanning`, `LatticeSchemaComplianceReportTests.Ungoverned_reports_no_policy_and_all_zero_counts`. |
| `StableKeysCounted` | A completed scan counted every key that was live and untouched for the whole scan. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_mixed_values_group_non_compliant_by_failure_reason`, `LatticeSchemaComplianceAdminProgressTests.An_untracked_scan_reports_nothing_and_does_not_count_the_tree`. |
| `TotalNeverBelowScanned` | A known total never reads below the entries scanned; once passed it turns unknown. | Yes: `LatticeSchemaComplianceAdminProgressTests.A_count_that_falls_behind_the_scan_turns_the_total_unknown_rather_than_below_the_scanned_entries`. |
| `ReportOnlyWhenSucceeded` | Only a completed scan publishes a report; a cancelled or failed one has none. | Yes: `LatticeSchemaComplianceOperationsTests.Cancelling_a_running_scan_stops_it_and_reads_as_cancelled`, `LatticeSchemaComplianceOperationsTests.A_failed_scan_reads_as_failed_with_the_engine_reason`. |
| `FailsOnlyWhenLost` | No stored value faults the scan; it fails only when its runner is lost. | Yes: `SchemaComplianceUndecodableValueIntegrationTests.Tracked_compliance_scan_over_undecodable_values_succeeds_and_records_them_as_non_compliant`, `LatticeSchemaComplianceAdminUndecodableTests.ScanCompliance_after_a_decode_fault_judges_the_rest_from_the_stored_form`. |
| `PolicyFixedForScan` | A running scan keeps the policy it read at its start. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_judges_every_value_with_the_policy_read_at_start`. |
| `JudgedByStartPolicy` | Every judged value is judged by the start policy. | Yes: `LatticeSchemaComplianceAdminTests.ScanCompliance_judges_every_value_with_the_policy_read_at_start`. |
| `UndecodableCountedNonCompliant` | An undecodable stored value is counted non-compliant under enforcement's decode reason. | Yes: `LatticeSchemaComplianceAdminUndecodableTests.ScanCompliance_after_a_decode_fault_judges_the_rest_from_the_stored_form`, `SchemaComplianceUndecodableValueIntegrationTests.ScanComplianceAsync_counts_undecodable_values_as_non_compliant_and_completes`. |
| `ScanTerminates` | A running scan reaches a terminal state. | Yes: `LatticeSchemaComplianceOperationsTests.A_started_scan_returns_a_handle_and_polls_to_the_report`, `LatticeSchemaComplianceAdminProgressTests.A_tracked_scan_counts_first_then_reports_entries_scanned_against_the_count`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, CountsAddUp, ReasonsSumToNonCompliant, RuleCountIsPolicyRules,
FailureReasonCountIsBreakdown, UngovernedReportEmpty, StableKeysCounted,
TotalNeverBelowScanned, ReportOnlyWhenSucceeded and FailsOnlyWhenLost are state
safety. PolicyFixedForScan, JudgedByStartPolicy and
UndecodableCountedNonCompliant are action safety. ScanTerminates is temporal
liveness under weak fairness of the count, the scan step and its end. The
TwoWrites variant re-checks every state and action check with two writes
racing the scan; the temporal check is bounded-out there for the TLC liveness
budget and is retained on the base.

Every experiment is an anchored action perturbation:

| Mutation | Classification |
|----------|----------------|
| TypeOKCountUndeclaredPhase | Anchored action: Count. |
| CountsAddUpDoubleCounts | Anchored action: Step. |
| ReasonsSumToNonCompliantReasonDropped | Anchored action: Step. |
| RuleCountIsPolicyRulesCountsReasons | Anchored action: Finish. |
| FailureReasonCountIsBreakdownCountsRules | Anchored action: Finish. |
| UngovernedReportEmptyCarriesRules | Anchored action: Start. |
| StableKeysCountedSkipsFirstKey | Anchored action: Step. |
| TotalNeverBelowScannedKeepsTotal | Anchored action: Step. |
| ReportOnlyWhenSucceededCancelPublishes | Anchored action: Cancel. |
| FailsOnlyWhenLostUndecodableFaults | Anchored action: Step. |
| JudgedByStartPolicyReadsLivePolicy | Anchored action: Step. |
| UndecodableCountedNonCompliantCountedCompliant | Anchored action: Step. |
| ScanTerminatesCountNeverEnds | Anchored action: Count. |
| FailsOnlyWhenLostUnattributed | Anchored action: Lose. |
| StableKeysCountedUntrackedWrite | Anchored action: Write. |
| PolicyFixedForScanChangeReachesScan | Anchored action: ChangePolicy. |

## Detectors

The scan detectors drive the real `LatticeSchemaComplianceAdmin` over a
substituted tree and policy provider, so each one observes exactly the
policy read, the count, the per-value tally and the report the actions name;
the progress detectors capture the tracked phase and unit reports. The
operation detectors start, poll and cancel scans through the facade's tracked
compliance operations, and the result-map detectors round-trip the report the
operation records.

## Defect mutations whose fixes have landed

RuleCountIsPolicyRulesCountsReasons is the defect #4966 fixed: a report's rule
count was derived from the failure breakdown, so a fully compliant tree read as
having no rules. FailsOnlyWhenLostUndecodableFaults is the behaviour #5253
replaces: the scan read through the decoding enumeration, so one value it could
not decode faulted the whole scan with no report and no offending key. It is
fixed in the schema-assurance integration bucket; this is integration-branch
evidence, not a claim the fix is released.

## Deliberate abstraction gaps

- **Not a snapshot.** A key written during the scan may be judged before or
  after the write, or not at all if it was created behind the cursor or deleted
  ahead of it; only keys untouched for the whole scan are promised. Atomic
  capture of a whole tree is not available and is not claimed.
  No production symbol: the gap states the documented consistency of the scan, not a mechanism.
- **Policy read once, possibly stale.** The policy is the scanning silo's
  cached policy at the start, which may trail a change made through another
  silo within the propagation bound the policy-configuration model checks.
  Production symbols: `ILatticeSchemaPolicyProvider.GetCompiledPolicyAsync`.
- **Policy content and reasons.** Two abstract policies (two rules and one)
  over four value shapes stand for rule semantics, JSON predicates and
  compilation; a reason is the first failing rule's reason. The decode reason
  carries the decoder's message, so distinct version gaps can appear as
  distinct reason rows.
  Production symbols: `CompiledSchemaPolicy.Validate`.
- **Enumeration under topology change.** The ordered, routing-aware
  enumeration is modelled as a strictly advancing cursor over a fixed key
  order; its continuation across a concurrent reshard, split or alias move is
  owned by the shard-ownership models.
  No production symbol: the gap delegates to another model, not to a production mechanism.
- **Progress coalescing.** The scan reports progress every fixed number of
  values and the sink coalesces reports, so the total turns unknown at a report
  boundary rather than at the exact value that passes it.
  Production symbols: `LatticeOperationProgressSink`.
- **The stored-form tail.** The scan streams decoded values and, at the first
  value the read path cannot decode, judges the rest of the tree from the stored
  form: keys from the access-gated key scan after the last key it judged, values
  point-read with their envelopes and decoded one by one. The model's single
  cursor stands for both reads; a key deleted after the key scan reached it is
  skipped, which the model's deletes ahead of the cursor cover.
  Production symbols: `ILatticeValueDecoder.DecodeAsync`, `IReplicationApplyGrain.ReadStoredWithVersionAsync`.
- **The operation around the scan.** Admission, the record, idempotent starts,
  cancellation delivery and loss detection are the shared operation runner's,
  checked by the durable-operation model; here they are the start, cancel and
  lose steps.
  No production symbol: the gap delegates to another model, not to a production mechanism.
