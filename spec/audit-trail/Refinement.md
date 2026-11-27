# Refinement note: AdminAuditTrail

This maps the admin audit trail's completeness protocol (issue #6056) onto its
production seams: the ordering in `LatticeAdminAudit`, the durable trail
`DurableLatticeAdminAuditTrail`, the exactly-once finalize
`AdminAuditStore.WriteFinalAsync`, and the reconciler
`AdminAuditReconciler.SweepAsync` with its evidence rule
`AdminAuditEvidence.ResolveAsync`. The record shape is the #3638 schema as
approved: the reserved tree `sys-auth-admin-audit`, actor kinds `Subject` and
`BootstrapAdministrator`, and a reconciler that never claims a commit.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `op` | Each facade call: its caller, its shape, how far it got, the anchor base its intent recorded, whether its own write landed, whether it can still commit, the age of its intent, and the counted write failure of a refusal | One invocation of a facade method through `LatticeAdminAudit.RunAsync`, the intent `LatticeAdminAuditRecord` that `DurableLatticeAdminAuditTrail.BeginAsync` writes, and `LatticeAuthMetrics.AdminAuditWriteFailures` |
| `present`, `hist` | The anchor record and its revision history, each revision tagged local or replicated | The anchor `AdminAuditAnchorReader.ReadAsync` observes and the history `ILattice.ScanEntryHistoryAsync` returns, with `EntryRevision.OriginClusterId` |
| `pending` | Intents not yet finalized | `pending/{operationId}` rows of the admin audit tree (`AdminAuditKeys.Pending`) |
| `finals` | Final audit records | Final rows at `AdminAuditKeys.Final`, one per operation id |
| `viewComplete` | Whether the anchor's history is complete for the reconciler | The completeness `AdminAuditEvidence.ResolveAsync` derives from the history source, its truncation and whether it reaches the anchor's present state |
| `remoteBudget` | The replicated revision still to arrive | No production state: bounds the environment. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Authorize` | Authorize the call; a refusal of a mutation or a read writes one Denied record, bounded, attributing the resolved subject, before it is returned, and a failed write is counted | `LatticeAdminAudit.AuthorizeAsync`, `DurableLatticeAdminAuditTrail.RecordDeniedAsync`, `AdminAuditBounds.Bound` | Yes: `LatticeAdminAuditTests.AuthorizeAsync_records_the_denial_before_the_refusal_is_rethrown`, `DurableLatticeAdminAuditTrailTests.RecordDeniedAsync_writes_one_final_denied_record_and_swallows_a_failed_write`, `LatticeAuthAdminAuditTests.ListRulesAsync_refused_records_a_read_denial` and `DurableLatticeAdminAuditTrailTests.RecordDeniedAsync_bounds_caller_supplied_target_and_detail_text`. |
| `BeginIntent` | Write the intent with the resolved actor and the anchor base, or refuse the call before any change | `DurableLatticeAdminAuditTrail.BeginAsync`, `LatticeAdminAudit.RunAsync` | Yes: `DurableLatticeAdminAuditTrailTests.BeginAsync_writes_a_pending_intent_attributing_the_resolved_actor_and_the_anchor_base`, `DurableLatticeAdminAuditTrailTests.BeginAsync_fails_closed_when_the_intent_cannot_be_written` and `LatticeAdminAuditTests.RunAsync_refuses_the_call_before_the_commit_when_the_intent_cannot_be_written`. |
| `Commit` | The facade's own validate-and-commit, run only after the intent; whether it changed anything comes from the store's merge against the record it holds, not from an earlier read | `LatticeAdminAudit.RunAsync` | Yes: `LatticeAdminAuditTests.RunAsync_writes_the_intent_before_the_commit_and_finalizes_committed`, `LatticeAuthAdminAuditTests.PutRuleAsync_authorized_writes_the_intent_before_the_store_then_finalizes` and `LatticeAuthAdminAuditTests.RemoveRuleAsync_that_removed_nothing_reports_no_change`. |
| `Finalize` | Classify the outcome (Committed only from the call's own result; no change only when the movement check proves the anchor unmoved; otherwise Indeterminate), then write the final record create-only and remove the intent; leave a possibly-committed or undecided call pending | `LatticeAdminAudit.Classify`, `DurableLatticeAdminAuditTrail.CompleteAsync`, `AdminAuditStore.WriteFinalAsync` | Yes: `LatticeAdminAuditTests.RunAsync_leaves_the_intent_pending_on_a_failure_that_may_follow_the_commit`, `LatticeAdminAuditTests.RunAsync_leaves_the_intent_pending_when_the_result_is_undecided`, `LatticeAdminAuditTests.RunAsync_finalizes_a_domain_refusal_as_failed_and_rethrows_it`, `DurableLatticeAdminAuditTrailTests.WriteFinalAsync_admits_exactly_one_final_record_per_intent` and `DurableLatticeAdminAuditTrailTests.CompleteAsync_committed_reports_no_change_for_a_removal_of_an_anchor_that_never_existed`. |
| `Reconcile` | Resolve this region's stale intents from anchor evidence: not committed only once past the maximum pending age, Indeterminate on a local revision or unproven history, never Committed | `AdminAuditReconciler.SweepAsync`, `AdminAuditEvidence.ResolveAsync` | Yes: `AdminAuditReconcilerTests.Sweep_resolves_an_intent_whose_anchor_never_moved_as_not_committed_only_after_the_max_pending_age`, `AdminAuditReconcilerTests.Sweep_never_records_not_committed_for_a_long_running_activation_that_commits_after_the_lease`, `AdminAuditReconcilerTests.Sweep_never_resolves_an_intent_another_region_wrote`, `AdminAuditReconcilerTests.Sweep_records_indeterminate_not_committed_when_a_local_write_moved_the_anchor` and `AdminAuditReconcilerTests.Sweep_waits_on_incomplete_history_then_records_indeterminate_at_the_max_pending_age`. |
| `ReplicateApply` | A replicated revision is applied as data and audited nowhere | Replication apply of a system tree, which has no audit call site | Yes: `LatticeAdminAuditRecordShapeTests.Only_the_administration_facades_and_the_trail_emit_admin_audit_records` and `AdminAuditReconcilerTests.Sweep_records_not_committed_when_complete_history_holds_only_replicated_writes`. |
| `Crash` | Environment: the caller stops between its intent and its commit, or between its commit and its finalize | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `Settle` | Environment: an accepted app activation lands, or does not, after the facade returned | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `Tick` | Environment: an intent ages past the lease, then past the maximum pending age | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |
| `Stutter` | Environment: every call has finished and every intent is resolved | Modelling device. | Not applicable: an environment step, so there is no production behaviour to detect here. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `EveryCommitAudited` | The intent precedes every commit and fails closed, the final record is written create-only under a key derived from the intent, and the actor is the resolved subject with its actor kind | Yes: `DurableLatticeAdminAuditTrailTests.BeginAsync_fails_closed_when_the_intent_cannot_be_written`, `DurableLatticeAdminAuditTrailTests.WriteFinalAsync_admits_exactly_one_final_record_per_intent`, `DurableLatticeAdminAuditTrailTests.BeginAsync_writes_a_pending_intent_attributing_the_resolved_actor_and_the_anchor_base` and `DurableLatticeAdminAuditTrailTests.BeginAsync_marks_a_bootstrap_administrator`. |
| `NoPhantomAudit` | A record claims a commit only when the call's own result reports a change; a removal that matched nothing is no change | Yes: `LatticeAdminAuditTests.RunAsync_finalizes_a_reported_no_op_as_failed_no_change`, `LatticeAuthAdminAuditTests.RemoveRuleAsync_that_removed_nothing_reports_no_change` and `DurableLatticeAdminAuditTrailTests.CompleteAsync_committed_reports_no_change_for_a_removal_of_an_anchor_that_never_existed`. |
| `NoFalseAbsence` | No change, failure and not committed are recorded only for a call that changed nothing; a fault that may follow the commit and an accepted activation stay pending | Yes: `LatticeAdminAuditTests.RunAsync_leaves_the_intent_pending_on_a_failure_that_may_follow_the_commit`, `LatticeAdminAuditTests.RunAsync_leaves_the_intent_pending_when_the_result_is_undecided` and `LatticeAdminAuditTests.Classify_maps_each_failure_to_the_outcome_it_can_prove`. |
| `NeverNotCommittedWhileLive` | Not committed is recorded only past the maximum pending age, which outlives every operation | Yes: `AdminAuditReconcilerTests.Sweep_never_records_not_committed_for_a_long_running_activation_that_commits_after_the_lease` and `AdminAuditReconcilerTests.Sweep_resolves_an_intent_whose_anchor_never_moved_as_not_committed_only_after_the_max_pending_age`. |
| `ReconcilerNeverClaimsCommitted` | The reconciler's verdicts are not committed and indeterminate only | Yes: `AdminAuditReconcilerTests.Sweep_records_indeterminate_not_committed_when_a_local_write_moved_the_anchor`. |
| `DenialsAudited` | Every refusal of an authenticated caller, read or mutation, writes one Denied record or is counted; a validation error after authorization is a failure, not a denial | Yes: `LatticeAdminAuditTests.AuthorizeAsync_records_the_denial_before_the_refusal_is_rethrown`, `LatticeAdminAuditTests.AuthorizeAsync_does_not_record_a_validation_error_as_a_denial`, `LatticeAuthAdminAuditTests.ListRulesAsync_refused_records_a_read_denial` and `DurableLatticeAdminAuditTrailTests.RecordDeniedAsync_writes_one_final_denied_record_and_swallows_a_failed_write`. |
| `ReplicatedNotReAudited` | Only facades emit records, and the reconciler resolves only its own region's intents | Yes: `LatticeAdminAuditRecordShapeTests.Only_the_administration_facades_and_the_trail_emit_admin_audit_records` and `AdminAuditReconcilerTests.Sweep_never_resolves_an_intent_another_region_wrote`. |
| `NoSecretMaterial` | The record is an allow-list of identifiers and scalars, the request carries no payload and no actor, and caller-supplied text is bounded | Yes: `LatticeAdminAuditRecordShapeTests.Record_carries_only_the_allow_listed_identifier_and_scalar_fields`, `LatticeAdminAuditRecordShapeTests.Request_carries_no_payload_and_no_actor` and `DurableLatticeAdminAuditTrailTests.RecordDeniedAsync_bounds_caller_supplied_target_and_detail_text`. |
| `IntentsResolve` | Every stale intent is eventually finalized: an unprovable one becomes indeterminate at the maximum pending age | Yes: `AdminAuditReconcilerTests.Sweep_waits_on_incomplete_history_then_records_indeterminate_at_the_max_pending_age` and `AdminAuditReconcilerTests.Sweep_only_removes_the_intent_when_the_caller_already_finalized_it`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **One anchor per call.** A call that writes several records (a group removal's cascade) is anchored on its last write, so its intent, its finalize and its evidence see one record; the cascade's earlier writes are not separately audited.
  Production symbols: `MembershipAuditAnchors.Group`, `TenantAuditAnchors`.
- **Anchors without versions.** An alias lives in the core registry tree, which is read without a version or history, so only an unchanged value proves no write there; the model's anchor is versioned.
  Production symbols: `AdminAuditAnchorReader.IsSystemTree`.
- **The history window.** For an anchor absent at the intent the history scan starts a clock-skew allowance before the intent, and it reads a bounded number of pages; an earlier or unread revision can only turn a verdict to indeterminate, never to a claim.
  Production symbols: `AdminAuditEvidence.ClockSkewAllowance`, `AdminAuditEvidence.ResolveAsync`.
- **The finalize is one step.** The create-only write and the intent's removal are two writes; a lost removal leaves an intent the next sweep removes without writing a second record.
  Production symbols: `AdminAuditStore.WriteFinalAsync`, `AdminAuditReconciler.SweepAsync`.
- **Failure classification.** The model's three commit results stand for the classes `LatticeAdminAudit.Classify` maps exception types to; the mapping of each type is pinned by its own test.
  Production symbols: `LatticeAdminAudit.Classify`.
- **Retention and reads.** Ageing out final records and the operator and tenant readers do not change what is written, so they are not modelled; intents are never aged out.
  Production symbols: `AdminAuditReconciler.SweepAsync`.
