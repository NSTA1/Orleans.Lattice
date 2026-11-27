# Refinement note: TenantGrants

This maps the cross-tenant grant protocol (issue #6050) onto its production
seams: the two-party agreement behind `ILatticeTenantGrantAdmin`, the record
join that converges it, and the tenancy engine's cross-tenant read resolution
at the auth gate. A caller's own authority is the F1 contract's abstract
`Allowed(caller, op, scope)` (`spec/auth-decision/`, contract version 1): the
platform operator is `Admin` on the policy tree, a delegated tenant admin is
`Admin` on that tenant's capability scope, and a reader holds a `Read` on the
granting tenant's tree or does not. The cfg binds `Allowed` to
`ContractAllowed`, the contract's minimal grant-set definition, which satisfies
C1-C6 by construction. The model relies on no more than "the caller is a
platform operator", "the caller is an admin of tenant T", which is what
`TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync` decides, and "the
policy decision allows the reader's read", which is what
`PolicyAccessGate.AuthorizeAsync` decides before the tenant enforcer is
consulted.

Snapshot freshness is the epoch and lease protocol of `spec/tenant-isolation/`
in the form `TenantIsolation` guarantees it (`NoStaleAuthority`,
`PublicationWindowTracked`): a registry write returns only once a silo that
trusted its snapshot was told or had its lease waited out, and a failed
publication returns early and owes a re-publish. `RevocationTakesEffect` bounds
the staleness a read admitted after a returned revocation may observe to
exactly that owed re-publish.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `status` | Whether each tenant is registered | The record `LatticeTenantRegistry.GetAsync` returns, removed by `LatticeTenantRegistry.DeleteAsync` |
| `incB` | The grantee's incarnation | No production state: a tenant created again under a deleted id is a fresh record, `LatticeTenantAdmin.CreateTenantAsync`. |
| `grant` | The grant slot on the granting tenant's record | `TenantGrantSlot` in `TenantRecord.GrantSlots`, with its `TenantGrantSlot.Generation` and `CrossTenantGrant.State` |
| `seen` | The grant as the peer silo's compiled snapshot holds it | The snapshot `CompiledTenantPolicySnapshotMaintainer.RebuildNowAsync` compiles |
| `trusted` | Whether the peer holds its snapshot authoritative | `CompiledTenantPolicySnapshotMaintainer.IsSnapshotAuthoritative` |
| `wstate` | Whether a write's publication is in flight | `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` awaiting `TenantPolicyEpochLedger.AdvanceAsync` |
| `owed` | Whether a failed publication is owed a re-publish | `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry` |
| `captured` | Whether the publication still waits on the peer | The lease captured by `TenantPolicyEpochLedger.AdvanceAsync` |
| `ack` | The agreement the in-flight write closes once it returns | No production state: the history the property judges. |
| `closed` | The agreements whose revocation has returned | No production state: the history the property judges. |
| `offers` | The generations offered with the granting side's authority | No production state: the history the property judges. |
| `approvals` | The generations approved with the grantee side's authority | No production state: the history the property judges. |
| `fl` | A call between its registry read and its write | The record a `LatticeTenantGrantAdmin` call read and is committing through `LatticeTenantRegistry.PutMergeAsync` |
| `budget` | The write budget | No production state: a bound on the checked instance. |
| `failures` | The publication-failure budget | No production state: a bound on the checked instance. |
| `obs` | Each property's verdict on the last step | No production state: the response or decision the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Offer` | The granting tenant's authority, the offer rule, then a pending grant in a new generation | `LatticeTenantGrantAdmin.OfferGrantAsync`, `TenantGrantLifecycle.IsLegalOffer`, `TenantRecord.OfferGrant` | Yes: `LatticeTenantGrantAdminTests.OfferGrantAsync_creates_the_grant_pending`, `LatticeTenantGrantAdminTests.The_grantees_admin_may_not_offer_a_grant_from_the_granting_tenant` and `LatticeTenantGrantAdminTests.Offering_new_terms_over_a_live_agreement_is_refused`. |
| `Answer` | The grantee's authority, then the granting tenant's record, where an unregistered tenant reads as an unoffered grant, then the transition | `LatticeTenantGrantAdmin.ApproveGrantAsync`, `LatticeTenantGrantAdmin.RejectGrantAsync`, `TenantGrantLifecycle.IsLegalTransition`, `TenantRecord.TransitionGrant` | Yes: `LatticeTenantGrantAdminTests.The_granting_tenants_admin_may_not_approve_its_own_offer`, `LatticeTenantGrantAdminTests.A_non_operator_approving_a_grant_from_an_unregistered_tenant_is_told_denied` and `LatticeTenantGrantAdminTests.An_unregistered_granting_tenant_is_reported_as_an_unoffered_grant`. |
| `Revoke` | The granting side probed without throwing, then the grantee side, then the transition | `LatticeTenantGrantAdmin.RevokeGrantAsync`, `TenantRegionResidencyAuthorizer.TryAuthorizeTenantAdminAsync` | Yes: `LatticeTenantGrantAdminTests.The_grantees_admin_may_also_revoke_an_active_grant` and `LatticeTenantGrantAdminTests.Revoking_a_grant_from_an_unregistered_tenant_reports_not_found`. |
| `CommitCall` | A copy of the record joined with what was committed since it was read, the call refused when the join is not its target, and refused as an unoffered grant when the granting tenant was deleted since the read; a replicated or restored record is joined the same way | `LatticeTenantRegistry.PutMergeAsync`, `LatticeTenantGrantAdmin.CommitAsync`, `TenantRecord.MergeFrom`, `TenantGrantSlot.Merge` | Yes: `LatticeTenantGrantAdminTests.A_concurrent_revoke_beats_an_approve_that_carries_a_higher_stamp`, `LatticeTenantGrantAdminTests.Approving_while_the_granting_tenant_is_deleted_does_not_recreate_it`, `TenantGrantSlotTests.Merge_keeps_a_revoke_that_lost_the_stamp_race_to_a_concurrent_approve` and `TenantRecordGrantLifecycleTests.Merging_grant_state_is_commutative_across_replicas`. |
| `Read` | The policy decision, then the tenant enforcer: an authoritative snapshot, a registry confirmation or a denial; active-tenant validation, then an active grant covering the tree | `PolicyAccessGate.AuthorizeAsync`, `TenantGateEnforcer.EnforceAsync`, `LatticeTenantPolicyEngine.ValidateActiveTenant`, `LatticeTenantPolicyEngine.ResolveCrossTenantGrant` | Yes: `PolicyAccessGateTenantEnforcementTests.AuthorizeAsync_policy_deny_is_not_consulted_by_an_allowing_enforcer`, `TenantGateEnforcerTests.Enforce_cross_tenant_validates_the_active_tenant_before_resolving_a_grant`, `LatticeTenantPolicyEngineGrantStateTests.A_pending_grant_denies`, `LatticeTenantPolicyEngineTests.ResolveCrossTenantGrant_denies_a_sibling_tree_that_only_shares_a_name_prefix`, `TenantGateEnforcerSnapshotLagTests.EnforceAsync_read_immediately_after_revoking_the_grant_is_refused` and `TenantGateEnforcerTests.Enforce_gives_one_crossing_reason_whatever_the_cause_and_names_no_tenant`. |
| `Probe` | A policy grant reveals the tree only when the tenant enforcer admits it too | `PolicyAccessGate.HasAnyGrantAsync` | Yes: `PolicyAccessGateAsyncTenantEnforcementTests.HasAnyGrantAsync_awaits_an_asynchronous_tenant_deny_and_hides_the_tree`. |
| `Deliver` | The epoch push marks the peer's snapshot out of date before it acknowledges | `TenantPolicyEpochSubscription.OnEpochAdvancedAsync` | Yes: `TenantPolicyEpochSubscriptionTests.OnEpochAdvancedAsync_marks_the_snapshot_out_of_date_before_acknowledging`. |
| `Lapse` | The peer's lease lapses, margin included, and the publication stops waiting on it | `TenantPolicyEpochLedger.AdvanceAsync`, `TenantSnapshotCurrency.IsCurrent` | Yes: `TenantPolicyCrossSiloCurrencyTests.Peer_silo_whose_lease_lapses_is_not_authoritative` and `TenantPolicyCrossSiloCurrencyTests.Unreachable_peer_holds_the_write_open_until_its_lease_lapses_and_then_denies`. |
| `Complete` | Every captured silo told or lapsed: the write returns | `TenantPolicyEpochLedger.AdvanceAsync` completes and `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync` returns | Yes: `TenantPolicyCrossSiloCurrencyTests.Every_peer_silo_is_told_before_the_write_completes`. |
| `Fail` | The advance faults; the durable write returns and a re-publish is owed | `CompiledTenantPolicySnapshotMaintainer.OnMutationAsync`, `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry` | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `Retry` | The background loop re-publishes the owed advance | `CompiledTenantPolicySnapshotMaintainer.EnsureAdvanceRetry` | Yes: `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `Rebuild` | The peer renews its lease and recompiles from the registry | `CompiledTenantPolicySnapshotMaintainer.RebuildNowAsync` | Yes: `TenantPolicyCrossSiloCurrencyTests.Peer_silo_converges_and_regains_authority_once_its_rebuild_lands` and `CompiledTenantPolicySnapshotMaintainerCurrencyTests.ObserveEpoch_newer_epoch_revokes_authority_until_the_rebuild_lands`. |
| `DeleteTenant` | The grants issued to the tenant are removed from the granting tenants' records, then its own record goes | `LatticeTenantAdmin.DeleteTenantAsync`, `LatticeTenantRegistry.DeleteAsync`, `LatticeTenantRegistry.PurgeInboundGrantsAsync` | Yes: `TenantDeletionAccessPurgeIntegrationTests.Deleting_a_tenant_removes_the_grants_issued_to_it_so_a_recreated_tenant_inherits_none` and `LatticeTenantAdminTests.DeleteTenantAsync_cascades_the_trees_then_removes_the_registry_record`. |
| `Recreate` | Only an unregistered id is created, as a fresh record, through the registry's one creating write | `LatticeTenantAdmin.CreateTenantAsync`, `LatticeTenantRegistry.CreateAsync` | Yes: `LatticeTenantAdminTests.CreateTenantAsync_of_a_duplicate_fails_closed` and `LatticeTenantAdminTests.CreateTenantAsync_registers_a_new_active_tenant`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `TwoPartyActivation` | Only an approved grant authorizes, approval is the grantee's alone, an approval of superseded terms never activates the new ones, and a tenant created under a deleted id inherits no grant | Yes: `LatticeTenantPolicyEngineGrantStateTests.A_pending_grant_denies`, `LatticeTenantGrantAdminTests.The_granting_tenants_admin_may_not_approve_its_own_offer`, `LatticeTenantGrantAdminTests.A_concurrent_approve_cannot_activate_terms_it_never_saw` and `TenantDeletionAccessPurgeIntegrationTests.Deleting_a_tenant_removes_the_grants_issued_to_it_so_a_recreated_tenant_inherits_none`. |
| `AdminScopedToOwnSide` | An offer needs the granting tenant's authority, an approval or rejection the grantee's, a revocation either's | Yes: `LatticeTenantGrantAdminTests.The_grantees_admin_may_not_offer_a_grant_from_the_granting_tenant`, `LatticeTenantGrantAdminTests.The_granting_tenants_admin_may_not_reject_on_the_grantees_behalf` and `LatticeTenantGrantAdminTests.A_third_tenants_admin_may_not_approve_a_grant_offered_to_another_tenant`. |
| `ReadRequiresActiveGrant` | A crossing is admitted only after the policy allow, the active-tenant validation and an active grant from an active owner covering the tree | Yes: `PolicyAccessGateTenantEnforcementTests.AuthorizeAsync_policy_deny_is_not_consulted_by_an_allowing_enforcer`, `TenantGateEnforcerTests.Enforce_cross_tenant_denies_a_subject_that_may_not_act_as_the_active_tenant`, `LatticeTenantPolicyEngineGrantStateTests.A_pending_grant_denies` and `LatticeTenantPolicyEngineTests.ResolveCrossTenantGrant_denies_a_sibling_tree_that_only_shares_a_name_prefix`. |
| `RevocationTakesEffect` | A revoked grant stops admitting on every silo by the time the revocation returns, except while a failed publication is owed its re-publish | Yes: `TenantGateEnforcerSnapshotLagTests.EnforceAsync_read_immediately_after_revoking_the_grant_is_refused`, `TenantPolicyCrossSiloCurrencyTests.Peer_silo_never_notified_of_a_revocation_must_not_admit` and `CompiledTenantPolicySnapshotMaintainerCurrencyTests.Failed_advance_leaves_the_silo_non_authoritative_until_a_background_retry_publishes_it`. |
| `NonDisclosure` | An unauthorized caller is told denied, never not-found; an unregistered granting tenant reads as an unoffered grant; a refused read gets one reason per stage, naming no tenant; the existence probe hides a tree the enforcer refuses | Yes: `LatticeTenantGrantAdminTests.A_non_operator_approving_a_grant_from_an_unregistered_tenant_is_told_denied`, `LatticeTenantGrantAdminTests.An_unregistered_granting_tenant_is_reported_as_an_unoffered_grant`, `TenantGateEnforcerTests.Enforce_gives_one_crossing_reason_whatever_the_cause_and_names_no_tenant`, `TenantGateEnforcerTests.Enforce_gives_one_active_tenant_reason_whatever_the_cause_and_names_no_tenant` and `PolicyAccessGateAsyncTenantEnforcementTests.HasAnyGrantAsync_awaits_an_asynchronous_tenant_deny_and_hides_the_tree`. |
| `DeletionIsFinal` | A registry update never creates a record, so a facade write that read a record before its tenant was deleted cannot re-create the tenant; only tenant creation does | Yes: `LatticeTenantRegistryConcurrencyTests.PutMergeAsync_update_of_an_absent_record_throws_and_writes_nothing`, `LatticeTenantGrantAdminTests.Approving_while_the_granting_tenant_is_deleted_does_not_recreate_it`, `LatticeTenantAccessAdminTests.Adding_an_admin_while_the_tenant_is_deleted_does_not_recreate_it` and `LatticeTenantRegionAdminConcurrencyTests.SetResidencyAsync_while_the_tenant_is_deleted_does_not_recreate_it`. |
| `NoUnilateralTrap` | Either party, or an operator, may revoke an active grant | Yes: `LatticeTenantGrantAdminTests.The_granting_tenants_admin_may_revoke_an_active_grant` and `LatticeTenantGrantAdminTests.The_grantees_admin_may_also_revoke_an_active_grant`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **One grant, one direction.** Tenant A grants tenant B one scope with read operations; a second grant, a grant in the other direction and the operation set are not modelled, because the decision is per grant and every check is symmetric in the two tenants.
  Production symbols: `CrossTenantGrant.Operations`, `TenantGrantOperations`.
- **Terms are the generation.** Amended terms are represented only by the new agreement generation an offer begins, not by a scope or operation payload, so the check that an approval never activates terms it did not see is the generation order of the join.
  Production symbols: `TenantGrantSlot.Generation`, `TenantRecord.OfferGrant`.
- **The stamp tiebreak.** Two writes of the same state in one generation are joined by their stamps; both carry the same state, so the model keeps either.
  Production symbols: `TenantClock.Supersedes`.
- **Suspension.** A suspended tenant's grants admit nothing and its admins hold no tenant-tier authority; suspension, including the suspension that opens a deletion, is not modelled, and deletion is one step.
  Production symbols: `LatticeTenantPolicyEngine.ResolveCrossTenantGrant`, `TenantRegionResidencyAuthorizer.TryAuthorizeTenantAdminAsync`, `LatticeTenantAdmin.DeleteTenantAsync`.
- **One peer silo and one failure.** The facades and every write run on one silo, whose decisions confirm against the registry; one peer holds a compiled snapshot. The writer's own snapshot, silo crashes and epoch grain re-activation are `TenantIsolation`'s, and so is the lease clock margin.
  Production symbols: `TenantPolicyEpochGrain`, `TenantPolicyEpochLedger.AdvanceAsync`.
- **Only the operator's calls are split.** A tenant admin's call commits in one step; the operator's approvals and revocations may hold their read open across other steps, which reaches every interleaving of one call's read-to-write window.
  Production symbols: `LatticeTenantGrantAdmin.CommitAsync`.
- **Cross-cluster replication of the registry.** The tenant registry is not among the system trees replication carries; a record that arrives by a restore or a host's own replication is joined like any copy, which is what `CommitCall` checks, and the lag between clusters is not modelled.
  Production symbols: `LatticeSystemTreeNames.BuildReplicationConfigEnrolmentMap`, `TenantRecord.MergeFrom`.
- **The single-step host path.** A host holding registry authority may write an active grant directly, bypassing the agreement; it is not the facade's and is not modelled.
  Production symbols: `TenantRecord.AddGrant`.
- **The inbox and listing.** Listing a tenant's issued and received grants is not modelled.
  Production symbols: `LatticeTenantGrantAdmin.ListGrantsAsync`.
