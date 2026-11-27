# Refinement note: TenantAccessAuthorization

This maps the authorization half of delegated tenant-access administration
(epic #4154) onto its production seams. Authorization is the F1 contract's
abstract `Allowed(caller, op, scope)` (`spec/auth-decision/`, contract version
1): the platform operator is `Admin` on the policy tree and a delegated tenant
admin is `Admin` on that tenant's capability scope. The cfg binds `Allowed` to
`ContractAllowed`, the contract's minimal grant-set definition, which satisfies
C1-C6 by construction (no bootstrap administrator, no scopeless grant, no
unmatched control-kind allow). The model never relies on more than "the caller
is a platform operator" and "the caller is an admin of tenant T", which is what
`TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync` decides.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `flag` | Each silo's live delegated-access flag | `DelegatedTenantAccessFlag.IsEnabled`, read by `TenancyTenantRuleLayer.IsActive` and by the facades' `isEnabled` delegate |
| `vreq` | Whether identity-directory validation is required | `LatticeIdentityDirectoryOptions.ValidationRequired` with a real `ILatticeIdentityDirectory` registered |
| `rules` | The authorization policy tree's rules, with their author | `LatticeAuthorizationPolicyStore.PutRuleAsync` writes; a replication apply or restore writes the tree directly |
| `clusterGroups` | Group records created through the cluster facade | `LatticeAuthAdmin.UpsertGroupAsync` over `LatticeMembershipDirectory.UpsertGroupAsync` |
| `tenantGroups` | Tenant group records | `LatticeTenantDirectoryAdmin.UpsertGroupAsync` over `ITenantDirectoryStore` |
| `edges` | Membership edges `<<member, group>>` | `LatticeMembershipDirectory.AddMemberAsync` |
| `adminEntries` | Each tenant's admin-set entries | `TenantRecord.HasAdminSubject`, written by `LatticeTenantAccessAdmin.AddAdminSubjectAsync` |
| `memberEntries` | Each tenant's member-set entries | `TenantRecord.HasMemberSubject`, written by `LatticeTenantDirectoryAdmin.AddMemberAsync` |
| `budget` | The write budget | No production state: a bound on the checked instance. |
| `obs` | What the last step returned or decided | No production state: the response, the decision or the resolved claim set the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `FacadePutRule` | Authorize, flag, reserved tenant, confinement, then a system-origin store write | `LatticeTenantPolicyAdmin.PutRuleAsync`, `LatticeTenantPolicyAdmin.AuthorizeAsync`, `TenantPolicyScope.ToStoredRule`, `LatticeAuthorizationPolicyStore.PutRuleAsync` | Yes: `LatticeTenantPolicyAdminTests.Operation_by_a_caller_who_is_neither_operator_nor_tenant_admin_is_denied_before_anything_is_read`, `LatticeTenantPolicyAdminTests.PutRule_refuses_a_tree_outside_the_tenant_layer` and `LatticeTenantPolicyAdminTests.PutRule_composes_the_tenant_rule_id_and_tree_and_writes_under_system_origin`. |
| `OperatorPutRule` | An operator's rule: tenant-tier ids and tenant-wide scopes refused, then the store's confinement for every caller | `LatticeAuthAdmin.PutRuleAsync`, `LatticeAuthorizationPolicyStore.PutRuleAsync`, `TenantRuleConfinement.EnsureConfined` | Yes: `LatticeAuthAdminTenantTierTests.PutRuleAsync_refuses_a_tenant_tier_rule_id_before_reaching_the_store` and `LatticeAuthorizationPolicyStoreTenantRuleTests.D4_refuses_a_tenant_group_on_another_tenants_tree`. |
| `ArriveRule` | A policy record written by replication or restore around the store; the snapshot compiler re-validates tenant rules | `CompiledTenantPartition.Build`, `TenantRuleConfinement.CheckTenantRule` | Yes: `CompiledTenantPartitionTests.Compile_drops_a_tenant_rule_that_escaped_the_store_confinement`. |
| `OperatorUpsertGroup` | Authorize, reserve the whole `t/` prefix, validate, write | `LatticeAuthAdmin.UpsertGroupAsync` | Yes: `LatticeAuthAdminTenantTierTests.UpsertGroupAsync_refuses_every_id_in_the_reserved_tenant_namespace` and `LatticeAuthAdminValidationTests.UpsertGroupAsync_rejects_an_unresolvable_group_when_validation_is_required`. |
| `FacadeUpsertGroup` | Authorize, flag, reserved tenant, then a group id composed from the named tenant | `LatticeTenantDirectoryAdmin.UpsertGroupAsync`, `LatticeTenantDirectoryAdmin.AuthorizeAsync` | Yes: `LatticeTenantDirectoryAdminTests.UpsertGroupAsync_stores_the_group_under_the_composed_tenant_group_id` and `LatticeTenantDirectoryAdminTests.Every_operation_denies_an_unauthorized_caller_before_revealing_the_feature_posture`. |
| `ClusterAddEdge` | Authorize, validate the non-`t/` ids, then the directory's nesting invariant for every caller | `LatticeAuthAdmin.AddMemberAsync`, `LatticeMembershipDirectory.AddMemberAsync`, `TenantGroupNesting.EnsureAllowed` | Yes: `TenantGroupNestingTests.AddMemberAsync_refuses_an_operator_origin_caller_too` and `LatticeAuthAdminValidationTests.AddMemberAsync_rejects_an_unresolvable_member_when_validation_is_required`. |
| `FacadeAddEdge` | Authorize, flag, reserved tenant, refuse a raw `t/` id, require the group, validate, nesting | `LatticeTenantDirectoryAdmin.AddGroupMemberAsync`, `LatticeTenantDirectoryAdmin.ToStoredId` | Yes: `LatticeTenantDirectoryAdminTests.AddGroupMemberAsync_refuses_a_raw_tenant_group_id_named_as_a_user_or_cluster_group` and `LatticeTenantDirectoryAdminTests.AddGroupMemberAsync_validates_a_user_against_the_identity_directory_when_required`. |
| `AddAdminEntry` | Authorize, reserved tenant, admin-entry confinement, own group exists or the id resolves, then write | `LatticeTenantAccessAdmin.AddAdminSubjectAsync`, `LatticeTenantAccessAdmin.EnsureAdmissibleAdminEntry` | Yes: `LatticeTenantAccessAdminAdminSetConfinementTests.AddAdminSubjectAsync_refuses_a_reserved_namespace_entry_that_is_not_the_tenants_own_group` and `LatticeTenantAccessAdminTests.AddAdminSubjectAsync_with_an_unresolvable_id_is_denied_before_any_write`. |
| `AddMemberEntry` | Authorize, flag, reserved tenant, refuse a raw `t/` id, own group exists or the id resolves, then write | `LatticeTenantDirectoryAdmin.AddMemberAsync` | Yes: `LatticeTenantDirectoryAdminTests.AddMemberAsync_refuses_another_tenants_group_named_as_a_cluster_group` and `LatticeTenantDirectoryAdminTests.Every_operation_refuses_an_authorized_caller_while_the_feature_is_disabled`. |
| `ResolveClaims` | Claim-derived `t/` groups stripped before expansion whenever tenancy is registered; directory-recorded groups kept | `MembershipContext.ApplyTenantGroupClaimFilter`, `TenantGroupClaimFilter.Filter`, `LatticeTenancyServiceCollectionExtensions.AddLatticeTenancy` | Yes: `MembershipContextTenantGroupClaimFilterTests.Active_filter_strips_malformed_and_default_tenant_ids_too` and `TenancyTenantGroupClaimReservationTests.With_the_flag_off_an_asserted_tenant_group_is_stripped_and_matches_no_operator_rule`. |
| `Evaluate` | The tenant layer is consulted only while the silo's flag is on, beneath an operator match | `LatticeDecisionEngine.Evaluate`, `TenancyTenantRuleLayer.IsActive` | Yes: `TenantAccessConformanceTests.Case10b_with_the_flag_off_a_pre_existing_tenant_rule_is_inert` and `TenancyDelegatedAccessSeamsTests.RuleLayer_IsActive_follows_the_flag`. |
| `FlipFlag` | One silo's options reload flips its flag and deletes nothing | `DelegatedTenantAccessFlag.Set` | Yes: `TenantAccessConformanceTests.Case10b_with_the_flag_off_a_pre_existing_tenant_rule_is_inert` and `LatticeTenantDirectoryAdminTests.The_feature_flag_is_read_live_on_every_call`. |
| `Posture` | Authorized like every verb, and answers while the flag is off | `LatticeTenantPolicyAdmin.GetPostureAsync` | Yes: `LatticeTenantDirectoryAdminFeatureOffIntegrationTests.An_unauthorized_caller_is_denied_before_the_posture_is_revealed` and `LatticeTenantPolicyAdminTests.GetPosture_answers_while_the_feature_is_off_and_reports_it_off`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `TenantRuleConfinement` | A `tenant:` rule is written only under system origin by the tenant policy facade after its admin check, is confined to its tenant's tenant-layer trees and tenant-wide scope within `LatticeAuthOperations.All`, and no rule names a tenant group outside that group's tenant; a non-conforming arrival never takes effect | Yes: `LatticeAuthorizationPolicyStoreTenantRuleTests.PutRuleAsync_refuses_a_tenant_rule_id_off_system_origin_before_reading`, `LatticeAuthorizationPolicyStoreTenantRuleTests.D7_refuses_a_tenant_rule_outside_its_tenants_data_trees`, `CompiledTenantPartitionTests.Compile_drops_a_tenant_rule_that_escaped_the_store_confinement` and `TenantAccessConformanceTests.Case06_a_rule_naming_a_tenant_group_cannot_be_scoped_outside_its_tenant_by_anyone_and_an_app_binding_in_another_tenant_naming_it_is_refused`. |
| `ReservedGroupNamespace` | No cluster group is created anywhere in `t/` (malformed and `t/default/...` ids included), and no edge nests a tenant group in a cluster group or another tenant's group | Yes: `LatticeAuthAdminTenantTierTests.UpsertGroupAsync_refuses_every_id_in_the_reserved_tenant_namespace`, `TenantGroupNestingTests.Evaluate_refuses_the_forbidden_combinations` and `TenantAccessConformanceTests.Case04_neither_a_tenant_admin_nor_an_operator_can_make_a_tenant_group_a_member_of_a_cluster_group_or_of_another_tenants_group`. |
| `ClaimsNeverConferTenantGroups` | With tenancy registered no asserted `t/` group survives claim filtering, whatever the flag, on any silo | Yes: `TenancyTenantGroupClaimReservationTests.AddLatticeTenancy_claim_filter_is_active_whatever_the_flag`, `TenantAccessConformanceTests.Case05b_with_the_flag_off_a_token_asserting_a_tenant_group_does_not_match_an_operator_rule_naming_it` and `MembershipContextTenantGroupClaimFilterTests.Active_filter_strips_malformed_and_default_tenant_ids_too`. |
| `FeatureOffInert` | While the flag is off the delegated facades refuse (posture excepted) and the evaluator ignores tenant rules | Yes: `TenantAccessConformanceTests.Case10a_with_the_flag_off_every_facade_member_refuses_except_posture` and `TenantAccessConformanceTests.Case10b_with_the_flag_off_a_pre_existing_tenant_rule_is_inert`. |
| `AuthorizeBeforeDisclose` | Every facade verb authorizes before the flag, the reserved-tenant check, directory validation and any read | Yes: `LatticeTenantDirectoryAdminTests.Every_operation_denies_an_unauthorized_caller_before_revealing_the_feature_posture` and `LatticeTenantPolicyAdminTests.Operation_while_the_feature_is_off_still_denies_an_unauthorized_caller_first`. |
| `DirectoryValidationOrder` | Authorize, then validate, then write; a refused validation writes nothing | Yes: `LatticeTenantAccessAdminTests.AddAdminSubjectAsync_with_an_unresolvable_id_is_denied_before_any_write`, `LatticeAuthAdminValidationTests.UpsertGroupAsync_rejects_an_unresolvable_group_when_validation_is_required` and `LatticeAuthAdminTenantTierTests.UpsertGroupAsync_refuses_a_tenant_group_before_directory_validation`. |
| `FlipDeletesNothing` | A flip to off takes effect on the next decision and deletes nothing, so turning it back on restores the rules | Yes: `TenantAccessConformanceTests.Case10b_with_the_flag_off_a_pre_existing_tenant_rule_is_inert`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **Tenant-admin authority is a fixed grant.** Who is an admin of a tenant is the contract's constant grant set, not the admin set the model writes. Admin-set and member-set concurrency, the last-admin guard and removal stamps are the sibling module's concern.
  Production symbols: `TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync`, `TenantRecord.IsAdmin`.
- **Group deletion and its cascade.** The cascade that removes a deleted group's entries, rules and edges is not modelled here; its interleavings with concurrent removals belong to the sibling module.
  Production symbols: `LatticeTenantDirectoryAdmin.RemoveGroupAsync`.
- **Arriving operator rules naming a tenant group.** An arrival is drawn from records its source's store admitted. The snapshot compiler re-validates tenant-tier rules on arrival but not an operator rule's tenant group subject, so an operator rule written around every store would not be dropped (#6210).
  Production symbols: `CompiledTenantPartition.Build`, `TenantRuleConfinement.CheckTenantGroupSubject`.
- **App rules on app-owned trees.** The `app:` allowance for a tenant group on its tenant's app-owned trees is the installable-apps module's concern.
  Production symbols: `LatticeAppRuleIds.IsAppOwned`.
- **Arriving membership records.** Group records and edges that arrive by replication or restore are not modelled here; cross-tenant entries in replicated records are the sibling module's concern.
  Production symbols: `LatticeMembershipDirectory.AddMemberAsync`.
- **One-level directory expansion.** A claim resolution sees the subject's direct directory groups, not the transitive closure; the claim filter's verdict does not depend on depth.
  Production symbols: `LatticeMembershipDirectory.GroupsOfAsync`.
- **Snapshot rebuild on a flip.** The evaluator reads the flag live; the compiled snapshot's rebuild after a flip is not modelled.
  Production symbols: `CompiledPolicySnapshotMaintainer`.
