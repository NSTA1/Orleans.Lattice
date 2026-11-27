# Refinement note: TenantAccessMembership

This maps the concurrency half of delegated tenant-access administration
(epic #4154) onto its production seams: the tenant record's admin and member
sets, their removal through the tenant registry's compare-and-set loop, tenant
group deletion, removal stamps on silos whose clocks are skewed, and entries
that arrive by replication or restore around the facades. Authorization is
taken as decided: every modelled operation is one an admin of the tenant (or a
platform operator) has already been authorized for, which is what
`TenantRegionResidencyAuthorizer.AuthorizeTenantAdminAsync` decides and the
sibling module `TenantAccessAuthorization` checks.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `slots` | The tenant's admin-set and member-set slots: a presence bit and a stamp per entry | `TenantRecord.Subjects` and `TenantRecord.MemberSlots`, each slot a `TenantSubjectSlot` merged by `TenantSubjectSlot.Merge` |
| `clock` | Each silo's hybrid logical clock; one silo lags the other | `ITenantAdminClock.Next` on each silo |
| `groupExists` | Whether the tenant's group record exists | `ITenantDirectoryStore.GetGroupAsync`, removed by `ITenantDirectoryStore.RemoveGroupCascadeAsync` |
| `bindings` | The role bindings stored on an app installed in the tenant | `AppRegistryRecord.RoleBindings` |
| `pending` | A removal that has read the record and stamped its tombstone but not yet committed | The local `TenantRecord` a facade mutates between its read and `TenantAdminSetCommit.CommitAsync` |
| `removed` | The slots committed removals superseded, with the stamp each one read | No production state: the history `RemovalWinsUnderSkew` judges. |
| `budget` | The write budget | No production state: a bound on the checked instance. |
| `obs` | What the last step returned or decided | No production state: the facade's answer or the membership decision the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `BeginRemoveAdmin` | Read the record, the idempotent no-op, the last-admin pre-check, then a tombstone stamped later than the local clock and the slot | `LatticeTenantAccessAdmin.RemoveAdminSubjectAsync`, `TenantRemovalStamp.ForAdminEntry` | Yes: `LatticeTenantAccessAdminTests.RemoveAdminSubjectAsync_of_the_last_admin_subject_is_refused` and `LatticeTenantAccessAdminTests.RemoveAdminSubjectAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_subject`. |
| `BeginRemoveMember` | Read the record, the idempotent no-op, then a member-set tombstone stamped later than the local clock and the slot | `LatticeTenantDirectoryAdmin.RemoveMemberAsync`, `TenantRemovalStamp.ForMemberEntry` | Yes: `LatticeTenantDirectoryAdminTests.RemoveMemberAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_entry`. |
| `BeginDeleteGroup` | The group must exist, a group that is the last admin entry is refused, then the group's member-set and admin-set entries are tombstoned with superseding stamps | `LatticeTenantDirectoryAdmin.RemoveGroupAsync`, `LatticeTenantDirectoryAdmin.RemoveRegistryEntriesAsync` | Yes: `LatticeTenantDirectoryAdminTests.RemoveGroupAsync_refuses_to_remove_the_last_admin_entry_and_writes_nothing`, `LatticeTenantDirectoryAdminTests.RemoveGroupAsync_cascades_edges_member_and_admin_entries_and_tenant_rules` and `LatticeTenantDirectoryAdminTests.RemoveGroupAsync_from_a_silo_whose_clock_is_behind_the_slots_still_removes_the_entries`. |
| `Commit` | One committing attempt of the compare-and-set loop: re-read, merge the tombstones, re-apply the last-admin guard to the merged record before the conditional write; a refusal writes nothing | `TenantAdminSetCommit.CommitAsync`, `LatticeTenantRegistry.PutGuardedAsync` | Yes: `LatticeTenantRegistryConcurrencyTests.Guarded_put_validates_the_re_merged_record_and_refuses_a_racer_whose_change_would_empty_the_admin_set`, `LatticeTenantAccessAdminTests.A_racing_removal_against_a_guarded_registry_is_refused_inside_the_commit_with_no_repair_write` and `LatticeTenantAccessAdminTests.Two_concurrent_removals_of_different_subjects_cannot_orphan_the_tenant`. |
| `AddEntry` | An admin-set or member-set add of an admissible entry, stamped by the local clock and merged in one put | `LatticeTenantAccessAdmin.AddAdminSubjectAsync`, `LatticeTenantDirectoryAdmin.AddMemberAsync`, `TenantRecord.AddAdminSubject`, `TenantRecord.AddMemberSubject` | Yes: `LatticeTenantAccessAdminTests.AddAdminSubjectAsync_re_grants_a_previously_removed_subject` and `LatticeTenantAccessAdminTests.Concurrent_add_and_remove_on_two_replicas_converge_independently_of_merge_order`. |
| `ArriveEntry` | A tenant record arriving by replication or restore, carrying any entry, merged around the facades | `TenantRecord.MergeFrom` | Yes: `TenantAccessEntriesTests.Compile_never_admits_through_a_foreign_or_malformed_admin_entry` and `TenantAccessEntriesTests.TenantRecord_group_probes_ignore_another_tenants_group`. |
| `ArriveBinding` | An app registry record arriving with a role binding the install facade would refuse | `AppRegistryRecord.RoleBindings`, `AppActivationEngine` | Yes: `AppActivationEngineTests.A_stored_binding_to_another_tenants_group_fails_closed_and_withdraws_earlier_rules`. |
| `Count` | A membership decision: the exact-id probe screens group-shaped ids, the group-aware probes count only admissible entries, and role compilation fails on any mismatched binding | `TenantRecord.IsAdmin`, `TenantRecord.IsMember`, `TenantAccessEntries.IsAdmissible`, `TenantAccessEntries.IsGroupShapedSubject`, `AppRoleCompiler.Compile` | Yes: `TenantRegionResidencyAuthorizerGroupAwareTests.Another_tenants_group_in_the_admin_set_never_authorizes`, `TenantRegionResidencyAuthorizerTests.AuthorizeTenantAdminAsync_a_subject_id_shaped_like_a_tenant_group_never_matches_by_exact_id` and `AppRoleCompilerTests.A_binding_to_another_tenants_group_fails_compilation_with_the_typed_mismatch_and_emits_no_rules`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `LastAdminGuard` | No interleaving of admin-set removals and group deletions commits a record with an empty admin set, and a refused removal writes nothing | Yes: `LatticeTenantAccessAdminTests.Two_concurrent_removals_of_different_subjects_cannot_orphan_the_tenant`, `LatticeTenantAccessAdminTests.A_racing_removal_against_a_guarded_registry_is_refused_inside_the_commit_with_no_repair_write` and `LatticeTenantDirectoryAdminTests.RemoveGroupAsync_refuses_to_remove_the_last_admin_entry_and_writes_nothing`. |
| `RemovalWinsUnderSkew` | An admin-set or member-set removal, by the access facade or a group deletion, supersedes the slot it read even when that slot was stamped by a silo or peer whose clock runs ahead | Yes: `LatticeTenantAccessAdminTests.RemoveAdminSubjectAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_subject`, `LatticeTenantDirectoryAdminTests.RemoveMemberAsync_from_a_silo_whose_clock_is_behind_the_slot_still_removes_the_entry` and `LatticeTenantDirectoryAdminTests.RemoveGroupAsync_from_a_silo_whose_clock_is_behind_the_slots_still_removes_the_entries`. |
| `NoCrossTenantMembership` | Another tenant's group, or a malformed `t/` id, in an admin-set, member-set or role-binding entry never admits anyone, including in records that arrived by replication or restore | Yes: `TenantAccessEntriesTests.Compile_never_admits_a_member_of_another_tenants_group_listed_in_the_member_set`, `TenantAccessEntriesTests.TenantRecord_group_probes_ignore_another_tenants_group`, `AppRoleGrantEvaluatorTenantConfinementTests.CompileRoles_confers_no_grant_when_the_only_binding_is_another_tenants_group` and `TenantAccessConformanceTests.Case06_a_rule_naming_a_tenant_group_cannot_be_scoped_outside_its_tenant_by_anyone_and_an_app_binding_in_another_tenant_naming_it_is_refused`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **Which entries the guard counts.** The last-admin guard, in production and in the model, counts every live admin-set slot. An entry that admits nobody, such as another tenant's group that arrived by replication, therefore holds the set open, and the tenant can be left with no admin entry that admits anyone. The model states the guard as production implements it rather than as the delegated-access design intends.
  Production symbols: `TenantRecord.AdminSubjectCount`, `TenantAdminSetCommit.CommitAsync`, `LatticeTenantAccessAdmin.RemoveAdminSubjectAsync`, `LatticeTenantDirectoryAdmin.RemoveGroupAsync`.
- **One committing attempt per step.** The compare-and-set loop's retries on a version conflict, and its bounded attempt budget, are folded into one atomic commit; an exhausted budget writes nothing, which the model's refusal already covers.
  Production symbols: `LatticeTenantRegistry.PutGuardedAsync`, `TenantRegistryConcurrencyException`.
- **Host-supplied registries.** A registry without the guarded commit gets a best-effort fallback that re-grants its own entry when the set came out empty; only the built-in registry is modelled.
  Production symbols: `TenantAdminSetCommit.CommitAsync`, `IGuardedTenantRegistry`.
- **Ties on equal stamps.** Production breaks a tie between equal stamps by writer id; the model gives every tie to the incumbent slot, which is the outcome a removal can least afford.
  Production symbols: `TenantSubjectSlot.Merge`, `TenantClock.Supersedes`.
- **Arriving removals and whole-record replacement.** An arrival adds slots. Across regions the serialized record is resolved by last-writer-wins on the whole record, after each source applied its own guard; concurrent removals in two regions, and a restore of a record with fewer admins, are not modelled.
  No production symbol: whole-record resolution is a property of the replicated tree's declared mode, not of a tenant type.
- **Adds under skew.** An add stamped by a lagging silo can lose to a later-stamped tombstone; the response reports the merged set, so nothing is claimed that did not happen, and no property here judges it.
  Production symbols: `LatticeTenantAccessAdmin.AddAdminSubjectAsync`.
- **The rest of the group cascade.** A group deletion also removes its edges, the tenant rules that name it and finally the group record; only the registry entries and the record's existence are modelled, since only they interact with the last-admin guard.
  Production symbols: `ITenantDirectoryStore.RemoveGroupCascadeAsync`, `LatticeTenantDirectoryAdmin.RemoveGroupAsync`.
- **Group resolution.** A subject's resolved groups are drawn freely; how they are resolved, and the claim filter that strips asserted `t/` groups, are the sibling module's concern.
  Production symbols: `LatticeMembershipDirectory.GroupsOfAsync`.
