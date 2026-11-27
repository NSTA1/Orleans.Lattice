# Refinement note: AppsInstall

This maps the installable-app security seams (issue #6051) onto their
production code: the `AppInstall` gate on the apps control facade and on the
activation runner, the manifest digest pin on install, the role compiler, the
`app:` rule namespace in the authorization policy store, the alias
tree-ownership guard the registry grain consults, and the app UI bridge's
workspace authorization. A caller's own authority is the F1 contract's
abstract `Allowed(caller, op, scope)` (`spec/auth-decision/`, contract version
1): the cfg binds it to `ContractAllowed`, the contract's minimal grant-set
definition, which satisfies C1-C6 by construction. The model relies on no more
than "the caller holds `AppInstall` over the cluster-wide tree", which is what
`AppInstallAuthorizer.AuthorizeAsync` decides.

It is a design check with detectors, not a machine-checked refinement proof.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
| --- | --- | --- |
| `source` | The manifest the app source currently serves | The manifest `LatticeAppCatalog.DescribeFromSourceAsync` resolves, and `LatticeAppsControl.InstallAsync` resolves again at commit |
| `seen` | The manifest the operator reviewed | No production state: the history `InstalledEqualsReviewed` judges. |
| `pin` | The digest the review recorded | `AppDescriptor.ManifestDigest`, sent back as `AppInstallRequest.ExpectedManifestDigest` |
| `installed` | The installed manifest | The record `AppRegistry.InstallAsync` writes |
| `roleGrants` | The compiled grants of the app's role | The rules `AppRoleCompiler.Compile` emits |
| `rules` | The origin of the last write to each app-owned rule id | The rules `LatticeAuthorizationPolicyStore.PutRuleAsync` admits under `LatticeAppRuleIds.IsAppOwned` |
| `alias` | The target each alias name resolves to | The alias entry `LatticeRegistryGrain.SetAliasAsync` writes |
| `obs` | Each property's verdict on the last step | No production state: the response or decision the properties judge. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
| --- | --- | --- | --- |
| `Review` | The operator reads the manifest from its source and records its digest | `LatticeAppCatalog.DescribeFromSourceAsync`, `AppManifestDigest` | Yes: `LatticeAppsControlManifestPinTests.DescribeFromSourceAsync_reports_the_digest_the_install_pins_against`. |
| `ChangeManifest` | The source serves a different manifest after the review | `LatticeAppsControl.InstallAsync` resolving the manifest again at commit | Yes: `LatticeAppsControlManifestPinTests.InstallAsync_pinned_to_the_reviewed_digest_refuses_a_manifest_that_changed_since_review` and `LatticeAppsControlManifestPinTests.InstallAsync_pinned_to_the_reviewed_digest_refuses_an_upgrade_whose_manifest_changed`. |
| `Install` | `AppInstall` over the cluster-wide tree unless system origin, then the pinned digest compared before any write | `LatticeAppsControl.InstallAsync`, `AppInstallAuthorizer.AuthorizeAsync` | Yes: `LatticeAppsControlAuthorizationTests.Denied_caller_is_refused_before_any_engine_access`, `LatticeAppsControlAuthorizationTests.Key_filtered_allow_is_refused_before_any_engine_access`, `LatticeAppsControlAuthorizationTests.System_origin_caller_skips_the_gate`, `AppRegistryTests.Every_transition_authorizes_AppInstall_over_the_cluster_wide_sentinel` and `LatticeAppsControlManifestPinTests.InstallAsync_pinned_to_the_reviewed_digest_installs_an_unchanged_manifest`. |
| `CompileRoles` | A role ceiling entry is admitted only for a scoped capability on an ordinary data tree | `AppRoleCompiler.Compile`, `AppTreeIds.IsGrantable` | Yes: `AppRoleCompilerTests.Compile_never_emits_a_scopeless_capability_even_when_the_ceiling_allows_it`, `AppRoleCompilerTests.Compile_fails_an_over_requesting_role_with_the_excess_operation_bits_and_no_rules` and `AppTreeIdsTests.IsGrantable_admits_only_ordinary_data_trees`. |
| `ActivateRules` | Activation, gated on `AppInstall`, persists the app's rules under system origin | `AppActivationRunner.RunAsync` | Yes: `AppActivationRunnerTests.A_caller_without_AppInstall_is_denied_before_any_side_effect`, `AppActivationRunnerTests.A_system_origin_caller_skips_the_gate` and `AppActivationRunnerTests.An_authorized_caller_runs_the_engine`. |
| `WriteAppRule` | A put of an app-owned rule id is refused before any read or write unless system origin | `LatticeAuthorizationPolicyStore.PutRuleAsync`, `LatticeAppRuleIds.IsAppOwned` | Yes: `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.PutRuleAsync_rejects_an_operator_write_to_an_app_owned_rule_id` and `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.PutRuleAsync_admits_an_app_owned_rule_id_under_system_origin`. |
| `DeleteAppRule` | A delete of an app-owned rule id is refused before the store reads the rule unless system origin | `LatticeAuthorizationPolicyStore.RemoveRuleAsync`, `LatticeAppRuleIds.IsAppOwned` | Yes: `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.RemoveRuleAsync_rejects_an_operator_delete_of_an_app_owned_rule_id` and `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.RemoveRuleAsync_admits_an_app_owned_rule_id_under_system_origin`. |
| `SetAlias` | The guard compares the alias name's owner with the owner of the target's derivation root; only an allow writes, for every caller including system origin | `LatticeRegistryGrain.SetAliasAsync`, `AppTreeOwnershipGuard`, `NullTreeOwnershipGuard` | Yes: `AppTreeOwnershipGuardTests.Allows_a_derived_copy_of_the_owned_tree`, `AppTreeOwnershipGuardTests.Denies_an_alias_into_an_owned_tree_with_a_reason_naming_only_the_callers_ids`, `AppTreeOwnershipGuardTests.A_ledger_failure_propagates`, `LatticeRegistryGrainTests.SetAliasAsync_denied_ownership_writes_and_publishes_nothing_even_for_system_origin`, `LatticeRegistryGrainTests.SetAliasAsync_allow_receives_authoritative_derivation_then_writes_and_publishes`, `LatticeRegistryGrainTests.SetAliasAsync_default_decision_denies_an_unregistered_target` and `LatticeRegistryGrainTests.SetAliasAsync_guard_failure_propagates_without_writes_or_notifications`. |
| `BridgeAuthorize` | The install at the requested revision, then the declared tree, then the caller's role grant on it | `LatticeAppBridge.AuthorizeAsync` | Yes: `LatticeAppBridgeAuthorizationTests.A_revision_mismatch_is_indistinguishable_from_an_app_that_does_not_exist`, `LatticeAppBridgeAuthorizationTests.An_app_that_is_not_installed_is_denied`, `LatticeAppBridgeAuthorizationTests.An_undeclared_tree_is_not_found`, `LatticeAppBridgeAuthorizationTests.A_caller_holding_no_role_of_the_app_is_denied_even_a_read`, `LatticeAppBridgeAuthorizationTests.The_install_is_checked_before_the_tree_is_resolved` and `LatticeAppBridgeAuthorizationTests.The_tree_is_resolved_before_the_app_role_grant_is_checked`. |

## Property mapping

| Spec property | Code-level property it abstracts | Detector |
| --- | --- | --- |
| `InstallOnlyWithClusterWideAppInstall` | Every install, upgrade and activation needs `AppInstall` over the cluster-wide sentinel, never a key-filtered or tree-scoped allow, unless the caller is system origin | Yes: `LatticeAppsControlAuthorizationTests.Key_filtered_allow_is_refused_before_any_engine_access`, `AppRegistryTests.Every_transition_authorizes_AppInstall_over_the_cluster_wide_sentinel`, `AppRegistryTests.A_key_filtered_allow_does_not_authorize_AppInstall` and `AppActivationRunnerTests.A_caller_without_AppInstall_is_denied_before_any_side_effect`. |
| `InstalledEqualsReviewed` | A pinned install or upgrade refuses a manifest whose digest no longer matches the one the operator reviewed, before any write | Yes: `LatticeAppsControlManifestPinTests.InstallAsync_pinned_to_the_reviewed_digest_refuses_a_manifest_that_changed_since_review`, `LatticeAppsControlManifestPinTests.InstallAsync_pinned_to_the_reviewed_digest_refuses_an_upgrade_whose_manifest_changed` and `LatticeAppsControlManifestPinTests.DescribeFromSourceAsync_reports_the_digest_the_install_pins_against`. |
| `RoleCompilerNeverEmitsScopeless` | A compiled app role never carries a scopeless capability and never reaches a reserved or structural tree | Yes: `AppRoleCompilerTests.Compile_never_emits_a_scopeless_capability_even_when_the_ceiling_allows_it` and `AppTreeIdsTests.IsGrantable_admits_only_ordinary_data_trees`. |
| `AppRulesSystemOriginOnly` | Only system origin writes or deletes a rule in the `app:` namespace, and a refused delete is refused before the rule is read | Yes: `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.PutRuleAsync_rejects_an_operator_write_to_an_app_owned_rule_id`, `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.RemoveRuleAsync_rejects_an_operator_delete_of_an_app_owned_rule_id` and `LatticeAuthorizationPolicyStoreAppOwnedRuleTests.RemoveRuleAsync_admits_an_app_owned_rule_id_under_system_origin`. |
| `AliasOwnershipPreserved` | An alias never points an owned name at another owner's tree or an unowned name at an owned one, judged on the target's derivation root; a default decision or a guard fault writes and publishes nothing, even for system origin | Yes: `LatticeRegistryGrainTests.SetAliasAsync_denied_ownership_writes_and_publishes_nothing_even_for_system_origin`, `LatticeRegistryGrainTests.SetAliasAsync_default_decision_denies_an_unregistered_target`, `LatticeRegistryGrainTests.SetAliasAsync_guard_failure_propagates_without_writes_or_notifications` and `AppTreeOwnershipGuardTests.Allows_a_derived_copy_of_the_owned_tree`. |
| `WorkspaceNonDisclosure` | For a role member, a revision mismatch or an uninstalled app is denied exactly as an absent app, and a verb needs the role grant on the declared tree | Yes: `LatticeAppBridgeAuthorizationTests.A_revision_mismatch_is_indistinguishable_from_an_app_that_does_not_exist`, `LatticeAppBridgeAuthorizationTests.An_app_that_is_not_installed_is_denied` and `LatticeAppBridgeAuthorizationTests.A_caller_holding_no_role_of_the_app_is_denied_even_a_read`. |

## Excluded properties

| Spec property | Reason |
| --- | --- |
| `TypeOK` | Finite model-domain constraint. |

## Deliberate abstraction gaps

- **A bridge caller with no grant of the app.** Only a member of the app's role calls the bridge. A caller holding no role of the app is not modelled, so the order in which the bridge resolves the tree before it checks the role grant, and what that order tells such a caller about an installed app, is not judged here.
  Production symbols: `LatticeAppBridge.AuthorizeAsync`.
- **Swap shares the guard.** An alias swap consults the same ownership guard as an alias assignment and is not modelled separately.
  Production symbols: `LatticeRegistryGrain.SwapAliasAsync`.
- **One app, one rule.** One app with one role and one app-owned rule id are modelled; the decision is per app and per rule id, so a second app or rule adds no interleaving the properties judge.
  Production symbols: `LatticeAppRuleIds.IsAppOwned`, `AppRoleCompiler.Compile`.
- **Consent and the request limiter.** The bridge's tenant and anonymous refusal, its request limiter and the consent check are not modelled; each refuses before or independently of the install and grant checks.
  Production symbols: `LatticeAppBridge.AuthorizeAsync`.
- **Uninstall, disable and the registry's lifecycle.** Only install and upgrade are modelled; uninstall, disable and enable pass the same `AppInstall` gate.
  Production symbols: `AppRegistry.InstallAsync`, `AppInstallAuthorizer.AuthorizeAsync`.
