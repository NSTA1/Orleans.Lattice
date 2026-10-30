# Refinement note: schema policy and version-config identity, change and caching

## Model scope in one paragraph

One governed tree's schema setting, which is either its enforcement policy or
its version config: both live as one row of a reserved tree, keyed by the
governed tree id, behind a per-silo cache that holds a null sentinel for "no
setting". An admin on some silo commits a set or a clear, evicts its own silo's
entry and returns; the change then reaches every other silo and evicts its
entry there. A cache miss reads the durable row and installs it unless an
eviction overtook the read. A local write through a silo is judged by that
silo's cached setting before it reaches the WAL, and a rejected write changes
nothing. An uncompilable policy is refused before it is stored. The model
checks the contract the providers state once #5238 and #5247 are fixed:
eventual propagation to every silo, with the admin's own silo current before
the call returns, and no load installing a setting older than an eviction that
overtook it.

## Variable mapping

| Spec variable | Protocol role | Code counterpart |
|---------------|---------------|------------------|
| `stored`, `rev` | The durable setting row and a ghost count of committed changes. | `LatticeSchemaPolicyStore.SetPolicyAsync`, `LatticeSchemaPolicyStore.ClearPolicyAsync`, `LatticeSchemaVersionStore.SetConfigAsync`. |
| `admin`, `adminAt` | An admin call between its durable write and its return, and the silo serving it. | `LatticeSchemaAdmin.SetPolicyAsync`, `LatticeSchemaAdmin.ClearPolicyAsync`, `LatticeSchemaVersionAdmin.SetVersionConfigAsync`. |
| `cache`, `cacheRev` | Each silo's cached setting or null sentinel, and the change it was read at. | `LatticeSchemaPolicyProvider.GetCompiledPolicyAsync`, `LatticeSchemaVersionProvider.GetConfigAsync`. |
| `loading`, `loadVal`, `loadRev`, `overtaken` | A cache-miss read in flight, what it read, and whether an eviction overtook it. | `LatticeSchemaPolicyProvider.LoadAndCacheAsync`, `LatticeSchemaVersionProvider.LoadAndCacheAsync`, `SchemaTreeCacheExtensions.CacheIfSpaceAvailable`. |
| `pending` | A change not yet delivered to a silo's cache. | `LatticeSchemaPolicyProvider.OnMutationAsync`, `LatticeSchemaVersionProvider.OnMutationAsync`. |
| `data`, `commits`, `writes`, `judge`, `outcome` | Local writes, the setting that judged each, and their verdict. | `LatticeSchemaWriteInterceptor.OnWriteAsync`, `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. |

## Action mapping

| Spec action | Protocol step | Code counterpart | Detector |
|-------------|---------------|------------------|----------|
| `Change(s,p)` | Commit a set or clear of the setting row; the change is owed to every silo. | `LatticeSchemaAdmin.SetPolicyAsync`, `LatticeSchemaAdmin.ClearPolicyAsync`, `LatticeSchemaVersionAdmin.SetVersionConfigAsync`. | Yes: `LatticeSchemaAdminTests.SetPolicyAsync_delegates_and_invalidates_cache`, `LatticeSchemaAdminTests.ClearPolicyAsync_delegates_and_invalidates_cache`, `LatticeSchemaVersionAdminTests.SetVersionConfigAsync_writes_store_and_invalidates_cache`. |
| `ChangeRefused` | An uncompilable policy is refused before the durable write. | `LatticeSchemaPolicyStore.SetPolicyAsync`, `CompiledSchemaPolicy.Compile`. | Yes: `LatticeSchemaPolicyStoreTests.SetPolicyAsync_negative_max_byte_length_is_rejected_and_not_stored`. |
| `Acknowledge` | The admin evicts its own silo's entry before it returns. | `LatticeSchemaAdmin.SetPolicyAsync`, `LatticeSchemaVersionAdmin.ClearVersionConfigAsync`. | Yes: `LatticeSchemaAdminTests.SetPolicyAsync_delegates_and_invalidates_cache`, `LatticeSchemaVersionAdminTests.ClearVersionConfigAsync_removes_and_invalidates`. |
| `Propagate(s)` | The change reaches silo `s` and evicts its entry. | `LatticeSchemaPolicyProvider.OnMutationAsync`, `LatticeSchemaVersionProvider.OnMutationAsync`, `SchemaTreeCacheExtensions.InvalidateForMutation`. | Yes: `LatticeSchemaPolicyProviderTests.OnMutationAsync_policy_tree_write_evicts_affected_tree`, `LatticeSchemaVersionProviderTests.OnMutationAsync_version_tree_write_evicts_affected_tree`. |
| `BeginLoad(s)` | A cache miss reads the durable row. | `LatticeSchemaPolicyProvider.GetCompiledPolicyAsync`, `LatticeSchemaVersionProvider.GetConfigAsync`. | Yes: `LatticeSchemaPolicyProviderTests.Invalidate_forces_reload_on_next_get`, `LatticeSchemaVersionProviderTests.Invalidate_forces_reload_on_next_get`. |
| `InstallLoad(s)` | The read installs its result unless an eviction overtook it. | `LatticeSchemaPolicyProvider.LoadAndCacheAsync`, `LatticeSchemaVersionProvider.LoadAndCacheAsync`. | Yes: `LatticeSchemaPolicyProviderTests.GetCompiledPolicyAsync_caches_after_first_load`, `LatticeSchemaPolicyProviderTests.GetCompiledPolicyAsync_caches_null_sentinel_for_ungoverned_tree`, `LatticeSchemaVersionProviderTests.GetConfigAsync_caches_after_first_load`. |
| `Write(s,v)` | A local write is judged by the silo's cached setting before the WAL append. | `LatticeSchemaWriteInterceptor.OnWriteAsync`, `LatticeWriteInterceptorEnforcement.InterceptPointAsync`. | Yes: `LatticeSchemaWriteInterceptorTests.OnWriteAsync_valid_local_value_accepts`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_throws_violation`, `SchemaValidationDataApiTests.SetAsync_strict_json_rejection_preserves_typed_diagnostics_and_commits_nothing`. |
| `Stutter` | No protocol step. | No production counterpart. | Not applicable. |

## Property mapping

| Spec property | Production property | Detector |
|---------------|--------------------|----------|
| `StoredPolicyCompiles` | A policy that does not compile is never written to the reserved tree. | Yes: `LatticeSchemaPolicyStoreTests.SetPolicyAsync_negative_max_byte_length_is_rejected_and_not_stored`. |
| `CacheCurrentOrPending` | A silo's cached setting is current or an eviction for it is on its way; no overtaken load installs an older setting. | Yes: `LatticeSchemaPolicyProviderTests.OnMutationAsync_policy_tree_write_evicts_affected_tree`, `LatticeSchemaVersionProviderTests.OnMutationAsync_version_tree_write_evicts_affected_tree`, `LatticeSchemaPolicyProviderTests.Invalidate_forces_reload_on_next_get`. |
| `AdminSiloCurrentAtAck` | The admin's own silo has evicted its entry before the admin call returns. | Yes: `LatticeSchemaAdminTests.SetPolicyAsync_delegates_and_invalidates_cache`, `LatticeSchemaAdminTests.ClearPolicyAsync_delegates_and_invalidates_cache`, `LatticeSchemaVersionAdminTests.SetVersionConfigAsync_writes_store_and_invalidates_cache`. |
| `AcceptedWriteValid` | A local write is committed only when the judging setting accepts it. | Yes: `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_throws_violation`, `SchemaValidationDataApiTests.SetAsync_strict_json_rejection_preserves_typed_diagnostics_and_commits_nothing`. |
| `RejectLeavesNoMutation` | A rejected local write is thrown before the WAL append and changes no data. | Yes: `SchemaValidationDataApiTests.SetAsync_strict_json_rejection_preserves_typed_diagnostics_and_commits_nothing`, `LatticeSchemaWriteInterceptorTests.OnWriteAsync_invalid_local_value_persists_nothing_to_dlq`. |
| `StaleEventuallyEvicted` | Every silo's stale setting is eventually evicted or refreshed. | Yes: `LatticeSchemaPolicyProviderTests.OnMutationAsync_policy_tree_write_evicts_affected_tree`, `LatticeSchemaVersionProviderTests.OnMutationAsync_version_tree_write_evicts_affected_tree`. |

## Excluded properties

| Spec property | Reason |
|---------------|--------|
| `TypeOK` | Finite-domain model sanity, not a production representation. It retains its own paired experiment. |

<a id="property-classification"></a>

## Property classification (issue #2321)

TypeOK, StoredPolicyCompiles and CacheCurrentOrPending are state safety.
AdminSiloCurrentAtAck, AcceptedWriteValid and RejectLeavesNoMutation are
action safety. StaleEventuallyEvicted is temporal liveness under weak fairness
of change delivery to each silo and of the admin's return. The ThreeSilos
variant re-checks every state and action check with a third silo racing the
two; the temporal check is bounded-out there for the TLC liveness budget and is
retained on the base.

Every experiment is an anchored action perturbation:

| Mutation | Classification |
|----------|----------------|
| TypeOKLoadReadsUndeclared | Anchored action: BeginLoad. |
| StoredPolicyCompilesUncompilableStored | Anchored action: ChangeRefused. |
| CacheCurrentOrPendingOvertakenLoadInstalls | Anchored action: InstallLoad. |
| CacheCurrentOrPendingAdminSiloOnly | Anchored action: Change. |
| StaleEventuallyEvictedNeverDelivered | Anchored action: Propagate. |
| AdminSiloCurrentAtAckNoLocalEviction | Anchored action: Acknowledge. |
| AcceptedWriteValidUnchecked | Anchored action: Write. |
| RejectLeavesNoMutationAppendedFirst | Anchored action: Write. |

## Detectors

The detectors drive the real admins, stores, providers and the enforcement
interceptor. The provider and admin tests use substituted stores, so they
observe exactly the cache transitions the actions name: a durable write, the
local eviction, the change-feed eviction, a miss-driven load and its install.
`SchemaValidationDataApiTests` drives a strict JSON rejection through a real
cluster and data facade and reads back that nothing was committed.

## Defect mutations whose fixes have landed

CacheCurrentOrPendingAdminSiloOnly is the #5238 mechanism: a change reached
only the silo that served the admin call and the silo whose grain committed the
row, so every other silo kept its setting indefinitely.
CacheCurrentOrPendingOvertakenLoadInstalls is the #5247 race: a load that read
the row before a change installed its result after the eviction ran, leaving the
pre-change setting cached even on the committing silo. Both are fixed in the
schema-assurance integration bucket; this is integration-branch evidence, not a
claim the fixes are released.

## Deliberate abstraction gaps

- **One governed tree, one setting kind.** The model tracks a single governed
  tree and one abstract setting that stands for either the enforcement policy
  or the version config, which share the cache admission and eviction rules.
  Key isolation between governed trees, the reserved-tree short-circuit and the
  bounded cache admission are checked by unit detectors, not modelled.
  Production symbols: `SchemaTreeCacheExtensions.CacheIfSpaceAvailable`, `SchemaTreeCacheExtensions.InvalidateForMutation`.
- **Propagation bound.** Delivery of a change to a silo other than the admin's
  is an environment step under weak fairness. The model checks eventual
  eviction, not the stated time bound within which delivery happens; that bound
  is pinned by the propagation detectors of the fix.
  No production symbol: the gap states a premise of the model, not a production mechanism.
- **Concurrent writes judged by an overtaken load.** A write whose own miss-load
  an eviction overtook is still judged by that load's result; the model
  abstracts it as concurrent with the change and checks only what is cached.
  No production symbol: the gap states an abstraction of write concurrency, not a production mechanism.
- **Policy content.** Policies are three abstract values over a two-value
  domain. Rule semantics, JSON predicates and compilation are not modelled;
  only "compiles or not" is.
  No production symbol: the gap states a bound of the model, not a production mechanism.
- **Version-config admission rules.** Monotonic target advance, family changes
  and catalog validation of a version config are owned by the version-family
  model; here a version-config change is any committed change.
  No production symbol: the gap delegates to another model, not to a production mechanism.
