# Tools

The MCP server exposes its capabilities as **tools**, grouped into opt-in modules plus a `lattice_capabilities` and a `lattice_list_regions` meta-tool. Every tool is a thin adapter over the matching `Orleans.Lattice.Api.*` facade and is named `lattice_<group>_<verb>`. The server ships with no tools; each module is added explicitly.

## Opting in

```csharp verify
var services = new ServiceCollection();

services.AddLatticeMcp();
services.AddStateTools();
services.AddDataTools(enableWrites: true);
services.AddBackupTools(enableControl: true);
services.AddAuthTools(enableAdministration: true);
services.AddReplicationTools(enableControl: true);
services.AddTreeAdminTools(enableSchemaControl: true, enableLifecycle: true);
```

Each module registration is idempotent except `AddDataTools`, which is meant to be called once (a second call registers a second data tool group rather than replacing the first), and within a module the destructive verbs stay hidden unless the host opts them in:

| Module | Extension | Read/inspect verbs | Destructive verbs (opt-in flag) |
|---|---|---|---|
| State | `AddStateTools()` | always | none (read-only facade) |
| Data | `AddDataTools(enableWrites)` | always | writes, gated by `enableWrites` |
| Backup | `AddBackupTools(enableControl)` | always | backup and backup-set starts, restore starts, health/catalog rebuild/catalog scrub starts, operation cancel, revert, delete, and the deprecated create/restore aliases, gated by `enableControl` |
| Auth | `AddAuthTools(enableAdministration)` | always | group / membership / rule mutation, gated by `enableAdministration` |
| Replication | `AddReplicationTools(enableControl)` | always | enable / disable replication, gated by `enableControl` |
| TreeAdmin | `AddTreeAdminTools(enableSchemaControl, enableLifecycle)` | always | schema policy / version / remediation mutation, gated by `enableSchemaControl`; tree lifecycle, restore, bulk-load, WAL-move, orphaned-leaf repair, view, tag-index, compaction, and retention control, gated by `enableLifecycle` |
| Tenant self-awareness | `AddTenantSelfAwarenessTools()` | always (self-gates on tenancy) | none (read-only facade) |
| Tenant-admin | `AddTenantAdminTools(enableControl)` | none (its inspect tools - `lattice_tenant_region_status` and the [delegated tenant access](#delegated-tenant-access-tools) reads - are contributed only with `enableControl`) | tenant create / suspend / resume / delete / set-quotas, region authorize / set-residency, and the delegated tenant access writes, gated by `enableControl` |

Read tools carry `readOnlyHint = true`; destructive tools carry `destructiveHint = true` and `readOnlyHint = false`, so a well-behaved MCP client can surface the distinction to the operator. Enabling a destructive verb only advertises it - it stays subject to the same fail-closed access gate the facade enforces (see [Security](security.md)).

## Discovery

The `lattice_capabilities` meta-tool reports, for the authenticated caller, its resolved subject id, the connected cluster's cluster and service ids, and one entry per facade group saying whether the group is available - its tool module is registered on this server **and** the caller's effective permissions grant an operation the group covers - plus, on a remote head, the endpoint the group is served from. It reports groups, not individual tools; the session's tool list is the per-tool view. Discovery is permission-scoped: a group's tools are listed only to a caller holding an Allow grant for an operation the group covers (and only the tools the registered authorizer permits by name), so a caller never sees a group it holds no grant for. The scopeless `Telemetry` and `AppInstall` capabilities count only from a whole-tree grant written at cluster-wide scope (`LatticeScope.ClusterWide()`), so neither a tree-scoped rule carrying the `Telemetry` bit nor a key- or prefix-scoped rule on the cluster-wide tree id lists the telemetry group. The data group narrows the listing per tool: its mutating tools are listed only to a caller whose grants include a mutating data-plane operation (see the data tools below). Otherwise the filter is deliberately coarse - one grant lists the whole group - and the facade's access gate still authorizes every call per tree and per verb, so a listed tool can be refused for a tree, a verb, or a Deny rule the caller's grants do not cover. `lattice_capabilities` is offered to every authenticated caller and is the one tool the coarse authorizer does not gate; an unauthenticated session is offered no tools at all.

## Installable app tools

A host that registers [`Orleans.Lattice.Api.Mcp.Apps`](../lattice.api.mcp.apps/README.md) (`AddAppMcpTools()`) also advertises every enabled [installable app](../lattice.apps/README.md)'s tools on this endpoint. They are not named `lattice_<group>_<verb>`: each is namespaced by its app slug as `{slug}_{tool}`, for example `crm_find_contact`. They are added to an authenticated caller's session after the group tools, through the same per-session tool collection and the same coarse authorizer, and each is listed only to a caller that holds the tool's declared app role. A role is held by binding - the caller is a member of a group the install binds to that role - and the shared access gate can then only take it away, never confer it: the tool is withheld when, on each of the role's scopes, the gate refuses at least one of the role's operations, so an explicit deny on a bound member wins and the caller's other rights never list an app tool (see [the app tool surface](../lattice.api.mcp.apps/README.md#authorization) for the exact rule). An app tool whose name collides with a tool already in the session is skipped, so an app can never shadow a built-in or group tool, and an app whose slug is `lattice` or `repocontext` (the leading segments of the built-in tool namespaces) contributes no tools at all. App tools never appear in the `lattice_capabilities` report, which describes facade groups only, and with the package unregistered the tool list is unchanged.

## Region targeting

A single MCP server can front more than one region (the current cluster plus configured, reachable peers - see [Remote host](remote.md)). Two additive surfaces expose this:

- **`lattice_list_regions`** - a read-only meta-tool that lists the regions the server can route a call to, current region first, each with its region id, cluster id, and per-group endpoint availability. Because it discloses peer topology, it is gated like a group tool rather than riding along with `lattice_capabilities`: it is advertised only to a caller holding at least one facade-group grant, and only when the registered authorizer permits it by name. A region that does not serve a group is reported unavailable for it and rejected fail-closed when a call targets it for that group; only the regions configured on this server are listed, and with `VerifyRegionIdentity` on, a peer whose endpoint is unreachable or answers as a different cluster than the one it advertises is omitted (fail-closed discovery), while a peer the probe cannot check - for example one with no advertised `ClusterId` or no `State` endpoint - stays listed (see [Remote hosting](remote.md#options)). The tool is projected from the shared `Orleans.Lattice.Api.Region.ILatticeRegionCatalog` contract, so a client reads the same region model the facade layer exposes.
- **An optional `region` argument on every facade-group tool** (the two meta-tools take none) - pass a listed region id to route that single call to the named region; omit it to target the current region. Omitting it is byte-for-byte identical to a region-unaware call. The result is annotated with the region it was served from (in the result's `_meta.region`) whenever a `region` was supplied.

Region targeting is fail-closed at both ends. Targeting an unknown region, or a region that does not serve the tool's group, returns a clean typed fault that points the caller at `lattice_list_regions` - never a leaked exception. A cross-region call forwards the **same** caller credential to the target region, so the target authorizes it independently: a caller lacking rights in the target region is denied there. A region is never an authorization bypass.

A tool call targeting a named region passes the region id as the optional `region` argument:

```jsonc
// lattice_data_get, explicitly targeting the "us-east" region.
{
  "treeId": "orders",
  "key": "order-42",
  "region": "us-east"
}
```

The result carries the served region in its `_meta.region` field. Omit `region` to target the current region; the call and its result are then identical to a region-unaware binding. Call `lattice_list_regions` (no arguments) first to discover the routable region ids.

### Tenant-scoped region discovery

On a cluster running the tenancy add-on, what `lattice_list_regions` returns depends on whether the call asserts an active tenant (the `lattice-active-tenant` header - see [Security](security.md#3a-the-active-tenant-bridge)):

- **No tenant asserted** (an operator, or any caller on a non-tenancy cluster) - the full routing topology, unannotated and byte-for-byte as before. The reserved `default` tenant is treated the same way, and so is any call to a head that cannot resolve tenant standing: a non-tenancy cluster, or a remote head without the `TenantAdmin` endpoint (see [Remote hosting](remote.md#region-targeting-interacts-with-tenant-residency)).
- **A non-default tenant assertion the caller may not act as** - refused by the head's `ITenantContextResolver` (the seam the tenancy add-on registers to validate an assertion against the caller's own membership), resolved to a different tenant, or made to a head with no validating resolver - the current region alone, with no `tenantScope` annotation, so the asserted id is never echoed back. The tenant's standing is never looked up.
- **A validated non-default tenant** - the current region plus only those peers in the tenant's **actionable set**: the regions its operator has authorized it into, plus the regions it is resident in. Each entry gains an additive `tenantScope` object reporting `tenantId`, `isAllowed`, `status`, and `isResident`. The current region is always listed (the caller is already talking to it) and is annotated truthfully, which may say the tenant is neither allowed into nor resident in it.
- **A validated tenant whose standing the head's tenancy resolver cannot establish** - the current region alone, fail-closed. It never falls back to the full topology.

In the shipped registrations the two validated-tenant cases are not reached. The discovery tool runs without the caller's credential, so on a co-hosted head the validating resolver sees an anonymous caller and refuses every non-default assertion, and a remote head registers no validating resolver at all. A non-default tenant assertion is therefore currently answered with the current region alone and no `tenantScope` annotation - or, by a remote head without the `TenantAdmin` endpoint, with the full unscoped topology.

A region reported with `isResident: false` is a legitimate `lattice_tenant_set_residency` destination but **not** yet a routing destination: targeting it with a `region` argument is refused by the residency gate until its status reaches `Online`. Receiver replication is admitted while it is `Backfilling`; the local lifecycle driver makes it routable only after bootstrap and parked tenant-offline entries are verified complete (see [Tenant region residency](#tenant-region-residency-lattice_tenant_authorize_regions-lattice_tenant_set_residency-lattice_tenant_region_status)). See [the region sets](../lattice.tenancy/README.md#the-region-sets).

## State tools (`lattice_state_*`)

Read-only introspection over `ILatticeStateQuery`. Registered by `AddStateTools()`.

| Tool | Purpose |
|---|---|
| `lattice_state_get_cluster_info` | The connected cluster's identity (Orleans cluster and service ids). |
| `lattice_state_list_trees` | Paged catalog of registered trees. |
| `lattice_state_list_views` | Paged catalog of materialised views. |
| `lattice_state_list_tag_indexes` | Tag indexes defined on the cluster. |
| `lattice_state_list_tag_values` | Distinct tag values one tag index carries over one subject tree. |
| `lattice_state_list_covered_trees` | Trees covered by a tag index. |
| `lattice_state_list_index_tags` | Distinct tag values one tag index carries across every tree it covers. |
| `lattice_state_scan_tag_members` | Members matching a tag value. |
| `lattice_state_get_tree_summary` | Summary of one tree. |
| `lattice_state_get_shard_summaries` | Per-shard summaries for a tree. |
| `lattice_state_get_physical_shard_count` | Physical shard count for a tree. |
| `lattice_state_get_tree_structure` | Depth-bounded shard-root node graph. |
| `lattice_state_scan_entries` | Key-ordered entry page; the optional `mode` selects the cursor (`Snapshot`, the default, or the cheaper `Live` / `LivePointInTime`). |
| `lattice_state_get_entry` | One key's full record. |
| `lattice_state_get_entry_history` | Version history for a key. |
| `lattice_state_cancel_scan` | Cancel an in-flight scan. |

## Data tools (`lattice_data_*`)

Read/write access over `ILatticeDataApi`. Registered by `AddDataTools(enableWrites)`. The read tools - the two point / range reads plus the thirteen typed-CRDT reads - are always exposed; the write tools (the six point / batch writes plus the thirteen typed-CRDT writes) require `enableWrites: true`. Discovery then applies a per-tool minimum inside the group: the read tools are listed to any caller the data group admits, while each write tool is listed only to a caller whose Allow grants include at least one of `Write`, `Delete`, `RangeDelete`, `CrdtApply`, `AtomicWrite`, or `BulkLoad`, so a caller holding only `Read` / `RangeRead` is offered the reads alone and cannot invoke a write it was not offered.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_data_get` | read | Fetch a single key. |
| `lattice_data_read_range` | read | Read a key range. Only `treeId` is required; the range bounds, page size, and continuation token are optional (omit them for a full, unbounded first page). |
| `lattice_data_set` | write | Set a single key. |
| `lattice_data_delete` | write | Delete a single key. |
| `lattice_data_delete_range` | write | Delete every key in a half-open `[startInclusive, endExclusive)` range, returning `deletedCount`. Both bounds are required. The drain reopens transparently across a transient enumerator loss so a large range completes; authorization is all-or-nothing across the span. |
| `lattice_data_set_many` | write | Non-atomic single-tree batch: apply each key independently (best-effort, per-key authorized). |
| `lattice_data_set_many_atomic` | write | Atomic single-tree batch. |
| `lattice_data_set_many_atomic_cross_tree` | write | Atomic cross-tree batch. |

### Typed CRDT tools

These surface the replicated CRDT primitives directly, so a caller reads and writes a value's convergent type without hand-encoding CRDT state. Element and value bytes are base64-encoded. Every write except the G-Set add and the Max- / Min-Register sets names its writer with a `replicaId`. The writes that offer more than one operation (PN-Counter, OR-Set, OR-Flag, RW-Flag, RW-Set, Sequence, and OR-Map) take an `operation` discriminator with the values shown in parentheses below; the remaining writes perform one fixed operation and take no discriminator. Each type also has a paired read. See [CRDT primitives](../crdt/readme.md) for the merge rules summarised below.

| Type | Write tool | Read tool | Merge rule |
|---|---|---|---|
| PN-Counter | `lattice_data_pncounter` (increment / decrement) | `lattice_data_pncounter_get` | Per-replica signed sum. |
| G-Counter | `lattice_data_gcounter` (increment only) | `lattice_data_gcounter_get` | Per-replica grow-only sum. |
| OR-Set | `lattice_data_orset` (add / remove) | `lattice_data_orset_get` | Add-wins, observed-remove. |
| OR-Flag | `lattice_data_orflag` (enable / disable) | `lattice_data_orflag_get` | Enable-wins. |
| RW-Flag | `lattice_data_rwflag` (enable / disable) | `lattice_data_rwflag_get` | Disable-wins. |
| RW-Set | `lattice_data_rwset` (add / remove) | `lattice_data_rwset_get` | Remove-wins observed set. |
| Version Vector | `lattice_data_version_vector_tick` | `lattice_data_version_vector_get` | Per-replica max clock. |
| MV-Register | `lattice_data_mvregister_set` | `lattice_data_mvregister_get` | Keep concurrent values. |
| Max-Register | `lattice_data_maxregister_set` | `lattice_data_maxregister_get` | Keep the greatest observed value. |
| Min-Register | `lattice_data_minregister_set` | `lattice_data_minregister_get` | Keep the least observed value. |
| Sequence | `lattice_data_sequence` (insertAt / removeAt) | `lattice_data_sequence_get` | Ordered insert / tombstone. |
| OR-Map | `lattice_data_ormap` (set / remove) | `lattice_data_ormap_get` | Recursive per-key merge. |
| G-Set | `lattice_data_gset` (add only) | `lattice_data_gset_get` | Grow-only set. |

The OR-Map tools operate on an `OrMap<string, MvRegister>` (string field keys; each field value a multi-value register of base64 bytes). The host must register that shape for the target tree name at silo startup (`AddOrMapShape<string, MvRegister>(treeName)`); no MCP tool registers one. On a tree without it, `lattice_data_ormap` is rejected with a caller error naming the missing registration, while `lattice_data_ormap_get` still returns an empty map - so an empty read does not mean the map is writable.

## Backup tools (`lattice_backup_*`)

Backup control over `ILatticeBackupControl` and `ILatticeBackupOperations`. Registered by `AddBackupTools(enableControl)`. The read-only tools are always exposed; the control tools require `enableControl: true`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_backup_list` | inspect | Paged, read-filtered catalog page. |
| `lattice_backup_describe` | inspect | A manifest and its restore chain. |
| `lattice_backup_inventory` | inspect | Catalog-wide inventory summary. |
| `lattice_backup_scope_status` | inspect | A scope's schedule and last-run status. |
| `lattice_backup_export_artifact` | inspect | Export one bounded, base64-encoded page of a backup artifact's bytes, resumed from `chunkOffset` until `endOfStream`. |
| `lattice_backup_operation_status` | inspect | Read a tracked backup or restore operation by operation id; returns `found` plus the operation view when visible. |
| `lattice_backup_operation_list` | inspect | Page the caller's tracked backup and restore operations newest-first. |
| `lattice_backup_start` | control | Start a tracked full backup and return `{ operationId, kind, treeIds, created, statusTool }`. |
| `lattice_backup_start_incremental` | control | Start a tracked incremental backup layered on a base backup and return an operation handle. |
| `lattice_backup_start_set` | control | Start a tracked backup set over `treeIds` and return an operation handle. |
| `lattice_backup_start_restore` | control | Start a tracked restore and return an operation handle; a succeeded status includes `restoreResult` for `lattice_backup_revert_restore`. |
| `lattice_backup_start_health_check` | control | Start a tracked health check of one backup against the sink and return an operation handle; progress counts `artifacts`, the verdict is the `healthStatus` result key, and the fresh report is persisted as the backup's latest health state. |
| `lattice_backup_start_catalog_rebuild` | control | Start a tracked rebuild of the backup catalog from the sink and return an operation handle; needs the restore grant over the backup catalog. |
| `lattice_backup_start_catalog_scrub` | control | Start a tracked scrub of the backup catalog against the sink, pruning orphans when `pruneOrphans` is true, and return an operation handle; needs the restore grant over the backup catalog. |
| `lattice_backup_operation_cancel` | control | Request cancellation of a tracked backup or restore operation. |
| `lattice_backup_revert_restore` | control | Undo a shadow-cutover restore from a prior restore result. |
| `lattice_backup_delete` | control | Delete a backup and its unshared artifacts. |

The operation view returned by `lattice_backup_operation_status`, `lattice_backup_operation_list`, and `lattice_backup_operation_cancel` includes `operationId`, `kind`, `treeIds`, `state`, `phase`, `phaseIndex`, `phaseCount`, `completedUnits`, `totalUnits`, `unitName`, start and finish timestamps, `failureReason`, `resultReference`, the `result` map, `restoreResult` for a succeeded restore, and `cancelRequested`. Without control enabled the backup group exposes 7 tools; with control enabled it exposes 17.

## Auth tools (`lattice_auth_*`)

Authorization administration over `ILatticeAuthAdmin`. Registered by `AddAuthTools(enableAdministration)`. The introspection tools are always exposed; the mutating administration verbs require `enableAdministration: true`, and remain administrator-gated by the facade regardless.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_auth_explain` | inspect | Explain an authorization decision. |
| `lattice_auth_effective_permissions` | inspect | A subject's effective permissions. |
| `lattice_auth_get_group` | inspect | Get a group. |
| `lattice_auth_list_groups` | inspect | List groups. |
| `lattice_auth_list_group_members` | inspect | List a group's members. |
| `lattice_auth_list_subject_groups` | inspect | List the groups a subject belongs to. |
| `lattice_auth_get_rule` | inspect | Get an authorization rule. |
| `lattice_auth_list_rules` | inspect | List all rules. |
| `lattice_auth_list_rules_for_tree` | inspect | List rules for a tree. |
| `lattice_auth_upsert_group` | admin | Create or replace a group. |
| `lattice_auth_remove_group` | admin | Remove a group. |
| `lattice_auth_add_member` | admin | Add a group member. |
| `lattice_auth_remove_member` | admin | Remove a group member. |
| `lattice_auth_put_rule` | admin | Create or replace a rule. |
| `lattice_auth_remove_rule` | admin | Remove a rule. |

`lattice_auth_list_groups` lists cluster groups only. Pass `includeTenantGroups: true` to also list every tenant's tenant groups (the ids in the reserved `t/{tenant}/{name}` grammar that tenant administrators manage through the [delegated tenant access tools](#delegated-tenant-access-tools)); the filter applies before the page is cut. `lattice_auth_upsert_group` refuses an id starting with `t/`, and `lattice_auth_put_rule` refuses a rule id starting with `tenant:` or a tenant-wide `t/{tenant}/*` scope: those belong to the tenant tier. `lattice_auth_remove_rule` can still remove a tenant-tier rule, as a break-glass action. See [Delegated tenant access administration](../lattice.tenancy/README.md#delegated-tenant-access-administration).

`lattice_auth_explain` and `lattice_auth_effective_permissions` take an optional `subjectKind` argument (`User` by default). Set it to `Group` when `subjectId` names a group, so the tool resolves the group's rule closure instead of treating the id as a user; otherwise a group subject matches no rules and the decision falls through to the tree's default effect.

Both `lattice_auth_explain` and `lattice_auth_effective_permissions` also report the cluster's authorization posture (whether the all-trees grant tier and access-administration delegation are enabled). This is the discovery path for the posture - `lattice_capabilities` does not carry it - so an agent can tell whether a cluster-wide `Tree:*` grant is actually enforced and whether a policy-tree delegation rule is authorable. Consistent with that posture, `lattice_auth_put_rule` rejects a `Tree:*` data-plane rule while the all-trees grant tier is off, and a whole-tree `Admin` rule on the reserved policy tree while access-administration delegation is off.

## Replication tools (`lattice_replication_*`)

Runtime per-tree cross-cluster replication control over `ILatticeReplicationControl`. Registered by `AddReplicationTools(enableControl)`. The inspect tool is always exposed; the mutating control tools require `enableControl: true`, and remain subject to the facade's fail-closed replication access gate regardless. The module is served under both topologies: in-silo, and out-of-silo via `AddLatticeMcpRemote(o => { o.Replication = ...; o.EnableReplicationControl = ...; })` over the replication-API gRPC client (see [Remote hosting](remote.md)).

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_replication_get_config` | inspect | Report each authorized tree's enrolled state, the merge mode in force, its ambiguity status, and which enrollment source (`Runtime`, `Static`, or `RuntimeAndStatic`) put it in force. |
| `lattice_replication_enable` | control | Enable replication for a tree under a fixed merge mode. |
| `lattice_replication_disable` | control | Disable replication for a tree without purging already-replicated peer data; a shipper already active for the tree is not stopped. |

The control tools carry `destructiveHint = true`; the inspect tool carries `readOnlyHint = true`. Discovery is permission-scoped by the `LatticeOperation.Replication` grant, so a caller without that grant is not shown the group.

`lattice_replication_get_config` reconciles **both** enrollment sources a replication-enabled host resolves against: trees enabled at runtime through `lattice_replication_enable`, and trees declared in the static deployment-time replicated-tree map. Each entry's `source` says which one is in force, so an estate configured purely at deployment time reports its trees rather than an empty set. That static map always holds the `sys-replication-config` tree itself, which `enableRuntimeConfig: true` enrols under `OrMap`, so the report lists that tree as `Static` to any caller authorized to manage it. A tree reported `Static` keeps shipping even after `lattice_replication_disable` - the static map is a floor - and is turned off by editing the deployment configuration instead. See [Runtime replication configuration](../lattice.replication/runtime-config.md).

## TreeAdmin schema tools (`lattice_treeadmin_schema_*`)

Schema-management control over `ILatticeSchemaControl` and `ILatticeSchemaOperations`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools(enableSchemaControl)`. The read-only schema-inspection tools are always exposed; the mutating schema-management tools require `enableSchemaControl: true`, and every tool remains subject to the facade's own fail-closed schema access gate regardless (a read authorizes on ordinary read authority; a mutation authorizes on schema-management authority). The group is discovered by a caller granted any one of `LatticeOperation.Admin`, `TreeLifecycle`, `BulkLoad`, or `Restore`; each tool is still authorized by the facade at call time.

The MCP group holds the `ILatticeSchemaControl` and `ILatticeSchemaOperations` facades and delegates to them verbatim - it adds no method to the tree-administration facade and no authorization path of its own. The schema facade and its packages are unchanged.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_schema_get_policy` | inspect | Read a tree's enforcement policy, or none when unset. |
| `lattice_treeadmin_schema_list_dead_letters` | inspect | Stream a tree's strict-mode dead-letter entries. |
| `lattice_treeadmin_schema_count_dead_letters` | inspect | Count a tree's strict-mode dead-letter entries. |
| `lattice_treeadmin_schema_get_version_config` | inspect | Read a tree's envelope-version config, or none when unversioned. |
| `lattice_treeadmin_schema_get_remediation_status` | inspect | Read a tree's current or last-known remediation status. |
| `lattice_treeadmin_schema_probe_capabilities` | inspect | Probe which schema operations the caller may perform, side-effect free. |
| `lattice_treeadmin_schema_set_policy` | manage | Set or replace a tree's enforcement policy. |
| `lattice_treeadmin_schema_clear_policy` | manage | Clear a tree's enforcement policy. |
| `lattice_treeadmin_schema_set_version_config` | manage | Opt a tree in to envelope versioning (or replace its config). |
| `lattice_treeadmin_schema_clear_version_config` | manage | Opt a tree back out of envelope versioning. |
| `lattice_treeadmin_schema_advance_target_version` | manage | Advance a tree's target schema version. |
| `lattice_treeadmin_schema_remediation_start` | manage | Start a tracked remediation: every value rewritten by a transform and checked against a target policy, then the tree cut over. Returns an operation handle at once. |
| `lattice_treeadmin_schema_migration_start` | manage | Start a tracked eager migration of every value to the tree's current target version. Returns an operation handle at once. |
| `lattice_treeadmin_schema_advance_and_migrate_start` | manage | Start a tracked advance of the target version, then an eager migration to it. Returns an operation handle at once. |
| `lattice_treeadmin_schema_operation_status` | inspect | Read a schema operation's state, phase, values processed of total, and its result map. |
| `lattice_treeadmin_schema_operation_list` | inspect | List the caller's schema operations, newest-first. |
| `lattice_treeadmin_schema_operation_cancel` | manage | Request cancellation of a schema operation; it takes effect only before cutover. |

The manage tools carry `destructiveHint = true` and `readOnlyHint = false`, except `lattice_treeadmin_schema_operation_cancel`, which only stops a run before cutover and so carries `destructiveHint = false`; the inspect tools carry `readOnlyHint = true`. The status and list tools are always exposed; the start and cancel tools require `enableSchemaControl: true`. `lattice_treeadmin_schema_set_version_config` takes the version config as scalar `schemaId` / `targetVersion` / `strictIngest` arguments; `lattice_treeadmin_schema_set_policy` and `lattice_treeadmin_schema_remediation_start` take the schema policy and value-transform model objects directly.

Every start tool takes an optional `operationId`: starting again with an id in use returns the existing operation with `created = false`, so a retried start is safe. Poll `lattice_treeadmin_schema_operation_status` with the handle's `operationId` until the state is terminal. The operation kinds, phases and result keys are described in [Schema operations](../lattice.api.schema/operations.md#remediation-and-migration).

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeSchemaControl` and `ILatticeSchemaOperations` facades directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires one schema-API gRPC adapter, `GrpcLatticeSchemaControl`, serving both facades, off the same endpoint as the tree-administration group (`LatticeApiMcpRemoteOptions.TreeAdmin`, since the schema-API and tree-administration gRPC services are co-hosted on the same silo address). The remote host honours the same read-always / write-gated split: the read-only schema-inspection tools are served whenever the tree-administration endpoint is configured, and the mutating schema-management tools additionally require `LatticeApiMcpRemoteOptions.EnableSchemaControl = true` (which maps onto `enableSchemaControl`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## TreeAdmin diagnostics tools (`lattice_treeadmin_*`)

Read-only administrative diagnostics and storage accounting over `ILatticeTreeAdmin`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools` and always exposed (no opt-in flag). Each tool wraps the existing public grain surface (`ILattice`, `ILatticeAdmin`) rather than re-implementing shard fan-out, and every tool remains subject to the facade's own fail-closed access gate: the per-tree verbs authorize on whole-tree `LatticeOperation.Read` authority, and `lattice_treeadmin_storage_usage` authorizes on the distinct cluster-wide `LatticeOperation.Telemetry` capability. The group is discovered by a caller granted any one of `LatticeOperation.Admin`, `TreeLifecycle`, `BulkLoad`, or `Restore`; each tool is still authorized by the facade at call time.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_shard_hotness` | inspect | Read a tree's per-shard read/write hotness with tree-level totals. |
| `lattice_treeadmin_shard_diagnostics` | inspect | Read a whole-tree diagnostic report. Both modes walk every shard's leaf chain: the default counts live keys only (its tombstone counts read zero), and the `deep` flag also counts tombstoned and expired entries. |
| `lattice_treeadmin_shard_map_inspect` | inspect | Inspect a tree's shard-map topology (physical tree id, virtual/physical shard counts, map version). |
| `lattice_treeadmin_projection_digest` | inspect | Read a single shard's leaf-projection content digest for cheap divergence detection. |
| `lattice_treeadmin_tree_stats` | inspect | Read a tree's rolled-up topology, live-key counts, and storage byte breakdown in one call. |
| `lattice_treeadmin_storage_usage` | inspect | Read cluster-wide storage accounting. By default each tree's figures come from its short-lived storage-usage cache (`LatticeOptions.StorageUsageCacheTtl`), refilled from each shard root's maintained byte totals and each WAL partition without walking the leaf chain; the `deep` flag forces a fresh leaf-walk that re-measures every shard, in one call - prefer `lattice_treeadmin_storage_usage_refresh_start`, which re-measures in the background with progress. |

Every tool carries `readOnlyHint = true` and `destructiveHint = false`. `lattice_treeadmin_shard_diagnostics` and `lattice_treeadmin_storage_usage` take an optional `deep` flag (default `false`, the cheap path); `lattice_treeadmin_projection_digest` takes a `treeId` and a non-negative `shardIndex`; the remaining per-tree tools take a `treeId`. `lattice_treeadmin_storage_usage` is cluster-wide and takes no tree id.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeTreeAdmin` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires a tree-administration-API gRPC adapter off the `LatticeApiMcpRemoteOptions.TreeAdmin` endpoint. Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## TreeAdmin operation tools (`lattice_treeadmin_*`)

Accept-then-poll compliance scans and fresh storage-usage refreshes, on the shared [long-running operation contract](../lattice.api.abstractions/operations.md), surfaced under the tree-administration group and always exposed (no opt-in flag). A start tool returns a handle naming the operation id and the status tool to poll; the status, list and cancel tools are scoped by the facade to the tool's own kind, the caller's tenant and what the caller may read, and report `found = false` for an operation the caller may not see.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_schema_compliance_scan_start` | operate | Start a tracked compliance scan of a tree (`ILatticeSchemaComplianceOperations`); needs read over the tree. |
| `lattice_treeadmin_schema_compliance_scan_status` | inspect | Read a compliance scan's state, phase, entries scanned of total, and on success the report in the result map. |
| `lattice_treeadmin_schema_compliance_scan_list` | inspect | List the caller's compliance scans, newest-first. |
| `lattice_treeadmin_schema_compliance_scan_cancel` | operate | Request cancellation of a compliance scan. |
| `lattice_treeadmin_storage_usage_refresh_start` | operate | Start a tracked deep re-measure of every tree's storage usage (`ILatticeStorageUsageOperations`); needs cluster telemetry. |
| `lattice_treeadmin_storage_usage_refresh_status` | inspect | Read a refresh's state, trees measured of total, and on success the cluster totals in the result map. |
| `lattice_treeadmin_storage_usage_refresh_list` | inspect | List the caller's refreshes, newest-first. |
| `lattice_treeadmin_storage_usage_refresh_cancel` | operate | Request cancellation of a refresh. |

The inspect tools carry `readOnlyHint = true`; the operate tools record or stop an operation, so they carry `readOnlyHint = false`, but never mutate data, so every tool carries `destructiveHint = false`. Each start takes an optional `operationId` that makes it idempotent. Over the remote topology `AddLatticeMcpRemote` wires gRPC adapters for both surfaces off the `LatticeApiMcpRemoteOptions.TreeAdmin` endpoint. See [Schema compliance operations](../lattice.api.schema/operations.md) and [Storage usage operations](../lattice.api.treeadmin/operations.md) for the phases, units and result keys.

## TreeAdmin lifecycle and control tools (`lattice_treeadmin_*`)

Explicit tree lifecycle, per-tree registry configuration, bulk-load, restore, WAL placement, view, tag-index, compaction, and retention operations over `ILatticeTreeAdmin`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools(enableLifecycle: true)`. The read-only lifecycle/control tools are always exposed; the mutating lifecycle/control tools require `enableLifecycle: true`. Each tool delegates to the tree-administration facade instead of re-implementing registry or shard fan-out behaviour, and every tool remains subject to the facade's own fail-closed access gate. The group is advertised to callers whose effective permissions include one of the tree-administration group capabilities (`Admin`, `TreeLifecycle`, `BulkLoad`, or `Restore`); individual verbs are still authorized by the facade at call time. Registration is idempotent under matching parameters, and reserved system tree ids in the `_lattice_` namespace are rejected for mutating verbs.

### Tree lifecycle and registry

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_tree_exists` | read | Report whether a tree is registered. |
| `lattice_treeadmin_tree_resolve_alias` | read | Resolve the physical tree a logical tree maps to. |
| `lattice_treeadmin_tree_get_config` | read | Read a tree's registry-backed configuration (sizing, alias, per-tree overrides). |
| `lattice_treeadmin_tree_get_shard_map` | read | Read a tree's registry-persisted shard map (custom-map flag, version, virtual/physical shard counts). |
| `lattice_treeadmin_tree_deletion_status` | read | Read a tree's soft-deletion state, recovery window, and purge status - including, while a purge runs, `purgeInProgress` with `purgedShardCount` of `purgeShardCount` shards done. Answers without waiting for a shard's purge. |
| `lattice_treeadmin_tree_reshard_status` | read | Read the current online-reshard state and shard-map fan-out, with the running reshard's target and starting shard counts to measure its progress against. |
| `lattice_treeadmin_tree_resize_status` | read | Read the current online-resize state - running, an accepted undo still unwinding (`undoRequested`), or none - and effective B+ node capacities, with the phase and the completed and total work units of a running resize. |
| `lattice_treeadmin_tree_snapshot_status` | read | Read whether a point-in-time snapshot capture is in flight for a tree and, while one runs, its phase and how many of its shards are copied. |
| `lattice_treeadmin_tree_create` | manage | Explicitly create or register a tree with optional initial sizing. |
| `lattice_treeadmin_tree_set_alias` | manage | Point a logical tree at a physical tree. The target's shard map moves onto the logical tree in the same registry write as the alias, and the physical tree the alias leaves redirects routers that still address it; refused when the registered tree-ownership guard denies the alias. |
| `lattice_treeadmin_tree_set_config` | manage | Apply per-tree configuration overrides - publish-events, projection-digest maintenance, durable-history retention, and the advisory WAL retained-byte ceiling - each written only when its `apply*` flag is set (a null value on an applied dimension clears that override). |
| `lattice_treeadmin_tree_delete` | manage | Soft-delete a tree. |
| `lattice_treeadmin_tree_recover` | manage | Recover a soft-deleted tree within its recovery window. |
| `lattice_treeadmin_tree_purge` | manage | Hard-purge a soft-deleted tree, irreversibly and bypassing the soft-delete window. Requires `confirm = true`; a false or omitted `confirm` is rejected. Accept-then-poll: the shard walk runs in the background, and the call returns within a bounded wait - with `purgeInProgress` still `true` for a tree too large to purge in that time, which is not a failure; poll `lattice_treeadmin_tree_deletion_status`. A call while the purge runs or after it completed returns the status without error. |
| `lattice_treeadmin_tree_reshard` | manage | Start an online reshard that grows or shrinks a tree to a target physical shard count: at least 2, and at most the tree's virtual slot count, never more than 4096. |
| `lattice_treeadmin_tree_resize` | manage | Start an online B+ node-capacity resize. |
| `lattice_treeadmin_tree_resize_undo` | manage | Undo a tree's most recent resize - an in-flight one at any phase, or a completed one while the pre-resize tree is still within its soft-delete window. Accept-then-poll: admitted even while a resize phase runs, it returns within a bounded wait with `undoRequested` set if the unwind is still in progress. A replicated tree cannot be undone once its alias has swapped onto the resized copy; resize it again, back to its previous sizing, instead. |
| `lattice_treeadmin_tree_snapshot` | manage | Capture a point-in-time tree snapshot into a fresh destination tree, in `Offline` or `Online` `mode`; any other `mode` value is rejected before a tree is resolved. |

### Bulk load, restore, and WAL placement

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_bulk_load_begin` | manage | Begin a streamed bulk-load session. |
| `lattice_treeadmin_bulk_load_append` | manage | Append a batch to an active bulk-load session. |
| `lattice_treeadmin_bulk_load_commit` | manage | Commit an active bulk-load session. |
| `lattice_treeadmin_tree_restore` | manage | Restore one tree from a backup. |
| `lattice_treeadmin_tree_restore_set` | manage | Restore a set of trees from a backup set. |
| `lattice_treeadmin_tree_restore_revert` | manage | Revert a shadow-cutover restore. |
| `lattice_treeadmin_wal_placement_inspect` | read | Inspect a tree's durable WAL placement. |
| `lattice_treeadmin_wal_placement_audit` | read | Audit WAL placement against the reporting silo's storage-provider catalog. |
| `lattice_treeadmin_wal_reclamation` | read | Read which durable pin holds a tree's WAL floor (consumer id, leaf, partition, pin offset), the leaf's persisted checkpoint and durable state, and `isWedged`: true exactly when the holder has a usable offset above a checkpoint of `-1`, a pin that never moves. Keyed on the holder, not on WAL growth; `pinStoreReadable = false` means nothing was established. Served by `ILatticeWalReclamation`; a host without it fails the call. |
| `lattice_treeadmin_wal_move_plan` | read | Preview moving a WAL partition to a target storage provider. |
| `lattice_treeadmin_wal_move_reclaim` | manage | Reclaim source WAL storage after a move. |
| `lattice_treeadmin_orphaned_leaves_audit` | read | Audit descent-unreachable leaves and repair eligibility. Optional `survey=true` counts every key outcome per leaf (100,000-key bound, read-only, off by default); `VerifiedKeyCount` remains a prefix, not a census. Nullable survey counts distinguish unknown from zero; findings identify shard, leaf, range and first failure. Batch totals include orphan/repairable/refused leaves and surveyed missing keys. Keep survey enabled on resumed batches; pass each batch's `resumeFrom` back until the report's `isComplete` is `true`, then check `verdictComplete` and unknown counts. |
| `lattice_treeadmin_orphaned_leaves_repair` | manage | Unsplice every orphaned leaf whose keys were all verified readable elsewhere, releasing the WAL trim floor. One bounded batch per call; pass each batch's `resumeFrom` back until `isComplete` is `true`, then re-audit. On a timeout the return value is not authoritative. |

### Views, tag indexes, compaction, and retention

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_view_create` | manage | Create or update a provider-backed runtime materialised view from a provider key and a base64 payload (64 KiB decoded maximum). |
| `lattice_treeadmin_view_list` | read | List runtime-registered materialised views with provider key and projection version; payloads are never returned. |
| `lattice_treeadmin_view_status` | read | Read one materialised view's source, lag, active generation, provider key, and projection version; payloads are never returned. |
| `lattice_treeadmin_view_drop` | manage | Drop a runtime materialised view. |
| `lattice_treeadmin_tag_index_list` | read | List tag indexes and their backing membership trees. |
| `lattice_treeadmin_tag_index_status` | read | Read one tag index's backing tree, covered trees, and reconcile state. |
| `lattice_treeadmin_compaction_trigger` | manage | Trigger an out-of-cycle tombstone-compaction pass on one physical shard of a tree (`shardIndex`), bypassing the shard's cooldown; reaps only tombstones and TTL-expired entries. |
| `lattice_treeadmin_retention_get` | read | Read a tree's durable-history retention policy. |
| `lattice_treeadmin_retention_set` | manage | Set or clear a tree's durable-history retention policy: a null `mode` or `windowSeconds` clears that part, a non-positive window is rejected, and a `mode` other than `MetadataOnly`, `FullValue` or `Hybrid` is rejected before a tree is resolved. |

The read tools carry `readOnlyHint = true` and `destructiveHint = false`; the manage tools carry `destructiveHint = true` and `readOnlyHint = false`, except `lattice_treeadmin_compaction_trigger` and `lattice_treeadmin_retention_set`, which are mutating but non-destructive to readable state and so carry `readOnlyHint = false` and `destructiveHint = false`. The registry-persisted shard-map read is distinct from the diagnostics `lattice_treeadmin_shard_map_inspect` tool, which inspects live routing rather than the durable registry map.

### Accept-then-poll operations

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_view_rebuild_start` | manage | Start a tracked view rebuild; returns the operation handle at once. |
| `lattice_treeadmin_view_reconcile_start` | manage | Start a tracked view reconcile. |
| `lattice_treeadmin_tag_index_reconcile_start` | manage | Start a tracked tag-index reconcile sweep. |
| `lattice_treeadmin_wal_move_start` | manage | Start a tracked WAL partition move (same tunables as `wal_move_execute`). |
| `lattice_treeadmin_orphaned_leaves_repair_start` | manage | Start a tracked whole-tree orphaned-leaf repair. |
| `lattice_treeadmin_orphaned_leaves_audit_start` | manage | Start a tracked whole-tree orphaned-leaf audit; mutates nothing but the operation record. |
| `lattice_treeadmin_operation_cancel` | manage | Request cancellation of a tracked tree-administration operation; needs the grant that starting it needed. |
| `lattice_treeadmin_operation_status` | read | Read a tracked tree-administration operation by id; returns `found` plus the operation view when visible. |
| `lattice_treeadmin_operation_list` | read | List one newest-first page of the caller's tracked tree-administration operations. |

Every start tool takes an optional `operationId` for idempotency and returns `operationId`, `kind`, `treeIds`, `created` and the `statusTool` to poll. The operation view includes `state`, `phase`, `phaseIndex`, `phaseCount`, `completedUnits`, `totalUnits`, `unitName`, `failureReason`, `resultReference`, `result` and `cancelRequested`; the kinds, phases, units and result keys are in [Tree-administration operations](../lattice.api.treeadmin/operations.md). `lattice_treeadmin_operation_status` and `lattice_treeadmin_operation_list` are always contributed; the start tools and `lattice_treeadmin_operation_cancel` need the lifecycle opt-in. `lattice_treeadmin_orphaned_leaves_audit_start` and `lattice_treeadmin_operation_cancel` are non-destructive (`destructiveHint = false`); the other start tools are destructive.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeTreeAdmin` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires the same tree-administration-API gRPC adapter off the `LatticeApiMcpRemoteOptions.TreeAdmin` endpoint, with the mutating lifecycle/control tools additionally requiring `LatticeApiMcpRemoteOptions.EnableLifecycleControl = true` (which maps onto `enableLifecycle`). `lattice_treeadmin_wal_reclamation` resolves the separate `ILatticeWalReclamation` facade instead: in-silo the one `AddLatticeTreeAdminApi` registers, and over the remote topology a `GrpcLatticeWalReclamation` adapter off the same endpoint that calls the `GetWalReclamation` RPC. Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## Tenant self-awareness tools (`lattice_tenant_current`, `lattice_tenant_list`, `lattice_tenant_get`)

Read-only tenant discovery over the tenant self-service facade, registered by `AddTenantSelfAwarenessTools()`. The module **self-gates on whether tenancy is enabled**: it takes no opt-in flag of its own and contributes its tools only when the tenancy-gated self-service facade is present, so a non-tenancy deployment - even one that calls the extension - is byte-for-byte unchanged. The tools advertise under the existing read-only `State` group rather than a new discovery group.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_tenant_current` | inspect | Report the tenant the calling credential is operating as, with its lifecycle status and whether it is the reserved default tenant. |
| `lattice_tenant_list` | inspect | List the tenants the caller is authorized to access, in ascending tenant-id order, scoped fail-closed to the caller. |
| `lattice_tenant_get` | inspect | Read one authorized tenant's lifecycle status, per-region residency, and authored resource quotas; fails closed with a not-found when the tenant does not exist or the caller may not see it. |

Every tool carries `readOnlyHint = true` and `destructiveHint = false`. The module adds no authorization path of its own: each tool stamps the caller credential onto the ambient context and defers to the facade's leak-free, fail-closed per-tenant scoping, so an unauthorized caller sees only its own default context, an empty accessible list, and a fail-closed not-found on inspect.

This module is served under both topologies. In-silo it delegates to the co-hosted self-service facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires a tenant self-service gRPC adapter off the `LatticeApiMcpRemoteOptions.TenantAdmin` endpoint (the self-service reads share the tenant-administration gRPC service address). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed per-tenant scoping.

## Tenant-admin tools (`lattice_tenant_create`, `lattice_tenant_suspend`, `lattice_tenant_resume`, `lattice_tenant_delete`, `lattice_tenant_set_quotas`)

Tenant lifecycle control over the tenant-administration facade, registered by `AddTenantAdminTools(enableControl)`. The lifecycle verbs are all mutating, so the module contributes tools only when `enableControl: true`; called without it, the `tenantadmin` capability is advertised to an `Admin` caller but no tools are contributed, and a cluster that never calls `AddTenantAdminTools` exposes no tenant-admin capability at all. The same registration also contributes the [region-residency tools](#tenant-region-residency-lattice_tenant_authorize_regions-lattice_tenant_set_residency-lattice_tenant_region_status). The group is discovered only by a caller granted `LatticeOperation.Admin`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_tenant_create` | manage | Register a new tenant in the active status, seeding the admin subjects that may see it. Omit `adminSubjects` (or pass an empty list) and the calling subject is seeded so the creator can see what it created; supply a non-empty list and that set is used instead (the caller is not added on top): a null, empty or whitespace entry is rejected, duplicates collapse, and where an identity directory is registered with validation required, an id it cannot resolve is refused. Fails closed if a tenant with the same id already exists (it is not an idempotent upsert). |
| `lattice_tenant_suspend` | manage | Move a tenant to the suspended status. Idempotent; the reserved default tenant cannot be suspended. |
| `lattice_tenant_resume` | manage | Return a suspended tenant to the active status. Idempotent; fails closed if the tenant does not exist. |
| `lattice_tenant_delete` | manage | Delete a tenant, cascading a soft-delete to every tree the tenant owns before removing its registry record. The reserved default tenant cannot be deleted. |
| `lattice_tenant_set_quotas` | manage | Author a tenant's resource quotas and burst allowance, replacing whatever quotas it currently carries. Each ceiling (`maxBytes`, `maxKeys`, `maxMemoryBytes`, `maxTreeCount`, `maxOpsPerSecond`) is null for unbounded on that dimension, and a bounded ceiling must be non-negative; pass every dimension null to lift the caps again. `burstPercent` must be non-negative. The delegated access caps (`maxGroups`, `maxMembershipEdges`, `maxMemberSubjects`, `maxTenantRules`) are each null for their default (500, 10000, 5000 and 1000), never unbounded, and lifting the resource ceilings does not lift them; the result echoes them. The reserved default tenant cannot be given quotas, and it fails closed if the tenant does not exist. |

Every tool carries `destructiveHint = true` and `readOnlyHint = false`. The module adds no authorization path of its own: each tool stamps the caller credential onto the ambient context and defers to the facade's own fail-closed tenant-admin access gate, so an unauthorized caller is default-denied on every mutation.

This module is served under both topologies. In-silo it delegates to the co-hosted tenant-administration facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires a tenant-administration gRPC adapter off the `LatticeApiMcpRemoteOptions.TenantAdmin` endpoint, with the mutating tools additionally requiring `LatticeApiMcpRemoteOptions.EnableTenantControl = true` (which maps onto `enableControl`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## Tenant region residency (`lattice_tenant_authorize_regions`, `lattice_tenant_set_residency`, `lattice_tenant_region_status`)

Per-tenant region-residency control over the region-residency facade, contributed by the same `AddTenantAdminTools(enableControl)` registration and gated behind the same `enableControl` opt-in. Of the [region sets](../lattice.tenancy/README.md#the-region-sets), they author the operator-owned **allowed** set and the tenant-owned **resident** set.

| Tool | Kind | Arguments | Purpose |
|---|---|---|---|
| `lattice_tenant_authorize_regions` | manage | `tenantId`, `allowedRegions` (both required) | Author the complete set of regions a tenant is allowed to place residency in. **Operator action.** |
| `lattice_tenant_set_residency` | manage | `tenantId`, `residencyRegions` (both required) | Author the complete set of regions a tenant is resident in, within its allowed set. **Tenant-admin action.** |
| `lattice_tenant_region_status` | inspect | `tenantId` (required) | Read the tenant's per-region residency lifecycle, ordered by region id. **Tenant-admin action.** |

Both region-set arguments are a **replacement, not a delta**: a currently-allowed region absent from `allowedRegions` is revoked, and a currently-resident region absent from `residencyRegions` begins draining. Because an omitted list would be indistinguishable from "revoke everything", both are mandatory in the tool schema - an agent must state the set it wants rather than wiping a tenant's standing by forgetting an argument.

The two mutating tools carry `destructiveHint = true` and `readOnlyHint = false`; `lattice_tenant_region_status` carries `readOnlyHint = true` and `destructiveHint = false`, so this group is no longer uniformly mutating.

Authorization is **two-tier and inherited from the facade**, which the tools do not widen:

- `lattice_tenant_authorize_regions` is **operator-only** - the server authorizes it as cluster-wide admin on the reserved auth policy tree and denies every non-operator caller, including a tenant admin. The allowed set is the operator's containment boundary.
- `lattice_tenant_set_residency` and `lattice_tenant_region_status` are **operator-or-tenant-admin** - the caller is authorized as the platform operator or as a live admin subject on the tenant record.
- `lattice_tenant_advance_region` is **operator-only** and requires explicit acknowledgement that data was independently placed in the target region. It is an override, not approval required for automatic backfill.

Both tiers are independent of the data-plane `DefaultEffect`, so an unmatched request resolves to deny even under `DefaultEffect = Allow`.

Ordering matters and the tools fail closed when it is violated: `lattice_tenant_set_residency` refuses a region outside the allowed set, refuses to remove the last resident region, and `lattice_tenant_authorize_regions` refuses to revoke a region the tenant is still resident in. Adding an allowed region does not add it to residency or imply its data is present. When a region is added to residency, its local driver starts automatic backfill: receiver replication is admitted in `Backfilling`, while client requests remain refused until every configured tree bootstrap and parked tenant-offline entry is verified complete. An empty tenant can advance directly to `Online`. The operator-only `lattice_tenant_advance_region` is an acknowledged override only for data independently placed in the target region; it is not part of the normal backfill workflow. A dropped region's drain completes on its own, `Draining` -> `Offline` -> `Removed`, on each silo of that region that registers the tenant-admin control API (see [Lifecycle states](../lattice.tenancy/README.md#lifecycle-states)). Once a tenant's residency is set it is served only in a region whose status is exactly `Online`.

The typical workflow is:

1. An operator calls `lattice_tenant_authorize_regions` to widen the allowed set.
2. A tenant admin calls `lattice_tenant_region_status` and sees the new region as `isAllowed: true` with status `None`.
3. The tenant admin calls `lattice_tenant_set_residency` to move into it. The region enters `Provisioning`, and the local driver begins `Backfilling` without a separate approval.
4. The tenant admin follows `lattice_tenant_region_status` or the Explorer Replication view until each tree bootstrap is verified and the region reaches `Online`; only then do tenant client calls route there.

An operator may instead call `lattice_tenant_advance_region` one legal lifecycle step at a time when data was placed out of band, with `acknowledgeDataInPlace` set to `true` only after independently verifying that data. This override does not copy or verify the data.

This module is served under both topologies. In-silo it delegates to the co-hosted region-residency facade directly; over the remote topology `AddLatticeMcpRemote` wires a region-residency gRPC adapter off the same `LatticeApiMcpRemoteOptions.TenantAdmin` endpoint.

## Delegated tenant access tools

Tenant groups, the tenant member set, tenant-tier rules, a layer-aware explain and the tenant access posture, as thin adapters over `ILatticeTenantDirectoryAdmin` and `ILatticeTenantPolicyAdmin` (see [`Orleans.Lattice.Api.TenantAdmin`](../lattice.api.tenantadmin/README.md#delegated-tenant-access-administration)). They are contributed by the same `AddTenantAdminTools(enableControl)` registration and advertised under the existing tenant-admin group, so `lattice_capabilities` gains no new group. Two conditions decide whether they appear:

- **`enableControl` gates reads and writes alike.** Without it none of the delegated tenant access tools is contributed, exactly as for `lattice_tenant_region_status`.
- **Each facade's tools need that facade registered.** The directory tools appear only when `ILatticeTenantDirectoryAdmin` is registered, and the policy tools only when `ILatticeTenantPolicyAdmin` is; the check reads the service collection without resolving the facade. `AddLatticeTenantAdminApi` registers both on a silo.

Every tool takes `tenantId` and names groups, trees and rules by their **tenant-local** names; the facade composes `t/{tenant}/{name}`, `t/{tenant}/{tree}` and `tenant:{tenant}:{id}`. A `subjectKind` or `memberKind` argument (`User` by default, `TenantGroup`, or `ClusterGroup`) says how to read a subject id. The paged reads take `pageSize` and `pageToken` and return `nextPageToken`.

| Tool | Kind | Facade member | Purpose |
|---|---|---|---|
| `lattice_tenant_group_list` | inspect | `ListGroupsAsync` | One page of the tenant's own groups by local name. Another tenant's groups are never listed. |
| `lattice_tenant_group_get` | inspect | `GetGroupAsync` | One group by local name; `found: false` when it does not exist, including another tenant's group. |
| `lattice_tenant_group_members` | inspect | `ListGroupMembersAsync` | A group's direct members, each with its kind. |
| `lattice_tenant_member_list` | inspect | `ListMembersAsync` | One page of the tenant member set, each entry with its kind. |
| `lattice_tenant_rule_list` | inspect | `ListRulesAsync` | One page of the tenant's tenant-tier rules (editable) and the operator rules scoped to its own trees (layer `Platform`, read-only). Cluster-wide and app rules are not listed. |
| `lattice_tenant_rule_get` | inspect | `GetRuleAsync` | One tenant-tier rule by local id; `found: false` when none exists. |
| `lattice_tenant_explain` | inspect | `ExplainAsync` | Whether a subject may perform an operation on one of the tenant's trees, optionally one key, with the deciding layer and rule. |
| `lattice_tenant_effective_permissions` | inspect | `EffectivePermissionsAsync` | The rules of both layers that apply to a subject on the tenant's trees, optionally one tree, labelled by layer and origin. |
| `lattice_tenant_access_posture` | inspect | `GetPostureAsync` | Whether the feature is enabled, whether the caller is an admin of the tenant or a platform operator, and the delegated access caps with their usage. The one tool that answers while the feature is off. |
| `lattice_tenant_group_upsert` | manage | `UpsertGroupAsync` | Create a group, or change its display name. Creating counts against `MaxGroups`. |
| `lattice_tenant_group_remove` | manage | `RemoveGroupAsync` | Remove a group, cascading its edges in both directions, its member-set and admin-set entries and the tenant rules that name it. `removed: false` when it does not exist. Removing the tenant's last admin entry is refused. |
| `lattice_tenant_group_member_add` | manage | `AddGroupMemberAsync` | Add a user, a cluster group or another of the tenant's groups to a group. Idempotent; counts against `MaxMembershipEdges`. |
| `lattice_tenant_group_member_remove` | manage | `RemoveGroupMemberAsync` | Remove a direct member; `changed: false` when it was absent. |
| `lattice_tenant_member_add` | manage | `AddMemberAsync` | Add a user, a cluster group or one of the tenant's groups to the member set. Idempotent; counts against `MaxMemberSubjects`. |
| `lattice_tenant_member_remove` | manage | `RemoveMemberAsync` | Remove a member-set entry; `changed: false` when it was absent. |
| `lattice_tenant_rule_put` | manage | `PutRuleAsync` | Create or replace a tenant-tier rule over a tree, a key, a prefix, or every tree the tenant owns (`scopeKind: TenantWide`, with no `treeName`). Counts against `MaxTenantRules`. |
| `lattice_tenant_rule_remove` | manage | `RemoveRuleAsync` | Remove a tenant-tier rule by local id; `removed: false` when none existed. Operator rules cannot be removed here. |

The reads carry `readOnlyHint = true` and `destructiveHint = false`; the writes carry `destructiveHint = true` and `readOnlyHint = false`. A rule that a cluster-wide (`Tree:*`) rule or an app role contributed is reported by `ruleId`, `layer`, `origin` and `effect` only, with `subjectWithheld: true`. `ResolveSubjectAsync` has no tool.

The module adds no authorization path of its own: each tool stamps the caller credential and defers to the facade, which authorizes a platform operator or an admin of the tenant, directly or through a group, before it reads or writes anything. A denial stays a denial. The facades' other typed failures are mapped to fixed messages:

| Facade failure | What the client sees |
|---|---|
| `TenantAccessAdministrationDisabledException` | An error saying delegated tenant access administration is not enabled on the cluster and pointing at `lattice_tenant_access_posture`. |
| `TenantAccessConfinementException` | A client error (`rejected_content`) naming the confinement rule (`GroupNesting`, `ForeignTenantGroup`, `RuleTree`, `RuleOperations` or `ReservedRuleId`) with fixed text; the facade's own message is not echoed. |
| `ReservedTenantOperationException` | A client error (`invalid_argument`) saying the reserved default tenant has no delegated access administration. |
| `TenantLastAdminSubjectException` | A client error (`invalid_argument`) saying the change would leave the tenant with no admin subject. |
| `LatticeQuotaExceededException` | An error naming the cap's dimension and limit and pointing at `lattice_tenant_set_quotas`. |
| Any other `ArgumentException` | A client error (`invalid_argument`) carrying the facade's message, sanitised. |

Both topologies serve them. In-silo they delegate to the co-hosted facades directly. A remote (out-of-silo) head that sets `LatticeApiMcpRemoteOptions.TenantAdmin` registers one tenant-administration gRPC client (`LatticeTenantAdminApiGrpcClient`, which implements both interfaces) as both facades, so the tools light up there too; the writes still need `EnableTenantControl`, which maps onto `enableControl`.

## Error handling

Every facade-backed tool call is routed through a single translation seam, so a fault is surfaced to the client as an actionable error result rather than the SDK's opaque generic mask. The translated message names the failure class:

| Fault | What the client sees |
|---|---|
| A remote gRPC `RpcException` of any status | The binding's sanitised detail, prefixed with the status code - except a `FailedPrecondition` guidance message, which is surfaced verbatim on its own. A `PermissionDenied`/`Unauthenticated` denial stays a denial, and a server-side fault code points at the cluster logs. |
| A local MCP-host fault (assembly load failure, argument or mapping error) | The exception type name and message, so an operator can diagnose a host-side problem directly. |
| A fail-closed authorization denial | Surfaced as a denial with its safe message; it is never downgraded or swallowed. |

The seam never forwards a raw server exception or stack trace across the gRPC boundary: the deliberately generic `Internal` wire message stays generic, and the translation only ever adds the gRPC status code and the detail the binding already chose to expose (see [Security](security.md)).

Every group tool (everything except the two meta-tools) binds its arguments strictly: an argument the tool does not declare - typically a misspelled parameter name - is rejected before any facade call, with a message naming the offending argument and listing the accepted ones, rather than being silently ignored. The echoed names are sanitised: at most five are named (the rest are counted), each is cut to 64 characters, and any character other than an ASCII letter or digit, `_`, `-` or `.` is replaced with `?`. Caller mistakes on the data and state tools surface as client-error statuses, never as a generic `Internal` fault that points at the cluster logs. On `lattice_data_set_many_atomic` and `lattice_data_set_many_atomic_cross_tree`, reusing an `operationId` with a different key set (or, cross-tree, a different tree or key set) than its first submission is a `FailedPrecondition` with a self-contained message; a duplicate key or an empty / `'/'`-bearing `operationId` is an `InvalidArgument`. Those two statuses are what a remote head reports from the data gRPC binding; a co-hosted server surfaces the same fault as the facade's own exception type and message. On `lattice_data_set`, a `value` that is not valid base64 is rejected up front, before any facade call, with a tool error that names the parameter ("The 'value' parameter must be base64-encoded; the supplied text is not valid base64.") rather than leaking a JSON decode error. Unknown-target reads (`lattice_state_get_tree_summary`, `lattice_state_get_shard_summaries`, `lattice_state_get_entry`, `lattice_state_get_tree_structure`, `lattice_state_scan_entries`, `lattice_state_get_entry_history`) are typed statuses on a normal result - `TreeNotFound`, `KeyNotFound`, or `IndexNotFound` - not gRPC faults, and `lattice_state_get_physical_shard_count` answers an unknown tree with a null count and `treeExists` set to `false`.

### Client errors are answered, not logged as faults

The ModelContextProtocol SDK logs every exception a tool throws at Error level with its stack, as "threw an unhandled exception", before it turns the exception into an error result. A caller that omits a required argument would therefore read, in the server log, exactly like a server fault. So a call rejected as the caller's mistake is answered with the same error result (`isError: true`, text `An error occurred invoking '<tool>': <message>`) without being thrown: the MCP host logs it at Debug with no stack under event `McpToolClientError`, and counts it on `orleans.lattice.api.mcp.tool.client_errors` - a counter on the MCP host's own meter, `orleans.lattice.api.mcp` (exposed as `LatticeApiMcpMetrics.MeterName` on the public `LatticeApiMcpMetrics` class), so an OpenTelemetry pipeline must subscribe to that meter to export it - tagged by `tool`, `reason`, and a `tenant` tag fixed to the platform sentinel `_platform_`:

| `reason` | Raised for |
|---|---|
| `unknown_argument` | An argument the tool does not declare (the strict-binding rejection above). |
| `invalid_argument` | A missing, empty, or unrecognised argument, including one the SDK's argument binder cannot bind. |
| `rejected_content` | An argument whose content is refused, for example a repository-context memory body carrying leaked tool-call framing or a credential-bearing URL. |
| `not_found` | A record or resource the call names that does not exist, for example `repocontext_update` against a key with no record. |

The message of a classified client error is sanitised once, before it is returned or logged: every control character and Unicode line or paragraph separator is replaced with `?`, and a message longer than 2,048 characters is cut there and ends in `...`, so a caller-chosen key, path or argument name cannot forge a record in a line-oriented log.

The built-in `lattice_*` tools raise the two argument-binding reasons - `unknown_argument`, and `invalid_argument` for an argument the SDK's binder cannot bind - and the [delegated tenant access tools](#delegated-tenant-access-tools) also raise `rejected_content` for a confinement refusal and `invalid_argument` for the facade failures that table lists; `not_found` and the other `rejected_content` and `invalid_argument` cases come from the [repository-context tools](../lattice.api.mcp.repocontext/tools.md#tool-parameters). A caller mistake that a tool does not classify is still thrown, so it still logs at Error and is not counted. In the built-in groups that includes a `value` that is not valid base64 on `lattice_data_set`, an argument the facade rejects, and an `InvalidArgument` status from a remote head. Several repository-context refusals are unclassified too, for example a fencing conflict or a claim on a non-memory key; the repository-context page lists them.

The Debug line is the MCP host's own record, and a tool group can log a failure itself before the host sees it. The repository-context tools do: they log every call that reaches one of their tools and fails at Warning with its exception, a classified client error included. So a caller mistake on a `repocontext_*` tool still leaves a Warning line with a stack beside the host's Debug line. An undeclared argument, a refused `region`, and a refusal by the registered `ILatticeApiMcpAuthorizer` are turned away before the tool runs, so they leave no such line. An access-gate denial that fails the call from inside a `repocontext_*` tool - the `LatticeAuthorizationDeniedException` the tree access gate throws when it refuses the caller's credential on one of the tool's tree operations - does reach that logger, so it leaves the Warning line with its stack. The host then turns it into an unclassified error, so it also logs at Error and is not counted as a client error.

What the client sees is unchanged. A server fault, any fault a tool raises without classifying it as one of the reasons above, an authorization denial, and a region refusal are still thrown, so they still log at Error: a denial is never downgraded to a client error.

## Next

- [Security](security.md) - how tools are gated and how the caller credential flows.
- [Remote hosting](remote.md) - the same tool modules over gRPC clients.
