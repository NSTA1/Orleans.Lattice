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

Each module registration is idempotent, and within a module the destructive verbs stay hidden unless the host opts them in:

| Module | Extension | Read/inspect verbs | Destructive verbs (opt-in flag) |
|---|---|---|---|
| State | `AddStateTools()` | always | none (read-only facade) |
| Data | `AddDataTools(enableWrites)` | always | writes, gated by `enableWrites` |
| Backup | `AddBackupTools(enableControl)` | always | capture / restore / delete, gated by `enableControl` |
| Auth | `AddAuthTools(enableAdministration)` | always | user / group / rule mutation, gated by `enableAdministration` |
| Replication | `AddReplicationTools(enableControl)` | always | enable / disable replication, gated by `enableControl` |
| TreeAdmin | `AddTreeAdminTools(enableSchemaControl, enableLifecycle)` | always | schema policy / version / remediation mutation, gated by `enableSchemaControl`; tree lifecycle, restore, bulk-load, WAL-move, view, tag-index, compaction, and retention control, gated by `enableLifecycle` |
| Tenant self-awareness | `AddTenantSelfAwarenessTools()` | always (self-gates on tenancy) | none (read-only facade) |
| Tenant-admin | `AddTenantAdminTools(enableControl)` | none | tenant create / suspend / resume / delete, gated by `enableControl` |

Read tools carry `readOnlyHint = true`; destructive tools carry `destructiveHint = true` and `readOnlyHint = false`, so a well-behaved MCP client can surface the distinction to the operator. Enabling a destructive verb only advertises it - it stays subject to the same fail-closed access gate the facade enforces (see [Security](security.md)).

## Discovery

The `lattice_capabilities` meta-tool reports which groups are enabled on the server and, for the authenticated caller, which tools its effective permissions unlock. Discovery is permission-scoped: a tool the caller may not use is never listed, so an agent's tool list is exactly the set it can invoke.

## Region targeting

A single MCP server can front more than one region (the current cluster plus configured, reachable peers - see [Remote host](remote.md)). Two additive surfaces expose this:

- **`lattice_list_regions`** - a read-only meta-tool that lists the regions the server can route a call to, current region first, each with its region id, cluster id, and per-group endpoint availability. A region with no route or credentials for a group is reported unavailable for it, and a region with no route at all is omitted entirely (fail-closed discovery). The tool is projected from the shared `Orleans.Lattice.Api.Region.ILatticeRegionCatalog` contract, so a client reads the same region model the facade layer exposes.
- **An optional `region` argument on every tool** - pass a listed region id to route that single call to the named region; omit it to target the current region. Omitting it is byte-for-byte identical to a region-unaware call. The result is annotated with the region it was served from (in the result's `_meta.region`) whenever a `region` was supplied.

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

- **No tenant asserted** (an operator, or any caller on a non-tenancy cluster) - the full routing topology, unannotated and byte-for-byte as before. The reserved `default` tenant is treated the same way.
- **A non-default tenant asserted** - the current region plus only those peers in the tenant's **actionable set**: the regions its operator has authorized it into, plus the regions it is resident in. Each entry gains an additive `tenantScope` object reporting `tenantId`, `isAllowed`, `status`, and `isResident`. The current region is always listed (the caller is already talking to it) and is annotated truthfully, which may say the tenant is neither allowed into nor resident in it.
- **A tenant asserted whose standing cannot be resolved** - the current region alone, fail-closed. It never falls back to the full topology.

A region reported with `isResident: false` is a legitimate `lattice_tenant_set_residency` destination but **not** yet a routing destination: targeting it with a `region` argument is refused by the residency gate until its status reaches `Online`. See [the three region sets](../lattice.tenancy/README.md#the-three-region-sets).

## State tools (`lattice_state_*`)

Read-only introspection over `ILatticeStateQuery`. Registered by `AddStateTools()`.

| Tool | Purpose |
|---|---|
| `lattice_state_get_cluster_info` | Cluster-wide summary. |
| `lattice_state_list_trees` | Paged catalog of registered trees. |
| `lattice_state_list_views` | Paged catalog of materialised views. |
| `lattice_state_list_tag_indexes` | Tag indexes defined on the cluster. |
| `lattice_state_list_tag_values` | Values seen for a tag index. |
| `lattice_state_list_covered_trees` | Trees covered by a tag index. |
| `lattice_state_list_index_tags` | Tags an index covers. |
| `lattice_state_scan_tag_members` | Members matching a tag value. |
| `lattice_state_get_tree_summary` | Summary of one tree. |
| `lattice_state_get_shard_summaries` | Per-shard summaries for a tree. |
| `lattice_state_get_physical_shard_count` | Physical shard count for a tree. |
| `lattice_state_get_tree_structure` | Depth-bounded shard-root node graph. |
| `lattice_state_scan_entries` | Key-ordered, snapshot-isolated entry page. |
| `lattice_state_get_entry` | One key's full record. |
| `lattice_state_get_entry_history` | Version history for a key. |
| `lattice_state_cancel_scan` | Cancel an in-flight scan. |

## Data tools (`lattice_data_*`)

Read/write access over `ILatticeDataApi`. Registered by `AddDataTools(enableWrites)`. The read tools - the two point / range reads plus the eight typed-CRDT reads - are always exposed; the write tools (the point / batch writes plus the eight typed-CRDT writes) require `enableWrites: true`.

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

These surface the replicated CRDT primitives directly, so a caller reads and writes a value's convergent type without hand-encoding CRDT state. Element and value bytes are base64-encoded; a write attributes its mutation to a `replicaId`. Each write tool takes an `operation` discriminator; each type also has a paired read. See [CRDT primitives](../crdt/readme.md) for the merge rules summarised below.

| Type | Write tool | Read tool | Merge rule |
|---|---|---|---|
| PN-Counter | `lattice_data_pncounter` (increment / decrement) | `lattice_data_pncounter_get` | Per-replica signed sum. |
| G-Counter | `lattice_data_gcounter` (increment) | `lattice_data_gcounter_get` | Per-replica grow-only sum. |
| OR-Set | `lattice_data_orset` (add / remove) | `lattice_data_orset_get` | Add-wins, observed-remove. |
| OR-Flag | `lattice_data_orflag` (enable / disable) | `lattice_data_orflag_get` | Enable-wins. |
| RW-Flag | `lattice_data_rwflag` (enable / disable) | `lattice_data_rwflag_get` | Disable-wins. |
| RW-Set | `lattice_data_rwset` (add / remove) | `lattice_data_rwset_get` | Remove-wins observed set. |
| Version Vector | `lattice_data_version_vector_tick` | `lattice_data_version_vector_get` | Per-replica max clock. |
| MV-Register | `lattice_data_mvregister_set` | `lattice_data_mvregister_get` | Keep concurrent values. |
| Max-Register | `lattice_data_maxregister_set` | `lattice_data_maxregister_get` | Keep the greatest observed value. |
| Min-Register | `lattice_data_minregister_set` | `lattice_data_minregister_get` | Keep the least observed value. |
| Sequence | `lattice_data_sequence` (insert-at / remove-at) | `lattice_data_sequence_get` | Ordered insert / tombstone. |
| OR-Map | `lattice_data_ormap` (set / remove) | `lattice_data_ormap_get` | Recursive per-key merge. |
| G-Set | `lattice_data_gset` (add) | `lattice_data_gset_get` | Grow-only set. |

The OR-Map tools operate on an `OrMap<string, MvRegister>` (string field keys; each field value a multi-value register of base64 bytes). The host must register that shape for the target tree name (`AddOrMapShape<string, MvRegister>(treeName)`) for these tools to resolve.

## Backup tools (`lattice_backup_*`)

Backup control over `ILatticeBackupControl`. Registered by `AddBackupTools(enableControl)`. The five inspect tools are always exposed; the five control tools require `enableControl: true`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_backup_list` | inspect | Paged, read-filtered catalog page. |
| `lattice_backup_describe` | inspect | A manifest and its restore chain. |
| `lattice_backup_inventory` | inspect | Catalog-wide inventory summary. |
| `lattice_backup_scope_status` | inspect | A scope's schedule and last-run status. |
| `lattice_backup_export_artifact` | inspect | Stream a backup artifact. |
| `lattice_backup_create` | control | Capture a full backup. |
| `lattice_backup_create_incremental` | control | Capture an incremental backup. |
| `lattice_backup_restore` | control | Restore a backup. |
| `lattice_backup_revert_restore` | control | Undo a shadow-cutover restore. |
| `lattice_backup_delete` | control | Delete a backup and its unshared artifacts. |

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

`lattice_auth_explain` and `lattice_auth_effective_permissions` take an optional `subjectKind` argument (`User` by default). Set it to `Group` when `subjectId` names a group, so the tool resolves the group's rule closure instead of treating the id as a user; otherwise a group subject matches no rules and the decision falls through to the tree's default effect.

Both `lattice_auth_explain` and `lattice_auth_effective_permissions` also report the cluster's authorization posture (whether the all-trees grant tier and access-administration delegation are enabled). This is the discovery path for the posture - `lattice_capabilities` does not carry it - so an agent can tell whether a cluster-wide `Tree:*` grant is actually enforced and whether a policy-tree delegation rule is authorable. Consistent with that posture, `lattice_auth_put_rule` rejects a `Tree:*` data-plane rule while the all-trees grant tier is off, and a whole-tree `Admin` rule on the reserved policy tree while access-administration delegation is off.

## Replication tools (`lattice_replication_*`)

Runtime per-tree cross-cluster replication control over `ILatticeReplicationControl`. Registered by `AddReplicationTools(enableControl)`. The inspect tool is always exposed; the mutating control tools require `enableControl: true`, and remain subject to the facade's fail-closed replication access gate regardless. The module is served under both topologies: in-silo, and out-of-silo via `AddLatticeMcpRemote(o => { o.Replication = ...; o.EnableReplicationControl = ...; })` over the replication-API gRPC client (see [Remote hosting](remote.md)).

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_replication_get_config` | inspect | Report each authorized tree's enabled state, fixed merge mode, and ambiguity status. |
| `lattice_replication_enable` | control | Enable replication for a tree under a fixed merge mode. |
| `lattice_replication_disable` | control | Disable replication for a tree without purging already-replicated peer data. |

The control tools carry `destructiveHint = true`; the inspect tool carries `readOnlyHint = true`. Discovery is permission-scoped by the `LatticeOperation.Replication` grant, so a caller without that grant is not shown the group.

## TreeAdmin schema tools (`lattice_treeadmin_schema_*`)

Schema-management control over `ILatticeSchemaControl`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools(enableSchemaControl)`. The read-only schema-inspection tools are always exposed; the mutating schema-management tools require `enableSchemaControl: true`, and every tool remains subject to the facade's own fail-closed schema access gate regardless (a read authorizes on ordinary read authority; a mutation authorizes on schema-management authority). The group is discovered only by a caller granted `LatticeOperation.Admin`.

The MCP group holds the `ILatticeSchemaControl` facade and delegates to it verbatim - it adds no method to the tree-administration facade and no authorization path of its own. The schema facade and its packages are unchanged.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_schema_get_policy` | inspect | Read a tree's enforcement policy, or none when unset. |
| `lattice_treeadmin_schema_list_dead_letters` | inspect | Stream a tree's strict-mode dead-letter entries. |
| `lattice_treeadmin_schema_count_dead_letters` | inspect | Count a tree's strict-mode dead-letter entries. |
| `lattice_treeadmin_schema_get_version_config` | inspect | Read a tree's envelope-version config, or none when unversioned. |
| `lattice_treeadmin_schema_get_remediation_status` | inspect | Read a tree's current or last-known remediation status. |
| `lattice_treeadmin_schema_scan_compliance` | inspect | Scan every current value against the compiled policy and report compliance. |
| `lattice_treeadmin_schema_probe_capabilities` | inspect | Probe which schema operations the caller may perform, side-effect free. |
| `lattice_treeadmin_schema_set_policy` | manage | Set or replace a tree's enforcement policy. |
| `lattice_treeadmin_schema_clear_policy` | manage | Clear a tree's enforcement policy. |
| `lattice_treeadmin_schema_set_version_config` | manage | Opt a tree in to envelope versioning (or replace its config). |
| `lattice_treeadmin_schema_clear_version_config` | manage | Opt a tree back out of envelope versioning. |
| `lattice_treeadmin_schema_advance_target_version` | manage | Advance a tree's target schema version. |
| `lattice_treeadmin_schema_advance_and_migrate` | manage | Advance a tree's target version and eagerly migrate existing values. |
| `lattice_treeadmin_schema_migrate_to_target` | manage | Re-stamp every existing value to the current target version. |
| `lattice_treeadmin_schema_remediate` | manage | Run a per-value remediation transform toward a target policy. |

The manage tools carry `destructiveHint = true` and `readOnlyHint = false`; the inspect tools carry `readOnlyHint = true`. `lattice_treeadmin_schema_set_version_config` takes the version config as scalar `schemaId` / `targetVersion` / `strictIngest` arguments; `lattice_treeadmin_schema_set_policy` and `lattice_treeadmin_schema_remediate` take the schema policy and value-transform model objects directly.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeSchemaControl` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeSchemaControl` - a schema-API gRPC adapter - off the same endpoint as the tree-administration group (`RemoteOptions.TreeAdmin`, since the schema-API and tree-administration gRPC services are co-hosted on the same silo address). The remote host honours the same read-always / write-gated split: the read-only schema-inspection tools are served whenever the tree-administration endpoint is configured, and the mutating schema-management tools additionally require `RemoteOptions.EnableSchemaControl = true` (which maps onto `enableSchemaControl`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## TreeAdmin diagnostics tools (`lattice_treeadmin_*`)

Read-only administrative diagnostics and storage accounting over `ILatticeTreeAdmin`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools` and always exposed (no opt-in flag). Each tool wraps the existing public grain surface (`ILattice`, `ILatticeAdmin`) rather than re-implementing shard fan-out, and every tool remains subject to the facade's own fail-closed access gate: the per-tree verbs authorize on whole-tree `LatticeOperation.Read` authority, and `lattice_treeadmin_storage_usage` authorizes on the distinct cluster-wide `LatticeOperation.Telemetry` capability. The group is discovered only by a caller granted `LatticeOperation.Admin`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_shard_hotness` | inspect | Read a tree's per-shard read/write hotness with tree-level totals. |
| `lattice_treeadmin_shard_diagnostics` | inspect | Read a whole-tree diagnostic report; the `deep` flag walks leaf state for authoritative counts. |
| `lattice_treeadmin_shard_map_inspect` | inspect | Inspect a tree's shard-map topology (physical tree id, virtual/physical shard counts, map version). |
| `lattice_treeadmin_projection_digest` | inspect | Read a single shard's leaf-projection content digest for cheap divergence detection. |
| `lattice_treeadmin_tree_stats` | inspect | Read a tree's rolled-up topology, live-key counts, and storage byte breakdown in one call. |
| `lattice_treeadmin_storage_usage` | inspect | Read cluster-wide storage accounting; the `deep` flag forces a fresh leaf-walk instead of the cheap cached WAL-poll aggregate. |

Every tool carries `readOnlyHint = true` and `destructiveHint = false`. `lattice_treeadmin_shard_diagnostics` and `lattice_treeadmin_storage_usage` take an optional `deep` flag (default `false`, the cheap path); `lattice_treeadmin_projection_digest` takes a `treeId` and a non-negative `shardIndex`; the remaining per-tree tools take a `treeId`. `lattice_treeadmin_storage_usage` is cluster-wide and takes no tree id.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeTreeAdmin` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeTreeAdmin` - a tree-administration-API gRPC adapter - off the `RemoteOptions.TreeAdmin` endpoint. Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## TreeAdmin lifecycle and control tools (`lattice_treeadmin_*`)

Explicit tree lifecycle, per-tree registry configuration, bulk-load, restore, WAL placement, view, tag-index, compaction, and retention operations over `ILatticeTreeAdmin`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools(enableLifecycle: true)`. The read-only lifecycle/control tools are always exposed; the mutating lifecycle/control tools require `enableLifecycle: true`. Each tool delegates to the tree-administration facade instead of re-implementing registry or shard fan-out behaviour, and every tool remains subject to the facade's own fail-closed access gate. The group is advertised to callers whose effective permissions include one of the tree-administration group capabilities (`Admin`, `TreeLifecycle`, `BulkLoad`, or `Restore`); individual verbs are still authorized by the facade at call time. Registration is idempotent under matching parameters, and reserved system tree ids in the `_lattice_` namespace are rejected for mutating verbs.

### Tree lifecycle and registry

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_tree_exists` | read | Report whether a tree is registered. |
| `lattice_treeadmin_tree_resolve_alias` | read | Resolve the physical tree a logical tree maps to. |
| `lattice_treeadmin_tree_get_config` | read | Read a tree's registry-backed configuration (sizing, alias, per-tree overrides). |
| `lattice_treeadmin_tree_get_shard_map` | read | Read a tree's registry-persisted shard map (custom-map flag, version, virtual/physical shard counts). |
| `lattice_treeadmin_tree_deletion_status` | read | Read a tree's soft-deletion state, recovery window, and purge status. |
| `lattice_treeadmin_tree_reshard_status` | read | Read the current online-reshard state and shard-map fan-out. |
| `lattice_treeadmin_tree_resize_status` | read | Read the current online-resize state and effective B+ node capacities. |
| `lattice_treeadmin_tree_snapshot_status` | read | Read whether a point-in-time snapshot capture is in flight for a tree. |
| `lattice_treeadmin_tree_create` | manage | Explicitly create or register a tree with optional initial sizing. |
| `lattice_treeadmin_tree_set_alias` | manage | Point a logical tree at a physical tree. |
| `lattice_treeadmin_tree_set_config` | manage | Apply per-tree configuration overrides. |
| `lattice_treeadmin_tree_delete` | manage | Soft-delete a tree. |
| `lattice_treeadmin_tree_recover` | manage | Recover a soft-deleted tree within its recovery window. |
| `lattice_treeadmin_tree_purge` | manage | Hard-purge a soft-deleted tree. |
| `lattice_treeadmin_tree_reshard` | manage | Start an online reshard to a target physical shard count. |
| `lattice_treeadmin_tree_resize` | manage | Start an online B+ node-capacity resize. |
| `lattice_treeadmin_tree_resize_undo` | manage | Undo an in-flight or staged tree resize when supported. |
| `lattice_treeadmin_tree_snapshot` | manage | Capture a point-in-time tree snapshot. |

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
| `lattice_treeadmin_wal_move_plan` | read | Preview moving a WAL partition to a target storage provider. |
| `lattice_treeadmin_wal_move_execute` | manage | Execute a planned WAL partition move. |
| `lattice_treeadmin_wal_move_reclaim` | manage | Reclaim source WAL storage after a move. |

### Views, tag indexes, compaction, and retention

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_view_create` | manage | Create or update a provider-backed runtime materialised view from a provider key and a base64 payload (64 KiB decoded maximum). |
| `lattice_treeadmin_view_list` | read | List runtime-registered materialised views with provider key and projection version; payloads are never returned. |
| `lattice_treeadmin_view_status` | read | Read one materialised view's source, lag, active generation, provider key, and projection version; payloads are never returned. |
| `lattice_treeadmin_view_rebuild` | manage | Rebuild a materialised view. |
| `lattice_treeadmin_view_reconcile` | manage | Reconcile a materialised view. |
| `lattice_treeadmin_view_drop` | manage | Drop a runtime materialised view. |
| `lattice_treeadmin_tag_index_list` | read | List tag indexes and their backing membership trees. |
| `lattice_treeadmin_tag_index_status` | read | Read one tag index's backing tree, covered trees, and reconcile state. |
| `lattice_treeadmin_tag_index_reconcile` | manage | Reconcile a tag index. |
| `lattice_treeadmin_compaction_trigger` | manage | Trigger shard compaction for a tree. |
| `lattice_treeadmin_retention_get` | read | Read a tree's durable-history retention policy. |
| `lattice_treeadmin_retention_set` | manage | Set a tree's durable-history retention policy. |

The read tools carry `readOnlyHint = true` and `destructiveHint = false`; the manage tools carry `destructiveHint = true` and `readOnlyHint = false`. The registry-persisted shard-map read is distinct from the diagnostics `lattice_treeadmin_shard_map_inspect` tool, which inspects live routing rather than the durable registry map.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeTreeAdmin` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeTreeAdmin` off the `RemoteOptions.TreeAdmin` endpoint, with the mutating lifecycle/control tools additionally requiring `RemoteOptions.EnableLifecycleControl = true` (which maps onto `enableLifecycle`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## Tenant self-awareness tools (`lattice_tenant_current`, `lattice_tenant_list`, `lattice_tenant_get`)

Read-only tenant discovery over the tenant self-service facade, registered by `AddTenantSelfAwarenessTools()`. The module **self-gates on whether tenancy is enabled**: it takes no opt-in flag of its own and contributes its tools only when the tenancy-gated self-service facade is present, so a non-tenancy deployment - even one that calls the extension - is byte-for-byte unchanged. The tools advertise under the existing read-only `State` group rather than a new discovery group.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_tenant_current` | inspect | Report the tenant the calling credential is operating as, with its lifecycle status and whether it is the reserved default tenant. |
| `lattice_tenant_list` | inspect | List the tenants the caller is authorized to access, in ascending tenant-id order, scoped fail-closed to the caller. |
| `lattice_tenant_get` | inspect | Read one authorized tenant's lifecycle status, per-region residency, and authored resource quotas; fails closed with a not-found when the tenant does not exist or the caller may not see it. |

Every tool carries `readOnlyHint = true` and `destructiveHint = false`. The module adds no authorization path of its own: each tool stamps the caller credential onto the ambient context and defers to the facade's leak-free, fail-closed per-tenant scoping, so an unauthorized caller sees only its own default context, an empty accessible list, and a fail-closed not-found on inspect.

This module is served under both topologies. In-silo it delegates to the co-hosted self-service facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeTenantSelfService` off the `RemoteOptions.TenantAdmin` endpoint (the self-service reads share the tenant-administration gRPC service address). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed per-tenant scoping.

## Tenant-admin tools (`lattice_tenant_create`, `lattice_tenant_suspend`, `lattice_tenant_resume`, `lattice_tenant_delete`, `lattice_tenant_set_quotas`)

Tenant lifecycle control over the tenant-administration facade, registered by `AddTenantAdminTools(enableControl)`. The lifecycle verbs are all mutating, so the module contributes tools only when `enableControl: true`; called without it, the `tenantadmin` capability is advertised to an `Admin` caller but no tools are contributed, and a cluster that never calls `AddTenantAdminTools` exposes no tenant-admin capability at all. The same registration also contributes the three [region-residency tools](#tenant-region-residency-lattice_tenant_authorize_regions-lattice_tenant_set_residency-lattice_tenant_region_status). The group is discovered only by a caller granted `LatticeOperation.Admin`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_tenant_create` | manage | Register a new tenant in the active status, seeding the admin subjects that may see it. Omit `adminSubjects` and the calling subject is seeded so the creator can see what it created; supply it and that set is used verbatim (the caller is not added on top). Fails closed if a tenant with the same id already exists (it is not an idempotent upsert). |
| `lattice_tenant_suspend` | manage | Move a tenant to the suspended status. Idempotent; the reserved default tenant cannot be suspended. |
| `lattice_tenant_resume` | manage | Return a suspended tenant to the active status. Idempotent; fails closed if the tenant does not exist. |
| `lattice_tenant_delete` | manage | Delete a tenant, cascading a soft-delete to every tree the tenant owns before removing its registry record. The reserved default tenant cannot be deleted. |
| `lattice_tenant_set_quotas` | manage | Author a tenant's resource quotas and burst allowance, replacing whatever quotas it currently carries. Each ceiling (`maxBytes`, `maxKeys`, `maxMemoryBytes`, `maxTreeCount`, `maxOpsPerSecond`) is null for unbounded on that dimension; pass every dimension null to lift the caps again. `burstPercent` must be non-negative. The reserved default tenant cannot be given quotas, and it fails closed if the tenant does not exist. |

Every tool carries `destructiveHint = true` and `readOnlyHint = false`. The module adds no authorization path of its own: each tool stamps the caller credential onto the ambient context and defers to the facade's own fail-closed tenant-admin access gate, so an unauthorized caller is default-denied on every mutation.

This module is served under both topologies. In-silo it delegates to the co-hosted tenant-administration facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeTenantAdmin` off the `RemoteOptions.TenantAdmin` endpoint, with the mutating tools additionally requiring `RemoteOptions.EnableTenantControl = true` (which maps onto `enableControl`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

## Tenant region residency (`lattice_tenant_authorize_regions`, `lattice_tenant_set_residency`, `lattice_tenant_region_status`)

Per-tenant region-residency control over the region-residency facade, contributed by the same `AddTenantAdminTools(enableControl)` registration and gated behind the same `enableControl` opt-in. They author two of the [three region sets](../lattice.tenancy/README.md#the-three-region-sets): the operator-owned **allowed** set and the tenant-owned **resident** set.

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

Both tiers are independent of the data-plane `DefaultEffect`, so an unmatched request resolves to deny even under `DefaultEffect = Allow`.

Ordering matters and the tools fail closed when it is violated: `lattice_tenant_set_residency` refuses a region outside the allowed set, refuses to remove the last resident region, and `lattice_tenant_authorize_regions` refuses to revoke a region the tenant is still resident in. Transitions are asynchronous, so a newly added region reports `Provisioning`, not `Online`; poll `lattice_tenant_region_status` until it reaches `Online` before routing traffic there with a `region` argument.

The typical workflow is:

1. An operator calls `lattice_tenant_authorize_regions` to widen the allowed set.
2. A tenant admin calls `lattice_tenant_region_status` and sees the new region as `isAllowed: true` with status `None`.
3. The tenant admin calls `lattice_tenant_set_residency` to move into it; it reports `Provisioning`.
4. Once it reaches `Online`, `lattice_list_regions` advertises it with `tenantScope.isResident: true` and a `region`-targeted call routed there succeeds.

This module is served under both topologies. In-silo it delegates to the co-hosted region-residency facade directly; over the remote topology `AddLatticeMcpRemote` wires `GrpcLatticeTenantRegionAdmin` off the same `RemoteOptions.TenantAdmin` endpoint.

## Error handling

Every facade-backed tool call is routed through a single translation seam, so a fault is surfaced to the client as an actionable error result rather than the SDK's opaque generic mask. The translated message names the failure class:

| Fault | What the client sees |
|---|---|
| A remote gRPC `RpcException` of any status | The status code plus the binding's sanitised detail (for example a `FailedPrecondition` guidance message verbatim, a `PermissionDenied`/`Unauthenticated` denial, or a server-side fault code that points at the cluster logs). |
| A local MCP-host fault (assembly load failure, argument or mapping error) | The exception type name and message, so an operator can diagnose a host-side problem directly. |
| A fail-closed authorization denial | Surfaced as a denial with its safe message; it is never downgraded or swallowed. |

The seam never forwards a raw server exception or stack trace across the gRPC boundary: the deliberately generic `Internal` wire message stays generic, and the translation only ever adds the gRPC status code and the detail the binding already chose to expose (see [Security](security.md)).

Caller mistakes on the data and state tools surface as client-error statuses, never as a generic `Internal` fault that points at the cluster logs. On `lattice_data_set_many_atomic` and `lattice_data_set_many_atomic_cross_tree`, reusing an `operationId` with a different key set (or, cross-tree, a different tree or key set) than its first submission is a `FailedPrecondition` with a self-contained message; a duplicate key or an empty / `'/'`-bearing `operationId` is an `InvalidArgument`. On `lattice_data_set`, a `value` that is not valid base64 is rejected up front as an `InvalidArgument` ("value must be base64-encoded") rather than leaking a JSON decode error. Unknown-target reads (`lattice_state_get_entry`, `lattice_state_get_tree_structure`, `lattice_state_scan_entries`, `lattice_state_get_entry_history`) are typed statuses on a normal result - `TreeNotFound`, `KeyNotFound`, or `IndexNotFound` - not gRPC faults.

## Next

- [Security](security.md) - how tools are gated and how the caller credential flows.
- [Remote hosting](remote.md) - the same tool modules over gRPC clients.
