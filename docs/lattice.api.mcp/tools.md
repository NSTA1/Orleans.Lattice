# Tools

The MCP server exposes its capabilities as **tools**, grouped into six opt-in modules plus a `lattice_capabilities` and a `lattice_list_regions` meta-tool. Every tool is a thin adapter over the matching `Orleans.Lattice.Api.*` facade and is named `lattice_<group>_<verb>`. The server ships with no tools; each module is added explicitly.

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
| TreeAdmin | `AddTreeAdminTools(enableSchemaControl, enableLifecycle)` | always | schema policy / version / remediation mutation, gated by `enableSchemaControl`; tree create / set-alias / set-config, gated by `enableLifecycle` |

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
| `lattice_data_set_many` | write | Non-atomic single-tree batch: apply each key independently (best-effort, per-key authorized). |
| `lattice_data_set_many_atomic` | write | Atomic single-tree batch. |
| `lattice_data_set_many_atomic_cross_tree` | write | Atomic cross-tree batch. |

### Typed CRDT tools

These surface the replicated CRDT primitives directly, so a caller reads and writes a value's convergent type without hand-encoding CRDT state. Element and value bytes are base64-encoded; a write attributes its mutation to a `replicaId`. Each write tool takes an `operation` discriminator; each type also has a paired read. See [CRDT primitives](../crdt/readme.md) for the merge rules summarised below.

| Type | Write tool | Read tool | Merge rule |
|---|---|---|---|
| PN-Counter | `lattice_data_pncounter` (increment / decrement) | `lattice_data_pncounter_get` | Per-replica signed sum. |
| OR-Set | `lattice_data_orset` (add / remove) | `lattice_data_orset_get` | Add-wins, observed-remove. |
| OR-Flag | `lattice_data_orflag` (enable / disable) | `lattice_data_orflag_get` | Enable-wins. |
| RW-Flag | `lattice_data_rwflag` (enable / disable) | `lattice_data_rwflag_get` | Disable-wins. |
| Version Vector | `lattice_data_version_vector_tick` | `lattice_data_version_vector_get` | Per-replica max clock. |
| MV-Register | `lattice_data_mvregister_set` | `lattice_data_mvregister_get` | Keep concurrent values. |
| Sequence | `lattice_data_sequence` (insert-at / remove-at) | `lattice_data_sequence_get` | Ordered insert / tombstone. |
| OR-Map | `lattice_data_ormap` (set / remove) | `lattice_data_ormap_get` | Recursive per-key merge. |

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

## TreeAdmin lifecycle tools (`lattice_treeadmin_tree_*`)

Explicit tree lifecycle and per-tree registry configuration over `ILatticeTreeAdmin`, surfaced under the tree-administration group. Registered by `AddTreeAdminTools(enableLifecycle: true)`. The read-only lifecycle tools are always exposed; the mutating lifecycle tools require `enableLifecycle: true`. Each tool wraps the existing internal tree registry (`ILatticeRegistry`) rather than re-implementing registration or per-tree config, and every tool remains subject to the facade's own fail-closed access gate: the read verbs authorize on whole-tree `LatticeOperation.Read` authority, and the mutating verbs authorize on whole-tree `LatticeOperation.Admin` authority. Registration is idempotent under matching parameters, and a reserved system tree id (the `_lattice_` namespace) is rejected for the mutating verbs. The group is discovered only by a caller granted `LatticeOperation.Admin`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_treeadmin_tree_exists` | read | Report whether a tree is registered. |
| `lattice_treeadmin_tree_resolve_alias` | read | Resolve the physical tree a logical tree maps to. |
| `lattice_treeadmin_tree_get_config` | read | Read a tree's registry-backed configuration (sizing, alias, per-tree overrides). |
| `lattice_treeadmin_tree_get_shard_map` | read | Read a tree's registry-persisted shard map (custom-map flag, version, virtual/physical shard counts). |
| `lattice_treeadmin_tree_create` | manage | Explicitly create (register) a tree with an optional initial sizing. Idempotent; reserved ids rejected. |
| `lattice_treeadmin_tree_set_alias` | manage | Point a logical tree at a physical tree. Reserved ids rejected. |
| `lattice_treeadmin_tree_set_config` | manage | Apply a partial per-tree configuration update (publish-events, projection-digest, history-retention), each override written only when its apply flag is set. Reserved ids rejected. |

The read tools carry `readOnlyHint = true` and `destructiveHint = false`; the manage tools carry `destructiveHint = true` and `readOnlyHint = false`. `lattice_treeadmin_tree_create` takes a `treeId` and optional `shardCount` / `maxLeafKeys` / `maxInternalChildren` sizing (honoured only on first creation); `lattice_treeadmin_tree_set_alias` takes a `treeId` and `physicalTreeId`; `lattice_treeadmin_tree_set_config` exposes the update as flat `apply*` / value parameter pairs; the remaining tools take a `treeId`. The registry-persisted shard-map read is distinct from the diagnostics `lattice_treeadmin_shard_map_inspect` tool, which inspects live routing rather than the durable registry map. Shard-map mutation is out of scope here (it is driven by the reshard / resize operations); only the read is exposed.

This module is served under both topologies. In-silo it delegates to the co-hosted `ILatticeTreeAdmin` facade directly; over the remote (out-of-silo) topology the `AddLatticeMcpRemote` composition wires `GrpcLatticeTreeAdmin` off the `RemoteOptions.TreeAdmin` endpoint, with the mutating lifecycle tools additionally requiring `RemoteOptions.EnableLifecycleControl = true` (which maps onto `enableLifecycle`). Caller credentials are forwarded on every gRPC call by the shared credential-forwarding interceptor, so the remote cluster re-runs the facade's own fail-closed access gate.

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
