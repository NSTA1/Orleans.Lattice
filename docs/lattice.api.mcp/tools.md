# Tools

The MCP server exposes its capabilities as **tools**, grouped into five opt-in modules plus a `lattice_capabilities` and a `lattice_list_regions` meta-tool. Every tool is a thin adapter over the matching `Orleans.Lattice.Api.*` facade and is named `lattice_<group>_<verb>`. The server ships with no tools; each module is added explicitly.

## Opting in

```csharp verify
var services = new ServiceCollection();

services.AddLatticeMcp();
services.AddStateTools();
services.AddDataTools(enableWrites: true);
services.AddBackupTools(enableControl: true);
services.AddAuthTools(enableAdministration: true);
services.AddReplicationTools(enableControl: true);
```

Each module registration is idempotent, and within a module the destructive verbs stay hidden unless the host opts them in:

| Module | Extension | Read/inspect verbs | Destructive verbs (opt-in flag) |
|---|---|---|---|
| State | `AddStateTools()` | always | none (read-only facade) |
| Data | `AddDataTools(enableWrites)` | always | writes, gated by `enableWrites` |
| Backup | `AddBackupTools(enableControl)` | always | capture / restore / delete, gated by `enableControl` |
| Auth | `AddAuthTools(enableAdministration)` | always | user / group / rule mutation, gated by `enableAdministration` |
| Replication | `AddReplicationTools(enableControl)` | always | enable / disable replication, gated by `enableControl` |

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

Read/write access over `ILatticeDataApi`. Registered by `AddDataTools(enableWrites)`. The two read tools are always exposed; the four write tools require `enableWrites: true`.

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_data_get` | read | Fetch a single key. |
| `lattice_data_read_range` | read | Read a key range. Only `treeId` is required; the range bounds, page size, and continuation token are optional (omit them for a full, unbounded first page). |
| `lattice_data_set` | write | Set a single key. |
| `lattice_data_delete` | write | Delete a single key. |
| `lattice_data_set_many_atomic` | write | Atomic single-tree batch. |
| `lattice_data_set_many_atomic_cross_tree` | write | Atomic cross-tree batch. |

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

## Replication tools (`lattice_replication_*`)

Runtime per-tree cross-cluster replication control over `ILatticeReplicationControl`. Registered by `AddReplicationTools(enableControl)`. The inspect tool is always exposed; the mutating control tools require `enableControl: true`, and remain subject to the facade's fail-closed replication access gate regardless. The module is served under both topologies: in-silo, and out-of-silo via `AddLatticeMcpRemote(o => { o.Replication = ...; o.EnableReplicationControl = ...; })` over the replication-API gRPC client (see [Remote hosting](remote.md)).

| Tool | Kind | Purpose |
|---|---|---|
| `lattice_replication_get_config` | inspect | Report each authorized tree's enabled state, fixed merge mode, and ambiguity status. |
| `lattice_replication_enable` | control | Enable replication for a tree under a fixed merge mode. |
| `lattice_replication_disable` | control | Disable replication for a tree without purging already-replicated peer data. |

The control tools carry `destructiveHint = true`; the inspect tool carries `readOnlyHint = true`. Discovery is permission-scoped by the `LatticeOperation.Replication` grant, so a caller without that grant is not shown the group.

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
