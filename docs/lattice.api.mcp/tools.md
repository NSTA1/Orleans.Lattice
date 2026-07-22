# Tools

The MCP server exposes its capabilities as **tools**, grouped into four opt-in modules plus a `lattice_capabilities` meta-tool. Every tool is a thin adapter over the matching `Orleans.Lattice.Api.*` facade and is named `lattice_<group>_<verb>`. The server ships with no tools; each module is added explicitly.

## Opting in

```csharp verify
var services = new ServiceCollection();

services.AddLatticeMcp();
services.AddStateTools();
services.AddDataTools(enableWrites: true);
services.AddBackupTools(enableControl: true);
services.AddAuthTools(enableAdministration: true);
```

Each module registration is idempotent, and within a module the destructive verbs stay hidden unless the host opts them in:

| Module | Extension | Read/inspect verbs | Destructive verbs (opt-in flag) |
|---|---|---|---|
| State | `AddStateTools()` | always | none (read-only facade) |
| Data | `AddDataTools(enableWrites)` | always | writes, gated by `enableWrites` |
| Backup | `AddBackupTools(enableControl)` | always | capture / restore / delete, gated by `enableControl` |
| Auth | `AddAuthTools(enableAdministration)` | always | user / group / rule mutation, gated by `enableAdministration` |

Read tools carry `readOnlyHint = true`; destructive tools carry `destructiveHint = true` and `readOnlyHint = false`, so a well-behaved MCP client can surface the distinction to the operator. Enabling a destructive verb only advertises it - it stays subject to the same fail-closed access gate the facade enforces (see [Security](security.md)).

## Discovery

The `lattice_capabilities` meta-tool reports which groups are enabled on the server and, for the authenticated caller, which tools its effective permissions unlock. Discovery is permission-scoped: a tool the caller may not use is never listed, so an agent's tool list is exactly the set it can invoke.

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

## Next

- [Security](security.md) - how tools are gated and how the caller credential flows.
- [Remote hosting](remote.md) - the same tool modules over gRPC clients.
