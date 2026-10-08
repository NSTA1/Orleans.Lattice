---
agent_spec: "docs/agents/api/schema.json"
---

# Schema operations

Accept-then-poll schema operations for [`Orleans.Lattice.Api.Schema`](README.md). Two facades start long schema work in the background and return a handle at once; the caller polls the operation's status for its progress and outcome. Both are the schema facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

| Facade | Starts | Kinds |
|---|---|---|
| `ILatticeSchemaComplianceOperations` | A read-only compliance scan of a tree. | `schema.compliance-scan` |
| `ILatticeSchemaOperations` | A remediation, an eager migration, or an advance-and-migrate. | `schema.remediation`, `schema.migration`, `schema.advance-and-migrate` |

Each facade's status, list and cancel verbs see only its own kinds. `AddLatticeSchemaApi` registers both beside `ILatticeSchemaControl`.

## Compliance scans

### Why

A compliance scan reads every value of a tree. The original `ScanComplianceAsync` on `ILatticeSchemaControl` blocks until the last value is read, so on a large tree it is cut off by the caller's request timeout - the Explorer reported "The cluster did not answer in time" while the scan was still running - and it exposes no progress. `StartComplianceScanAsync` returns in milliseconds, the scan survives the caller, and its progress counts the entries it has actually validated.

### Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartComplianceScanAsync(treeId, operationId)` | A compliance scan of one tree. | `ScanComplianceAsync`: read over the tree, after the tree name is composed under the caller's tenant. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the compliance-scan kind, the caller's tenant and the trees the caller may read. Cancelling needs read over the scanned tree. |

The optional `operationId` makes a start idempotent: a retried start with the same id returns the existing operation (`Created = false`) and starts nothing.

`AddLatticeSchemaApi` registers the default implementation as a silo singleton beside `ILatticeSchemaControl`. Its status, list and cancel verbs see only compliance scans, never another schema operation kind.

### Kind, phases and units

| Kind | Phases (in order) | Units |
|---|---|---|
| `schema.compliance-scan` (`SchemaComplianceScanOperation.Kind`) | `Counting`, `Scanning` | `Scanning` counts `entries` validated against the tree's live entry count, taken in `Counting`. |

The count is taken before the scan and the tree stays writable while it runs, so a scan can pass it. The total then becomes unknown (`null`) rather than reading below the entries already scanned. An ungoverned tree (no policy) neither counts nor scans: the operation succeeds at once with `hasPolicy = false`.

Progress is counted in entries, not shards: the scan reads through the tree's routing-aware, ordered enumeration, which merges every shard into one key order, so a per-shard count is not observable without bypassing the routing that keeps a scan correct across a concurrent reshard.

### Results

A succeeded operation's `ResultReference` is the scanned tree id (the effective, tenant-scoped id, as on the blocking report). Its `Result` map uses the `SchemaComplianceScanResults` keys: `treeId`, `hasPolicy`, `compliantCount`, `nonCompliantCount`, `scannedCount`, `ruleCount`, and one `rule.{i}.reason` / `rule.{i}.count` pair per failure reason. `SchemaComplianceScanResults.TryReadReport` rebuilds the `LatticeSchemaComplianceReport` the blocking scan returns.

### Example

```csharp verify
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

static async Task<LatticeSchemaComplianceReport?> ScanAsync(
    ILatticeSchemaComplianceOperations operations,
    CancellationToken cancellationToken)
{
    var handle = await operations.StartComplianceScanAsync("orders", cancellationToken: cancellationToken);

    while (true)
    {
        var status = await operations.GetOperationStatusAsync(handle.OperationId, cancellationToken);
        if (status is null)
        {
            return null;
        }

        if (status.IsTerminal)
        {
            return status.State == LatticeOperationState.Succeeded
                && SchemaComplianceScanResults.TryReadReport(status.Result, out var report)
                    ? report
                    : null;
        }

        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

## Remediation and migration

### Why

A remediation or migration rewrites every value of a tree, so it can run for minutes. The original verbs on `ILatticeSchemaControl` block until the work completes, so a long run is cut off by the caller's request timeout while the work carries on, and it shows no progress. The start verbs below return as soon as the cluster has accepted the run, the work survives the caller, and its progress is real data: the values the dry run has checked and the build has copied.

### Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartRemediationAsync` | A remediation: every value rewritten by a transform and checked against a target policy, then the tree cut over with the policy installed. | `RemediateAsync`: schema management over the tree. |
| `StartMigrationAsync` | An eager migration of every value to the tree's current target schema version. | `MigrateToTargetVersionAsync`: schema management over the tree. |
| `StartAdvanceAndMigrateAsync` | An advance of the target version, then an eager migration to it. | `AdvanceAndMigrateAsync`: schema management over the tree. |
| `GetOperationStatusAsync`, `ListOperationsAsync` | - | Scoped to the schema kinds, the caller's tenant and the trees the caller may read. An operation outside that scope is not found. |
| `CancelOperationAsync` | - | Visible as above, and schema management over the tree. A caller who may read but not manage is refused. |

Every start verb takes an optional `operationId`. A retried start with the same id returns the existing operation (`Created = false`) and starts nothing. A start that finds a remediation with the same parameters already in flight on the tree follows that remediation instead of starting another, and a start with different parameters fails in-band with the reason.

The default facade registered by `AddLatticeSchemaApi` implements both `ILatticeSchemaControl` and `ILatticeSchemaOperations` on one singleton, so a start and its blocking twin share one authorization path.

### Kinds, phases and units

The kinds are the `SchemaOperationKinds` constants, and every one starts with `schema.`. The phase names are the `SchemaOperationPhases` constants.

| Kind | Phases (in order) | Units |
|---|---|---|
| `schema.remediation` | `DryRun`, `Build`, `Cutover` | `DryRun` counts `values` checked, with no total, because the tree is not counted up front; `Build` counts `values` copied out of the dry run's count; `Cutover` reports none. |
| `schema.migration` | `DryRun`, `Build`, `Cutover` | As a remediation. |
| `schema.advance-and-migrate` | `Advance`, `DryRun`, `Build`, `Cutover` | As a remediation; `Advance` reports none. |

The tree's own remediation report (`ILatticeSchemaControl.GetRemediationStatusAsync`) names the tracked operation in its `OperationId`, so a client that lost the handle - a reloaded page, say - finds the operation from the tree.

### Outcomes

| Outcome | State | `Result` map (`SchemaOperationResultKeys`) |
|---|---|---|
| The tree was cut over. | `Succeeded` | `outcome` = `completed`, `valuesProcessed`, `remediationOperationId`. |
| A value could not be remediated. Nothing was cut over. | `Failed`, with `FailureReason` naming the key and the reason. | `outcome` = `aborted`, `valuesProcessed`, `offendingKey`, `reason`, `remediationOperationId`. The tree's remediation report carries a bounded preview of the value. |
| Cancelled before cutover. Nothing was cut over. | `Cancelled` | `outcome` = `cancelled`, `valuesProcessed`, `remediationOperationId`. |
| The work failed, for example a target version that does not advance. | `Failed`, with the exception type and message. | Empty. |

A remediation never runs as one long call. The tree's coordinator records the run when it is accepted, and the work then drives it one bounded slice of values at a time from a durable cursor. A cancel therefore takes effect between two slices. Cutover is the point of no return: a cancel that arrives during cutover is declined, and the run completes. If the silo running the work is lost, the operation reads as `Failed`, as for any operation. The remediation itself stays recorded in flight at its last slice, so starting the same remediation again resumes it from there.

## Migrating from the removed blocking scan

`ScanComplianceAsync` on `ILatticeSchemaControl`, and the matching call that previously existed on the gRPC client and MCP surface, were deprecated in 9.9.0 with warning `LATTICE0002` and are removed in this major version. Use the tracked compliance-scan operation instead:

| Removed blocking verb | Replacement |
|---|---|
| `ScanComplianceAsync(treeId)` | `StartComplianceScanAsync(treeId)`, then poll `GetOperationStatusAsync` and read the report with `SchemaComplianceScanResults.TryReadReport`. |

Over gRPC, use `StartComplianceScan`, `GetComplianceScanStatus`, `ListComplianceScans` and `CancelComplianceScan` (see the [gRPC API reference](../lattice.api.schema.grpc/api.md)). Over MCP, use the `lattice_treeadmin_schema_compliance_scan_*` tools (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

## Migrating from the removed blocking verbs

`RemediateAsync`, `MigrateToTargetVersionAsync` and `AdvanceAndMigrateAsync` on `ILatticeSchemaControl`, and the matching blocking calls that previously existed on the gRPC client and MCP surface, were deprecated in 9.9.0 with warning `LATTICE0002` and are removed in this major version. Migrate to the tracked operations below:

| Removed blocking verb | Replacement |
|---|---|
| `RemediateAsync(tree, transform, policy)` | `StartRemediationAsync(tree, transform, policy)`, then poll `GetOperationStatusAsync`. |
| `MigrateToTargetVersionAsync(tree)` | `StartMigrationAsync(tree)`. |
| `AdvanceAndMigrateAsync(tree, version)` | `StartAdvanceAndMigrateAsync(tree, version)`. |

Over gRPC, use `StartRemediation`, `StartMigration`, `StartAdvanceAndMigrate`, `GetSchemaOperationStatus`, `ListSchemaOperations` and `CancelSchemaOperation` instead (see the [gRPC API reference](../lattice.api.schema.grpc/api.md)). Over MCP, use the `lattice_treeadmin_schema_remediation_start`, `lattice_treeadmin_schema_migration_start` and `lattice_treeadmin_schema_advance_and_migrate_start` tools with the `lattice_treeadmin_schema_operation_*` tools; the `lattice_treeadmin_schema_remediate`, `lattice_treeadmin_schema_migrate_to_target` and `lattice_treeadmin_schema_advance_and_migrate` aliases are removed with the same major-version change (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [API reference](api.md) - the facade's operations by name.
- [`Orleans.Lattice.Schema`](../lattice.schema/README.md) - the compliance engine the scans run.
- [Schema enforcement](../lattice.schema/schema-enforcement.md) - the remediation engine the remediations and migrations run.
