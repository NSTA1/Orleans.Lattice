# Schema compliance operations

Accept-then-poll compliance scans for [`Orleans.Lattice.Api.Schema`](README.md). `ILatticeSchemaComplianceOperations` starts a compliance scan of a tree in the background and returns a handle at once; the caller polls the operation's status for progress and the report. It is the schema facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

## Why

A compliance scan reads every value of a tree. The original `ScanComplianceAsync` on `ILatticeSchemaControl` blocks until the last value is read, so on a large tree it is cut off by the caller's request timeout - the Explorer reported "The cluster did not answer in time" while the scan was still running - and it exposes no progress. `StartComplianceScanAsync` returns in milliseconds, the scan survives the caller, and its progress counts the entries it has actually validated.

## Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartComplianceScanAsync(treeId, operationId)` | A compliance scan of one tree. | `ScanComplianceAsync`: read over the tree, after the tree name is composed under the caller's tenant. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the compliance-scan kind, the caller's tenant and the trees the caller may read. Cancelling needs read over the scanned tree. |

The optional `operationId` makes a start idempotent: a retried start with the same id returns the existing operation (`Created = false`) and starts nothing.

`AddLatticeSchemaApi` registers the default implementation as a silo singleton beside `ILatticeSchemaControl`. Its status, list and cancel verbs see only compliance scans, never another schema operation kind.

## Kind, phases and units

| Kind | Phases (in order) | Units |
|---|---|---|
| `schema.compliance-scan` (`SchemaComplianceScanOperation.Kind`) | `Counting`, `Scanning` | `Scanning` counts `entries` validated against the tree's live entry count, taken in `Counting`. |

The count is taken before the scan and the tree stays writable while it runs, so a scan can pass it. The total then becomes unknown (`null`) rather than reading below the entries already scanned. An ungoverned tree (no policy) neither counts nor scans: the operation succeeds at once with `hasPolicy = false`.

Progress is counted in entries, not shards: the scan reads through the tree's routing-aware, ordered enumeration, which merges every shard into one key order, so a per-shard count is not observable without bypassing the routing that keeps a scan correct across a concurrent reshard.

## Results

A succeeded operation's `ResultReference` is the scanned tree id (the effective, tenant-scoped id, as on the blocking report). Its `Result` map uses the `SchemaComplianceScanResults` keys: `treeId`, `hasPolicy`, `compliantCount`, `nonCompliantCount`, `scannedCount`, `ruleCount`, and one `rule.{i}.reason` / `rule.{i}.count` pair per failure reason. `SchemaComplianceScanResults.TryReadReport` rebuilds the `LatticeSchemaComplianceReport` the blocking scan returns.

## Example

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

## Migrating from the blocking scan

`ScanComplianceAsync` on `ILatticeSchemaControl`, and the matching call on the gRPC client, are **deprecated** and **will be removed in the next major version**. They raise compiler warning `LATTICE0002`, whose help link points here; existing code still compiles and runs, and the blocking scan behaves exactly as before.

| Deprecated | Replacement |
|---|---|
| `ScanComplianceAsync(treeId)` | `StartComplianceScanAsync(treeId)`, then poll `GetOperationStatusAsync` and read the report with `SchemaComplianceScanResults.TryReadReport`. |

Over gRPC, the `ScanCompliance` RPC stays on the wire, deprecated, until the next major version; use `StartComplianceScan`, `GetComplianceScanStatus`, `ListComplianceScans` and `CancelComplianceScan` (see the [gRPC API reference](../lattice.api.schema.grpc/api.md)). Over MCP, use the `lattice_treeadmin_schema_compliance_scan_*` tools (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

To keep a deliberate use of the deprecated scan building warning-free, suppress the diagnostic locally:

```text
#pragma warning disable LATTICE0002
var report = await control.ScanComplianceAsync(treeId, cancellationToken);
#pragma warning restore LATTICE0002
```

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [API reference](api.md) - the facade's operations by name.
- [`Orleans.Lattice.Schema`](../lattice.schema/README.md) - the compliance engine these scans run.
