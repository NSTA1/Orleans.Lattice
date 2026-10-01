# Schema operations

Accept-then-poll schema remediation and eager migration for [`Orleans.Lattice.Api.Schema`](README.md). `ILatticeSchemaOperations` starts a remediation, a migration or an advance-and-migrate in the background and returns a handle at once; the caller polls the operation's status for its phase, the values processed and the outcome. It is the schema facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

## Why

A remediation or migration rewrites every value of a tree, so it can run for minutes. The original verbs on `ILatticeSchemaControl` block until the work completes, so a long run is cut off by the caller's request timeout while the work carries on, and it shows no progress. The start verbs below return as soon as the cluster has accepted the run, the work survives the caller, and its progress is real data: the values the dry run has checked and the build has copied.

## Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartRemediationAsync` | A remediation: every value rewritten by a transform and checked against a target policy, then the tree cut over with the policy installed. | `RemediateAsync`: schema management over the tree. |
| `StartMigrationAsync` | An eager migration of every value to the tree's current target schema version. | `MigrateToTargetVersionAsync`: schema management over the tree. |
| `StartAdvanceAndMigrateAsync` | An advance of the target version, then an eager migration to it. | `AdvanceAndMigrateAsync`: schema management over the tree. |
| `GetOperationStatusAsync`, `ListOperationsAsync` | - | Scoped to the schema kinds, the caller's tenant and the trees the caller may read. An operation outside that scope is not found. |
| `CancelOperationAsync` | - | Visible as above, and schema management over the tree. A caller who may read but not manage is refused. |

Every start verb takes an optional `operationId`. A retried start with the same id returns the existing operation (`Created = false`) and starts nothing. A start that finds a remediation with the same parameters already in flight on the tree follows that remediation instead of starting another, and a start with different parameters fails in-band with the reason.

The default facade registered by `AddLatticeSchemaApi` implements both `ILatticeSchemaControl` and `ILatticeSchemaOperations` on one singleton, so a start and its blocking twin share one authorization path.

## Kinds, phases and units

The kinds are the `SchemaOperationKinds` constants, and every one starts with `schema.`. The phase names are the `SchemaOperationPhases` constants.

| Kind | Phases (in order) | Units |
|---|---|---|
| `schema.remediation` | `DryRun`, `Build`, `Cutover` | `DryRun` counts `values` checked, with no total, because the tree is not counted up front; `Build` counts `values` copied out of the dry run's count; `Cutover` reports none. |
| `schema.migration` | `DryRun`, `Build`, `Cutover` | As a remediation. |
| `schema.advance-and-migrate` | `Advance`, `DryRun`, `Build`, `Cutover` | As a remediation; `Advance` reports none. |

The tree's own remediation report (`ILatticeSchemaControl.GetRemediationStatusAsync`) names the tracked operation in its `OperationId`, so a client that lost the handle - a reloaded page, say - finds the operation from the tree.

## Outcomes

| Outcome | State | `Result` map (`SchemaOperationResultKeys`) |
|---|---|---|
| The tree was cut over. | `Succeeded` | `outcome` = `completed`, `valuesProcessed`, `remediationOperationId`. |
| A value could not be remediated. Nothing was cut over. | `Failed`, with `FailureReason` naming the key and the reason. | `outcome` = `aborted`, `valuesProcessed`, `offendingKey`, `reason`, `remediationOperationId`. The tree's remediation report carries a bounded preview of the value. |
| Cancelled before cutover. Nothing was cut over. | `Cancelled` | `outcome` = `cancelled`, `valuesProcessed`, `remediationOperationId`. |
| The work failed, for example a target version that does not advance. | `Failed`, with the exception type and message. | Empty. |

A remediation never runs as one long call. The tree's coordinator records the run when it is accepted, and the work then drives it one bounded slice of values at a time from a durable cursor. A cancel therefore takes effect between two slices. Cutover is the point of no return: a cancel that arrives during cutover is declined, and the run completes. If the silo running the work is lost, the operation reads as `Failed`, as for any operation. The remediation itself stays recorded in flight at its last slice, so starting the same remediation again resumes it from there.

## Migrating from the blocking verbs

`RemediateAsync`, `MigrateToTargetVersionAsync` and `AdvanceAndMigrateAsync` on `ILatticeSchemaControl`, and the matching blocking calls on the gRPC client, are **deprecated** and **will be removed in the next major version**. They raise compiler warning `LATTICE0002`, whose help link points here; existing code still compiles and runs.

The deprecated verbs drive the same bounded slices and return the terminal report as before, so no single cluster call times out under them. They still wait for the whole run, though, so a long run is still exposed to the caller's own timeout. To migrate:

| Deprecated | Replacement |
|---|---|
| `RemediateAsync(tree, transform, policy)` | `StartRemediationAsync(tree, transform, policy)`, then poll `GetOperationStatusAsync`. |
| `MigrateToTargetVersionAsync(tree)` | `StartMigrationAsync(tree)`. |
| `AdvanceAndMigrateAsync(tree, version)` | `StartAdvanceAndMigrateAsync(tree, version)`. |

Over gRPC, the `Remediate`, `MigrateToTargetVersion` and `AdvanceAndMigrate` RPCs stay on the wire, deprecated, until the next major version. Use `StartRemediation`, `StartMigration`, `StartAdvanceAndMigrate`, `GetSchemaOperationStatus`, `ListSchemaOperations` and `CancelSchemaOperation` instead (see the [gRPC API reference](../lattice.api.schema.grpc/api.md)).

To keep a deliberate use of a deprecated verb building warning-free, suppress the diagnostic locally:

```text
#pragma warning disable LATTICE0002
var report = await control.RemediateAsync(treeId, transform, policy, cancellationToken);
#pragma warning restore LATTICE0002
```

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [API reference](api.md) - the facade's operations by name.
- [Schema enforcement](../lattice.schema/schema-enforcement.md) - the remediation engine these operations run.
