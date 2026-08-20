# Orleans.Lattice.Api.Schema API reference

The public surface is the registration extension, the options type, and the control facade interface (`ILatticeSchemaControl`, published in the shared `Orleans.Lattice.Api.Abstractions` package under the `Orleans.Lattice.Api.Schema` namespace). The facade interface is the contract the gRPC binding adapts over, and is described by its operations below and in [Architecture](architecture.md).

The schema policy, versioning, dead-letter, remediation, compliance, and transform records are defined in [`Orleans.Lattice.Schema`](../lattice.schema/README.md). This package adds the control facade and its capability result, not a second schema model.

## Registration

### `LatticeApiSchemaServiceCollectionExtensions`

Static extension method on `ISiloBuilder`.

- `ISiloBuilder AddLatticeSchemaApi(this ISiloBuilder builder, Action<LatticeApiSchemaOptions>? configure = null)`

  Adds the transport-agnostic schema-management control facade: binds `LatticeApiSchemaOptions`, registers the internal silo singleton that every transport binding adapts over, and an idempotency marker. Adds no transport behaviour of its own. Must be called after `AddLatticeSchemaEnforcement(...)`; throws `InvalidOperationException` when called first. Throws `ArgumentNullException` when `builder` is null. Idempotent.

## Options

### `LatticeApiSchemaOptions`

The options type reserved for future read-bounding and audit-tuning knobs, mirroring the sibling control-API facades. See [Configuration](configuration.md) for defaults.

It currently has no tunable properties.

## Facade operations

The control facade exposes these methods. Each method corresponds to one RPC in the [gRPC binding](../lattice.api.schema.grpc/api.md), with `ListDeadLettersAsync` projected as the server-streaming `StreamDeadLetters` RPC. Every facade method authorizes its tree scope fail-closed before touching the admin plane.

| Method | Signature |
|---|---|
| `SetPolicyAsync` | `Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)` |
| `ClearPolicyAsync` | `Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)` |
| `GetPolicyAsync` | `Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ListDeadLettersAsync` | `IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)` |
| `CountDeadLettersAsync` | `Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)` |
| `SetVersionConfigAsync` | `Task SetVersionConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)` |
| `GetVersionConfigAsync` | `Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)` |
| `AdvanceTargetVersionAsync` | `Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)` |
| `AdvanceAndMigrateAsync` | `Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)` |
| `MigrateToTargetVersionAsync` | `Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ClearVersionConfigAsync` | `Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)` |
| `RemediateAsync` | `Task<LatticeSchemaRemediationReport> RemediateAsync(string treeId, LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, CancellationToken cancellationToken = default)` |
| `GetRemediationStatusAsync` | `Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ScanComplianceAsync` | `Task<LatticeSchemaComplianceReport> ScanComplianceAsync(string treeId, CancellationToken cancellationToken = default)` |
| `ProbeCapabilitiesAsync` | `Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(string treeId, CancellationToken cancellationToken = default)` |

Policy operations manage a tree's write-validation policy. `SetPolicyAsync` and `ClearPolicyAsync` require SchemaAdmin authority; `GetPolicyAsync` requires Read authority. The policy type and its enforcement semantics are defined in [`Orleans.Lattice.Schema`](../lattice.schema/README.md).

Dead-letter operations inspect diverted, schema-rejected writes. `ListDeadLettersAsync` streams entries with bounded memory and `CountDeadLettersAsync` returns the current count. Both require Read authority.

Versioning operations require the separate schema-versioning add-on. If the host did not register `AddLatticeSchemaVersioning(...)`, these calls throw a clear `InvalidOperationException` rather than failing dependency resolution. Reads require Read authority; mutations require SchemaAdmin authority. The config and migration semantics are defined in [`Orleans.Lattice.Schema`](../lattice.schema/README.md).

Remediation operations apply or report a tree-wide repair. `RemediateAsync` requires SchemaAdmin authority, applies a `LatticeValueTransform` across a tree, and adopts the supplied target policy. `GetRemediationStatusAsync` requires Read authority and returns the status or last report.

Scan compliance is read-only. It scans a tree's entries against the cached compiled policy and reports per-tree compliant and non-compliant counts plus a reason breakdown; when no policy is set, the report state is `Ungoverned`. It never mutates values or policy.

Probe capabilities has no side effects. It performs two fail-closed probes, Read and SchemaAdmin, and maps them to capability flags. The result is advisory only: every real operation still performs its own authorization immediately before touching data.

## Model records

### `LatticeSchemaCapabilities`

The allowed-operation set the read-only capability probe reports for one tree. Every flag is default-deny (`false` means "not known to be permitted"), and the flags are advisory: the server still authorizes each real operation fail-closed. The probe distinguishes the two authorization grants the access gate models - Read and SchemaAdmin - so read-only flags move together and administrative flags move together.

- `string TreeId` - the tree id these capabilities were evaluated over.
- `bool CanViewPolicy` - whether the caller may read the tree policy.
- `bool CanViewDeadLetters` - whether the caller may read the tree's dead-letter entries.
- `bool CanViewVersionConfig` - whether the caller may read the tree's version config.
- `bool CanViewRemediationStatus` - whether the caller may read remediation status for the tree.
- `bool CanScanCompliance` - whether the caller may run a read-only compliance audit.
- `bool CanManagePolicy` - whether the caller may set or clear the tree policy.
- `bool CanManageVersion` - whether the caller may set, advance, migrate, or clear the version config.
- `bool CanRemediate` - whether the caller may run remediation for the tree.

The DTO types `LatticeSchemaPolicy`, `LatticeSchemaVersionConfig`, `LatticeSchemaDeadLetterEntry`, `LatticeSchemaRemediationReport`, `LatticeSchemaComplianceReport`, and `LatticeValueTransform` are defined in [`Orleans.Lattice.Schema`](../lattice.schema/README.md).
