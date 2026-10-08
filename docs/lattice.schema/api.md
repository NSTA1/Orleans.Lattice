# Public API

The package provides opt-in schema enforcement and value versioning, a serializable transform representation, durable schema stores and host-side administration. The [package guide](README.md) introduces the independent opt-ins. The exact public declarations below include every explicitly declared member and overload; policy and versioning semantics are described in their topic guides.

## Registration and administration

Register `AddLatticeSchemaEnforcement` and/or `AddLatticeSchemaVersioning` after `AddLattice`. When both are present, enforcement must be registered first. See [configuration](configuration.md) for all five option properties and the registration-only merge observer.

`ILatticeSchemaAdmin`, `ILatticeSchemaVersionAdmin` and `ILatticeSchemaRemediationAdmin` are trusted host-side control services, not independently authorized remote endpoints. Put caller-facing changes behind [the Schema facade](../lattice.api.schema/README.md). `ILatticeSchemaComplianceAdmin` scans via the ordinary data plane, so caller read authority still applies.

The public store contracts persist policies, version configuration and dead-letter rows. `LatticeSchemaReservedTrees` exposes the reserved names and guard helpers. Replacing a store is a storage composition decision, not a grant to mutate schema from an untrusted caller.

## Policies, validation and transforms

`LatticeSchemaPolicy` contains ordered rules and its per-tree strict flag. `LatticeSchemaPolicyValidator` compiles the policy once, rejects invalid configuration at construction, and returns the first failure reason from validation or `null` on success. Strip a schema envelope before validating a stored versioned value: policies judge the body bytes, not the envelope header.

`LatticeValueTransform`, predicate/constant/value-kind models and transform translation/evaluation helpers describe portable remediation and upcasting work. A custom `ILatticeValueTransform` is resolved through `ILatticeValueTransformRegistry`; it is not silently embedded as executable code in the portable representation. See [value transforms](value-transforms.md).

## Version and operation contracts

`LatticeSchemaEnvelope` provides tag inspection and body extraction. The exact [wire format](wire-format.md) is distinct from a schema's JSON layout. `ILatticeSchemaRegistry` and its builder define registered schema versions/upcaster chains. `ILatticeSchemaVersionAdmin` governs each tree's target; advancing the target is monotonic and does not eagerly rewrite every existing value.

Remediation and eager migration use resumable shadow-build/cutover work with reports/phases; caller cancellation of a request is not proof that durable work was undone. Compliance scans and operation result/status helpers expose tracked progress through the public operation contracts. See [enforcement](schema-enforcement.md) and [versioning](schema-versioning.md).

## Related

- [Configuration](configuration.md)
- [Architecture](architecture.md)
- [Chaos tests](chaos-tests.md)

## Public declaration reference

The following inventory includes directly declared public members and overloads, enum values, and record positional members. Inherited framework members and compiler-generated record equality helpers are not additional package operations. Each source link identifies the declaration that defines its signature.

### `Orleans.Lattice.Schema.ILatticeSchemaAdmin`

[Source](../../src/lattice.schema/ILatticeSchemaAdmin.cs) (line 10).

`public interface ILatticeSchemaAdmin`

- `Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)`
- `Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<int> CountDeadLettersAsync(string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaComplianceAdmin`

[Source](../../src/lattice.schema/ILatticeSchemaComplianceAdmin.cs) (line 11).

`public interface ILatticeSchemaComplianceAdmin`

- `Task<LatticeSchemaComplianceReport> ScanComplianceAsync( string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaDeadLetterStore`

[Source](../../src/lattice.schema/ILatticeSchemaDeadLetterStore.cs) (line 11).

`public interface ILatticeSchemaDeadLetterStore`

- `Task AppendAsync(string treeId, LatticeSchemaDeadLetterEntry entry, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<LatticeSchemaDeadLetterEntry> ListAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaPolicyStore`

[Source](../../src/lattice.schema/ILatticeSchemaPolicyStore.cs) (line 11).

`public interface ILatticeSchemaPolicyStore`

- `Task SetPolicyAsync(string treeId, LatticeSchemaPolicy policy, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaPolicy?> GetPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<bool> ClearPolicyAsync(string treeId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<KeyValuePair<string, LatticeSchemaPolicy>> ListPoliciesAsync(CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaRegistry`

[Source](../../src/lattice.schema/ILatticeSchemaRegistry.cs) (line 17).

`public interface ILatticeSchemaRegistry`

- `bool TryGetDescriptor(uint schemaId, uint version, out LatticeSchemaDescriptor descriptor)`
- `bool CanUpcast(uint schemaId, uint fromVersion, uint toVersion)`
- `byte[] Upcast(uint schemaId, uint fromVersion, uint toVersion, byte[] body)`

### `Orleans.Lattice.Schema.ILatticeSchemaRemediationAdmin`

[Source](../../src/lattice.schema/ILatticeSchemaRemediationAdmin.cs) (line 15).

`public interface ILatticeSchemaRemediationAdmin`

- `Task<LatticeSchemaRemediationReport> RemediateAsync( string treeId, LatticeValueTransform transform, LatticeSchemaPolicy targetPolicy, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync( string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaVersionAdmin`

[Source](../../src/lattice.schema/ILatticeSchemaVersionAdmin.cs) (line 17).

`public interface ILatticeSchemaVersionAdmin`

- `Task SetVersionConfigAsync( string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync( string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync( string treeId, uint newTargetVersion, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync( string treeId, CancellationToken cancellationToken = default)`
- `Task<bool> ClearVersionConfigAsync(string treeId, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeSchemaVersionProvider`

[Source](../../src/lattice.schema/ILatticeSchemaVersionProvider.cs) (line 9).

`public interface ILatticeSchemaVersionProvider`

- `bool StrictIngestEnabled { get; }`
- `ValueTask<LatticeSchemaVersionConfig?> GetConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `void Invalidate(string treeId)`

### `Orleans.Lattice.Schema.ILatticeSchemaVersionStore`

[Source](../../src/lattice.schema/ILatticeSchemaVersionStore.cs) (line 13).

`public interface ILatticeSchemaVersionStore`

- `Task SetConfigAsync(string treeId, LatticeSchemaVersionConfig config, CancellationToken cancellationToken = default)`
- `Task<LatticeSchemaVersionConfig?> GetConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `Task<bool> ClearConfigAsync(string treeId, CancellationToken cancellationToken = default)`
- `IAsyncEnumerable<KeyValuePair<string, LatticeSchemaVersionConfig>> ListConfigsAsync( CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.ILatticeValueTransform`

[Source](../../src/lattice.schema/ILatticeValueTransform.cs) (line 19).

`public interface ILatticeValueTransform`

- `string Id { get; }`
- `byte[] Transform(byte[] value)`

### `Orleans.Lattice.Schema.ILatticeValueTransformRegistry`

[Source](../../src/lattice.schema/ILatticeValueTransformRegistry.cs) (line 8).

`public interface ILatticeValueTransformRegistry`

- `bool TryGet(string id, out ILatticeValueTransform? transform)`
- `ILatticeValueTransform Get(string id)`

### `Orleans.Lattice.Schema.LatticeComputeOperator`

[Source](../../src/lattice.schema/LatticeComputeOperator.cs) (line 11).

`public enum LatticeComputeOperator : byte`

- `Concat = 0`
- `Coalesce = 1`

### `Orleans.Lattice.Schema.LatticeSchemaComplianceReport`

[Source](../../src/lattice.schema/LatticeSchemaComplianceReport.cs) (line 18).

`public readonly record struct LatticeSchemaComplianceReport`

- `public required string TreeId { get; init; }`
- `public required bool HasPolicy { get; init; }`
- `public required int CompliantCount { get; init; }`
- `public required int NonCompliantCount { get; init; }`
- `public required int ScannedCount { get; init; }`
- `public required IReadOnlyList<LatticeSchemaComplianceRuleCount> RuleBreakdown { get; init; }`
- `public static LatticeSchemaComplianceReport Ungoverned(string treeId)`

### `Orleans.Lattice.Schema.LatticeSchemaComplianceRuleCount`

[Source](../../src/lattice.schema/LatticeSchemaComplianceRuleCount.cs) (line 10).

`public readonly record struct LatticeSchemaComplianceRuleCount`

- `public required string Reason { get; init; }`
- `public required int Count { get; init; }`

### `Orleans.Lattice.Schema.LatticeSchemaDeadLetterEntry`

[Source](../../src/lattice.schema/LatticeSchemaDeadLetterEntry.cs) (line 14).

`public sealed class LatticeSchemaDeadLetterEntry`

- `public LatticeSchemaDeadLetterEntry( string key, byte[] valuePreview, int valueByteLength, string reason, LatticeSchemaDeadLetterSource source, DateTimeOffset timestampUtc)`
- `public string Key { get; }`
- `public byte[] ValuePreview`
- `public int ValueByteLength { get; }`
- `public string Reason { get; }`
- `public LatticeSchemaDeadLetterSource Source { get; }`
- `public DateTimeOffset TimestampUtc { get; }`

### `Orleans.Lattice.Schema.LatticeSchemaDeadLetterSource`

[Source](../../src/lattice.schema/LatticeSchemaDeadLetterSource.cs) (line 8).

`public enum LatticeSchemaDeadLetterSource : byte`

- `Replication = 0`
- `Restore = 1`
- `LocalRejected = 2`

### `Orleans.Lattice.Schema.LatticeSchemaDescriptor`

[Source](../../src/lattice.schema/LatticeSchemaDescriptor.cs) (line 10).

`public readonly record struct LatticeSchemaDescriptor`

- `public LatticeSchemaDescriptor(uint schemaId, uint version, string name)`
- `public uint SchemaId { get; }`
- `public uint Version { get; }`
- `public string Name { get; }`

### `Orleans.Lattice.Schema.LatticeSchemaEncodingKind`

[Source](../../src/lattice.schema/LatticeSchemaEncodingKind.cs) (line 8).

`public enum LatticeSchemaEncodingKind : byte`

- `Utf8 = 0`
- `Json = 1`
- `MaxByteLength = 2`

### `Orleans.Lattice.Schema.LatticeSchemaEnforcementOptions`

[Source](../../src/lattice.schema/LatticeSchemaEnforcementOptions.cs) (line 10).

`public sealed class LatticeSchemaEnforcementOptions`

- `public bool StrictIngest { get; set; }`
- `public bool ValidateCrdtMergeResults { get; set; }`
- `public int DeadLetterPreviewMaxBytes { get; set; }`

### `Orleans.Lattice.Schema.LatticeSchemaEnforcementServiceCollectionExtensions`

[Source](../../src/lattice.schema/LatticeSchemaEnforcementServiceCollectionExtensions.cs) (line 19).

`public static class LatticeSchemaEnforcementServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeSchemaEnforcement( this ISiloBuilder builder, Action<LatticeSchemaEnforcementOptions>? configure = null)`
- `public static ISiloBuilder ConfigureLatticeSchemaEnforcement( this ISiloBuilder builder, Action<LatticeSchemaEnforcementOptions> configure)`

### `Orleans.Lattice.Schema.LatticeSchemaEnvelope`

[Source](../../src/lattice.schema/LatticeSchemaEnvelope.cs) (line 38).

`public static class LatticeSchemaEnvelope`

- `public const byte Magic`
- `public const byte FormatVersion`
- `public const int HeaderLength`
- `public static bool IsEnveloped(ReadOnlySpan<byte> value)`
- `public static byte[] Encode(uint schemaId, uint version, ReadOnlySpan<byte> body)`
- `public static bool TryReadHeader(ReadOnlySpan<byte> value, out uint schemaId, out uint version)`
- `public static byte[] StripToBody(byte[] value)`

### `Orleans.Lattice.Schema.LatticeSchemaPolicy`

[Source](../../src/lattice.schema/LatticeSchemaPolicy.cs) (line 15).

`public sealed class LatticeSchemaPolicy`

- `public LatticeSchemaPolicy(IReadOnlyList<LatticeSchemaRule> rules, bool strictIngest = false)`
- `public IReadOnlyList<LatticeSchemaRule> Rules { get; }`
- `public bool StrictIngest { get; }`

### `Orleans.Lattice.Schema.LatticeSchemaPolicyValidator`

[Source](../../src/lattice.schema/LatticeSchemaPolicyValidator.cs) (line 16).

`public sealed class LatticeSchemaPolicyValidator`

- `public LatticeSchemaPolicyValidator(LatticeSchemaPolicy policy)`
- `public LatticeSchemaPolicy Policy { get; }`
- `public int RuleCount`
- `public string? Validate(byte[] value)`
- `public string? ValidateRule(int ruleIndex, byte[] value)`

### `Orleans.Lattice.Schema.LatticeSchemaRegistryBuilder`

[Source](../../src/lattice.schema/LatticeSchemaRegistryBuilder.cs) (line 15).

`public sealed class LatticeSchemaRegistryBuilder`

- `public LatticeSchemaRegistryBuilder AddSchema(uint schemaId, uint version, string name)`
- `public LatticeSchemaRegistryBuilder AddUpcaster( uint schemaId, uint fromVersion, uint toVersion, LatticeValueTransform transform)`
- `public LatticeSchemaRegistryBuilder AddUpcaster( uint schemaId, uint fromVersion, uint toVersion, string transformId)`
- `public LatticeSchemaRegistryBuilder AddUpcaster(LatticeSchemaUpcaster upcaster)`
- `public ILatticeSchemaRegistry Build(ILatticeValueTransformRegistry? transformRegistry = null)`

### `Orleans.Lattice.Schema.LatticeSchemaRemediation`

[Source](../../src/lattice.schema/LatticeSchemaRemediation.cs) (line 28).

`public static class LatticeSchemaRemediation`

- `public static async Task<LatticeSchemaRemediationOutcome> DryRunAsync( IAsyncEnumerable<KeyValuePair<string, byte[]>> entries, LatticeValueTransform transform, LatticeSchemaPolicy candidatePolicy, int previewMaxBytes = 4096, CancellationToken cancellationToken = default)`

### `Orleans.Lattice.Schema.LatticeSchemaRemediationOutcome`

[Source](../../src/lattice.schema/LatticeSchemaRemediationOutcome.cs) (line 18).

`public readonly record struct LatticeSchemaRemediationOutcome`

- `public bool Succeeded { get; }`
- `public int ScannedCount { get; }`
- `public string? OffendingKey { get; }`
- `public string? Reason { get; }`
- `public byte[]? OffendingValuePreview { get; }`
- `public static LatticeSchemaRemediationOutcome Success(int scannedCount)`
- `public static LatticeSchemaRemediationOutcome Aborted( int scannedCount, string offendingKey, string reason, byte[] offendingValuePreview)`
- `public bool Equals(LatticeSchemaRemediationOutcome other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Schema.LatticeSchemaRemediationPhase`

[Source](../../src/lattice.schema/LatticeSchemaRemediationPhase.cs) (line 10).

`public enum LatticeSchemaRemediationPhase`

- `Idle = 0`
- `DryRun = 1`
- `Build = 2`
- `Cutover = 3`
- `Completed = 4`
- `Aborted = 5`
- `Cancelled = 6`

### `Orleans.Lattice.Schema.LatticeSchemaRemediationReport`

[Source](../../src/lattice.schema/LatticeSchemaRemediationReport.cs) (line 14).

`public readonly record struct LatticeSchemaRemediationReport`

- `public LatticeSchemaRemediationPhase Phase { get; init; }`
- `public bool InProgress { get; init; }`
- `public int ScannedCount { get; init; }`
- `public string? OffendingKey { get; init; }`
- `public string? Reason { get; init; }`
- `public byte[]? OffendingValuePreview { get; init; }`
- `public string? DestinationTreeId { get; init; }`
- `public string? OperationId { get; init; }`
- `public bool Succeeded`
- `public bool DidAbort`
- `public bool WasCancelled`
- `public static LatticeSchemaRemediationReport Idle { get; }`
- `public static LatticeSchemaRemediationReport InFlight( LatticeSchemaRemediationPhase phase, int scannedCount, string? destinationTreeId, string? operationId)`
- `public static LatticeSchemaRemediationReport Completed( int scannedCount, string destinationTreeId, string operationId)`
- `public static LatticeSchemaRemediationReport Aborted( int scannedCount, string offendingKey, string reason, byte[] offendingValuePreview, string operationId)`
- `public static LatticeSchemaRemediationReport Cancelled(int scannedCount, string operationId)`
- `public bool Equals(LatticeSchemaRemediationReport other)`
- `public override int GetHashCode()`

### `Orleans.Lattice.Schema.LatticeSchemaReservedTrees`

[Source](../../src/lattice.schema/LatticeSchemaReservedTrees.cs) (line 11).

`public static class LatticeSchemaReservedTrees`

- `public static string Prefix`
- `public static string PolicyTreeId`
- `public static string DeadLetterTreeId`
- `public static string VersionConfigTreeId`
- `public static bool IsReserved(string treeId)`
- `public static void ThrowIfReserved(string treeId, string? paramName = null)`

### `Orleans.Lattice.Schema.LatticeSchemaRule`

[Source](../../src/lattice.schema/LatticeSchemaRule.cs) (line 16).

`public readonly record struct LatticeSchemaRule`

- `public LatticeSchemaRuleKind Kind { get; init; }`
- `public LatticePredicateNode? Predicate { get; init; }`
- `public string? RegexPattern { get; init; }`
- `public string? MemberPath { get; init; }`
- `public LatticeSchemaEncodingKind EncodingKind { get; init; }`
- `public int? MaxByteLength { get; init; }`
- `public string? Description { get; init; }`
- `public static LatticeSchemaRule Structured(LatticePredicateNode predicate, string? description = null)`
- `public static LatticeSchemaRule Regex(string pattern, string? memberPath = null, string? description = null)`
- `public static LatticeSchemaRule Utf8(string? description = null)`
- `public static LatticeSchemaRule Json(string? description = null)`
- `public static LatticeSchemaRule MaxLength(int maxByteLength, string? description = null)`

### `Orleans.Lattice.Schema.LatticeSchemaRuleKind`

[Source](../../src/lattice.schema/LatticeSchemaRuleKind.cs) (line 7).

`public enum LatticeSchemaRuleKind : byte`

- `Structured = 0`
- `Regex = 1`
- `Encoding = 2`

### `Orleans.Lattice.Schema.LatticeSchemaServiceCollectionExtensions`

[Source](../../src/lattice.schema/LatticeSchemaServiceCollectionExtensions.cs) (line 14).

`public static class LatticeSchemaServiceCollectionExtensions`

- `public static IServiceCollection AddLatticeValueTransform( this IServiceCollection services, ILatticeValueTransform transform)`
- `public static IServiceCollection AddLatticeValueTransform<TTransform>(this IServiceCollection services) where TTransform : class, ILatticeValueTransform`
- `public static ISiloBuilder AddLatticeValueTransform( this ISiloBuilder builder, ILatticeValueTransform transform)`
- `public static ISiloBuilder AddLatticeValueTransform<TTransform>(this ISiloBuilder builder) where TTransform : class, ILatticeValueTransform`

### `Orleans.Lattice.Schema.LatticeSchemaUpcaster`

[Source](../../src/lattice.schema/LatticeSchemaUpcaster.cs) (line 19).

`public sealed class LatticeSchemaUpcaster`

- `public uint SchemaId { get; }`
- `public uint FromVersion { get; }`
- `public uint ToVersion { get; }`
- `public string? TransformId`
- `public static LatticeSchemaUpcaster FromTransform( uint schemaId, uint fromVersion, uint toVersion, LatticeValueTransform transform)`
- `public static LatticeSchemaUpcaster FromTransformId( uint schemaId, uint fromVersion, uint toVersion, string transformId)`
- `public byte[] Apply(byte[] body, ILatticeValueTransformRegistry? transformRegistry)`

### `Orleans.Lattice.Schema.LatticeSchemaVersionConfig`

[Source](../../src/lattice.schema/LatticeSchemaVersionConfig.cs) (line 20).

`public readonly record struct LatticeSchemaVersionConfig`

- `public LatticeSchemaVersionConfig(uint schemaId, uint targetVersion, bool strictIngest = false)`
- `public uint SchemaId { get; init; }`
- `public uint TargetVersion { get; init; }`
- `public bool StrictIngest { get; init; }`

### `Orleans.Lattice.Schema.LatticeSchemaVersioningOptions`

[Source](../../src/lattice.schema/LatticeSchemaVersioningOptions.cs) (line 11).

`public sealed class LatticeSchemaVersioningOptions`

- `public bool StrictIngest { get; set; }`
- `public int DeadLetterPreviewMaxBytes { get; set; }`

### `Orleans.Lattice.Schema.LatticeSchemaVersioningServiceCollectionExtensions`

[Source](../../src/lattice.schema/LatticeSchemaVersioningServiceCollectionExtensions.cs) (line 21).

`public static class LatticeSchemaVersioningServiceCollectionExtensions`

- `public static ISiloBuilder AddLatticeSchemaVersioning( this ISiloBuilder builder, Action<LatticeSchemaRegistryBuilder>? configureRegistry = null, Action<LatticeSchemaVersioningOptions>? configureOptions = null)`
- `public static ISiloBuilder ConfigureLatticeSchemaVersioning( this ISiloBuilder builder, Action<LatticeSchemaVersioningOptions> configure)`

### `Orleans.Lattice.Schema.LatticeSchemaViolationException`

[Source](../../src/lattice.schema/LatticeSchemaViolationException.cs) (line 25).

`public sealed class LatticeSchemaViolationException : InvalidOperationException`

- `public string TreeId { get; }`
- `public string Key { get; }`
- `public string Reason { get; }`
- `public LatticeSchemaViolationException()`
- `public LatticeSchemaViolationException(string message)`
- `public LatticeSchemaViolationException(string message, Exception innerException)`
- `public LatticeSchemaViolationException(string treeId, string key, string reason)`

### `Orleans.Lattice.Schema.LatticeValueTransform`

[Source](../../src/lattice.schema/LatticeValueTransform.cs) (line 25).

`public readonly record struct LatticeValueTransform`

- `public LatticeValueTransformKind Kind { get; init; }`
- `public string? MemberPath { get; init; }`
- `public string? ToPath { get; init; }`
- `public LatticeConstant Constant { get; init; }`
- `public LatticePredicateNode Condition { get; init; }`
- `public LatticeComputeOperator ComputeOperator { get; init; }`
- `public LatticeValueTransform[]? Children { get; init; }`
- `public bool Equals(LatticeValueTransform other)`
- `public override int GetHashCode()`
- `public static LatticeValueTransform Passthrough(params LatticeValueTransform[] operations)`
- `public static LatticeValueTransform SetMember(string path, LatticeValueTransform valueExpression)`
- `public static LatticeValueTransform DropMember(string path)`
- `public static LatticeValueTransform RenameMember(string fromPath, string toPath)`
- `public static LatticeValueTransform Member(string path)`
- `public static LatticeValueTransform Const(LatticeConstant constant)`
- `public static LatticeValueTransform Conditional( LatticePredicateNode condition, LatticeValueTransform thenExpression, LatticeValueTransform elseExpression)`
- `public static LatticeValueTransform Compute(LatticeComputeOperator op, params LatticeValueTransform[] operands)`

### `Orleans.Lattice.Schema.LatticeValueTransformEvaluation`

[Source](../../src/lattice.schema/LatticeValueTransformEvaluation.cs) (line 18).

`public static class LatticeValueTransformEvaluation`

- `public static byte[] Evaluate(byte[]? value, in LatticeValueTransform transform)`

### `Orleans.Lattice.Schema.LatticeValueTransformKind`

[Source](../../src/lattice.schema/LatticeValueTransformKind.cs) (line 18).

`public enum LatticeValueTransformKind : byte`

- `Passthrough = 0`
- `SetMember = 1`
- `DropMember = 2`
- `RenameMember = 3`
- `Member = 4`
- `Constant = 5`
- `Conditional = 6`
- `Compute = 7`

### `Orleans.Lattice.Schema.LatticeValueTransformTranslator`

[Source](../../src/lattice.schema/LatticeValueTransformTranslator.cs) (line 27).

`public static class LatticeValueTransformTranslator`

- `public static LatticeValueTransform Translate<T>(Expression<Func<T, T>> transform)`
- `public static LatticeValueTransform Translate<TOld, TNew>(Expression<Func<TOld, TNew>> transform)`

### `Orleans.Lattice.Schema.SchemaComplianceScanOperation`

[Source](../../src/lattice.schema/SchemaComplianceScanOperation.cs) (line 16).

`public static class SchemaComplianceScanOperation`

- `public const string Kind`
- `public const string CountingPhase`
- `public const string ScanningPhase`
- `public const string EntriesUnit`

### `Orleans.Lattice.Schema.SchemaComplianceScanResults`

[Source](../../src/lattice.schema/SchemaComplianceScanResults.cs) (line 13).

`public static class SchemaComplianceScanResults`

- `public const string TreeIdKey`
- `public const string HasPolicyKey`
- `public const string CompliantCountKey`
- `public const string NonCompliantCountKey`
- `public const string ScannedCountKey`
- `public const string RuleCountKey`
- `public const string RulePrefix`
- `public static IReadOnlyDictionary<string, string> ToResultMap(LatticeSchemaComplianceReport report)`
- `public static bool TryReadReport(IReadOnlyDictionary<string, string> result, out LatticeSchemaComplianceReport report)`

### `Orleans.Lattice.Schema.SchemaOperationKinds`

[Source](../../src/lattice.schema/SchemaOperationKinds.cs) (line 8).

`public static class SchemaOperationKinds`

- `public const string Prefix`
- `public const string Remediation`
- `public const string Migration`
- `public const string AdvanceAndMigrate`

### `Orleans.Lattice.Schema.SchemaOperationPhases`

[Source](../../src/lattice.schema/SchemaOperationPhases.cs) (line 8).

`public static class SchemaOperationPhases`

- `public const string Advance`
- `public const string DryRun`
- `public const string Build`
- `public const string Cutover`
- `public const string ValuesUnit`

### `Orleans.Lattice.Schema.SchemaOperationResultKeys`

[Source](../../src/lattice.schema/SchemaOperationResultKeys.cs) (line 9).

`public static class SchemaOperationResultKeys`

- `public const string Outcome`
- `public const string ValuesProcessed`
- `public const string OffendingKey`
- `public const string Reason`
- `public const string RemediationOperationId`
- `public const string Completed`
- `public const string Aborted`
- `public const string Cancelled`
