namespace Orleans.Lattice.Schema;

/// <summary>
/// An append-only, tree-scoped schema catalog. Existing versions and hops cannot be
/// replaced or removed because stored envelopes may still depend on them.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRuntimeRegistration)]
[Immutable]
public sealed record LatticeSchemaRuntimeRegistration
{
    /// <summary>The registered schema versions.</summary>
    [Id(0)] public LatticeSchemaDescriptor[] Descriptors { get; init; } = [];
    /// <summary>The registered migration hops.</summary>
    [Id(1)] public LatticeSchemaRuntimeUpcaster[] Upcasters { get; init; } = [];
    /// <summary>Discovery-only stable ids provisioned by this host; registration never installs code.</summary>
    [Id(2)] public string[] SupportedTransformIds { get; init; } = [];
}
