namespace Orleans.Lattice.Schema;

/// <summary>A durable migration hop using declarative IR or a stable host transform id, never executable code.</summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaRuntimeUpcaster)]
[Immutable]
public sealed record LatticeSchemaRuntimeUpcaster
{
    /// <summary>The schema family.</summary>
    [Id(0)] public uint SchemaId { get; init; }
    /// <summary>The source version.</summary>
    [Id(1)] public uint FromVersion { get; init; }
    /// <summary>The strictly greater destination version.</summary>
    [Id(2)] public uint ToVersion { get; init; }
    /// <summary>The declarative transform, mutually exclusive with <see cref="TransformId"/>.</summary>
    [Id(3)] public LatticeValueTransform? Transform { get; init; }
    /// <summary>A stable transform id installed on every serving host.</summary>
    [Id(4)] public string? TransformId { get; init; }
}
