namespace Orleans.Lattice.Apps;

/// <summary>A schema-family binding, inspected as data without loading schema or migration code.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppSchemaDeclaration), Immutable]
public sealed record AppSchemaDeclaration
{
    /// <summary>Declared local tree whose envelopes use this schema.</summary>
    [Id(0)] public required string Tree { get; init; }

    /// <summary>Host-registered schema family identifier.</summary>
    [Id(1)] public required string Family { get; init; }

    /// <summary>Positive target envelope version.</summary>
    [Id(2)] public required int Version { get; init; }

    /// <summary>Whether invalid incoming envelopes should be diverted by schema enforcement.</summary>
    [Id(3)] public bool StrictIngest { get; init; }
}
