namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>One property of a described provider's JSON payload object.</summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewProviderField)]
[Immutable]
public sealed record TreeViewProviderField
{
    /// <summary>The JSON property name.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>The label a form shows for the property.</summary>
    [Id(1)] public required string Label { get; init; }

    /// <summary>An optional one-line explanation of the property.</summary>
    [Id(2)] public string? Description { get; init; }

    /// <summary>Whether the property must be supplied.</summary>
    [Id(3)] public bool Required { get; init; }

    /// <summary>The JSON value type of the property.</summary>
    [Id(4)] public TreeViewProviderFieldKind Kind { get; init; }
}
