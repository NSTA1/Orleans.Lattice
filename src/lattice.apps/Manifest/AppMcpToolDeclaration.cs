namespace Orleans.Lattice.Apps;

/// <summary>Inspectable MCP tool metadata; a declaration never causes a handler to load.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppMcpToolDeclaration), Immutable]
public sealed record AppMcpToolDeclaration
{
    /// <summary>Unique app-local tool name; the exposed name is <c>{slug}_{name}</c>.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Human-readable purpose shown during inspection.</summary>
    [Id(1)] public required string Description { get; init; }

    /// <summary>Declared role required for the tool, resolved against the manifest's flat role set.</summary>
    [Id(2)] public required string Role { get; init; }
}
