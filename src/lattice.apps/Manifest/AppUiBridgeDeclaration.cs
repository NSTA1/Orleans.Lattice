namespace Orleans.Lattice.Apps;

/// <summary>One bridge operation an app UI requests, shown and consented alongside the capability ceiling.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiBridgeDeclaration)]
public sealed record AppUiBridgeDeclaration
{
    /// <summary>A member of <see cref="AppUiBridgeOperations.All"/>; an unknown operation is a validation error.</summary>
    [Id(0)] public required string Operation { get; init; }

    /// <summary>
    /// For a <c>data.*</c> operation, the specific declared trees it covers; omit to cover every
    /// tree the app declares. Must be omitted for any other operation, and may never name a tree
    /// the app does not declare.
    /// </summary>
    [Id(1)] public string[]? Trees { get; init; }
}
