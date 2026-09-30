namespace Orleans.Lattice.Apps;

/// <summary>
/// One normalised unit of bridge consent: an operation, and for a <c>data.*</c> operation the
/// specific declared tree it covers, or <see langword="null"/> for every declared tree. A
/// non-data operation always has a null tree.
/// </summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppUiBridgeGrant), Immutable]
public readonly record struct AppUiBridgeGrant
{
    /// <summary>Creates a grant; validity is checked when it is added to an <see cref="AppUiBridgeRequest"/>.</summary>
    /// <param name="operation">A member of <see cref="AppUiBridgeOperations.All"/>.</param>
    /// <param name="tree">The declared tree a data operation covers, or null for every declared tree.</param>
    public AppUiBridgeGrant(string operation, string? tree = null)
    {
        Operation = operation;
        Tree = tree;
    }

    /// <summary>The bridge operation.</summary>
    [Id(0)] public string Operation { get; init; }

    /// <summary>The declared tree a data operation covers, or null for every declared tree (and for any non-data operation).</summary>
    [Id(1)] public string? Tree { get; init; }
}
