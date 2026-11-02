namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The import modes a caller may use on a tree (issue #5365), as the facades' capability
/// probes answered. Default-deny: <see cref="None"/> draws no Import control.
/// </summary>
/// <param name="CanUpsert">Whether the caller may write the tree, which an upsert needs.</param>
/// <param name="CanSeed">Whether the caller holds the tree's bulk-load grant, which a seed needs.</param>
internal readonly record struct DataImportRights(bool CanUpsert, bool CanSeed)
{
    /// <summary>No import at all.</summary>
    public static DataImportRights None => default;

    /// <summary>Whether any import mode is open to the caller.</summary>
    public bool Any => CanUpsert || CanSeed;

    /// <summary>Whether the caller may import in <paramref name="mode"/>.</summary>
    /// <param name="mode">The mode.</param>
    public bool Allows(DataImportMode mode) => mode == DataImportMode.Seed ? CanSeed : CanUpsert;
}
