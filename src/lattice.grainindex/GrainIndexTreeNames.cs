namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The reserved naming namespace for lattice trees owned by a grain index.
/// <para>
/// Every tree a grain index writes into is named under
/// <see cref="ReservedPrefix"/>. The prefix makes an index-owned tree
/// identifiable by name alone, which serves two purposes: it marks the tree as
/// cluster-local by intent (a grain index points at grain activations in one
/// cluster), and it gives authors of a custom replication resolver a documented
/// "do not select these" namespace they can screen out with
/// <see cref="IsIndexOwned(string)"/>.
/// </para>
/// </summary>
public static class GrainIndexTreeNames
{
    /// <summary>
    /// The reserved tree-name prefix every grain-index-owned tree carries.
    /// Host-owned trees must never be named under it.
    /// </summary>
    public const string ReservedPrefix = "__grainindex/";

    /// <summary>
    /// Builds the default tree name for the index called
    /// <paramref name="indexName"/>, by placing it under
    /// <see cref="ReservedPrefix"/>.
    /// </summary>
    /// <param name="indexName">The logical index name. Must not be <c>null</c>, empty, or white space.</param>
    /// <returns>The reserved tree name backing that index.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> is empty or white space.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    public static string ForIndex(string indexName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(indexName);
        return ReservedPrefix + indexName;
    }

    /// <summary>
    /// Reports whether <paramref name="treeName"/> names a tree inside the
    /// reserved grain-index namespace.
    /// </summary>
    /// <param name="treeName">The tree name to test.</param>
    /// <returns><c>true</c> when the name starts with <see cref="ReservedPrefix"/>; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeName"/> is <c>null</c>.</exception>
    public static bool IsIndexOwned(string treeName)
    {
        ArgumentNullException.ThrowIfNull(treeName);
        return treeName.StartsWith(ReservedPrefix, StringComparison.Ordinal);
    }
}
