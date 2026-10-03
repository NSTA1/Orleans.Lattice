namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Sequence helpers over the per-pass <see cref="RepoFileEntry"/> lists the
/// reconcilers walk.
/// </summary>
internal static class RepoFileEntrySequence
{
    /// <summary>
    /// Lazily yields every entry of <paramref name="first"/>, then
    /// <paramref name="second"/>, then <paramref name="third"/>, in order.
    /// </summary>
    /// <param name="first">The first list.</param>
    /// <param name="second">The second list.</param>
    /// <param name="third">The third list.</param>
    internal static IEnumerable<RepoFileEntry> Concat(
        IReadOnlyList<RepoFileEntry> first,
        IReadOnlyList<RepoFileEntry> second,
        IReadOnlyList<RepoFileEntry> third)
    {
        foreach (var entry in first)
        {
            yield return entry;
        }

        foreach (var entry in second)
        {
            yield return entry;
        }

        foreach (var entry in third)
        {
            yield return entry;
        }
    }
}
