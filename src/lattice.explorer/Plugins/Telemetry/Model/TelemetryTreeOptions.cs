namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The tree ids a panel offers in its tree filter, derived from the series the
/// facade actually returned.
/// </summary>
/// <remarks>
/// <para>
/// <b>The options are discovered, never typed.</b> There is no text input
/// anywhere in the telemetry surface, and this is why one is not needed: the
/// filter offers exactly the trees present in the last answer, so it can only
/// ever express a narrowing of data the caller has already been served. A tree
/// the caller may not see never appears in a result, so it never appears in this
/// list, and a filter is therefore incapable of widening anything.
/// </para>
/// <para>
/// <b>It is a presentation filter and not an authorization boundary.</b>
/// Narrowing happens client-side over series the facade already released;
/// scoping is the facade's job and is enforced there, because a desktop head
/// enforcing it would be trivially bypassable.
/// </para>
/// </remarks>
public static class TelemetryTreeOptions
{
    private static readonly string[] NoTrees = [];

    /// <summary>The option value standing for "do not filter".</summary>
    public const string AllTreesValue = "";

    /// <summary>The label the "do not filter" option renders.</summary>
    public const string AllTreesLabel = "All trees";

    /// <summary>
    /// The distinct tree ids present in <paramref name="result"/>, in ascending
    /// ordinal order so the list does not reshuffle between refreshes.
    /// </summary>
    /// <param name="result">The last evaluated result, or <see langword="null"/>.</param>
    /// <returns>
    /// The tree ids, empty when the result carried none - in which case the
    /// control offers only "all trees" and there is nothing to narrow to.
    /// </returns>
    public static IReadOnlyList<string> For(ExplorerTelemetryResult? result)
    {
        if (result is null || result.Series.Count == 0)
        {
            return NoTrees;
        }

        List<string>? trees = null;
        var series = result.Series;
        for (var i = 0; i < series.Count; i++)
        {
            if (!series[i].TryGetLabel(TelemetryLabelNames.Tree, out var tree) || tree.Length == 0)
            {
                continue;
            }

            trees ??= new List<string>(series.Count);
            if (!trees.Contains(tree, StringComparer.Ordinal))
            {
                trees.Add(tree);
            }
        }

        if (trees is null)
        {
            return NoTrees;
        }

        trees.Sort(StringComparer.Ordinal);
        return trees;
    }

    /// <summary>
    /// Whether <paramref name="tree"/> is still offered by
    /// <paramref name="options"/>, so a filter chosen against one answer can be
    /// dropped when the next answer no longer contains that tree.
    /// </summary>
    /// <param name="options">The tree ids currently offered.</param>
    /// <param name="tree">The retained filter, or <see langword="null"/> for none.</param>
    /// <returns>
    /// <see langword="true"/> when the filter is unset (always legal) or still
    /// present.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public static bool IsOffered(IReadOnlyList<string> options, string? tree)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrEmpty(tree))
        {
            return true;
        }

        for (var i = 0; i < options.Count; i++)
        {
            if (string.Equals(options[i], tree, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
