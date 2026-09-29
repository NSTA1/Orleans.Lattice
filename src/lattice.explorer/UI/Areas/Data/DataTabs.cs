namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The tree workspace's tabs and the query keys the Data area reads. The tab is a
/// query parameter (<c>?tab=history</c>) rather than a path segment, because every
/// path segment below the area belongs to the logical tree id.
/// </summary>
internal static class DataTabs
{
    /// <summary>The query key naming the open tab; absent means <see cref="Keys"/>.</summary>
    public const string TabQuery = "tab";

    /// <summary>The query key naming a tag index.</summary>
    public const string IndexQuery = "index";

    /// <summary>The query key naming a tag within a tag index.</summary>
    public const string TagQuery = "tag";

    /// <summary>The query key holding the directory's filter text.</summary>
    public const string FilterQuery = "filter";

    /// <summary>The key and prefix browser.</summary>
    public const string Keys = "keys";

    /// <summary>Revisions, timeline, diffs and the live tail.</summary>
    public const string History = "history";

    /// <summary>The per-tree measures.</summary>
    public const string Metrics = "metrics";

    /// <summary>The strict-mode dead-letter queue.</summary>
    public const string DeadLetters = "dead-letters";

    /// <summary>The tag indexes over the tree.</summary>
    public const string TagIndexes = "tag-indexes";

    /// <summary>The views over the tree.</summary>
    public const string Views = "views";

    /// <summary>Every tab, in the order the tab row shows them, with its title.</summary>
    public static IReadOnlyList<(string Id, string Title)> All { get; } =
    [
        (Keys, "Keys"),
        (History, "History"),
        (Metrics, "Metrics"),
        (DeadLetters, "Dead letters"),
        (TagIndexes, "Tag indexes"),
        (Views, "Views"),
    ];

    /// <summary>The tab a query value names, falling back to <see cref="Keys"/> for none or an unknown one.</summary>
    /// <param name="value">The <c>tab</c> query value.</param>
    public static string Parse(string? value)
    {
        foreach (var (id, _) in All)
        {
            if (string.Equals(id, value, StringComparison.Ordinal))
            {
                return id;
            }
        }

        return Keys;
    }
}
