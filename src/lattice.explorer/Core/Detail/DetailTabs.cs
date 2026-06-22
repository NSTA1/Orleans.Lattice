namespace Orleans.Lattice.Explorer.Core.Detail;

/// <summary>
/// Central definition of the detail tab strip: the ordered set of tabs, their
/// display labels, and the default. Keeping this in one place lets the shell and
/// the individual tab features agree on order and naming.
/// </summary>
public static class DetailTabs
{
    /// <summary>The tab selected when a tree or view is first shown.</summary>
    public const DetailTab Default = DetailTab.Metrics;

    /// <summary>The tabs in left-to-right display order.</summary>
    public static IReadOnlyList<DetailTab> Ordered { get; } = new[]
    {
        DetailTab.Metrics,
        DetailTab.Topology,
        DetailTab.Data,
    };

    /// <summary>The human-readable label for <paramref name="tab"/>.</summary>
    public static string Label(DetailTab tab) => tab switch
    {
        DetailTab.Metrics => "Metrics",
        DetailTab.Topology => "Topology",
        DetailTab.Data => "Data",
        _ => throw new ArgumentOutOfRangeException(nameof(tab), tab, "Unknown detail tab."),
    };
}
