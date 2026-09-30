using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The Cluster area's address grammar, in one place, so every link it draws and
/// every address it reads agree:
/// <list type="bullet">
/// <item><c>/cluster</c> - the estate overview.</item>
/// <item><c>/cluster/trees</c> - every tree.</item>
/// <item><c>/cluster/trees/{tree-path}</c> - one tree, its logical id's parts as segments.</item>
/// <item><c>/cluster/trees/{tree-path}/{view}</c> - <c>tools</c>, <c>reshard</c>, <c>resize</c> or <c>snapshot</c>.</item>
/// <item><c>/cluster/wal?tree=&amp;partition=&amp;target=</c> - WAL placement and moves.</item>
/// <item><c>/cluster/orphans?tree=</c> - orphaned leaves.</item>
/// </list>
/// </summary>
/// <remarks>
/// A tree path of two or more parts whose last part is a view word always reads
/// as that view. The page of a tree whose own last part is a view word is
/// therefore addressed with a trailing <c>overview</c>
/// (<c>/cluster/trees/jobs/tools/overview</c>), which keeps the grammar
/// unambiguous without a catch-all route.
/// </remarks>
internal static class ClusterAddresses
{
    /// <summary>The area key.</summary>
    public const string AreaKey = "cluster";

    /// <summary>The trees segment.</summary>
    public const string TreesSegment = "trees";

    /// <summary>The WAL segment.</summary>
    public const string WalSegment = "wal";

    /// <summary>The orphaned-leaves segment.</summary>
    public const string OrphansSegment = "orphans";

    /// <summary>The query key naming a tree on the WAL and orphans pages.</summary>
    public const string TreeQuery = "tree";

    /// <summary>The query key naming a WAL partition.</summary>
    public const string PartitionQuery = "partition";

    /// <summary>The query key naming a WAL move's target provider key.</summary>
    public const string TargetQuery = "target";

    /// <summary>The query key naming the open tab of a tree's page; absent means <see cref="SummaryTab"/>.</summary>
    public const string TabQuery = "tab";

    /// <summary>The tree page's default tab.</summary>
    public const string SummaryTab = "summary";

    /// <summary>
    /// The deepest path the area's routes match below <c>/cluster</c>. A tree whose
    /// logical id has more parts than fit is listed but has no address.
    /// </summary>
    public const int MaximumPathSegments = 8;

    private const string OverviewWord = "overview";

    private static readonly Dictionary<string, ClusterTreeView> ViewWords = new(StringComparer.Ordinal)
    {
        [OverviewWord] = ClusterTreeView.Overview,
        ["tools"] = ClusterTreeView.Tools,
        ["reshard"] = ClusterTreeView.Reshard,
        ["resize"] = ClusterTreeView.Resize,
        ["snapshot"] = ClusterTreeView.Snapshot,
    };

    /// <summary><c>/cluster</c>.</summary>
    public static ExplorerAddress Overview { get; } = ExplorerAddress.ForArea(AreaKey);

    /// <summary><c>/cluster/trees</c>.</summary>
    public static ExplorerAddress Trees { get; } = ExplorerAddress.ForArea(AreaKey, TreesSegment);

    /// <summary>The address of <paramref name="treeId"/>'s <paramref name="view"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="view">The view.</param>
    /// <returns>The address.</returns>
    /// <exception cref="ArgumentException">The tree has no address: an empty part, or too deep.</exception>
    public static ExplorerAddress Tree(string treeId, ClusterTreeView view = ClusterTreeView.Overview) =>
        TryTree(treeId, view, out var address)
            ? address
            : throw new ArgumentException($"The tree '{treeId}' has no Cluster address.", nameof(treeId));

    /// <summary>Tries to build the address of <paramref name="treeId"/>'s <paramref name="view"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="view">The view.</param>
    /// <param name="address">The address, when there is one.</param>
    /// <returns><see langword="true"/> when the tree has an address.</returns>
    public static bool TryTree(string? treeId, ClusterTreeView view, [NotNullWhen(true)] out ExplorerAddress? address)
    {
        address = null;
        if (string.IsNullOrEmpty(treeId))
        {
            return false;
        }

        var parts = treeId.Split('/');
        if (parts.Any(string.IsNullOrEmpty))
        {
            return false;
        }

        var segments = new List<string>(parts.Length + 2) { TreesSegment };
        segments.AddRange(parts);

        if (view != ClusterTreeView.Overview)
        {
            segments.Add(WordOf(view));
        }
        else if (parts.Length >= 2 && ViewWords.ContainsKey(parts[^1]))
        {
            segments.Add(OverviewWord);
        }

        if (segments.Count > MaximumPathSegments)
        {
            return false;
        }

        try
        {
            address = ExplorerAddress.ForArea(AreaKey, [.. segments]);
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary><c>/cluster/wal</c>, optionally scoped to a tree and a planned move.</summary>
    /// <param name="treeId">The tree, or <see langword="null"/>.</param>
    /// <param name="partition">The partition to plan a move for, or <see langword="null"/>.</param>
    /// <param name="target">The target provider key, or <see langword="null"/>.</param>
    /// <returns>The address.</returns>
    public static ExplorerAddress Wal(string? treeId = null, int? partition = null, string? target = null) =>
        ExplorerAddress.ForArea(AreaKey, WalSegment)
            .WithQuery(TreeQuery, NullIfEmpty(treeId))
            .WithQuery(PartitionQuery, partition?.ToString(CultureInfo.InvariantCulture))
            .WithQuery(TargetQuery, NullIfEmpty(target));

    /// <summary><c>/cluster/orphans</c>, optionally scoped to a tree.</summary>
    /// <param name="treeId">The tree, or <see langword="null"/>.</param>
    /// <returns>The address.</returns>
    public static ExplorerAddress Orphans(string? treeId = null) =>
        ExplorerAddress.ForArea(AreaKey, OrphansSegment).WithQuery(TreeQuery, NullIfEmpty(treeId));

    /// <summary>Reads <paramref name="address"/>.</summary>
    /// <param name="address">A Cluster address.</param>
    /// <returns>The page it names, or <see langword="null"/> when it names none.</returns>
    public static ClusterLocation? Parse(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);

        if (!string.Equals(address.Area, AreaKey, StringComparison.Ordinal))
        {
            return null;
        }

        var path = address.Path;
        if (path.Count == 0)
        {
            return new ClusterLocation(ClusterPageKind.Overview);
        }

        switch (path[0])
        {
            case TreesSegment when path.Count == 1:
                return new ClusterLocation(ClusterPageKind.Trees);

            case TreesSegment:
                var parts = path.Skip(1).ToList();
                var view = ClusterTreeView.Overview;
                if (parts.Count >= 2 && ViewWords.TryGetValue(parts[^1], out var word))
                {
                    view = word;
                    parts.RemoveAt(parts.Count - 1);
                }

                return new ClusterLocation(ClusterPageKind.Tree, string.Join('/', parts), view);

            case WalSegment when path.Count == 1:
                var partition = int.TryParse(address.GetQuery(PartitionQuery), NumberStyles.None, CultureInfo.InvariantCulture, out var index)
                    ? index
                    : (int?)null;
                return new ClusterLocation(
                    ClusterPageKind.Wal,
                    NullIfEmpty(address.GetQuery(TreeQuery)),
                    Partition: partition,
                    Target: NullIfEmpty(address.GetQuery(TargetQuery)));

            case OrphansSegment when path.Count == 1:
                return new ClusterLocation(ClusterPageKind.Orphans, NullIfEmpty(address.GetQuery(TreeQuery)));

            default:
                return null;
        }
    }

    /// <summary>
    /// How the address line groups <paramref name="address"/>'s path: for a tree's
    /// page, the trees node, the whole logical tree id as one node, then the view
    /// word when there is one. <see langword="null"/> for every other page.
    /// </summary>
    /// <param name="address">A Cluster address.</param>
    /// <returns>The spans, or <see langword="null"/> for one node per segment.</returns>
    public static IReadOnlyList<int>? ChainSpans(ExplorerAddress address)
    {
        if (Parse(address) is not { Kind: ClusterPageKind.Tree, TreeId: { Length: > 0 } treeId })
        {
            return null;
        }

        var treeParts = treeId.Split('/').Length;
        var rest = address.Path.Count - 1 - treeParts;
        return rest > 0 ? [1, treeParts, rest] : [1, treeParts];
    }

    private static string WordOf(ClusterTreeView view) => view switch
    {
        ClusterTreeView.Tools => "tools",
        ClusterTreeView.Reshard => "reshard",
        ClusterTreeView.Resize => "resize",
        ClusterTreeView.Snapshot => "snapshot",
        _ => OverviewWord,
    };

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
