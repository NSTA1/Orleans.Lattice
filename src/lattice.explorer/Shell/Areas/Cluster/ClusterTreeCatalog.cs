using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// The circuit's list of trees, by logical id, read from Core's state connection
/// and remembered briefly so completions do not page the catalogue on every
/// keystroke.
/// </summary>
/// <remarks>
/// Physical trees are filtered out: a resize or restore shadow appears in the
/// registry beside the logical tree that aliases it, and the Cluster area never
/// shows a physical id (a restore shadow is flagged by the registry; a resize
/// shadow is the target of another entry's alias).
/// </remarks>
/// <param name="facades">The area's facades.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal sealed class ClusterTreeCatalog(ClusterFacades facades, TimeProvider time)
{
    /// <summary>How long a read catalogue is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    /// <summary>The most pages one read follows.</summary>
    public const int MaximumPages = 50;

    private IReadOnlyList<ClusterTreeEntry>? _trees;
    private DateTimeOffset _readAt;

    /// <summary>Whether the last read stopped at <see cref="MaximumPages"/> with more to come.</summary>
    public bool Truncated { get; private set; }

    /// <summary>Lists the trees.</summary>
    /// <param name="refresh">Read again even when the remembered list is fresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The trees, ordered by logical id.</returns>
    /// <exception cref="InvalidOperationException">No cluster connection is configured.</exception>
    public async ValueTask<IReadOnlyList<ClusterTreeEntry>> GetAsync(bool refresh, CancellationToken cancellationToken)
    {
        if (!refresh && _trees is { } remembered && time.GetUtcNow() - _readAt < Freshness)
        {
            return remembered;
        }

        if (facades.Session is not { IsConfigured: true } session)
        {
            throw new InvalidOperationException("Connect to a cluster to list its trees.");
        }

        var entries = new List<TreeCatalogEntry>();
        string? token = null;
        var pages = 0;
        do
        {
            var page = await session.Connection
                .ListTreesAsync(new CatalogRequest { PageSize = CatalogRequest.MaxPageSize, PageToken = token }, cancellationToken)
                .ConfigureAwait(false);
            entries.AddRange(page.Entries);
            token = page.NextPageToken;
            pages++;
        }
        while (token is not null && pages < MaximumPages);

        Truncated = token is not null;
        _trees = Project(entries);
        _readAt = time.GetUtcNow();
        return _trees;
    }

    /// <summary>Forgets the remembered list, so the next read goes to the cluster.</summary>
    public void Invalidate() => _trees = null;

    /// <summary>Projects catalogue entries onto logical trees, dropping every physical shadow.</summary>
    /// <param name="entries">The raw catalogue.</param>
    /// <returns>The logical trees, ordered by id.</returns>
    internal static IReadOnlyList<ClusterTreeEntry> Project(IEnumerable<TreeCatalogEntry> entries)
    {
        var all = entries.ToList();
        var shadows = all
            .Where(entry => entry.IsAlias && entry.PhysicalTreeId is not null)
            .Select(entry => entry.PhysicalTreeId!)
            .ToHashSet(StringComparer.Ordinal);

        return all
            .Where(entry => entry.RestoreShadowOfTreeId is null && !shadows.Contains(entry.TreeId))
            .Select(entry => new ClusterTreeEntry(
                ClusterTreeName.Parse(entry.TreeId),
                entry.IsAlias,
                entry.Lifecycle,
                entry.ShardCount,
                entry.Config.VirtualShardCount,
                entry.Config.WalPartitions))
            .OrderBy(entry => entry.TreeId, StringComparer.Ordinal)
            .ToArray();
    }
}
