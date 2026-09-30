using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The circuit's list of trees, by logical id, read from Core's state connection
/// and remembered briefly so completions do not page the catalogue on every
/// keystroke.
/// </summary>
/// <remarks>
/// Physical trees are filtered out: a resize or restore shadow appears in the
/// registry beside the logical tree that aliases it, and the Cluster area never
/// shows a physical id (a restore shadow is flagged by the registry; a resize
/// shadow is the target of another entry's alias). The remembered list is keyed
/// on the caller (<see cref="ClusterFacades.Caller"/>), so a sign-in, a sign-out, a
/// new connection or a tenant switch reads it again.
/// </remarks>
/// <param name="facades">The area's facades.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal sealed class ClusterTreeCatalog(ClusterFacades facades, TimeProvider time)
{
    /// <summary>How long a read catalogue is reused.</summary>
    public static readonly TimeSpan Freshness = TimeSpan.FromSeconds(30);

    /// <summary>The most pages one read follows.</summary>
    public const int MaximumPages = 50;

    private Remembered? _remembered;

    /// <summary>Whether the last read stopped at <see cref="MaximumPages"/> with more to come.</summary>
    public bool Truncated { get; private set; }

    /// <summary>Lists the trees.</summary>
    /// <param name="refresh">Read again even when the remembered list is fresh.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The trees, ordered by logical id.</returns>
    /// <exception cref="InvalidOperationException">No cluster connection is configured.</exception>
    public async ValueTask<IReadOnlyList<ClusterTreeEntry>> GetAsync(bool refresh, CancellationToken cancellationToken)
    {
        var caller = facades.Caller;
        if (!refresh
            && _remembered is { } remembered
            && remembered.Caller == caller
            && time.GetUtcNow() - remembered.ReadAt < Freshness)
        {
            return remembered.Trees;
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
        var trees = Project(entries);

        // Remembered only for the caller it was read for, and only while that is
        // still the caller.
        if (facades.Caller == caller)
        {
            _remembered = new Remembered(trees, caller, time.GetUtcNow());
        }

        return trees;
    }

    /// <summary>Forgets the remembered list, so the next read goes to the cluster.</summary>
    public void Invalidate() => _remembered = null;

    /// <summary>
    /// The trees a listing for <paramref name="scope"/> shows: every tree the
    /// cluster listed on a cluster-wide address, and only the scope tenant's own
    /// on a tenant-rooted one. The cluster lists every tenant's trees to the
    /// reserved default tenant, so the narrowing is needed there above all.
    /// </summary>
    /// <param name="trees">The catalogue.</param>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/> for the cluster-wide listing.</param>
    /// <returns>The trees in scope; <paramref name="trees"/> itself when nothing is left out.</returns>
    public static IReadOnlyList<ClusterTreeEntry> InScope(IReadOnlyList<ClusterTreeEntry> trees, string? scope)
    {
        ArgumentNullException.ThrowIfNull(trees);
        if (scope is null || trees.All(tree => ShellAssertedTenant.Lists(scope, tree.TreeId)))
        {
            return trees;
        }

        return [.. trees.Where(tree => ShellAssertedTenant.Lists(scope, tree.TreeId))];
    }

    /// <summary>
    /// Whether a tenant-rooted address for <paramref name="scope"/> may name
    /// <paramref name="treeId"/>: the scope tenant's own qualified tree, or, under
    /// a tenant other than the default, a bare name, which the cluster reads as
    /// that tenant's own tree. Another tenant's tree, and a system tree, are never
    /// named. Every tree may be named on a cluster-wide address.
    /// </summary>
    /// <param name="scope">The tenant a tenant-rooted address names, or <see langword="null"/>.</param>
    /// <param name="treeId">The tree id the address names.</param>
    public static bool Names(string? scope, string treeId) => ShellAssertedTenant.Names(scope, treeId);

    /// <summary>
    /// The tenant the catalogue is narrowed to when the circuit asserts
    /// <paramref name="assertedTenant"/>: the cluster lists only that tenant's
    /// trees under any tenant but the reserved default. <see langword="null"/>
    /// when the listing is every tenant's - no tenant, or the default one.
    /// </summary>
    /// <param name="assertedTenant">The tenant the circuit asserts, or <see langword="null"/>.</param>
    /// <returns>The narrowing tenant, or <see langword="null"/>.</returns>
    public static string? NarrowingTenant(string? assertedTenant) =>
        string.IsNullOrEmpty(assertedTenant) || string.Equals(assertedTenant, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal)
            ? null
            : assertedTenant;

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

    /// <summary>A read catalogue, the caller it was read for, and when.</summary>
    private sealed record Remembered(IReadOnlyList<ClusterTreeEntry> Trees, ShellCallerKey Caller, DateTimeOffset ReadAt);
}
