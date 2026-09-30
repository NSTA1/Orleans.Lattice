using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

/// <summary>
/// <c>/cluster</c>: the estate overview - the cluster's identity and storage, the
/// regions it replicates with drawn as an order diagram, and the stops that
/// administer trees, WAL placement and orphaned leaves.
/// </summary>
public partial class ClusterOverview : IDisposable
{
    private static readonly (string Text, string Detail, ExplorerAddress Address)[] Stops =
    [
        ("Trees", "Every tree by logical name: configuration, shards, storage and lifecycle.", ClusterAddresses.Trees),
        ("WAL placement", "Audit where each WAL partition lives, then plan, execute and reclaim a move.", ClusterAddresses.Wal()),
        ("Orphaned leaves", "Survey, audit and repair leaves no descent reaches.", ClusterAddresses.Orphans()),
    ];

    private readonly ComponentLifetime _lifetime = new();
    private ClusterLoad<ClusterInfo> _info = ClusterLoad<ClusterInfo>.Loading;
    private ClusterLoad<ClusterStorageUsageSummary> _usage = ClusterLoad<ClusterStorageUsageSummary>.Loading;
    private bool _confirmDeep;
    private int? _scopedTreeCount;

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ClusterTreeCatalog Catalog { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        var token = _lifetime.Token;
        var info = ClusterLoad<ClusterInfo>.RunAsync(ReadInfoAsync, token);
        var usage = ClusterLoad<ClusterStorageUsageSummary>.RunAsync(ct => Facades.RequireTreeAdmin().GetStorageUsageAsync(false, ct), token);
        _info = await info;
        _usage = await usage;
        if (Scope is { } scope && !_lifetime.IsLeft)
        {
            var trees = await ClusterLoad<IReadOnlyList<ClusterTreeEntry>>.RunAsync(
                async ct => ClusterTreeCatalog.InScope(await Catalog.GetAsync(refresh: false, ct), scope),
                token);
            _scopedTreeCount = trees.Value?.Count;
        }
    }

    private Task<ClusterInfo> ReadInfoAsync(CancellationToken cancellationToken) =>
        Facades.Session is { IsConfigured: true } session
            ? session.Connection.GetClusterInfoAsync(new ClusterInfoRequest(), cancellationToken)
            : Task.FromException<ClusterInfo>(new InvalidOperationException("Connect to a cluster to see its identity."));

    private async Task RefreshUsageAsync()
    {
        _usage = ClusterLoad<ClusterStorageUsageSummary>.Loading;
        _usage = await ClusterLoad<ClusterStorageUsageSummary>.RunAsync(ct => Facades.RequireTreeAdmin().GetStorageUsageAsync(false, ct), _lifetime.Token);
    }

    private async Task MeasureDeeplyAsync()
    {
        _usage = ClusterLoad<ClusterStorageUsageSummary>.Loading;
        _usage = await ClusterLoad<ClusterStorageUsageSummary>.RunAsync(ct => Facades.RequireTreeAdmin().GetStorageUsageAsync(true, ct), _lifetime.Token);
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address.WithTenant(Scope)).ToHref();

    private string ClusterWideHref => Navigator.Canonicalize(ClusterAddresses.Overview.WithTenant(null)).ToHref();

    /// <summary>The administration stops the overview offers: at a tenant-rooted address only the tenant's trees, since WAL placement and orphaned leaves are cluster-wide.</summary>
    private IEnumerable<(string Text, string Detail, ExplorerAddress Address)> VisibleStops =>
        Scope is null ? Stops : Stops.Take(1);

    /// <summary>
    /// The storage the overview reports: the cluster's on a cluster-wide address,
    /// and at a tenant-rooted one only the tenant's own trees', summed from the
    /// cluster's per-tree figures, with the tree count the tree list shows.
    /// </summary>
    private (long Trees, long Total, long LeafState, long Snapshots, long Wal) Figures(ClusterStorageUsageSummary usage)
    {
        if (Scope is not { } scope)
        {
            return (usage.TreeCount, usage.TotalBytes, usage.LeafStateBytes, usage.SnapshotBytes, usage.WalRetainedBytes);
        }

        long total = 0, leaf = 0, snapshots = 0, wal = 0;
        foreach (var tree in usage.Trees)
        {
            if (ShellAssertedTenant.Lists(scope, tree.TreeId))
            {
                total += tree.TotalBytes;
                leaf += tree.LeafStateBytes;
                snapshots += tree.SnapshotBytes;
                wal += tree.WalRetainedBytes;
            }
        }

        return (_scopedTreeCount ?? 0, total, leaf, snapshots, wal);
    }
}
