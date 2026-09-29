using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

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

    private readonly CancellationTokenSource _lifetime = new();
    private ClusterLoad<ClusterInfo> _info = ClusterLoad<ClusterInfo>.Loading;
    private ClusterLoad<ClusterStorageUsageSummary> _usage = ClusterLoad<ClusterStorageUsageSummary>.Loading;
    private bool _confirmDeep;

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        var token = _lifetime.Token;
        var info = ClusterLoad<ClusterInfo>.RunAsync(ReadInfoAsync, token);
        var usage = ClusterLoad<ClusterStorageUsageSummary>.RunAsync(ct => Facades.RequireTreeAdmin().GetStorageUsageAsync(false, ct), token);
        _info = await info;
        _usage = await usage;
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

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();
}
