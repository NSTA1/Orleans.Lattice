using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;

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
    private OperationFollower? _refresh;
    private string? _refreshId;
    private bool _startingRefresh;
    private DateTimeOffset? _remeasuredAt;
    private string? _refreshError;

    /// <summary>The tenant a tenant-rooted address names, or <see langword="null"/> on a cluster-wide address; links keep it.</summary>
    [CascadingParameter(Name = ClusterScope.CascadeName)]
    internal string? Scope { get; set; }

    [Inject]
    private ClusterFacades Facades { get; set; } = default!;

    [Inject]
    private ClusterTreeCatalog Catalog { get; set; } = default!;

    [Inject]
    private ExplorerNavigator Navigator { get; set; } = default!;

    [Inject]
    private TimeProvider Time { get; set; } = default!;

    /// <summary>The storage re-measure while it has not finished, or <see langword="null"/>.</summary>
    private LatticeOperationStatus? RunningRefresh => _refresh?.Status is { IsTerminal: false } status ? status : null;

    /// <summary>Whether a re-measure is being started or is running.</summary>
    private bool IsRefreshing => _startingRefresh || RunningRefresh is not null;

    /// <inheritdoc />
    public void Dispose()
    {
        StopFollowingRefresh();
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
        else if (Scope is null && !_lifetime.IsLeft)
        {
            await ResumeRefreshAsync();
        }
    }

    /// <summary>
    /// Picks up a re-measure already running on the cluster - started in another tab
    /// or before a reload - and follows it (#4126).
    /// </summary>
    private async Task ResumeRefreshAsync()
    {
        if (Facades.StorageUsage is not { } operations)
        {
            return;
        }

        try
        {
            var page = await operations.ListOperationsAsync(new LatticeOperationListRequest(), _lifetime.Token);
            if (page.Operations.FirstOrDefault(status => !status.IsTerminal) is { } running)
            {
                await FollowRefreshAsync(operations, running.OperationId);
            }
        }
        catch (Exception) when (!_lifetime.IsLeft)
        {
            // Picking up a running re-measure only restores context; the page works without it.
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

    /// <summary>
    /// Starts the re-measure of every shard as a tracked cluster operation and follows
    /// it, so no request is held open across the whole leaf walk (#4126). A head
    /// that serves no storage-usage operations falls back to the blocking deep read.
    /// </summary>
    private async Task MeasureDeeplyAsync()
    {
        if (Facades.StorageUsage is not { } operations)
        {
            _usage = ClusterLoad<ClusterStorageUsageSummary>.Loading;
            _usage = await ClusterLoad<ClusterStorageUsageSummary>.RunAsync(ct => Facades.RequireTreeAdmin().GetStorageUsageAsync(true, ct), _lifetime.Token);
            return;
        }

        if (IsRefreshing)
        {
            return;
        }

        _startingRefresh = true;
        _refreshError = null;
        try
        {
            var handle = await operations.StartStorageUsageRefreshAsync(cancellationToken: _lifetime.Token);
            await FollowRefreshAsync(operations, handle.OperationId);
        }
        catch (Exception exception) when (!_lifetime.IsLeft)
        {
            _refreshError = ClusterFaults.Describe(exception);
        }
        finally
        {
            _startingRefresh = false;
        }
    }

    private async Task FollowRefreshAsync(ILatticeStorageUsageOperations operations, string operationId)
    {
        StopFollowingRefresh();
        var follower = new OperationFollower(Time);
        _refresh = follower;
        _refreshId = operationId;
        follower.Changed += OnRefreshChanged;
        await follower.StartAsync(ct => operations.GetOperationStatusAsync(operationId, ct), _lifetime.Token);
        if (ReferenceEquals(_refresh, follower) && follower.Status is { IsTerminal: true } status)
        {
            await SettleRefreshAsync(status);
        }
    }

    private async Task StopRefreshAsync()
    {
        if (_refreshId is not { } id || Facades.StorageUsage is not { } operations || _refresh is not { } follower)
        {
            return;
        }

        try
        {
            await operations.CancelOperationAsync(id, _lifetime.Token);
            await follower.RefreshAsync(_lifetime.Token);
        }
        catch (Exception exception) when (!_lifetime.IsLeft)
        {
            _refreshError = ClusterFaults.Describe(exception);
        }
    }

    private void OnRefreshChanged()
    {
        _ = InvokeAsync(async () =>
        {
            if (_refresh?.Status is { IsTerminal: true } status)
            {
                await SettleRefreshAsync(status);
            }

            StateHasChanged();
        });
    }

    /// <summary>Ends a finished re-measure: reads the figures it measured, or says why it ended.</summary>
    private async Task SettleRefreshAsync(LatticeOperationStatus status)
    {
        StopFollowingRefresh();
        if (status.State == LatticeOperationState.Succeeded)
        {
            _remeasuredAt = status.FinishedAtUtc ?? Time.GetUtcNow();
            _refreshError = null;
            await RefreshUsageAsync();
        }
        else
        {
            _refreshError = status.State == LatticeOperationState.Cancelled
                ? "The re-measure was stopped before it finished."
                : "The re-measure failed. " + (string.IsNullOrWhiteSpace(status.FailureReason) ? "No reason was given." : status.FailureReason);
        }
    }

    private void StopFollowingRefresh()
    {
        if (_refresh is { } follower)
        {
            follower.Changed -= OnRefreshChanged;
            follower.Dispose();
        }

        _refresh = null;
        _refreshId = null;
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
