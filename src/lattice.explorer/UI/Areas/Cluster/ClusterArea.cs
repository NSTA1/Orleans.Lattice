using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The Cluster area (A10, issue #3828): the estate and its regions, every tree's
/// topology, shards, storage and WAL placement, and tree administration. It is
/// cluster-wide, so its addresses never carry a tenant.
/// </summary>
/// <remarks>
/// Visibility fails closed on the cluster-wide storage summary, the cheapest read
/// that needs cluster telemetry authority: no tree administration facade hides the
/// area, a denial hides it, and an unconfigured connection or a facade the cluster
/// does not serve says why. A verdict is remembered for the circuit until the
/// connection changes; a transient failure is not remembered.
/// </remarks>
internal sealed class ClusterArea : IExplorerArea, IDisposable
{
    /// <summary>The "Reshard tree..." palette command's id.</summary>
    public const string ReshardCommandId = "cluster.reshard-tree";

    /// <summary>The "Plan WAL move..." palette command's id.</summary>
    public const string PlanWalMoveCommandId = "cluster.plan-wal-move";

    private readonly ClusterFacades _facades;
    private AreaAvailability? _verdict;
    private ClusterStorageUsageSummary? _usage;
    private string? _verdictTenant;

    /// <summary>Creates the area for one circuit.</summary>
    /// <param name="facades">The facades the area reads.</param>
    /// <param name="catalog">The circuit's tree catalogue.</param>
    /// <param name="signals">Carries palette commands to their pages.</param>
    public ClusterArea(ClusterFacades facades, ClusterTreeCatalog catalog, ClusterCommandSignals signals)
    {
        ArgumentNullException.ThrowIfNull(facades);
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(signals);

        _facades = facades;
        Completions = new ClusterCompletionSource(catalog);
        Commands =
        [
            new ExplorerCommand(ReshardCommandId, "Reshard tree...")
            {
                Detail = "Grow a tree to more physical shards, online.",
                Target = ClusterAddresses.Trees,
                InvokeAsync = _ => signals.RequestAsync(ReshardCommandId),
            },
            new ExplorerCommand(PlanWalMoveCommandId, "Plan WAL move...")
            {
                Detail = "Preview moving a WAL partition to another storage provider.",
                Target = ClusterAddresses.Wal(),
                InvokeAsync = _ => signals.RequestAsync(PlanWalMoveCommandId),
            },
        ];

        if (facades.Session is { } session)
        {
            session.ConfigurationChanged += Forget;
        }
    }

    /// <inheritdoc />
    public string Key => ClusterAddresses.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Cluster";

    /// <inheritdoc />
    public int DirectoryOrder => 90;

    /// <inheritdoc />
    public bool IsTenantScoped => false;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (_facades.TreeAdmin is not { } admin)
        {
            return AreaAvailability.Hidden;
        }

        if (!_facades.IsConnected)
        {
            return AreaAvailability.Unavailable("Connect to a cluster to see its estate.");
        }

        // The verdict and the usage it read belong to the tenant asserted when they
        // were read; under another tenant the cluster is asked again.
        var tenant = _facades.AssertedTenant;
        if (!string.Equals(_verdictTenant, tenant, StringComparison.Ordinal))
        {
            Forget();
            _verdictTenant = tenant;
        }

        if (_verdict is { } remembered)
        {
            return remembered;
        }

        try
        {
            _usage = await admin.GetStorageUsageAsync(deep: false, cancellationToken).ConfigureAwait(false);
            _verdict = AreaAvailability.Visible;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (ClusterFaults.IsDenied(exception))
        {
            _verdict = AreaAvailability.Hidden;
        }
        catch (NotSupportedException)
        {
            _verdict = AreaAvailability.Unavailable("This cluster does not serve tree administration.");
        }
        catch (Exception)
        {
            return AreaAvailability.Unavailable("The cluster did not answer. Try again in a moment.");
        }

        var answer = _verdict.Value;
        if (!string.Equals(_facades.AssertedTenant, tenant, StringComparison.Ordinal))
        {
            // The circuit moved to another tenant while the probe ran: answer this
            // caller, but remember nothing read under the tenant it left.
            Forget();
        }

        return answer;
    }

    /// <inheritdoc />
    public ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(_usage is { } usage && string.Equals(_verdictTenant, _facades.AssertedTenant, StringComparison.Ordinal)
            ? $"{ClusterFormat.Plural(usage.TreeCount, "tree")}, {ClusterFormat.Bytes(usage.TotalBytes)} stored."
            : null);

    /// <inheritdoc />
    public ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(_usage is { } usage && string.Equals(_verdictTenant, _facades.AssertedTenant, StringComparison.Ordinal)
            ? ClusterFormat.Count(usage.TreeCount)
            : null);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_facades.Session is { } session)
        {
            session.ConfigurationChanged -= Forget;
        }
    }

    private void Forget()
    {
        _verdict = null;
        _usage = null;
    }
}
