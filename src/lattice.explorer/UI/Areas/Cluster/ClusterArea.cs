using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The Cluster area (A10, issue #3828): the estate and its regions, every tree's
/// topology, shards, storage and WAL placement, and tree administration. It is
/// cluster-wide at its plain addresses; a tenant-rooted address (<c>/t/{tenant}/cluster</c>)
/// shows only that tenant's own trees and storage.
/// </summary>
/// <remarks>
/// Visibility fails closed on the cluster-wide storage summary, the cheapest read
/// that needs cluster telemetry authority: no tree administration facade hides the
/// area, a denial hides it, and an unconfigured connection or a facade the cluster
/// does not serve says why. A verdict is remembered for the caller it was read for
/// (<see cref="ClusterFacades.Caller"/>: the sign-in, the endpoint and the asserted
/// tenant), so a sign-in, a sign-out, a new connection or a tenant switch asks
/// again; a transient failure is not remembered.
/// </remarks>
internal sealed class ClusterArea : IExplorerArea, IDisposable
{
    /// <summary>The "Reshard tree..." palette command's id.</summary>
    public const string ReshardCommandId = "cluster.reshard-tree";

    /// <summary>The "Plan WAL move..." palette command's id.</summary>
    public const string PlanWalMoveCommandId = "cluster.plan-wal-move";

    private readonly ClusterFacades _facades;
    private readonly ClusterTreeCatalog _catalog;
    private AreaAvailability? _verdict;
    private ClusterStorageUsageSummary? _usage;
    private ShellCallerKey _verdictCaller;

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
        _catalog = catalog;
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

    /// <summary>
    /// Whether <paramref name="address"/> follows the active tenant: it does when
    /// it carries a tenant root, and then shows only that tenant's own trees. The
    /// plain <c>/cluster</c> addresses stay cluster-wide.
    /// </summary>
    /// <param name="address">An address in this area.</param>
    public bool IsTenantScopedAt(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Tenant is not null;
    }

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public IReadOnlyList<int>? GetChainSpans(ExplorerAddress address) => ClusterAddresses.ChainSpans(address);

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

        // The verdict and the usage it read belong to the caller they were read
        // for; for another sign-in, endpoint or tenant the cluster is asked again.
        var caller = _facades.Caller;
        if (_verdictCaller != caller)
        {
            Forget();
            _verdictCaller = caller;
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
        if (_facades.Caller != caller)
        {
            // The caller changed while the probe ran: answer this caller, but
            // remember nothing read for the one it left.
            Forget();
        }

        return answer;
    }

    /// <inheritdoc />
    /// <remarks>
    /// It counts the trees the tree list shows, and names the rest of the storage
    /// summary's count rather than folding it in: system trees - and, under a
    /// tenant other than the default, every other tenant's trees - are counted by
    /// the cluster but never listed.
    /// </remarks>
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        if (CurrentUsage() is not { } usage)
        {
            return null;
        }

        var stored = ClusterFormat.Bytes(usage.TotalBytes);
        var listed = await CountListedAsync(cancellationToken).ConfigureAwait(false);
        if (listed is not { } count)
        {
            return $"{ClusterFormat.Plural(usage.TreeCount, "tree")} including system trees, {stored} stored.";
        }

        if (_facades.ListingTenant is { } tenant)
        {
            // Home is tenant-rooted: the tenant's own trees and what they store.
            var owned = 0L;
            foreach (var tree in usage.Trees)
            {
                if (ShellAssertedTenant.Lists(tenant, tree.TreeId))
                {
                    owned += tree.TotalBytes;
                }
            }

            return $"{ClusterFormat.Plural(count, "tree")} of tenant {tenant}, {ClusterFormat.Bytes(owned)} stored.";
        }

        var unlisted = usage.TreeCount - count;
        return unlisted > 0
            ? $"{ClusterFormat.Plural(count, "tree")}, plus {ClusterFormat.Plural(unlisted, "system tree")}, {stored} stored."
            : $"{ClusterFormat.Plural(count, "tree")}, {stored} stored.";
    }

    /// <inheritdoc />
    /// <remarks>The count of the tree list at <c>/cluster/trees</c>, so the badge and the list it leads to agree.</remarks>
    public async ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) =>
        CurrentUsage() is not null && await CountListedAsync(cancellationToken).ConfigureAwait(false) is { } count
            ? ClusterFormat.Count(count)
            : null;

    /// <inheritdoc />
    public void Dispose()
    {
        if (_facades.Session is { } session)
        {
            session.ConfigurationChanged -= Forget;
        }
    }

    private ClusterStorageUsageSummary? CurrentUsage() =>
        _usage is { } usage && _verdictCaller == _facades.Caller ? usage : null;

    // The tree list's own count, read through the circuit's remembered catalogue;
    // a failed read has no count rather than a guessed one.
    private async ValueTask<int?> CountListedAsync(CancellationToken cancellationToken)
    {
        try
        {
            var trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
            return ClusterTreeCatalog.InScope(trees, _facades.ListingTenant).Count;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private void Forget()
    {
        _verdict = null;
        _usage = null;
    }
}
