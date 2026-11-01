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
/// Visibility fails closed. The side-effect-free capability probe of a reserved
/// tree id admits a caller holding the cluster-wide Telemetry capability without
/// reading the estate (#5602); otherwise the cluster-wide storage summary, which
/// needs that same authority, decides: no tree administration facade hides the
/// area, and an unconfigured connection or a facade the cluster does not serve says
/// why. A caller refused the summary is admitted only when the capability probe of
/// one of the trees the catalogue lists reports a tree-lifecycle or tree-admin grant
/// (#5581); a read, diagnostics or bulk-load grant admits nothing, so such a caller
/// sees no area. Each page and verb keeps its own gate, so the estate-wide figures
/// stay out of an admitted caller's reach. A verdict is remembered for the caller it
/// was read for
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

    /// <summary>The "New tree..." palette command's id (G4, issue #5368).</summary>
    public const string CreateTreeCommandId = "cluster.create-tree";

    /// <summary>The most catalogue trees probed for a grant when the storage summary is refused.</summary>
    public const int MaximumGrantProbes = 64;

    /// <summary>How many catalogue trees are probed at once; the probing stops after the first wave holding a grant.</summary>
    public const int GrantProbeConcurrency = 8;

    /// <summary>The reserved tree id the area-wide capability probe asks about. The probe has no side effects.</summary>
    public const string ProbeTreeId = "__cluster_capability_probe__";

    private readonly ClusterFacades _facades;
    private readonly ClusterTreeCatalog _catalog;
    private AreaAvailability? _verdict;
    private ClusterStorageUsageSummary? _usage;
    private bool _telemetry;
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
                Detail = "Grow or shrink a tree's physical shard count, online.",
                Target = ClusterAddresses.Trees,
                InvokeAsync = _ => signals.RequestAsync(ReshardCommandId),
            },
            new ExplorerCommand(PlanWalMoveCommandId, "Plan WAL move...")
            {
                Detail = "Preview moving a WAL partition to another storage provider.",
                Target = ClusterAddresses.Wal(),
                InvokeAsync = _ => signals.RequestAsync(PlanWalMoveCommandId),
            },
            new ExplorerCommand(CreateTreeCommandId, "New tree...")
            {
                Detail = "Create an empty tree with a chosen shard count and node sizes.",
                Target = ClusterAddresses.Trees,
                InvokeAsync = _ => signals.RequestAsync(CreateTreeCommandId),
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

        AreaAvailability answer;
        var denied = false;
        try
        {
            if (await HoldsClusterTelemetryAsync(admin, cancellationToken).ConfigureAwait(false))
            {
                // Admitted by the capability itself: the storage summary, which on a
                // large estate takes longer than the directory waits, is read for
                // Home later (#5602).
                _telemetry = true;
                answer = AreaAvailability.Visible;
            }
            else
            {
                _usage = await admin.GetStorageUsageAsync(deep: false, cancellationToken).ConfigureAwait(false);
                answer = AreaAvailability.Visible;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (ClusterFaults.IsDenied(exception))
        {
            denied = true;
            answer = AreaAvailability.Hidden;
        }
        catch (NotSupportedException)
        {
            answer = AreaAvailability.Unavailable("This cluster does not serve tree administration.");
        }
        catch (Exception)
        {
            return AreaAvailability.Unavailable("The cluster did not answer. Try again in a moment.");
        }

        if (denied)
        {
            // No cluster telemetry: the estate-wide figures stay out of reach, but a
            // caller who may administer a tree still reaches that tree's pages (#5581).
            // Hidden is the default arm: only a positive grant admits.
            if (await AdministersAnyTreeAsync(admin, cancellationToken).ConfigureAwait(false) is not { } administers)
            {
                return AreaAvailability.Unavailable("The cluster did not answer. Try again in a moment.");
            }

            answer = administers ? AreaAvailability.Visible : AreaAvailability.Hidden;
        }

        _verdict = answer;
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
        var usage = CurrentUsage();
        if (usage is null
            && _telemetry
            && _verdictCaller == _facades.Caller
            && _facades.TreeAdmin is { } admin)
        {
            usage = await ReadUsageAsync(admin, cancellationToken).ConfigureAwait(false);
        }

        if (usage is null)
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

    /// <summary>
    /// Whether a caller refused the cluster-wide storage summary may still administer
    /// one of the trees the catalogue lists: a tree-lifecycle or tree-admin grant
    /// admits the area. A read or diagnostics grant does not, and neither does a
    /// bulk-load grant, which no Cluster page serves. At most
    /// <see cref="MaximumGrantProbes"/> trees are probed, <see cref="GrantProbeConcurrency"/>
    /// at a time, stopping after the first wave that holds a grant. A catalogue or probe the cluster did not answer, when nothing else
    /// admitted the caller, is <see langword="null"/> (not checked); a refusal or any
    /// other fault admits nothing.
    /// </summary>
    private async Task<bool?> AdministersAnyTreeAsync(ILatticeTreeAdmin admin, CancellationToken cancellationToken)
    {
        IReadOnlyList<ClusterTreeEntry> trees;
        try
        {
            trees = await _catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (AreaProbeFaults.IsTransient(exception))
        {
            return null;
        }
        catch (Exception)
        {
            return false;
        }

        // Probed a wave at a time: one probe after another cannot finish inside the
        // directory's availability budget on a cluster of a few dozen trees, so the
        // area would read as not checked on every navigation.
        var unanswered = false;
        var bound = Math.Min(trees.Count, MaximumGrantProbes);
        for (var start = 0; start < bound; start += GrantProbeConcurrency)
        {
            var wave = new Task<bool?>[Math.Min(GrantProbeConcurrency, bound - start)];
            for (var i = 0; i < wave.Length; i++)
            {
                wave[i] = ProbeTreeAsync(admin, trees[start + i].TreeId, cancellationToken);
            }

            var answers = await Task.WhenAll(wave).ConfigureAwait(false);
            if (answers.Contains(true))
            {
                return true;
            }

            unanswered |= answers.Contains(null);
        }

        return unanswered ? null : false;
    }

    /// <summary>One tree's verdict: admitted, not admitted, or <see langword="null"/> when the cluster did not answer.</summary>
    private static async Task<bool?> ProbeTreeAsync(ILatticeTreeAdmin admin, string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var capabilities = await admin.ProbeCapabilitiesAsync(treeId, cancellationToken).ConfigureAwait(false);
            return capabilities is not null && AdmitsArea(capabilities);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception) when (AreaProbeFaults.IsTransient(exception))
        {
            return null;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>Whether a tree's capabilities admit a caller without cluster telemetry to the area.</summary>
    /// <param name="capabilities">The probe's answer for one tree.</param>
    /// <returns><see langword="true"/> for a tree-lifecycle or tree-admin grant only.</returns>
    internal static bool AdmitsArea(LatticeTreeAdminCapabilities capabilities) =>
        capabilities.CanManageTreeLifecycle || capabilities.CanAdministerTree;

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
        _telemetry = false;
    }

    /// <summary>
    /// Whether the side-effect-free capability probe says the caller holds cluster
    /// telemetry. Any fault or an older cluster that does not report the flag reads
    /// as no, so the caller falls through to the storage-summary read, which decides.
    /// </summary>
    private static async Task<bool> HoldsClusterTelemetryAsync(ILatticeTreeAdmin admin, CancellationToken cancellationToken)
    {
        try
        {
            var capabilities = await admin.ProbeCapabilitiesAsync(ProbeTreeId, cancellationToken).ConfigureAwait(false);
            return capabilities is { CanViewClusterTelemetry: true };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>Reads the storage summary for Home, for a caller admitted by the telemetry probe; <see langword="null"/> on any fault.</summary>
    private async Task<ClusterStorageUsageSummary?> ReadUsageAsync(ILatticeTreeAdmin admin, CancellationToken cancellationToken)
    {
        var caller = _facades.Caller;
        try
        {
            var usage = await admin.GetStorageUsageAsync(deep: false, cancellationToken).ConfigureAwait(false);
            if (_facades.Caller == caller && _verdictCaller == caller)
            {
                _usage = usage;
            }

            return usage;
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
}
