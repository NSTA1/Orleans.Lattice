using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo maintainer of the in-memory tenant-residency snapshot. It builds a
/// <see cref="TenantResidencySnapshot"/> - each residency-configured tenant's
/// status <b>in this silo's serving region</b> - from the tenant registry on first
/// use, observes the core change-feed (<see cref="IMutationObserver"/>) and
/// rebuilds when the tenant-registry tree mutates, and swaps the immutable snapshot
/// atomically on every rebuild. It also diffs each rebuild against the previous
/// local-region view and publishes every observed transition to the registered
/// <see cref="ITenantRegionStatusChangeListener"/>s.
/// </summary>
/// <remarks>
/// <para>
/// This mirrors <see cref="TenantPlacementSnapshotMaintainer"/> exactly and for the
/// same reason: the residency resolver runs on the gate and replicated-apply hot
/// paths, potentially inside the singleton, non-reentrant registry grain's turn, so
/// it must read from this in-memory snapshot rather than making a blocking registry
/// call that would re-enter the grain and self-deadlock. Reading residency from the
/// snapshot keeps <see cref="TenantResidencyResolver"/> a pure synchronous O(1)
/// lookup: zero grain hop, zero allocation.
/// </para>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it only
/// <i>schedules</i> a rebuild; the registry rescan runs on a background continuation
/// off the mutating grain's scheduler. Rebuilds are coalesced (a burst of writes
/// collapses into at most one in-flight rebuild plus at most one queued follow-up)
/// and serialized, so the snapshot always reflects a whole, self-consistent scan and
/// the epoch never regresses. A tenant observed before this snapshot catches up
/// simply resolves to admit-all until the rebuild lands, which is fail-open on
/// residency grounds only (never a wrong deny), closed by the startup warm-up.
/// </para>
/// </remarks>
internal sealed class TenantResidencySnapshotMaintainer : IMutationObserver
{
    private readonly ITenantRegistry _registry;
    private readonly ILogger<TenantResidencySnapshotMaintainer> _logger;
    private readonly ITenantRegionStatusChangeListener[] _listeners;
    private readonly string _regionId;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private TenantResidencySnapshot _current = TenantResidencySnapshot.Empty;
    private Dictionary<TenantId, TenantRegionStatus> _lastByTenant = new();
    private long _epoch;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;

    /// <summary>Initializes a new <see cref="TenantResidencySnapshotMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned to build the snapshot.</param>
    /// <param name="clusterOptions">Supplies this silo's serving region id (the cluster id).</param>
    /// <param name="listeners">The registered region-status change listeners (may be empty).</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantResidencySnapshotMaintainer(
        ITenantRegistry registry,
        IOptions<ClusterOptions> clusterOptions,
        IEnumerable<ITenantRegionStatusChangeListener> listeners,
        ILogger<TenantResidencySnapshotMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(clusterOptions);
        ArgumentNullException.ThrowIfNull(listeners);
        ArgumentNullException.ThrowIfNull(logger);

        _registry = registry;
        _logger = logger;
        _listeners = listeners as ITenantRegionStatusChangeListener[] ?? listeners.ToArray();

        var clusterId = clusterOptions.Value.ClusterId;
        _regionId = string.IsNullOrEmpty(clusterId) ? "default" : clusterId;
    }

    /// <summary>The current snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public TenantResidencySnapshot Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>This silo's serving region id (the cluster id).</summary>
    public string LocalRegionId => _regionId;

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it synchronously
    /// (awaited) when it is still cold. Idempotent: once any rebuild has advanced the
    /// epoch this returns immediately. Await only from a background / startup context,
    /// never from inside the registry grain's turn.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    public async Task EnsureWarmAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Read(ref _epoch) > 0)
        {
            return;
        }

        await RebuildOnceAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (string.Equals(mutation.TreeId, TenantTreeNames.RegistryTree, StringComparison.Ordinal))
        {
            ScheduleRebuild();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Rebuilds the snapshot synchronously and returns the epoch it produced. Exposed
    /// for tests that need to force a deterministic rebuild; unlike the production
    /// background path it does not swallow a genuine rebuild failure, but it tolerates
    /// a transient Orleans streaming <see cref="EnumerationAbortedException"/> on the
    /// registry scan by re-enumerating immediately, a small bounded number of times.
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>The epoch of the snapshot this rebuild produced.</returns>
    internal async Task<long> RebuildNowAsync(CancellationToken cancellationToken = default)
    {
        const int maxScanAttempts = 8;

        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Dictionary<TenantId, TenantRegionStatus> byTenant;
            var attempt = 1;
            while (true)
            {
                try
                {
                    byTenant = await ScanStatusesAsync(cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (EnumerationAbortedException) when (attempt < maxScanAttempts)
                {
                    attempt++;
                }
            }

            var changes = SwapSnapshot(byTenant);
            await NotifyListenersAsync(changes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _rebuildLock.Release();
        }

        return CurrentEpoch;
    }

    private void ScheduleRebuild()
    {
        while (true)
        {
            var state = Volatile.Read(ref _rebuildState);
            switch (state)
            {
                case 0:
                    if (Interlocked.CompareExchange(ref _rebuildState, 1, 0) == 0)
                    {
                        _ = Task.Run(RunRebuildLoopAsync);
                        return;
                    }

                    break;
                case 1:
                    if (Interlocked.CompareExchange(ref _rebuildState, 2, 1) == 1)
                    {
                        return;
                    }

                    break;
                default:
                    return;
            }
        }
    }

    private async Task RunRebuildLoopAsync()
    {
        while (true)
        {
            try
            {
                await RebuildOnceAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to rebuild the tenant-residency snapshot; the previous snapshot remains in effect.");
            }

            if (Interlocked.CompareExchange(ref _rebuildState, 0, 1) == 1)
            {
                return;
            }

            Volatile.Write(ref _rebuildState, 1);
        }
    }

    private async Task RebuildOnceAsync(CancellationToken cancellationToken)
    {
        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var byTenant = await ScanStatusesAsync(cancellationToken).ConfigureAwait(false);
            var changes = SwapSnapshot(byTenant);
            await NotifyListenersAsync(changes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    /// <summary>
    /// Enumerates the tenant registry into a local-region status map. Only
    /// residency-configured tenants are included; an unconfigured tenant is left out
    /// so it resolves to admit-all. This is the only step that touches the
    /// (grain-backed) registry, so it is the only step that can raise a transient
    /// <see cref="EnumerationAbortedException"/>.
    /// </summary>
    private async Task<Dictionary<TenantId, TenantRegionStatus>> ScanStatusesAsync(CancellationToken cancellationToken)
    {
        var byTenant = new Dictionary<TenantId, TenantRegionStatus>();
        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            if (record.HasResidencyConfiguration)
            {
                byTenant[record.Id] = record.GetRegionStatus(_regionId);
            }
        }

        return byTenant;
    }

    /// <summary>
    /// Publishes a freshly scanned status map as the current snapshot: computes the
    /// local-region transition diff against the previous map (only when a listener is
    /// registered), swaps the immutable snapshot in atomically, advances the epoch
    /// exactly once, and records the new map as the diff baseline. Pure and
    /// non-faulting - it never touches the registry.
    /// </summary>
    private IReadOnlyList<TenantRegionStatusChange> SwapSnapshot(Dictionary<TenantId, TenantRegionStatus> byTenant)
    {
        var changes = _listeners.Length == 0
            ? (IReadOnlyList<TenantRegionStatusChange>)Array.Empty<TenantRegionStatusChange>()
            : DiffChanges(byTenant);

        Volatile.Write(ref _current, TenantResidencySnapshot.Build(byTenant));
        _lastByTenant = byTenant;
        Interlocked.Increment(ref _epoch);
        return changes;
    }

    private List<TenantRegionStatusChange> DiffChanges(Dictionary<TenantId, TenantRegionStatus> byTenant)
    {
        var changes = new List<TenantRegionStatusChange>();

        foreach (var (tenant, current) in byTenant)
        {
            var previous = _lastByTenant.TryGetValue(tenant, out var p) ? p : TenantRegionStatus.None;
            if (previous != current)
            {
                changes.Add(new TenantRegionStatusChange(tenant, _regionId, previous, current));
            }
        }

        foreach (var (tenant, previous) in _lastByTenant)
        {
            if (!byTenant.ContainsKey(tenant) && previous != TenantRegionStatus.None)
            {
                changes.Add(new TenantRegionStatusChange(tenant, _regionId, previous, TenantRegionStatus.None));
            }
        }

        return changes;
    }

    private async Task NotifyListenersAsync(
        IReadOnlyList<TenantRegionStatusChange> changes, CancellationToken cancellationToken)
    {
        if (_listeners.Length == 0 || changes.Count == 0)
        {
            return;
        }

        foreach (var change in changes)
        {
            foreach (var listener in _listeners)
            {
                try
                {
                    await listener.OnRegionStatusChangedAsync(change, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "A tenant-region-status change listener failed for tenant '{Tenant}' in region '{Region}'; continuing.",
                        change.Tenant,
                        change.RegionId);
                }
            }
        }
    }
}
