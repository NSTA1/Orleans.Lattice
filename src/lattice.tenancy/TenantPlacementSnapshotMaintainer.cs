using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo maintainer of the in-memory tenant-placement snapshot. It builds
/// a <see cref="TenantPlacementSnapshot"/> from the tenant registry on first use,
/// observes the core change-feed (<see cref="IMutationObserver"/>) and rebuilds
/// when the tenant-registry tree mutates, and swaps the immutable snapshot
/// atomically on every rebuild.
/// </summary>
/// <remarks>
/// <para>
/// This maintainer exists to break a production re-entrancy hazard. Tree
/// registration runs inside the singleton, non-reentrant registry grain's turn,
/// so the placement resolver it invokes must <b>not</b> make a blocking call back
/// into the registry / tree / <c>ILattice</c> subsystem - doing so re-enters the
/// same grain and self-deadlocks. Reading placement from this in-memory snapshot
/// instead of a live <see cref="ITenantRegistry"/> read keeps
/// <see cref="TenantWalPlacementResolver"/> a pure synchronous lookup: zero grain
/// hop, zero re-entrancy.
/// </para>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it must return
/// quickly and must not scan storage synchronously. It therefore only
/// <i>schedules</i> a rebuild; the rescan of the registry runs on a background
/// continuation off the mutating grain's scheduler
/// (<see cref="ITenantRegistry.ListAsync"/> is a normal client-style grain call
/// from that background thread, never a re-entrant one). This gives eventual
/// snapshot consistency: a tenant created before this snapshot observes the write
/// simply resolves to the baseline placement until the rebuild lands, which is
/// fail-safe (a tree registered before its tenant record is visible gets the
/// default WAL provider, not a wrong one). Rebuilds are coalesced - a burst of
/// registry writes collapses into at most one in-flight rebuild plus at most one
/// queued follow-up - and serialized, so the snapshot always reflects a whole,
/// self-consistent scan and the epoch never regresses.
/// </para>
/// </remarks>
internal sealed class TenantPlacementSnapshotMaintainer : IMutationObserver
{
    private readonly ITenantRegistry _registry;
    private readonly ILogger<TenantPlacementSnapshotMaintainer> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private TenantPlacementSnapshot _current = TenantPlacementSnapshot.Empty;
    private long _epoch;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;

    /// <summary>Initializes a new <see cref="TenantPlacementSnapshotMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned to build the snapshot.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    public TenantPlacementSnapshotMaintainer(
        ITenantRegistry registry,
        ILogger<TenantPlacementSnapshotMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _logger = logger;
    }

    /// <summary>The current snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public TenantPlacementSnapshot Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it
    /// synchronously (awaited) when it is still cold. Idempotent: once any rebuild
    /// has advanced the epoch this returns immediately. Must be awaited only from a
    /// background / startup context, never from inside the registry grain's turn.
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
    /// Rebuilds the snapshot synchronously and returns the epoch it produced.
    /// Exposed for tests that need to force a deterministic rebuild.
    /// </summary>
    /// <remarks>
    /// Unlike the production background path (<see cref="RunRebuildLoopAsync"/>),
    /// which catches every rebuild failure and self-heals on the next change-feed
    /// tick, this test-facing entry point deliberately does <b>not</b> swallow
    /// failures - a genuine, persistent fault must still surface to the calling
    /// test. It does, however, tolerate a <i>transient</i> Orleans streaming
    /// <see cref="EnumerationAbortedException"/> on the registry scan (a cold
    /// <see cref="ITenantRegistry.ListAsync"/> enumeration can be aborted by
    /// concurrent silo activity when fixtures run cold together), which production
    /// never surfaces. It retries only the read, a small bounded number of times,
    /// re-enumerating immediately with no delay or wall-clock wait, so the test stays
    /// deterministic; on budget exhaustion the abort rethrows rather than being
    /// hidden. The atomic snapshot swap and epoch bump still happen exactly once,
    /// after a successful read.
    /// </remarks>
    internal async Task<long> RebuildNowAsync(CancellationToken cancellationToken = default)
    {
        // Matches the repo's other bounded scan-reopen budgets (see the resilient
        // scan extensions in the core library).
        const int maxScanAttempts = 8;

        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Dictionary<TenantId, TenantPlacement> byTenant;
            var attempt = 1;
            while (true)
            {
                try
                {
                    byTenant = await ScanPlacementsAsync(cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (EnumerationAbortedException) when (attempt < maxScanAttempts)
                {
                    attempt++;
                }
            }

            SwapSnapshot(byTenant);
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
                        // Run the rescan off the mutating grain's scheduler.
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
                _logger.LogWarning(ex, "Failed to rebuild the tenant-placement snapshot; the previous snapshot remains in effect.");
            }

            // Go idle if no follow-up was queued; otherwise reset to running and
            // loop so the latest committed change is captured.
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
            var byTenant = await ScanPlacementsAsync(cancellationToken).ConfigureAwait(false);
            SwapSnapshot(byTenant);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    /// <summary>
    /// Enumerates the tenant registry into a placement map. This is the only step
    /// that touches the (grain-backed) registry, so it is the only step that can
    /// raise a transient <see cref="EnumerationAbortedException"/>; callers that
    /// need resilience retry this method, never the swap.
    /// </summary>
    private async Task<Dictionary<TenantId, TenantPlacement>> ScanPlacementsAsync(CancellationToken cancellationToken)
    {
        var byTenant = new Dictionary<TenantId, TenantPlacement>();
        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            byTenant[record.Id] = record.Placement;
        }

        return byTenant;
    }

    /// <summary>
    /// Publishes a freshly scanned placement map as the current snapshot: builds the
    /// immutable snapshot, swaps it in atomically, and advances the epoch exactly
    /// once. Pure and non-faulting - it never touches the registry.
    /// </summary>
    private void SwapSnapshot(Dictionary<TenantId, TenantPlacement> byTenant)
    {
        Volatile.Write(ref _current, TenantPlacementSnapshot.Build(byTenant));
        Interlocked.Increment(ref _epoch);
    }
}
