using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo maintainer of the compiled tenant-policy snapshot. It builds the
/// snapshot from the tenant registry on first use, observes the core change-feed
/// (<see cref="IMutationObserver"/>) and rebuilds when the reserved
/// <c>sys-tenant-registry</c> tree mutates, swaps the immutable snapshot
/// atomically, and stamps a monotonic <see cref="CurrentEpoch"/> on every rebuild.
/// </summary>
/// <remarks>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it must return
/// quickly and must not scan the registry synchronously. It therefore only
/// <i>schedules</i> a rebuild; the actual rescan of the registry runs on a
/// background continuation. This gives eventual snapshot consistency: a committed
/// registry edit is reflected shortly after it commits, not necessarily before
/// the writing call returns.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of registry writes collapses into at most one
/// in-flight rebuild plus at most one queued follow-up - and serialized, so the
/// snapshot always reflects a whole, self-consistent scan and the epoch never
/// regresses.
/// </para>
/// </remarks>
internal sealed class CompiledTenantPolicySnapshotMaintainer : IMutationObserver
{
    private readonly ITenantRegistry _registry;
    private readonly ILogger<CompiledTenantPolicySnapshotMaintainer> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private CompiledTenantPolicy _current = CompiledTenantPolicy.Empty;
    private long _epoch;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    /// <summary>Initializes a new <see cref="CompiledTenantPolicySnapshotMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned to build the snapshot.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <exception cref="ArgumentNullException"><paramref name="registry"/> or <paramref name="logger"/> is <c>null</c>.</exception>
    public CompiledTenantPolicySnapshotMaintainer(
        ITenantRegistry registry,
        ILogger<CompiledTenantPolicySnapshotMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _logger = logger;
    }

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledTenantPolicy Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// The most recently scheduled background rebuild loop, or a completed task
    /// when none has been scheduled. Exposed so a test can await a change-feed-driven
    /// rebuild deterministically instead of polling.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <summary>
    /// <c>true</c> when <paramref name="mutation"/> targets the reserved tenant
    /// registry tree and so must trigger a snapshot rebuild. A pure predicate over
    /// the mutation's tree id.
    /// </summary>
    /// <param name="mutation">The observed mutation.</param>
    /// <returns><c>true</c> when the mutation should rebuild the snapshot.</returns>
    internal static bool IsRegistryMutation(LatticeMutation mutation) =>
        string.Equals(mutation.TreeId, TenantTreeNames.RegistryTree, StringComparison.Ordinal);

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it
    /// synchronously (awaited) when it is still cold. Idempotent: once any rebuild
    /// has advanced the epoch this returns immediately.
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
        if (IsRegistryMutation(mutation))
        {
            ScheduleRebuild();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Rebuilds the snapshot synchronously and returns the epoch it produced.
    /// Exposed for tests that need to force a deterministic rebuild.
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>The epoch of the rebuilt snapshot.</returns>
    internal async Task<long> RebuildNowAsync(CancellationToken cancellationToken = default)
    {
        await RebuildOnceAsync(cancellationToken).ConfigureAwait(false);
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
                        // Run the rescan off the mutating grain's scheduler and
                        // record it so a test can await the drain deterministically.
                        Volatile.Write(ref _backgroundRebuild, Task.Run(RunRebuildLoopAsync));
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
                _logger.LogWarning(ex, "Failed to rebuild the compiled tenant-policy snapshot; the previous snapshot remains in effect.");
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
            var records = new List<TenantRecord>();
            await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
            {
                records.Add(record);
            }

            var compiled = CompiledTenantPolicy.Compile(records);
            Volatile.Write(ref _current, compiled);
            Interlocked.Increment(ref _epoch);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }
}
