using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The per-silo maintainer of the compiled authorization snapshot. It builds the
/// snapshot from the full rule set on first use, observes the core change-feed
/// (<see cref="IMutationObserver"/>) and rebuilds when the reserved policy tree
/// mutates, swaps the immutable snapshot atomically, and stamps a monotonic
/// <see cref="CurrentEpoch"/> on every rebuild.
/// </summary>
/// <remarks>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it must return
/// quickly and must not scan storage synchronously. It therefore only
/// <i>schedules</i> a rebuild; the actual rescan of the policy tree runs on a
/// background continuation. This gives eventual snapshot consistency: a committed
/// policy edit is reflected shortly after it commits, not necessarily before the
/// writing call returns.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of policy writes collapses into at most one
/// in-flight rebuild plus at most one queued follow-up - and serialized, so the
/// snapshot always reflects a whole, self-consistent scan and the epoch never
/// regresses.
/// </para>
/// </remarks>
internal sealed class CompiledPolicySnapshotMaintainer : IMutationObserver
{
    private readonly ILatticeAuthorizationPolicyStore _store;
    private readonly ILogger<CompiledPolicySnapshotMaintainer> _logger;
    private readonly TimeProvider _time;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private CompiledPolicy _current = CompiledPolicy.Empty;
    private long _epoch;
    private long _lastRebuildUtcTicks;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;

    /// <summary>Initializes a new <see cref="CompiledPolicySnapshotMaintainer"/>.</summary>
    /// <param name="store">The policy store scanned to build the snapshot.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <param name="timeProvider">
    /// The clock used to stamp the last-rebuild time that backs the snapshot-age
    /// observable gauge; defaults to <see cref="TimeProvider.System"/>.
    /// </param>
    public CompiledPolicySnapshotMaintainer(
        ILatticeAuthorizationPolicyStore store,
        ILogger<CompiledPolicySnapshotMaintainer> logger,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;

        // Publish this maintainer as a source for the compiled-snapshot epoch and
        // age observable gauges. Registration is idempotent and holds only a weak
        // reference, so it never keeps a shut-down silo's maintainer alive.
        AuthSnapshotGaugeRegistry.Register(this);
    }

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledPolicy Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// The wall-clock instant the snapshot was last rebuilt, or <c>null</c> when
    /// it has never been built. Backs the snapshot-age observable gauge.
    /// </summary>
    public DateTimeOffset? LastRebuildUtc
    {
        get
        {
            var ticks = Interlocked.Read(ref _lastRebuildUtcTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

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
        if (string.Equals(mutation.TreeId, AuthConstants.PolicyTree, StringComparison.Ordinal))
        {
            ScheduleRebuild();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Rebuilds the snapshot synchronously and returns the epoch it produced.
    /// Exposed for tests that need to force a deterministic rebuild.
    /// </summary>
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
                _logger.LogWarning(ex, "Failed to rebuild the compiled authorization policy snapshot; the previous snapshot remains in effect.");
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
            // The store's scan is resilient to a transient enumeration abort
            // caused by a concurrent scan over the policy tree, so a plain
            // buffering scan here is sufficient.
            var rules = new List<LatticeAuthorizationRule>();
            await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                rules.Add(rule);
            }

            var compiled = CompiledPolicy.Compile(rules);
            Volatile.Write(ref _current, compiled);
            Interlocked.Increment(ref _epoch);
            Interlocked.Exchange(ref _lastRebuildUtcTicks, _time.GetUtcNow().UtcTicks);

            // Observability only: count the rebuild. Never affects the snapshot.
            if (LatticeAuthMetrics.SnapshotRebuilds.Enabled)
            {
                LatticeAuthMetrics.SnapshotRebuilds.Add(1);
            }
        }
        finally
        {
            _rebuildLock.Release();
        }
    }
}
