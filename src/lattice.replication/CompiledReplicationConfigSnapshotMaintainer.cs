using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The per-silo maintainer of the compiled runtime replication-configuration
/// snapshot. It builds the snapshot from the
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> tree on first use,
/// observes the core change feed (<see cref="IMutationObserver"/>) and rebuilds
/// when that config tree mutates, swaps the immutable
/// <see cref="CompiledReplicationConfig"/> atomically, and stamps a monotonic
/// <see cref="CurrentEpoch"/> on every rebuild. Mirrors the auth package's
/// <c>CompiledPolicySnapshotMaintainer</c>.
/// </summary>
/// <remarks>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it must return
/// quickly and must not scan storage synchronously. It therefore only
/// <i>schedules</i> a rebuild; the actual rescan of the config tree runs on a
/// background continuation. This gives eventual snapshot consistency: a
/// committed config edit is reflected shortly after it commits, not necessarily
/// before the writing call returns.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of config writes collapses into at most one
/// in-flight rebuild plus at most one queued follow-up - and serialized, so the
/// snapshot always reflects a whole, self-consistent scan and the epoch never
/// regresses.
/// </para>
/// </remarks>
internal sealed class CompiledReplicationConfigSnapshotMaintainer : IMutationObserver
{
    private readonly ILatticeReplicationConfigStore _store;
    private readonly ILogger<CompiledReplicationConfigSnapshotMaintainer> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private CompiledReplicationConfig _current = CompiledReplicationConfig.Empty;
    private long _epoch;
    private int _warmStarted;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running
    // with a queued follow-up.
    private int _rebuildState;

    /// <summary>Initializes a new <see cref="CompiledReplicationConfigSnapshotMaintainer"/>.</summary>
    /// <param name="store">The config store scanned to build the snapshot.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    public CompiledReplicationConfigSnapshotMaintainer(
        ILatticeReplicationConfigStore store,
        ILogger<CompiledReplicationConfigSnapshotMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _logger = logger;
    }

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledReplicationConfig Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it
    /// synchronously (awaited) when it is still cold. Idempotent: once any
    /// rebuild has advanced the epoch this returns immediately.
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

    /// <summary>
    /// Kicks off a one-shot background warm-up rebuild the first time it is
    /// called on a still-cold maintainer, without blocking the caller. The
    /// dynamic seams call this on their first read so a silo that starts with a
    /// pre-existing config tree converges its snapshot shortly after activation
    /// even before any local config-tree mutation is observed. Idempotent and
    /// allocation-free: once the epoch has advanced (or a warm-up has been
    /// started) this is a single volatile read.
    /// </summary>
    public void EnsureWarmStarted()
    {
        if (Interlocked.Read(ref _epoch) > 0)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _warmStarted, 1, 0) == 0)
        {
            ScheduleRebuild();
        }
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (string.Equals(mutation.TreeId, LatticeSystemTreeNames.ReplicationConfig, StringComparison.Ordinal))
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
                _logger.LogWarning(ex, "Failed to rebuild the compiled replication-configuration snapshot; the previous snapshot remains in effect.");
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
            var entries = await _store.ReadEntriesAsync(cancellationToken).ConfigureAwait(false);
            var compiled = CompiledReplicationConfig.Compile(entries);
            Volatile.Write(ref _current, compiled);
            Interlocked.Increment(ref _epoch);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }
}
