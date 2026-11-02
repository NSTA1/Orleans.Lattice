using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo maintainer of the compiled app-registry snapshot. It builds the
/// snapshot from the full registry on first use, observes the core change-feed
/// (<see cref="IMutationObserver"/>) and rebuilds when the reserved
/// <c>sys-app-registry</c> tree mutates, swaps the immutable snapshot atomically, and
/// stamps a monotonic <see cref="CurrentEpoch"/> on every rebuild.
/// </summary>
/// <remarks>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it must return
/// quickly and must not scan storage synchronously. It therefore only
/// <i>schedules</i> a rebuild; the actual rescan of the registry runs on a
/// background continuation. This gives eventual snapshot consistency: a committed
/// registry edit is reflected shortly after it commits, not necessarily before the
/// writing call returns.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of registry writes collapses into at most one
/// in-flight rebuild plus at most one queued follow-up - and serialized, so the
/// snapshot always reflects a whole, self-consistent scan and the epoch never
/// regresses.
/// </para>
/// <para>
/// <b>Cross-silo currency (#5595).</b> The change feed fires only on the silo hosting the
/// registry leaf that committed a write, so on its own it leaves every other silo on the
/// snapshot it first warmed on - hiding an app enabled since, and still granting one disabled
/// or re-bound since. When a cluster-wide <see cref="IAppRegistryEpochSource"/> is present, the
/// committing silo advances the <see cref="AppRegistryEpoch"/> inline, before its write returns
/// (retrying in the background if the advance fails), and every snapshot is stamped with the
/// epoch read <i>before</i> its scan began. <see cref="EnsureCurrentAsync"/> reads the epoch and
/// rebuilds from an authoritative scan whenever it differs from the stamp, so an evaluation
/// that starts after a write returned on any silo never answers from a snapshot that predates
/// it. It fails closed: an epoch it cannot read, or a snapshot built while the epoch was
/// unreadable, is never trusted - the registry is scanned afresh instead.
/// </para>
/// </remarks>
internal sealed class CompiledAppRegistrySnapshotMaintainer : IMutationObserver, IAppRegistryProjection, IDisposable
{
    private static readonly TimeSpan InitialPublishRetryDelay = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan MaxPublishRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IAppRegistry _registry;
    private readonly ILogger<CompiledAppRegistrySnapshotMaintainer> _logger;
    private readonly TimeProvider _time;
    private readonly IAppRegistryEpochSource? _epochSource;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);
    private readonly CancellationTokenSource _stopping = new();

    private CompiledAppRegistrySnapshot _current = CompiledAppRegistrySnapshot.Empty;
    private long _epoch;
    private long _lastRebuildUtcTicks;

    // The cluster epoch read before the scan that built _current, or null when it was not
    // read (no source, or unreadable) - a null stamp never vouches for the snapshot.
    private StampedEpoch? _builtAgainst;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    // Single-flight state for cluster epoch reads; see ReadEpochAsync.
    private readonly object _readGate = new();
    private bool _reading;
    private TaskCompletionSource<AppRegistryEpoch>? _nextRead;

    // Coalescing state for background epoch publishes, as for rebuilds.
    private int _publishState;
    private Task _publishLoop = Task.CompletedTask;

    /// <summary>Initializes a new <see cref="CompiledAppRegistrySnapshotMaintainer"/>.</summary>
    /// <param name="registry">The app registry scanned to build the snapshot.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <param name="timeProvider">
    /// The clock used to stamp <see cref="LastRebuildUtc"/>; defaults to
    /// <see cref="TimeProvider.System"/>.
    /// </param>
    /// <param name="epoch">
    /// The cluster-wide registry epoch the snapshot is measured against, or null when the host has no
    /// cluster (the snapshot is then only ever as current as this silo's change feed makes it).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="registry"/> or <paramref name="logger"/> is <c>null</c>.</exception>
    public CompiledAppRegistrySnapshotMaintainer(
        IAppRegistry registry,
        ILogger<CompiledAppRegistrySnapshotMaintainer> logger,
        TimeProvider? timeProvider = null,
        IAppRegistryEpochSource? epoch = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
        _epochSource = epoch;
    }

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledAppRegistrySnapshot Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>Whether the snapshot is measured against a cluster-wide registry epoch.</summary>
    internal bool HasClusterEpoch => _epochSource is not null;

    /// <summary>The most recently started background epoch publish, exposed so a test can await it.</summary>
    internal Task PublishLoop => Volatile.Read(ref _publishLoop);

    /// <inheritdoc />
    public async Task EnsureCurrentAsync(CancellationToken cancellationToken = default)
    {
        if (_epochSource is null)
        {
            await EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        AppRegistryEpoch observed;
        try
        {
            observed = await ReadEpochAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // Fail closed: without the epoch nothing vouches for the snapshot, so answer
            // from an authoritative scan rather than from whatever this silo last built.
            _logger.LogDebug(ex, "Could not read the cluster-wide app-registry epoch; rebuilding the snapshot from the registry.");
            await RebuildOnceAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (IsCurrentAgainst(observed))
        {
            return;
        }

        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A concurrent caller may have rebuilt against this epoch (or later) while we waited.
            if (!IsCurrentAgainst(observed))
            {
                await RebuildCoreAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _stopping.Cancel();

    /// <summary>
    /// Reads the cluster epoch, single-flight per silo: at most one read is in flight, and every caller that
    /// arrives while one is shares the <i>next</i> read, issued as soon as the current one completes. A caller
    /// never joins a read already in flight when it arrived, because that read may have reached the epoch
    /// grain before a registry write that returned before the caller began - joining it could vouch for a
    /// snapshot that predates a revocation.
    /// </summary>
    private Task<AppRegistryEpoch> ReadEpochAsync()
    {
        lock (_readGate)
        {
            if (_reading)
            {
                return (_nextRead ??= new TaskCompletionSource<AppRegistryEpoch>(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            }

            _reading = true;
        }

        // Issued outside the lock: a read that completes synchronously re-enters OnEpochReadCompleted.
        return LeadReadAsync();
    }

    private async Task<AppRegistryEpoch> LeadReadAsync()
    {
        try
        {
            return await _epochSource!.GetAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            OnEpochReadCompleted();
        }
    }

    private async Task FollowReadAsync(TaskCompletionSource<AppRegistryEpoch> waiters)
    {
        try
        {
            waiters.TrySetResult(await _epochSource!.GetAsync(CancellationToken.None).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            waiters.TrySetException(ex);
        }
        finally
        {
            OnEpochReadCompleted();
        }
    }

    /// <summary>Ends the read in flight, issuing the queued next read when callers are waiting on it.</summary>
    private void OnEpochReadCompleted()
    {
        TaskCompletionSource<AppRegistryEpoch>? next;
        lock (_readGate)
        {
            next = _nextRead;
            _nextRead = null;
            if (next is null)
            {
                _reading = false;
                return;
            }
        }

        // FollowReadAsync never faults: its outcome is delivered through the waiters.
        _ = FollowReadAsync(next);
    }

    private bool IsCurrentAgainst(AppRegistryEpoch observed) =>
        Interlocked.Read(ref _epoch) > 0
        && Volatile.Read(ref _builtAgainst) is { } stamp
        && stamp.Epoch.Incarnation == observed.Incarnation
        && stamp.Epoch.Version >= observed.Version;

    /// <summary>
    /// The wall-clock instant the snapshot was last rebuilt, or <c>null</c> when
    /// it has never been built.
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
    /// The most recently scheduled background rebuild loop, or a completed task when
    /// none has been scheduled. Exposed so a test can await a change-feed-driven
    /// rebuild deterministically instead of polling.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <summary>
    /// <c>true</c> when <paramref name="mutation"/> targets the reserved app-registry
    /// tree and so must trigger a snapshot rebuild. A pure, allocation-free ordinal
    /// comparison of the mutation's tree id.
    /// </summary>
    /// <param name="mutation">The observed mutation.</param>
    /// <returns><c>true</c> when the mutation should rebuild the snapshot.</returns>
    internal static bool IsRegistryMutation(LatticeMutation mutation) =>
        string.Equals(mutation.TreeId, AppRegistryTreeNames.RegistryTree, StringComparison.Ordinal);

    /// <summary>
    /// Ensures the snapshot has been built at least once, building it
    /// synchronously (awaited) when it is still cold. Idempotent: once any rebuild
    /// has advanced the epoch this returns immediately.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    /// <returns>A task that completes when the snapshot is warm.</returns>
    public async Task EnsureWarmAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Read(ref _epoch) > 0)
        {
            return;
        }

        await RebuildOnceAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// For a registry write, advances the cluster-wide epoch inline - registry writes are rare
    /// control-plane operations, and advancing before the write returns is what lets every other
    /// silo's next evaluation see it - then schedules this silo's own rebuild.
    /// </remarks>
    public async Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (!IsRegistryMutation(mutation))
        {
            return;
        }

        if (_epochSource is not null)
        {
            await PublishAsync(cancellationToken).ConfigureAwait(false);
        }

        ScheduleRebuild();
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
                        // Run the rescan off the mutating grain's scheduler and record it
                        // so a test can await the drain deterministically.
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
                _logger.LogWarning(ex, "Failed to rebuild the compiled app-registry snapshot; the previous snapshot remains in effect.");
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
            await RebuildCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    /// <summary>Scans and publishes a new snapshot. The caller holds <see cref="_rebuildLock"/>.</summary>
    private async Task RebuildCoreAsync(CancellationToken cancellationToken)
    {
        // Read the cluster epoch before the scan: every write that advanced the epoch to this
        // value had committed before the read, so the scan below includes it and the snapshot
        // may vouch for this epoch. An unreadable epoch stamps nothing, so the snapshot is
        // never trusted by EnsureCurrentAsync.
        StampedEpoch? stamp = null;
        if (_epochSource is not null)
        {
            try
            {
                stamp = new StampedEpoch(await _epochSource.GetAsync(cancellationToken).ConfigureAwait(false));
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                _logger.LogDebug(ex, "Could not read the cluster-wide app-registry epoch before a rebuild; the snapshot will not be trusted as current.");
            }
        }

        // The registry's scan is resilient to a transient enumeration abort
        // caused by the registry tree grain deactivating mid-stream, so a plain
        // buffering scan here is sufficient.
        var records = new List<AppRegistryRecord>();
        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            records.Add(record);
        }

        // The rebuild lock serializes publishers, so the next epoch is exactly one
        // past the current one and can be stamped onto the snapshot it describes.
        // The snapshot is published before the stamp, so a reader that sees the stamp
        // reads a snapshot at least as new as the one it vouches for.
        var epoch = Interlocked.Read(ref _epoch) + 1;
        var compiled = CompiledAppRegistrySnapshot.Compile(records, epoch);
        Volatile.Write(ref _current, compiled);
        Interlocked.Exchange(ref _epoch, epoch);
        Volatile.Write(ref _builtAgainst, stamp);
        Interlocked.Exchange(ref _lastRebuildUtcTicks, _time.GetUtcNow().UtcTicks);
    }

    /// <summary>Advances the cluster epoch inline, falling back to a background retry when it fails.</summary>
    private async Task PublishAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _epochSource!.AdvanceAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Publishing an app-registry change to the cluster failed; retrying in the background.");
            SchedulePublish();
        }
    }

    private void SchedulePublish()
    {
        while (true)
        {
            switch (Volatile.Read(ref _publishState))
            {
                case 0:
                    if (Interlocked.CompareExchange(ref _publishState, 1, 0) == 0)
                    {
                        Volatile.Write(ref _publishLoop, Task.Run(PublishLoopAsync));
                        return;
                    }

                    break;
                case 1:
                    if (Interlocked.CompareExchange(ref _publishState, 2, 1) == 1)
                    {
                        return;
                    }

                    break;
                default:
                    return;
            }
        }
    }

    private async Task PublishLoopAsync()
    {
        var cancellationToken = _stopping.Token;
        var retryDelay = InitialPublishRetryDelay;
        while (true)
        {
            try
            {
                await Task.Delay(retryDelay, _time, cancellationToken).ConfigureAwait(false);
                await _epochSource!.AdvanceAsync(cancellationToken).ConfigureAwait(false);
                retryDelay = InitialPublishRetryDelay;

                // Go idle unless another failed advance queued a follow-up while this one ran.
                if (Interlocked.CompareExchange(ref _publishState, 0, 1) == 1)
                {
                    return;
                }

                Volatile.Write(ref _publishState, 1);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Volatile.Write(ref _publishState, 0);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Publishing an app-registry change to the cluster failed; retrying.");
                retryDelay = retryDelay * 2 > MaxPublishRetryDelay ? MaxPublishRetryDelay : retryDelay * 2;
            }
        }
    }

    /// <summary>A boxed <see cref="AppRegistryEpoch"/>, so the stamp is swapped as one reference.</summary>
    private sealed record StampedEpoch(AppRegistryEpoch Epoch);
}
