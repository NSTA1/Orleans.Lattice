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
/// snapshot consistency; until a rebuild lands the snapshot is not authoritative
/// (see below), so the resolver never seeds a placement from it. Rebuilds are coalesced - a burst of
/// registry writes collapses into at most one in-flight rebuild plus at most one
/// queued follow-up - and serialized, so the snapshot always reflects a whole,
/// self-consistent scan and the epoch never regresses.
/// </para>
/// <para>
/// <b>Cross-silo currency (issue #4052).</b> The change-feed hook fires only on the
/// silo that committed the registry write, so on its own it would leave every other
/// silo resolving a tenant's old placement indefinitely - and a WAL placement pin is
/// immutable once seeded, so a tree registered there for a tenant just moved to a
/// dedicated WAL would land on the shared WAL for good. The snapshot is therefore
/// kept current by the same cluster-wide tenant-policy epoch and lease as the
/// compiled tenant-policy snapshot (<see cref="TenantSnapshotCurrency"/>, fed by
/// <see cref="TenantPolicyEpochSubscription"/>), and is authoritative
/// (<see cref="IsSnapshotAuthoritative"/>) only when it has been built, no rebuild
/// is outstanding, it was built for the latest generation the silo has observed,
/// and the lease is live. While it is not, <see cref="TenantWalPlacementResolver"/>
/// waits a bounded time for it to become authoritative
/// (<see cref="WaitUntilAuthoritativeAsync"/>) and otherwise refuses the
/// registration rather than seed a placement that may be stale.
/// </para>
/// </remarks>
internal sealed class TenantPlacementSnapshotMaintainer : IMutationObserver, ITenantEpochSubscriber
{
    private readonly ITenantRegistry _registry;
    private readonly TenantSnapshotCurrency _currency;
    private readonly ILogger<TenantPlacementSnapshotMaintainer> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private TenantPlacementSnapshot _current = TenantPlacementSnapshot.Empty;
    private long _epoch;
    private long _builtForGeneration;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    // Completed and replaced whenever authority may have changed (a rebuild loop
    // went idle, a snapshot was published, a lease was applied), so a waiter in
    // WaitUntilAuthoritativeAsync re-checks without polling.
    private TaskCompletionSource _changed = NewSignal();

    /// <summary>Initializes a new <see cref="TenantPlacementSnapshotMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned to build the snapshot.</param>
    /// <param name="timeProvider">The clock the silo's lease is measured on.</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantPlacementSnapshotMaintainer(
        ITenantRegistry registry,
        TimeProvider timeProvider,
        ILogger<TenantPlacementSnapshotMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _currency = new TenantSnapshotCurrency(timeProvider);
        _logger = logger;
    }

    /// <summary>The current snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public TenantPlacementSnapshot Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// <c>true</c> when the current snapshot reflects every committed
    /// tenant-registry write: it has been built, no rebuild is outstanding, it was
    /// built for the latest cluster generation this silo has observed, and the
    /// silo's lease from the tenant-policy epoch grain is live. A handful of field
    /// reads and one clock read; allocates nothing.
    /// </summary>
    public bool IsSnapshotAuthoritative =>
        Interlocked.Read(ref _epoch) > 0
        && Volatile.Read(ref _rebuildState) == 0
        && _currency.IsCurrent(Volatile.Read(ref _builtForGeneration));

    /// <summary>
    /// The most recently scheduled background rebuild loop, or a completed task when
    /// none has been scheduled. Exposed so a test can await a rebuild deterministically.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <summary>Completes when the silo's first lease has been applied. Exposed for tests.</summary>
    internal Task LeaseEstablished => _currency.LeaseEstablished;

    /// <summary>
    /// Waits until the snapshot is authoritative, for at most <paramref name="bound"/>
    /// on the maintainer's clock. Does no registry read of its own: the rebuild that
    /// restores authority runs on its own call chain, so this is safe to await from
    /// inside the tree registry grain's turn. Returns at once when the snapshot is
    /// already authoritative.
    /// </summary>
    /// <param name="bound">The longest time to wait.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns><c>true</c> when the snapshot is authoritative; <c>false</c> when the bound elapsed first.</returns>
    public async Task<bool> WaitUntilAuthoritativeAsync(TimeSpan bound, CancellationToken cancellationToken = default)
    {
        if (IsSnapshotAuthoritative)
        {
            return true;
        }

        using var expiry = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var deadline = Task.Delay(bound, _currency.Time, expiry.Token);
        try
        {
            while (true)
            {
                var changed = Volatile.Read(ref _changed).Task;
                if (IsSnapshotAuthoritative)
                {
                    return true;
                }

                if (await Task.WhenAny(changed, deadline).ConfigureAwait(false) == deadline)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return IsSnapshotAuthoritative;
                }
            }
        }
        finally
        {
            // Releases the deadline timer when the wait ends early.
            await expiry.CancelAsync().ConfigureAwait(false);
        }
    }

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
    /// <remarks>
    /// A write to the registry tree also marks the snapshot out of date, so it is not
    /// authoritative until the rebuild it schedules succeeds. Publishing the change
    /// to the other silos is the compiled tenant-policy maintainer's job; one advance
    /// per write reaches every snapshot on every silo.
    /// </remarks>
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (string.Equals(mutation.TreeId, TenantTreeNames.RegistryTree, StringComparison.Ordinal))
        {
            InvalidateClusterView();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void ObserveEpoch(TenantPolicyEpoch epoch)
    {
        if (_currency.Observe(epoch))
        {
            ScheduleRebuild();
        }
    }

    /// <inheritdoc />
    public void ApplyLease(TenantPolicyEpochLease lease, long requestedAt)
    {
        if (_currency.ApplyLease(lease, requestedAt))
        {
            ScheduleRebuild();
        }

        SignalChanged();
    }

    /// <inheritdoc />
    public void InvalidateClusterView()
    {
        _currency.Invalidate();
        ScheduleRebuild();
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
            var generation = _currency.Generation;
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

            SwapSnapshot(byTenant, generation);
        }
        finally
        {
            _rebuildLock.Release();
        }

        SignalChanged();
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
                _logger.LogWarning(ex, "Failed to rebuild the tenant-placement snapshot; the previous snapshot remains in effect.");
            }

            // Go idle if no follow-up was queued; otherwise reset to running and
            // loop so the latest committed change is captured.
            if (Interlocked.CompareExchange(ref _rebuildState, 0, 1) == 1)
            {
                SignalChanged();
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
            // Captured before the scan: a change observed while it runs bumps the
            // generation past it, so the result is not authoritative and the rebuild
            // that observation queued captures the change.
            var generation = _currency.Generation;
            var byTenant = await ScanPlacementsAsync(cancellationToken).ConfigureAwait(false);
            SwapSnapshot(byTenant, generation);
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
    /// immutable snapshot, swaps it in atomically, records the cluster generation it
    /// was built for, and advances the epoch exactly once. Pure and non-faulting - it
    /// never touches the registry.
    /// </summary>
    private void SwapSnapshot(Dictionary<TenantId, TenantPlacement> byTenant, long generation)
    {
        Volatile.Write(ref _current, TenantPlacementSnapshot.Build(byTenant));
        Volatile.Write(ref _builtForGeneration, generation);
        Interlocked.Increment(ref _epoch);
    }

    private void SignalChanged() => Interlocked.Exchange(ref _changed, NewSignal()).TrySetResult();

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
