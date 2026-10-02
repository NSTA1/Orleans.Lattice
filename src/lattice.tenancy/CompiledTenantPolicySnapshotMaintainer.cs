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
/// The change-feed hook fires inline on the grain write path, so it must not scan
/// the registry synchronously. It therefore only <i>schedules</i> a local rebuild;
/// the actual rescan of the registry runs on a background continuation. This gives
/// eventual snapshot consistency: a committed registry edit is reflected shortly
/// after it commits, not necessarily before the writing call returns.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of registry writes collapses into at most one
/// in-flight rebuild plus at most one queued follow-up - and serialized, so the
/// snapshot always reflects a whole, self-consistent scan and the epoch never
/// regresses.
/// </para>
/// <para>
/// <b>Cross-silo currency (issue #4030).</b> The core dispatches the change-feed
/// hook only on the silo whose grain committed the write, so on its own it tells
/// every other silo nothing. Before the write returns, the committing silo's hook
/// therefore also advances the cluster-wide <see cref="TenantPolicyEpoch"/>
/// through the <see cref="ITenantPolicyEpochPublisher"/>, which pushes it to every
/// silo (<see cref="ObserveEpoch"/>) and waits for each to acknowledge or for its
/// lease to lapse. Each silo's snapshot is authoritative only while it holds a
/// live lease from the epoch grain (<see cref="ApplyLease"/>, renewed by
/// <see cref="TenantPolicyEpochSubscription"/>) and was built for the latest epoch
/// the silo has observed. A silo that cannot confirm its snapshot is current -
/// cold, unleased, behind a pushed epoch, or unable to publish an advance of its
/// own - is not authoritative, so its consumers confirm against the registry or
/// deny.
/// </para>
/// <para>
/// <b>Known bounded window.</b> A silo that crashes after durably committing a
/// registry write but before its hook advances the epoch leaves the other silos'
/// snapshots unaware of that write. The subscription closes it when cluster
/// membership declares the crashed silo dead: every surviving silo then treats its
/// snapshot as out of date (<see cref="InvalidateClusterView"/>) and rebuilds. The
/// window is therefore bounded by membership failure detection.
/// </para>
/// </remarks>
internal sealed class CompiledTenantPolicySnapshotMaintainer : IMutationObserver, ITenantEpochSubscriber, IDisposable
{
    private static readonly TimeSpan InitialAdvanceRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MaxAdvanceRetryDelay = TimeSpan.FromSeconds(5);

    private readonly ITenantRegistry _registry;
    private readonly ITenantPolicyEpochPublisher _publisher;
    private readonly TimeProvider _time;
    private readonly TenantSnapshotCurrency _currency;
    private readonly DelegatedTenantAccessFlag _delegatedAccess;
    private readonly ILogger<CompiledTenantPolicySnapshotMaintainer> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);
    private readonly CancellationTokenSource _disposed = new();

    private CompiledTenantPolicy _current = CompiledTenantPolicy.Empty;
    private long _epoch;

    // Consecutive background-rebuild failures since the last successful publish.
    // Reset to zero by PublishSnapshot.
    private int _consecutiveRebuildFailures;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    // The cluster generation (see TenantSnapshotCurrency) the current snapshot was
    // built for.
    private long _builtForGeneration;

    // Advances this silo's hook is publishing inline, and a sequence pair that
    // records whether an advance failed and has not yet been re-published.
    private int _advancesInFlight;
    private long _failedAdvanceSequence;
    private long _repairedAdvanceSequence;
    private int _advanceRetryState;
    private Task _advanceRetry = Task.CompletedTask;

    /// <summary>Initializes a new <see cref="CompiledTenantPolicySnapshotMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned to build the snapshot.</param>
    /// <param name="publisher">Publishes a committed registry change to every silo.</param>
    /// <param name="timeProvider">The clock the silo's lease is measured on.</param>
    /// <param name="logger">The logger for background-rebuild and publish failures.</param>
    /// <param name="delegatedAccess">
    /// The silo's live delegated tenant access administration flag, or <c>null</c>
    /// for a fixed disabled flag. The snapshot is compiled under its value, and a
    /// change of that value invalidates the snapshot and schedules a rebuild.
    /// </param>
    /// <exception cref="ArgumentNullException">Any argument other than <paramref name="delegatedAccess"/> is <c>null</c>.</exception>
    public CompiledTenantPolicySnapshotMaintainer(
        ITenantRegistry registry,
        ITenantPolicyEpochPublisher publisher,
        TimeProvider timeProvider,
        ILogger<CompiledTenantPolicySnapshotMaintainer> logger,
        DelegatedTenantAccessFlag? delegatedAccess = null)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _publisher = publisher;
        _time = timeProvider;
        _currency = new TenantSnapshotCurrency(timeProvider);
        _logger = logger;
        _delegatedAccess = delegatedAccess ?? DelegatedTenantAccessFlag.Disabled;
        if (delegatedAccess is not null)
        {
            delegatedAccess.Changed += OnDelegatedAccessChanged;
        }
    }

    /// <summary>
    /// Whether delegated tenant access administration is enabled on this silo right
    /// now. The current snapshot reports the value it was compiled under through
    /// <see cref="CompiledTenantPolicy.IsDelegatedAccessEnabled"/>; a policy
    /// compiled outside the snapshot (the gate's registry confirmation) uses this.
    /// </summary>
    public bool IsDelegatedAccessEnabled => _delegatedAccess.IsEnabled;

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledTenantPolicy Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// <c>true</c> when the current snapshot can be trusted as an answer that
    /// reflects every committed tenant-registry write - both as a <em>negative</em>
    /// answer (a tenant's absence, a grant's absence) and as a positive one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is deliberately <b>not</b> an age bound on the snapshot. Rebuilds are
    /// mutation-driven rather than periodic, so on a quiet estate a snapshot hours
    /// old is exactly correct; an age bound would force every consumer back onto
    /// the authoritative registry on a system that had simply stopped changing,
    /// which is the per-request grain call the snapshot exists to remove.
    /// </para>
    /// <para>
    /// It is <c>false</c> whenever the silo cannot know its snapshot is current:
    /// before the first build; while a rebuild is pending (a registry change has
    /// been observed and not yet compiled) or failing; while this silo is
    /// publishing an advance, or owes one it failed to publish; when the snapshot
    /// was built before the latest cluster epoch the silo has observed; and when
    /// the silo's lease from the epoch grain has lapsed, because then a write on
    /// another silo may have advanced the epoch without reaching this one. In each
    /// case a consumer falls back to the registry or denies. The check is a handful
    /// of field reads and one clock read, and allocates nothing (see
    /// <see cref="TenantSnapshotCurrency"/> for how the lease is measured).
    /// </para>
    /// </remarks>
    public bool IsSnapshotAuthoritative =>
        Interlocked.Read(ref _epoch) > 0
        && Volatile.Read(ref _rebuildState) == 0
        && Volatile.Read(ref _consecutiveRebuildFailures) == 0
        && Volatile.Read(ref _advancesInFlight) == 0
        && Interlocked.Read(ref _failedAdvanceSequence) == Interlocked.Read(ref _repairedAdvanceSequence)
        && _currency.IsCurrent(Volatile.Read(ref _builtForGeneration));

    /// <summary>
    /// The most recently scheduled background rebuild loop, or a completed task
    /// when none has been scheduled. Exposed so a test can await a change-feed-driven
    /// rebuild deterministically instead of polling.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <summary>
    /// The most recently started background re-publish of a failed epoch advance,
    /// or a completed task when none has been started. Exposed for tests.
    /// </summary>
    internal Task AdvanceRetry => Volatile.Read(ref _advanceRetry);

    /// <summary>
    /// Completes when the silo's first lease from the epoch grain has been applied.
    /// Exposed so a test can await authority deterministically.
    /// </summary>
    internal Task LeaseEstablished => _currency.LeaseEstablished;

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
    /// <remarks>
    /// For a write to the registry tree this schedules the local rebuild and then
    /// publishes the change to every silo, completing only once the publish has
    /// completed (or failed and been handed to a background re-publish). The core
    /// awaits the hook before the write returns, so a caller that sees the write
    /// complete can rely on no silo still treating a pre-write snapshot as
    /// authoritative.
    /// </remarks>
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (!IsRegistryMutation(mutation))
        {
            return Task.CompletedTask;
        }

        ScheduleRebuild();
        return PublishAdvanceAsync(cancellationToken);
    }

    /// <summary>
    /// Records a cluster epoch pushed by the epoch grain or carried on a lease.
    /// When it supersedes the latest epoch this silo has observed, the snapshot
    /// stops being authoritative at once and a rebuild is scheduled; an epoch that
    /// does not supersede it (a late, out-of-order answer) is ignored.
    /// </summary>
    /// <param name="epoch">The observed cluster epoch.</param>
    public void ObserveEpoch(TenantPolicyEpoch epoch)
    {
        if (_currency.Observe(epoch))
        {
            ScheduleRebuild();
        }
    }

    /// <summary>
    /// Applies a lease granted by the epoch grain: observes its epoch and extends
    /// the silo's authority to <paramref name="requestedAt"/> plus the granted
    /// duration. Measuring from when the request was sent keeps the silo's deadline
    /// no later than the one the grain recorded. A lease never shortens an existing
    /// deadline.
    /// </summary>
    /// <param name="lease">The granted lease.</param>
    /// <param name="requestedAt">The <see cref="TimeProvider"/> timestamp taken before the lease was requested.</param>
    public void ApplyLease(TenantPolicyEpochLease lease, long requestedAt)
    {
        if (_currency.ApplyLease(lease, requestedAt))
        {
            ScheduleRebuild();
        }
    }

    /// <summary>
    /// Treats the snapshot as out of date without a new epoch and schedules a
    /// rebuild. Used when cluster membership declares a silo dead, since that silo
    /// may have committed a registry write it never got to publish.
    /// </summary>
    public void InvalidateClusterView()
    {
        _currency.Invalidate();
        ScheduleRebuild();
    }

    /// <summary>
    /// Rebuilds the snapshot synchronously and returns the epoch it produced.
    /// Exposed for tests that need to force a deterministic rebuild; unlike the
    /// production background path it does not swallow a genuine rebuild failure, but
    /// it tolerates a transient Orleans streaming <see cref="EnumerationAbortedException"/>
    /// on the registry scan by re-enumerating immediately, a small bounded number of
    /// times.
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>The epoch of the rebuilt snapshot.</returns>
    internal async Task<long> RebuildNowAsync(CancellationToken cancellationToken = default)
    {
        const int maxScanAttempts = 8;

        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var generation = _currency.Generation;
            List<TenantRecord> records;
            var attempt = 1;
            while (true)
            {
                try
                {
                    records = await ScanRecordsAsync(cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (EnumerationAbortedException) when (attempt < maxScanAttempts)
                {
                    attempt++;
                }
            }

            PublishSnapshot(records, generation);
        }
        finally
        {
            _rebuildLock.Release();
        }

        return CurrentEpoch;
    }

    /// <inheritdoc />
    /// <remarks>Stops any background re-publish of a failed epoch advance, and stops observing the delegated-access flag. Idempotent.</remarks>
    public void Dispose()
    {
        _delegatedAccess.Changed -= OnDelegatedAccessChanged;
        _disposed.Cancel();
    }

    /// <summary>
    /// Reacts to a change of the delegated tenant access administration flag. The
    /// current snapshot was compiled under the old value, so it stops being
    /// authoritative at once (its consumers confirm against the registry under the
    /// new value) and a rebuild is scheduled - the same trigger a silo uses when it
    /// cannot vouch for its snapshot (<see cref="InvalidateClusterView"/>).
    /// </summary>
    private void OnDelegatedAccessChanged()
    {
        _logger.LogInformation(
            "Delegated tenant access administration is now {State}; rebuilding the compiled tenant-policy snapshot.",
            _delegatedAccess.IsEnabled ? "enabled" : "disabled");
        InvalidateClusterView();
    }

    private async Task PublishAdvanceAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _advancesInFlight);
        try
        {
            await _publisher.AdvanceAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The write is already durable and cannot be rolled back, and the other
            // silos have not been told. Keep this silo non-authoritative and
            // re-publish in the background until an advance lands.
            _logger.LogWarning(
                ex,
                "Failed to publish a tenant-registry change to the other silos; retrying in the background. Until it is published this silo does not treat its tenant-policy snapshot as authoritative.");
            Interlocked.Increment(ref _failedAdvanceSequence);
            EnsureAdvanceRetry();
        }
        finally
        {
            Interlocked.Decrement(ref _advancesInFlight);
        }
    }

    private void EnsureAdvanceRetry()
    {
        if (Interlocked.CompareExchange(ref _advanceRetryState, 1, 0) == 0)
        {
            Volatile.Write(ref _advanceRetry, Task.Run(RetryAdvanceLoopAsync));
        }
    }

    private async Task RetryAdvanceLoopAsync()
    {
        var stopping = _disposed.Token;
        var delay = InitialAdvanceRetryDelay;
        while (true)
        {
            var owed = Interlocked.Read(ref _failedAdvanceSequence);
            if (owed == Interlocked.Read(ref _repairedAdvanceSequence))
            {
                // Nothing owed: go idle, then re-check so a failure recorded after the
                // read above is not stranded without a retry loop.
                Volatile.Write(ref _advanceRetryState, 0);
                if (Interlocked.Read(ref _failedAdvanceSequence) == Interlocked.Read(ref _repairedAdvanceSequence)
                    || Interlocked.CompareExchange(ref _advanceRetryState, 1, 0) != 0)
                {
                    return;
                }

                continue;
            }

            try
            {
                await Task.Delay(delay, _time, stopping).ConfigureAwait(false);
                await _publisher.AdvanceAsync(stopping).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Re-publishing a tenant-registry change to the other silos failed; retrying.");
                delay = delay * 2 > MaxAdvanceRetryDelay ? MaxAdvanceRetryDelay : delay * 2;
                continue;
            }

            // One successful advance covers every write committed before it started,
            // so it repairs every failure recorded up to the read above.
            var repaired = Interlocked.Read(ref _repairedAdvanceSequence);
            while (owed > repaired)
            {
                var observed = Interlocked.CompareExchange(ref _repairedAdvanceSequence, owed, repaired);
                if (observed == repaired)
                {
                    break;
                }

                repaired = observed;
            }

            delay = InitialAdvanceRetryDelay;
        }
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
                Interlocked.Increment(ref _consecutiveRebuildFailures);
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
            // Captured before the scan: an epoch observed while the scan runs bumps
            // the generation past it, so the result is not authoritative and the
            // rebuild that observation queued captures the change.
            var generation = _currency.Generation;
            PublishSnapshot(await ScanRecordsAsync(cancellationToken).ConfigureAwait(false), generation);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    /// <summary>
    /// Enumerates every tenant record from the registry. This is the only step that
    /// touches the (grain-backed) registry, so it is the only step that can raise a
    /// transient <see cref="EnumerationAbortedException"/> when the registry grain
    /// deactivates mid-scan.
    /// </summary>
    private async Task<List<TenantRecord>> ScanRecordsAsync(CancellationToken cancellationToken)
    {
        var records = new List<TenantRecord>();
        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            records.Add(record);
        }

        return records;
    }

    /// <summary>
    /// Compiles a freshly scanned record set into the current snapshot, swapping it
    /// in atomically, recording the cluster generation it was built for, and
    /// advancing the epoch exactly once. Pure and non-faulting - it never touches
    /// the registry. The caller holds <see cref="_rebuildLock"/>.
    /// </summary>
    private void PublishSnapshot(List<TenantRecord> records, long generation)
    {
        var compiled = CompiledTenantPolicy.Compile(records, _delegatedAccess.IsEnabled);
        Volatile.Write(ref _current, compiled);
        Volatile.Write(ref _builtForGeneration, generation);
        Volatile.Write(ref _consecutiveRebuildFailures, 0);
        Interlocked.Increment(ref _epoch);
    }
}
