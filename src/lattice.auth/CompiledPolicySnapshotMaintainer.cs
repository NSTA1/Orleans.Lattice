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
    private readonly ITenantRuleLayer? _tenantLayer;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private CompiledPolicy _current = CompiledPolicy.Empty;
    private long _epoch;
    private long _lastRebuildUtcTicks;

    // Whether the published snapshot may answer requests without a rebuild. Distinct
    // from the epoch, which only ever advances: a snapshot built over an empty policy
    // stops being warm the moment the first rule commits (see OnMutationAsync). The
    // three fields below change together under _warmGate, so a rebuild cannot mark a
    // snapshot warm after a write it did not see has marked it cold.
    private readonly Lock _warmGate = new();
    private volatile bool _warm;
    private bool _currentHasRules;

    // Advances on every policy-tree mutation, so a rebuild can tell whether a write
    // landed while it scanned.
    private long _policyMutations;

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
    /// <param name="tenantLayer">
    /// The tenant-layer switch, read at the start of each rebuild to decide whether
    /// the snapshot carries a tenant partition. <c>null</c> (a host-built maintainer)
    /// means the layer is inactive.
    /// </param>
    public CompiledPolicySnapshotMaintainer(
        ILatticeAuthorizationPolicyStore store,
        ILogger<CompiledPolicySnapshotMaintainer> logger,
        TimeProvider? timeProvider = null,
        ITenantRuleLayer? tenantLayer = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
        _tenantLayer = tenantLayer;

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
    /// The number of distinct subjects (users and groups) the current snapshot
    /// references - the count of members for which an authorization policy is
    /// configured. Backs the compiled-snapshot <c>subjects</c> observable gauge.
    /// </summary>
    public int CurrentSubjectCount => Current.DistinctSubjectCount;

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
    /// Whether <see cref="Current"/> may answer a request without first awaiting a
    /// rebuild. False until the first build, and false again from the moment the
    /// first rule commits to a policy whose published snapshot holds none, until a
    /// rebuild whose scan no later policy write overlapped publishes.
    /// </summary>
    /// <remarks>
    /// Once the snapshot holds rules this stays true across later edits, which are
    /// reflected eventually (see the type remarks). The cold window exists because
    /// an empty snapshot can be published at once - the store answers a policy tree
    /// that was never written without scanning it (issue 4128) - and the first
    /// rebuild after a host seeds its grants scans a tree whose shards those writes
    /// are still creating, which can take seconds. Serving the empty snapshot
    /// through that window denies every grant the host has just committed.
    /// </remarks>
    public bool IsWarm => _warm;

    /// <summary>
    /// Ensures the snapshot is warm (<see cref="IsWarm"/>), building it
    /// synchronously (awaited) when it is not. Returns immediately once warm, and
    /// skips its own scan when a rebuild it queued behind left the snapshot warm.
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    public async Task EnsureWarmAsync(CancellationToken cancellationToken = default)
    {
        if (_warm)
        {
            return;
        }

        await RebuildOnceAsync(skipIfWarm: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (string.Equals(mutation.TreeId, AuthConstants.PolicyTree, StringComparison.Ordinal))
        {
            lock (_warmGate)
            {
                _policyMutations++;

                // The first rule over an empty snapshot: requests must wait for a
                // scan that sees it rather than keep reading "no rules" (see IsWarm).
                if (!_currentHasRules)
                {
                    _warm = false;
                }
            }

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
        await RebuildOnceAsync(skipIfWarm: false, cancellationToken).ConfigureAwait(false);
        return CurrentEpoch;
    }

    /// <summary>
    /// Requests a coalesced background rebuild without a policy mutation. Called by
    /// the decision engine when the tenant layer is active over a snapshot compiled
    /// without its tenant partition (the layer was switched on). Cheap and
    /// idempotent: while a rebuild is already queued it returns after one read.
    /// </summary>
    internal void RequestRebuild() => ScheduleRebuild();

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
                await RebuildOnceAsync(skipIfWarm: false, CancellationToken.None).ConfigureAwait(false);
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

    private async Task RebuildOnceAsync(bool skipIfWarm, CancellationToken cancellationToken)
    {
        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (skipIfWarm && _warm)
            {
                return;
            }

            long mutationsAtStart;
            lock (_warmGate)
            {
                mutationsAtStart = _policyMutations;
            }

            // Read the tenant-layer switch before the scan: a flip during the scan is
            // caught by the engine, which requests another rebuild when it finds the
            // layer active over a snapshot built without the tenant partition.
            var includeTenantLayer = _tenantLayer?.IsActive == true;

            // The store's scan is resilient to a transient enumeration abort
            // caused by a concurrent scan over the policy tree, so a plain
            // buffering scan here is sufficient.
            var rules = new List<LatticeAuthorizationRule>();
            await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                rules.Add(rule);
            }

            var compiled = CompiledPolicy.Compile(rules, includeTenantLayer);
            lock (_warmGate)
            {
                Volatile.Write(ref _current, compiled);
                _currentHasRules = rules.Count > 0;

                // A cold snapshot turns warm only when no policy write landed during
                // its scan; otherwise the queued follow-up (or a waiting request)
                // rescans.
                if (!_warm && _policyMutations == mutationsAtStart)
                {
                    _warm = true;
                }
            }

            Interlocked.Increment(ref _epoch);
            Interlocked.Exchange(ref _lastRebuildUtcTicks, _time.GetUtcNow().UtcTicks);

            // Observability only: count the rebuild. Never affects the snapshot.
            if (LatticeAuthMetrics.SnapshotRebuilds.Enabled)
            {
                LatticeAuthMetrics.SnapshotRebuilds.Add(1, LatticeTenantLabel.Platform);
            }
        }
        finally
        {
            _rebuildLock.Release();
        }
    }
}
