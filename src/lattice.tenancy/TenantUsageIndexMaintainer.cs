using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The per-silo maintainer of the compiled per-tenant usage snapshot. It builds
/// the snapshot from the tenant registry (quotas) and the usage tree (per-cluster
/// slots) on first use, observes the core change-feed
/// (<see cref="IMutationObserver"/>) and rebuilds when either the reserved
/// <c>sys-tenant-registry</c> or <c>sys-tenant-usage</c> tree mutates, and swaps
/// the immutable snapshot atomically. It is the warm <see cref="ITenantUsageIndex"/>
/// the admission controller reads on the write path.
/// </summary>
/// <remarks>
/// <para>
/// The change-feed hook fires inline on the grain write path, so it only
/// <i>schedules</i> a rebuild; the actual rescan runs on a background continuation.
/// This gives eventual snapshot consistency: a committed registry or usage edit is
/// reflected shortly after it commits, not necessarily before the writing call
/// returns. Because enforcement admits against this eventually-consistent snapshot,
/// quota consistency is converged best-effort with bounded overshoot.
/// </para>
/// <para>
/// Rebuilds are coalesced - a burst of writes collapses into at most one in-flight
/// rebuild plus at most one queued follow-up - and serialized, so the snapshot
/// always reflects a whole, self-consistent scan. This coalescing state machine
/// mirrors <see cref="CompiledTenantPolicySnapshotMaintainer"/> verbatim.
/// </para>
/// </remarks>
internal sealed class TenantUsageIndexMaintainer : IMutationObserver, ITenantUsageIndex
{
    private readonly ITenantRegistry _registry;
    private readonly ITenantUsageStore _usage;
    private readonly ILogger<TenantUsageIndexMaintainer> _logger;
    private readonly string _localClusterId;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private CompiledTenantUsage _current = CompiledTenantUsage.Empty;
    private long _epoch;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with
    // a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    /// <summary>Initializes a new <see cref="TenantUsageIndexMaintainer"/>.</summary>
    /// <param name="registry">The tenant registry scanned for quotas.</param>
    /// <param name="usage">The usage store scanned for per-cluster slots.</param>
    /// <param name="cluster">The cluster options supplying this cluster's id (selects the local slot).</param>
    /// <param name="logger">The logger for background-rebuild failures.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantUsageIndexMaintainer(
        ITenantRegistry registry,
        ITenantUsageStore usage,
        IOptions<ClusterOptions> cluster,
        ILogger<TenantUsageIndexMaintainer> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(usage);
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(logger);
        _registry = registry;
        _usage = usage;
        _localClusterId = cluster.Value.ClusterId;
        _logger = logger;
    }

    /// <summary>The current compiled snapshot. Read without locking; swapped atomically on rebuild.</summary>
    public CompiledTenantUsage Current => Volatile.Read(ref _current);

    /// <summary>The monotonic epoch of the current snapshot; advances on every rebuild.</summary>
    public long CurrentEpoch => Interlocked.Read(ref _epoch);

    /// <summary>
    /// The most recently scheduled background rebuild loop, or a completed task
    /// when none has been scheduled. Exposed so a test can await a change-feed-driven
    /// rebuild deterministically instead of polling.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <inheritdoc />
    public bool TryGetView(TenantId tenant, out TenantUsageView view) =>
        Current.TryGetView(tenant, out view);

    /// <inheritdoc />
    public IReadOnlyDictionary<string, TenantUsageView> EnumerateViews() => Current.Tenants;

    /// <summary>
    /// <c>true</c> when <paramref name="mutation"/> targets the reserved tenant
    /// registry or usage tree and so must trigger a snapshot rebuild. A pure
    /// predicate over the mutation's tree id.
    /// </summary>
    /// <param name="mutation">The observed mutation.</param>
    /// <returns><c>true</c> when the mutation should rebuild the snapshot.</returns>
    internal static bool IsUsageOrRegistryMutation(LatticeMutation mutation) =>
        string.Equals(mutation.TreeId, TenantTreeNames.RegistryTree, StringComparison.Ordinal) ||
        string.Equals(mutation.TreeId, TenantTreeNames.UsageTree, StringComparison.Ordinal);

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
        if (IsUsageOrRegistryMutation(mutation))
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
                _logger.LogWarning(ex, "Failed to rebuild the compiled tenant-usage snapshot; the previous snapshot remains in effect.");
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
            var registryRecords = new List<TenantRecord>();
            await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
            {
                registryRecords.Add(record);
            }

            var usageRecords = new List<TenantUsageRecord>();
            await foreach (var record in _usage.ListAsync(cancellationToken).ConfigureAwait(false))
            {
                usageRecords.Add(record);
            }

            var compiled = CompiledTenantUsage.Compile(registryRecords, usageRecords, _localClusterId);
            Volatile.Write(ref _current, compiled);
            Interlocked.Increment(ref _epoch);
        }
        finally
        {
            _rebuildLock.Release();
        }
    }
}
