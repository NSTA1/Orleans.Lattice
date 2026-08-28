using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The silo-hosted background service that drives per-tenant usage metering: on a
/// <see cref="TimeProvider"/>-driven cadence it walks each registered tenant's own
/// trees, samples their storage footprint, and rolls the result up into the
/// tenant's per-cluster usage slot through <see cref="TenantUsagePublisher"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is the driver whose absence made every per-tenant quota inert.
/// <see cref="TenantUsagePublisher"/> is documented as "the low-frequency,
/// cadence-driven side of the accounting layer (the caller supplies both the
/// cadence and a monotonic stamp)" - but nothing supplied that cadence, so
/// <see cref="TenantUsagePublisher.RollUpAndPublishAsync"/> was never called
/// outside its own unit tests. With no sample ever landing,
/// <see cref="LatticeTenantAdmissionController"/> permanently took its documented
/// "fail open until the first sample lands" branch, so an authored quota could
/// never be breached however small it was set.
/// </para>
/// <para>
/// A tenant's trees are enumerated with the registry's prefix-scoped overload, so
/// the walk is a bounded range scan over the tenant's own <c>t/{tenant}/</c> key
/// range rather than a read of the whole cluster catalog.
/// </para>
/// <para>
/// Each cycle is wrapped so a transient failure (the storage-usage machinery not
/// yet reachable at start-up, a tree deactivating mid-walk) is logged and retried
/// on the next tick rather than tearing down the silo, mirroring the observability
/// publisher and the rate-limiter lease loop. A cluster with tenancy disabled never
/// registers this service and therefore meters nothing.
/// </para>
/// </remarks>
internal sealed class TenantUsageMeteringService : IHostedService
{
    private readonly ITenantRegistry _registry;
    private readonly TenantUsagePublisher _publisher;
    private readonly IGrainFactory _grainFactory;
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<TenantUsageAccountingOptions> _options;
    private readonly ILogger<TenantUsageMeteringService> _logger;

    private readonly CancellationTokenSource _stopping = new();
    private Task? _loop;

    /// <summary>
    /// Last-known good per-tree usage sample, keyed by tenant and then tree id.
    /// <para>
    /// A tree that fails to sample must not silently vanish from the roll-up.
    /// <see cref="TenantUsagePublisher.RollUpAndPublishAsync"/> sums exactly the
    /// samples it is handed and <i>replaces</i> the tenant's published slot with
    /// that sum, so an omitted tree does not read as "unknown" - it reads as
    /// "zero", and the tenant's measured footprint drops by that tree's entire
    /// contribution. The quota ceiling then effectively rises by the same amount,
    /// and if every tree fails at once (the shape a storage-subsystem overload
    /// takes) the tenant's usage rolls up to nothing and its quota stops binding
    /// altogether - fail-open, precisely when the tenant is pushing enough volume
    /// to break metering in the first place. Retaining the last-known figure keeps
    /// the roll-up conservative instead.
    /// </para>
    /// <para>
    /// Deliberately <b>not</b> aged out. Retention is the fail-closed direction
    /// (it keeps a tenant's accounted usage up, so quotas keep binding), and a TTL
    /// would reintroduce the very fail-open this closes. A tree that is genuinely
    /// gone stops being enumerated and is dropped from the cache by the rebuild
    /// below, so retention cannot outlive the tree it describes.
    /// </para>
    /// <para>
    /// Access is single-threaded: the metering loop awaits one cycle at a time and
    /// each cycle walks tenants sequentially, so a plain dictionary is sufficient
    /// and avoids the concurrent-collection overhead.
    /// </para>
    /// </summary>
    private readonly Dictionary<string, Dictionary<string, TreeUsageSample>> _lastKnownByTenant =
        new(StringComparer.Ordinal);

    /// <summary>Initializes the metering service over its registry, publisher, and schedule inputs.</summary>
    /// <param name="registry">The tenant registry supplying the tenants to meter. Must not be <c>null</c>.</param>
    /// <param name="publisher">The usage publisher each roll-up is handed to. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory used to sample each tree's footprint. Must not be <c>null</c>.</param>
    /// <param name="timeProvider">The timestamp source backing the metering timer. Must not be <c>null</c>.</param>
    /// <param name="options">The usage-accounting options carrying the cadence. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger for metering-cycle failures. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantUsageMeteringService(
        ITenantRegistry registry,
        TenantUsagePublisher publisher,
        IGrainFactory grainFactory,
        TimeProvider timeProvider,
        IOptionsMonitor<TenantUsageAccountingOptions> options,
        ILogger<TenantUsageMeteringService> logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _registry = registry;
        _publisher = publisher;
        _grainFactory = grainFactory;
        _timeProvider = timeProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>The background metering loop, exposed so a test can await it after <see cref="StopAsync"/>.</summary>
    internal Task? Loop => _loop;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_options.CurrentValue.MeterInterval <= TimeSpan.Zero)
        {
            return Task.CompletedTask;
        }

        // Fire-and-forget: the first cycle needs the cluster to be ready, so
        // awaiting it here would stall silo start-up.
        _loop = RunLoopAsync(_stopping.Token);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);

        if (_loop is { } loop)
        {
            try
            {
                await loop.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Shutdown raced the loop; nothing to drain.
            }
        }
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.CurrentValue.MeterInterval, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                await MeterOnceAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Metering is a best-effort accounting cycle: a transient failure
                // must not fault the silo, and the next tick retries from scratch.
                _logger.LogWarning(ex, "Tenant usage metering cycle failed; retrying on the next tick.");
            }
        }
    }

    /// <summary>
    /// Runs one metering cycle over every registered tenant. Exposed internally so a
    /// test can drive a single deterministic pass without the timer.
    /// </summary>
    internal async Task MeterOnceAsync(CancellationToken cancellationToken)
    {
        // Metering is infrastructure: it reads the registry and each tree's usage on
        // the tenant's behalf, so it runs system-origin and is not itself gated or
        // pruned to whichever tenant happens to be ambient.
        using var origin = LatticeSystemOrigin.Enter();
        using var noTenant = LatticeActiveTenantContext.With(null);

        var seenTenants = new HashSet<string>(StringComparer.Ordinal);

        await foreach (var record in _registry.ListAsync(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var tenant = record.Id;
            if (tenant.Value is null || tenant.IsDefault)
            {
                // The reserved default tenant is unbounded and cannot be given
                // quotas, so metering it would publish a slot nothing consults.
                continue;
            }

            seenTenants.Add(tenant.Value);

            var samples = await SampleTenantTreesAsync(tenant, cancellationToken).ConfigureAwait(false);
            await _publisher
                .RollUpAndPublishAsync(tenant, samples, HybridLogicalClock.Tick(HybridLogicalClock.Zero), cancellationToken)
                .ConfigureAwait(false);
        }

        // Only after a complete pass: a cycle that faulted part-way has not proved
        // a tenant absent, and pruning on that evidence would discard retained
        // footprints the next cycle still needs.
        PruneRetainedTenants(seenTenants);
    }

    private async Task<IReadOnlyCollection<TreeUsageSample>> SampleTenantTreesAsync(
        TenantId tenant,
        CancellationToken cancellationToken)
    {
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Prefix-scoped: a bounded range scan over the tenant's own contiguous key
        // range rather than a read of the whole cluster catalog.
        var treeIds = await registry
            .GetAllTreeIdsAsync(LatticeTenantTrees.ComposePrefix(tenant))
            .ConfigureAwait(false);

        _lastKnownByTenant.TryGetValue(tenant.Value, out var lastKnown);

        // The rebuilt cache doubles as the pruning mechanism: it is populated only
        // from the trees this cycle actually enumerated and still owns, so a tree
        // that was deleted or reassigned drops out without a separate sweep and
        // the cache stays bounded by the tenant's live tree count.
        var current = new Dictionary<string, TreeUsageSample>(treeIds.Count, StringComparer.Ordinal);
        var samples = new List<TreeUsageSample>(treeIds.Count);
        var retained = 0;
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Ownership is confirmed structurally: the prefix is a performance hint,
            // and this check is what decides whether a tree counts toward the tenant.
            if (!LatticeTenantTrees.TryGetTenant(treeId, out var owner) || !owner.Equals(tenant))
            {
                continue;
            }

            try
            {
                var usage = await _grainFactory
                    .GetGrain<ILatticeStorageUsage>(treeId)
                    .GetReportAsync(forceRefresh: false, cancellationToken)
                    .ConfigureAwait(false);

                // The key and memory figures are activation-scoped: a shard root
                // reports zero for them until every leaf has republished its
                // footprint on a commit boundary, which a tenant that is not
                // actively writing never triggers. The byte figure is unaffected
                // because it adds durable WAL retention, so a tree that genuinely
                // holds data but reports no keys and no leaf bytes is displaying a
                // cold cache, not an empty tree. Admitting on that reading is the
                // fail-open this re-anchor closes: maxKeys and maxMemoryBytes
                // would never bind after a routine reactivation (Orleans collects
                // idle grains, so this needs no restart or fault to happen).
                if (IsColdFootprint(usage))
                {
                    usage = await _grainFactory
                        .GetGrain<ILatticeStorageUsage>(treeId)
                        .GetReportAsync(forceRefresh: true, cancellationToken)
                        .ConfigureAwait(false);
                }

                var sample = new TreeUsageSample(
                    bytes: usage.TotalBytes,
                    keys: usage.LiveKeys,
                    memoryBytes: usage.LeafStateBytes);
                samples.Add(sample);
                current[treeId] = sample;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // One unreadable tree must not abandon the whole tenant's roll-up.
                // It contributes its last-known figure rather than nothing, so an
                // unreadable tree cannot quietly shrink the tenant's accounted
                // footprint and lift its quota ceiling (see _lastKnownByTenant).
                // A tree that has never been sampled has nothing to retain and is
                // the one case that still contributes nothing - it has no footprint
                // on record to under-count.
                if (lastKnown is not null && lastKnown.TryGetValue(treeId, out var previous))
                {
                    samples.Add(previous);
                    current[treeId] = previous;
                    retained++;
                }

                _logger.LogDebug(
                    ex,
                    "Tenant usage metering could not sample tree '{TreeId}' for tenant '{Tenant}'.",
                    treeId,
                    tenant);
            }
        }

        _lastKnownByTenant[tenant.Value] = current;

        if (retained > 0)
        {
            // Summarised once per tenant per cycle rather than once per tree, so a
            // broad storage-subsystem failure does not itself become a log flood.
            _logger.LogWarning(
                "Tenant usage metering retained the last-known footprint for {RetainedCount} of "
                + "{TreeCount} tree(s) for tenant '{Tenant}' because they could not be sampled; "
                + "the published usage may be stale but is not under-counted.",
                retained,
                current.Count,
                tenant);
        }

        return samples;
    }

    /// <summary>
    /// Drops retained samples for tenants that are no longer registered, so a
    /// deleted tenant cannot pin its footprint in memory indefinitely. Mirrors the
    /// per-tree pruning that rebuilding each tenant's map already performs.
    /// </summary>
    private void PruneRetainedTenants(HashSet<string> seenTenants)
    {
        if (_lastKnownByTenant.Count == seenTenants.Count)
        {
            return;
        }

        foreach (var key in _lastKnownByTenant.Keys.Where(k => !seenTenants.Contains(k)).ToList())
        {
            _lastKnownByTenant.Remove(key);
        }
    }

    /// <summary>
    /// Returns <c>true</c> when a usage report shows the signature of a cold
    /// activation-scoped cache: no live keys and no leaf-state bytes, yet a
    /// non-zero total.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="TreeStorageUsageReport.LiveKeys"/> and
    /// <see cref="TreeStorageUsageReport.LeafStateBytes"/> are summed from each
    /// shard root's in-memory totals, which reset on reactivation and are rebuilt
    /// only as leaves republish on commit boundaries.
    /// <see cref="TreeStorageUsageReport.TotalBytes"/> also adds durable WAL
    /// retention, so it survives. A tree holding real data therefore reports the
    /// combination below precisely when the cached figures are stale.
    /// </para>
    /// <para>
    /// The cost of re-anchoring is self-limiting. A large tree re-anchors once and
    /// then reports non-zero, so it never matches again for that activation; the
    /// only repeat cost falls on a tree that is genuinely empty but still retains
    /// WAL, and walking an empty tree is cheap. That is why this is a targeted
    /// re-anchor rather than forcing a deep walk on every cycle.
    /// </para>
    /// </remarks>
    private static bool IsColdFootprint(TreeStorageUsageReport usage)
        => usage.LiveKeys == 0 && usage.LeafStateBytes == 0 && usage.TotalBytes > 0;
}
