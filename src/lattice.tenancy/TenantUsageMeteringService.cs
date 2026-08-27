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

            var samples = await SampleTenantTreesAsync(tenant, cancellationToken).ConfigureAwait(false);
            await _publisher
                .RollUpAndPublishAsync(tenant, samples, HybridLogicalClock.Tick(HybridLogicalClock.Zero), cancellationToken)
                .ConfigureAwait(false);
        }
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

        var samples = new List<TreeUsageSample>(treeIds.Count);
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
                    .GetGrain<ILattice>(treeId)
                    .GetStorageUsageAsync(cancellationToken)
                    .ConfigureAwait(false);

                samples.Add(new TreeUsageSample(
                    bytes: usage.TotalBytes,
                    keys: usage.LiveKeys,
                    memoryBytes: usage.LeafStateBytes));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // One unreadable tree must not abandon the whole tenant's roll-up;
                // it simply contributes nothing until the next cycle.
                _logger.LogDebug(
                    ex,
                    "Tenant usage metering could not sample tree '{TreeId}' for tenant '{Tenant}'.",
                    treeId,
                    tenant);
            }
        }

        return samples;
    }
}
