using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The internal, system-driven promotion driver that advances a tenant's region
/// through the residency lifecycle one legal step at a time: it applies the
/// backfill-complete promotion (<see cref="TenantRegionStatus.Provisioning"/> -&gt;
/// <see cref="TenantRegionStatus.Backfilling"/> -&gt;
/// <see cref="TenantRegionStatus.Online"/>) on the add path, and the drain
/// completion (<see cref="TenantRegionStatus.Draining"/> -&gt;
/// <see cref="TenantRegionStatus.Offline"/> -&gt;
/// <see cref="TenantRegionStatus.Removed"/>) on the remove path.
/// </summary>
/// <remarks>
/// <para>
/// This is a system-driven mechanism, not a caller-facing operation, so it carries
/// no caller authorization: it is invoked by trusted co-hosted infrastructure (the
/// backfill and drain machinery) as a region reaches the next lifecycle milestone.
/// It never mints a step the lifecycle does not allow - it consults the single
/// lifecycle authority (<see cref="TenantRegionLifecycle.TryNextPromotion"/>) - and
/// is an idempotent no-op at a terminal or non-transitional status, so a redriven
/// or duplicated promotion signal cannot corrupt the record.
/// </para>
/// <para>
/// Each advance is stamped with a strictly increasing
/// <see cref="ITenantAdminClock"/> clock and the cluster's writer id, so it
/// converges with concurrent tenant-admin residency writes through the record's
/// per-field CRDT merge.
/// </para>
/// </remarks>
internal sealed class TenantRegionLifecycleDriver
{
    private readonly ITenantRegistry _registry;
    private readonly ITenantAdminClock _clock;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="TenantRegionLifecycleDriver"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="clock">The monotonic clock supplying last-writer-wins stamps. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRegionLifecycleDriver(
        ITenantRegistry registry, ITenantAdminClock clock, IOptions<ClusterOptions> clusterOptions)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _clock = clock;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <summary>
    /// Advances <paramref name="regionId"/> of <paramref name="tenant"/> by a single
    /// legal lifecycle step and returns the region's resulting status. A no-op that
    /// returns the current status when the region is at a terminal or
    /// non-transitional status (or has no status at all), so it is safe to redrive.
    /// </summary>
    /// <param name="tenant">The tenant whose region is advanced.</param>
    /// <param name="regionId">The region id to advance. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the advance.</param>
    /// <returns>The region's status after the advance (unchanged when no promotion applies).</returns>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    public async Task<TenantRegionStatus> AdvanceAsync(
        TenantId tenant, string regionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        var current = record.GetRegionStatus(regionId);
        if (!TenantRegionLifecycle.TryNextPromotion(current, out var next))
        {
            return current;
        }

        record.SetRegionStatus(regionId, next, _clock.Next(), _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
        return next;
    }
}
