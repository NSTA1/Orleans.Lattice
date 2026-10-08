using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Internal helper for trusted infrastructure to advance a tenant's region
/// through the residency lifecycle one legal step at a time: it applies
/// <see cref="TenantRegionStatus.Provisioning"/> -&gt;
/// <see cref="TenantRegionStatus.Backfilling"/> on the add path, and the drain
/// completion (<see cref="TenantRegionStatus.Draining"/> -&gt;
/// <see cref="TenantRegionStatus.Offline"/> -&gt;
/// <see cref="TenantRegionStatus.Removed"/>) on the remove path.
/// </summary>
/// <remarks>
/// <para>
/// This is not a caller-facing operation, so it carries no caller authorization.
/// The silo drives the remove path of its own serving region automatically
/// through <see cref="TenantRegionDrainCompletionListener"/> and drives the add
/// path through <see cref="TenantRegionBackfillService"/>. This driver never moves
/// a region to <see cref="TenantRegionStatus.Online"/> without a verified bootstrap
/// completion or an explicit operator acknowledgement.
/// </para>
/// <para>
/// Every advance consults the single lifecycle authority
/// (<see cref="TenantRecord.TryPromoteRegionStatus"/>) and is an idempotent no-op
/// at a terminal or non-transitional status, so a redriven or duplicated
/// promotion signal cannot corrupt the record. Each promotion is stamped as the
/// immediate successor of the status it was computed from and stamped with the
/// cluster's writer id, so it never supersedes a residency change a tenant admin
/// commits after the driver's read, and every silo of the region that races the
/// same promotion mints an identical slot.
/// </para>
/// </remarks>
internal sealed class TenantRegionLifecycleDriver
{
    private readonly ITenantRegistry _registry;
    private readonly string? _writerId;

    /// <summary>
    /// Initializes a new <see cref="TenantRegionLifecycleDriver"/>.
    /// </summary>
    /// <param name="registry">The tenancy engine's lifecycle store. Must not be <c>null</c>.</param>
    /// <param name="clusterOptions">The cluster options supplying the writer id stamped on registry writes. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRegionLifecycleDriver(ITenantRegistry registry, IOptions<ClusterOptions> clusterOptions)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(clusterOptions);

        _registry = registry;
        _writerId = clusterOptions.Value.ClusterId;
    }

    /// <summary>
    /// Advances <paramref name="regionId"/> of <paramref name="tenant"/> by one
    /// legal add-path step after an explicit data-in-place acknowledgement and
    /// returns the region's committed status. Automatic backfill uses the internal
    /// verified-completion path instead. A no-op that returns the current status
    /// when the region is at a terminal or non-transitional status (or has no
    /// status at all), so it is safe to redrive.
    /// </summary>
    /// <param name="tenant">The tenant whose region is advanced.</param>
    /// <param name="regionId">The region id to advance. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the advance.</param>
    /// <returns>
    /// The region's status in the registry's committed join after the advance
    /// (unchanged when no promotion applies, and the concurrent writer's status
    /// when a later residency change superseded the promotion).
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <param name="acknowledgeDataInPlace">Must be <see langword="true"/> to explicitly acknowledge that the operator has verified the tenant's data is present in this region.</param>
    public Task<TenantRegionStatus> AdvanceForOperatorAsync(
        TenantId tenant, string regionId, bool acknowledgeDataInPlace, CancellationToken cancellationToken = default)
    {
        if (!acknowledgeDataInPlace)
        {
            throw new ArgumentException("The operator must acknowledge that tenant data is already in place.", nameof(acknowledgeDataInPlace));
        }

        return AdvanceAddStepAsync(tenant, regionId, cancellationToken);
    }

    /// <summary>Moves a newly added region from Provisioning to Backfilling.</summary>
    /// <param name="tenant">The tenant whose region is advanced.</param>
    /// <param name="regionId">The region id to advance.</param>
    /// <param name="cancellationToken">Cancels the advance.</param>
    internal Task<TenantRegionStatus> BeginBackfillAsync(
        TenantId tenant, string regionId, CancellationToken cancellationToken = default) =>
        AdvanceExpectedAsync(tenant, regionId, TenantRegionStatus.Provisioning, cancellationToken);

    /// <summary>
    /// Moves a region from Backfilling to Online only after every configured tenant
    /// tree has completed its receiver-side bootstrap.
    /// </summary>
    /// <param name="tenant">The tenant whose region is advanced.</param>
    /// <param name="regionId">The region id to advance.</param>
    /// <param name="backfillVerified">Whether all tenant trees have a live incremental bootstrap.</param>
    /// <param name="cancellationToken">Cancels the advance.</param>
    internal Task<TenantRegionStatus> CompleteBackfillAsync(
        TenantId tenant, string regionId, bool backfillVerified, CancellationToken cancellationToken = default)
    {
        if (!backfillVerified)
        {
            throw new ArgumentException("A region cannot become Online until its backfill is verified complete.", nameof(backfillVerified));
        }

        return AdvanceExpectedAsync(tenant, regionId, TenantRegionStatus.Backfilling, cancellationToken);
    }

    private Task<TenantRegionStatus> AdvanceAddStepAsync(
        TenantId tenant, string regionId, CancellationToken cancellationToken = default) =>
        AdvanceExpectedAsync(tenant, regionId, expectedStatus: null, cancellationToken);

    /// <summary>
    /// Advances <paramref name="regionId"/> of <paramref name="tenant"/> by a single
    /// step of the <b>remove</b> path only (<see cref="TenantRegionStatus.Draining"/>
    /// -&gt; <see cref="TenantRegionStatus.Offline"/> -&gt;
    /// <see cref="TenantRegionStatus.Removed"/>) and returns the region's committed
    /// status. A no-op at any other status, so a region re-added between the
    /// trigger and this call is never pushed along the add path.
    /// </summary>
    /// <param name="tenant">The tenant whose region is advanced.</param>
    /// <param name="regionId">The region id to advance. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the advance.</param>
    /// <returns>The region's status in the registry's committed join after the advance.</returns>
    /// <exception cref="ArgumentException"><paramref name="regionId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    public Task<TenantRegionStatus> CompleteDrainStepAsync(
        TenantId tenant, string regionId, CancellationToken cancellationToken = default) =>
        AdvanceCoreAsync(tenant, regionId, drainOnly: true, cancellationToken);

    /// <summary>
    /// <c>true</c> for the statuses whose next step completes a drain.
    /// </summary>
    /// <param name="status">The status to classify.</param>
    /// <returns><c>true</c> for <see cref="TenantRegionStatus.Draining"/> and <see cref="TenantRegionStatus.Offline"/>.</returns>
    internal static bool IsDrainStep(TenantRegionStatus status) =>
        status is TenantRegionStatus.Draining or TenantRegionStatus.Offline;

    private async Task<TenantRegionStatus> AdvanceCoreAsync(
        TenantId tenant, string regionId, bool drainOnly, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);

        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);

        var current = record.GetRegionStatus(regionId);
        if (drainOnly && !IsDrainStep(current))
        {
            return current;
        }

        if (!record.TryPromoteRegionStatus(regionId, _writerId, out _))
        {
            return current;
        }

        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
        return merged.GetRegionStatus(regionId);
    }

    private async Task<TenantRegionStatus> AdvanceExpectedAsync(
        TenantId tenant,
        string regionId,
        TenantRegionStatus? expectedStatus,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(regionId);
        var record = await _registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false)
            ?? throw new TenantNotFoundException(tenant.Value);
        var current = record.GetRegionStatus(regionId);
        if (expectedStatus is { } expected && current != expected)
        {
            return current;
        }

        if (expectedStatus is null && current is not (TenantRegionStatus.Provisioning or TenantRegionStatus.Backfilling))
        {
            return current;
        }

        if (!record.TryPromoteRegionStatus(regionId, _writerId, out _))
        {
            return current;
        }

        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
        return merged.GetRegionStatus(regionId);
    }
}
