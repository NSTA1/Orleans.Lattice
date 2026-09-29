using Microsoft.Extensions.Logging;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Completes the drain of a tenant's residency in this silo's serving region
/// without an operator step: on every observed local-region transition into
/// <see cref="TenantRegionStatus.Draining"/> or <see cref="TenantRegionStatus.Offline"/>
/// it applies the next remove-path step through
/// <see cref="TenantRegionLifecycleDriver.CompleteDrainStepAsync"/>, so a dropped
/// region moves Draining -&gt; Offline -&gt; Removed on its own.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the drain completes immediately.</b> A region stops serving a tenant, and
/// stops admitting its replicated writes, the moment its status leaves
/// <see cref="TenantRegionStatus.Online"/>; outbound shipping of writes the region
/// accepted while it was online does not depend on the status at all. Nothing is
/// therefore left to wait for once the region is draining, and every remove-path
/// step only narrows what the region is (never widens what it serves or admits), so
/// completing it automatically cannot fail open.
/// </para>
/// <para>
/// <b>Why the add path is not driven here.</b> Promoting an added region to
/// <see cref="TenantRegionStatus.Online"/> widens both what it serves and what it
/// admits, and is only correct once the tenant's existing data has been copied into
/// the region. No shipped component performs that backfill, so this listener
/// never advances <see cref="TenantRegionStatus.Provisioning"/> or
/// <see cref="TenantRegionStatus.Backfilling"/>: that remains the explicit operator
/// step the tenancy documentation names.
/// </para>
/// <para>
/// Each step is chained off the next snapshot rebuild its own registry write
/// triggers, and the residency snapshot's first build after a silo start reports
/// every configured tenant's local status, so a step interrupted by a fault or a
/// restart is redriven at the next start. Every silo of the region observes the
/// same transition; the promotions they write are identical, so they converge.
/// </para>
/// </remarks>
internal sealed class TenantRegionDrainCompletionListener : ITenantRegionStatusChangeListener
{
    private readonly TenantRegionLifecycleDriver _driver;
    private readonly ILogger<TenantRegionDrainCompletionListener> _logger;

    /// <summary>
    /// Initializes a new <see cref="TenantRegionDrainCompletionListener"/>.
    /// </summary>
    /// <param name="driver">The single-step lifecycle driver. Must not be <c>null</c>.</param>
    /// <param name="logger">The logger. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TenantRegionDrainCompletionListener(
        TenantRegionLifecycleDriver driver, ILogger<TenantRegionDrainCompletionListener> logger)
    {
        ArgumentNullException.ThrowIfNull(driver);
        ArgumentNullException.ThrowIfNull(logger);

        _driver = driver;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task OnRegionStatusChangedAsync(TenantRegionStatusChange change, CancellationToken cancellationToken)
    {
        if (!TenantRegionLifecycleDriver.IsDrainStep(change.CurrentStatus))
        {
            return;
        }

        try
        {
            await _driver.CompleteDrainStepAsync(change.Tenant, change.RegionId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TenantNotFoundException)
        {
            // The tenant was deleted after the snapshot observed it: nothing is left
            // to drain, so this is not a fault.
            _logger.LogDebug(
                "Skipped completing the drain of tenant '{Tenant}' in region '{Region}': the tenant no longer exists.",
                change.Tenant,
                change.RegionId);
        }
    }
}
