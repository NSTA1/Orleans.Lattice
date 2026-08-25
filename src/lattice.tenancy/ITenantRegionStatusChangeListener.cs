namespace Orleans.Lattice.Tenancy;

/// <summary>
/// A listener notified when a tenant's <see cref="TenantRegionStatus"/> changes in
/// the local serving region, so a feature can react to residency lifecycle
/// transitions (for example to kick off or observe a backfill, or to update an
/// external topology view) without polling the registry. Listeners are registered
/// via <c>TryAddEnumerable</c>; when none is registered the residency maintainer
/// simply publishes nothing.
/// </summary>
/// <remarks>
/// The maintainer invokes every listener on its background rebuild continuation,
/// off the mutating grain's scheduler, after the snapshot swap has completed. A
/// listener must therefore be quick and must not throw for control flow: the
/// maintainer isolates each listener in a try/catch so one faulting listener
/// cannot stall the rebuild or suppress the other listeners.
/// </remarks>
public interface ITenantRegionStatusChangeListener
{
    /// <summary>
    /// Called once per observed local-region status transition after the residency
    /// snapshot has been swapped in.
    /// </summary>
    /// <param name="change">The observed status change.</param>
    /// <param name="cancellationToken">Cancels the notification.</param>
    /// <returns>A task that completes when the listener has handled the change.</returns>
    Task OnRegionStatusChangedAsync(TenantRegionStatusChange change, CancellationToken cancellationToken);
}
