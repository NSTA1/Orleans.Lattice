using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Reads the wire state of an installation, and the state a lifecycle run in flight is moving it
/// to, for every facade that reports one. A run can record interim failed evidence while it works
/// (a replicated run records a failed outcome before it enrols, so a crash part-way is never read
/// as success), so a failed status counts as <see cref="AppLifecycleState.Failed"/> only once no run
/// is executing (issue #5482).
/// </summary>
internal static class AppsLiveState
{
    /// <summary>Reads the wire state of <paramref name="record"/> and its pending target, if any.</summary>
    /// <param name="pipeline">The activation pipeline holding the recorded status and the run in flight.</param>
    /// <param name="tenant">The tenant the app is installed for.</param>
    /// <param name="record">The installation's registry record.</param>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>
    /// The state, never <see cref="AppLifecycleState.Failed"/> while a run is in flight, and the
    /// state that run is moving the installation to, or null when none is executing.
    /// </returns>
    public static async Task<(AppLifecycleState State, AppLifecycleState? Pending)> ReadAsync(
        IAppActivationPipeline pipeline,
        TenantId tenant,
        AppRegistryRecord record,
        CancellationToken cancellationToken)
    {
        if (record.State == AppRegistryLifecycleState.Uninstalled)
        {
            return (AppLifecycleState.Uninstalled, null);
        }

        var running = await pipeline.GetRunningOperationAsync(tenant, record.Slug, cancellationToken).ConfigureAwait(false);
        if (running is not { } operation)
        {
            var status = await pipeline.GetStatusAsync(tenant, record.Slug, cancellationToken).ConfigureAwait(false);
            var state = AppsControlMapping.ToWireState(record.State, status);
            if (state != AppLifecycleState.Failed)
            {
                return (state, null);
            }

            // A run may have started, and recorded interim evidence, since the first check. Once
            // none is executing, every run that had started has recorded its final outcome, so the
            // status read after that check is final.
            running = await pipeline.GetRunningOperationAsync(tenant, record.Slug, cancellationToken).ConfigureAwait(false);
            if (running is not { } started)
            {
                status = await pipeline.GetStatusAsync(tenant, record.Slug, cancellationToken).ConfigureAwait(false);
                return (AppsControlMapping.ToWireState(record.State, status), null);
            }

            operation = started;
        }

        return (AppsControlMapping.ToWireState(record.State), AppsControlMapping.ToPendingState(operation, record.State));
    }
}
