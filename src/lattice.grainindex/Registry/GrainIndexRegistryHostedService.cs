using Microsoft.Extensions.Hosting;

namespace Orleans.Lattice.GrainIndex.Registry;

/// <summary>
/// The startup lifecycle participant that drives
/// <see cref="GrainIndexRegistryReconciler"/> once per silo start, so a
/// configuration drift or a disallowed replication setting fails the host rather
/// than surfacing later as a quietly wrong query result.
/// </summary>
/// <remarks>
/// <para>
/// The reconciliation itself lives in
/// <see cref="GrainIndexRegistryReconciler"/>; this type exists only to attach
/// it to the host's start-up. Keeping the two apart is what lets every
/// reconciliation branch be exercised without a host.
/// </para>
/// <para>
/// The service is registered from <c>AddGrainIndex</c>, which runs inside the
/// silo builder's configuration, so it lands in the service collection after
/// Orleans' own silo hosted service. The host awaits each
/// <see cref="IHostedService.StartAsync"/> in registration order, so the silo is
/// already dispatch-ready by the time the registry tree is addressed. Unlike the
/// package's background services this one deliberately blocks start-up: its
/// whole purpose is to reject a silo whose index configuration would corrupt or
/// misread existing data, which a fire-and-forget retry could not do.
/// </para>
/// </remarks>
internal sealed class GrainIndexRegistryHostedService(GrainIndexRegistryReconciler reconciler)
    : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) =>
        reconciler.ReconcileAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
