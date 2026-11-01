using Microsoft.Extensions.Hosting;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Refuses to start a tenancy silo whose local region id cannot be resolved (see
/// <see cref="TenantLocalRegion"/>), so the misconfiguration surfaces with an
/// actionable message at host start instead of as tenants that never come online
/// or are refused service.
/// </summary>
/// <param name="localRegion">The silo's local-region authority.</param>
internal sealed class TenantLocalRegionStartupValidator(TenantLocalRegion localRegion) : IHostedService
{
    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">The local region id cannot be resolved.</exception>
    public Task StartAsync(CancellationToken cancellationToken) =>
        localRegion.IsResolved
            ? Task.CompletedTask
            : throw new InvalidOperationException(localRegion.UnresolvedMessage);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
