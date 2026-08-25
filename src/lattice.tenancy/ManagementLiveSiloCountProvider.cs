using Orleans.Runtime;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ILiveSiloCountProvider"/>: reads the live silo count
/// from cluster membership via <see cref="IManagementGrain.GetHosts(bool)"/> with
/// <c>onlyActive: true</c>. When no grain factory is available (for example a
/// client-hosted registration) or membership reports no active hosts, it degrades
/// to <c>1</c> so a tenant receives the whole cluster rate rather than a zero
/// share. Consulted at lease cadence only, never on the per-op hot path.
/// </summary>
internal sealed class ManagementLiveSiloCountProvider : ILiveSiloCountProvider
{
    private readonly IGrainFactory? _grainFactory;

    /// <summary>Initializes the provider over the optional grain factory.</summary>
    /// <param name="grainFactory">The grain factory used to reach the management grain, or <c>null</c> when unavailable.</param>
    public ManagementLiveSiloCountProvider(IGrainFactory? grainFactory = null) => _grainFactory = grainFactory;

    /// <inheritdoc />
    public async ValueTask<int> GetLiveSiloCountAsync(CancellationToken cancellationToken = default)
    {
        var management = _grainFactory?.GetGrain<IManagementGrain>(0);
        if (management is null)
        {
            return 1;
        }

        var hosts = await management.GetHosts(onlyActive: true).ConfigureAwait(false);
        if (hosts is null || hosts.Count == 0)
        {
            return 1;
        }

        return hosts.Count;
    }
}
