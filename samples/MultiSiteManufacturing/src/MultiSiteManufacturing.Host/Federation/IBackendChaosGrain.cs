namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Per-backend chaos grain. One grain activation per <see cref="IFactBackend.Name"/>
/// (<c>"baseline"</c>, <c>"lattice"</c>), keyed by that name. Holds the
/// jitter / transient-failure / write-amplification configuration for
/// its backend. State persists to Azure Table Storage
/// (<c>msmfgGrainState</c>), so chaos choices survive a process restart.
/// </summary>
/// <remarks>
/// Plan §4.3 Tier 2. The <see cref="ChaosFactBackend"/> decorator reads
/// the config on every emit; read paths bypass the grain.
/// </remarks>
public interface IBackendChaosGrain : IGrainWithStringKey
{
    /// <summary>Reads the current configuration.</summary>
    Task<BackendChaosConfig> GetConfigAsync();

    /// <summary>Replaces the configuration. Returns the persisted value.</summary>
    Task<BackendChaosConfig> ConfigureAsync(BackendChaosConfig config);
}
