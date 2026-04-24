using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Singleton aggregator grain that enumerates every <see cref="ProcessSite"/>
/// and fans chaos presets out to the per-site <see cref="IProcessSiteGrain"/>s.
/// Keyed with the fixed singleton key <see cref="SingletonKey"/>.
/// </summary>
public interface ISiteRegistryGrain : IGrainWithIntegerKey
{
    /// <summary>Fixed integer key used to address the singleton registry grain.</summary>
    public const long SingletonKey = 0;

    /// <summary>Returns a snapshot of every site's configuration + counters.</summary>
    Task<IReadOnlyList<SiteState>> ListSitesAsync();

    /// <summary>
    /// Updates a single site's configuration. Returns the new
    /// <see cref="SiteConfigureResult"/> straight through so the caller
    /// can fan out any released facts.
    /// </summary>
    Task<SiteConfigureResult> ConfigureSiteAsync(ProcessSite site, SiteConfig config);

    /// <summary>
    /// Applies a canned preset across multiple sites. Returns the merged
    /// set of drained facts so the caller (the router) can release them
    /// back into the fan-out.
    /// </summary>
    Task<IReadOnlyList<Fact>> ApplyPresetAsync(ChaosPreset preset);
}
