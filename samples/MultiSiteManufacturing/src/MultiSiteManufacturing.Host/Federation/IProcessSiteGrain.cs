using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Per-site chaos grain. One grain activation per <see cref="ProcessSite"/>
/// value, keyed by the site's PascalCase name (e.g. <c>OhioForge</c>).
/// Holds the site's pause / delay / reorder configuration plus the queue
/// of facts paused mid-flight. State persists to Azure Table Storage
/// (<c>msmfgGrainState</c>), so chaos choices survive a process restart.
/// </summary>
public interface IProcessSiteGrain : IGrainWithStringKey
{
    /// <summary>Reads the current configuration + counters snapshot.</summary>
    Task<SiteState> GetStateAsync();

    /// <summary>
    /// Replaces the site configuration. When the update transitions the
    /// grain from paused → unpaused, any facts that accumulated in the
    /// pending queue during the pause are returned in
    /// <see cref="SiteConfigureResult.Drained"/> for the caller to fan out.
    /// </summary>
    Task<SiteConfigureResult> ConfigureAsync(SiteConfig config);

    /// <summary>
    /// Offered a fact by the router. If the grain is paused, the fact is
    /// enqueued and the admission is <see cref="SiteAdmission.Hold"/>;
    /// otherwise the admission carries the delay (possibly zero) to apply
    /// before forwarding.
    /// </summary>
    Task<SiteAdmission> AdmitAsync(Fact fact);
}
