using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Fans every emitted <see cref="Fact"/> out to every registered
/// <see cref="IFactBackend"/> in parallel. This is the single ingress
/// point for facts; every producer (bulk loader, operator UI, gRPC
/// client) goes through the router.
/// </summary>
/// <remarks>
/// The router first consults the per-site <see cref="IProcessSiteGrain"/>
/// for the fact's origin site so pause / delay configuration takes effect
/// before fan-out. Facts held by a paused site are released through
/// <see cref="ConfigureSiteAsync"/> and <see cref="ApplyPresetAsync"/>.
/// </remarks>
public sealed class FederationRouter(
    IEnumerable<IFactBackend> backends,
    IGrainFactory grains,
    ILogger<FederationRouter> logger)
{
    private readonly IReadOnlyList<IFactBackend> _backends = [.. backends];

    /// <summary>Backends that receive every emitted fact, indexed by <see cref="IFactBackend.Name"/>.</summary>
    public IReadOnlyList<IFactBackend> Backends => _backends;

    /// <summary>Returns the backend with the given name, or throws if no such backend is registered.</summary>
    public IFactBackend GetBackend(string name)
    {
        foreach (var backend in _backends)
        {
            if (string.Equals(backend.Name, name, StringComparison.Ordinal))
            {
                return backend;
            }
        }
        throw new InvalidOperationException($"No backend registered with name '{name}'.");
    }

    /// <summary>
    /// Offers <paramref name="fact"/> to the origin site's chaos grain,
    /// then either forwards it (optionally after an artificial delay) or
    /// leaves it held by the grain.
    /// </summary>
    public async Task EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        var admission = await SiteGrain(fact.Site).AdmitAsync(fact);
        if (!admission.Forward)
        {
            logger.LogDebug(
                "Fact {FactId} held at {Site} (paused)",
                fact.FactId, fact.Site);
            return;
        }

        if (admission.DelayMs > 0)
        {
            await Task.Delay(admission.DelayMs, cancellationToken);
        }

        await FanOutAsync(fact, cancellationToken);
    }

    /// <summary>
    /// Updates a single site's chaos configuration and fans out any facts
    /// the grain released from its pending queue (pause → unpause).
    /// Returns the site's fresh <see cref="SiteState"/> snapshot.
    /// </summary>
    public async Task<SiteState> ConfigureSiteAsync(
        ProcessSite site,
        SiteConfig config,
        CancellationToken cancellationToken = default)
    {
        var result = await Registry.ConfigureSiteAsync(site, config);
        await ReleaseAsync(result.Drained, cancellationToken);
        return await SiteGrain(site).GetStateAsync();
    }

    /// <summary>
    /// Applies a canned chaos preset. Returns every site's fresh snapshot
    /// and fans out any facts released by the preset.
    /// </summary>
    public async Task<IReadOnlyList<SiteState>> ApplyPresetAsync(
        ChaosPreset preset,
        CancellationToken cancellationToken = default)
    {
        var drained = await Registry.ApplyPresetAsync(preset);
        await ReleaseAsync(drained, cancellationToken);
        return await Registry.ListSitesAsync();
    }

    /// <summary>Returns a snapshot of every site's configuration + counters.</summary>
    public Task<IReadOnlyList<SiteState>> ListSitesAsync() => Registry.ListSitesAsync();

    private ISiteRegistryGrain Registry =>
        grains.GetGrain<ISiteRegistryGrain>(ISiteRegistryGrain.SingletonKey);

    private IProcessSiteGrain SiteGrain(ProcessSite site) =>
        grains.GetGrain<IProcessSiteGrain>(site.ToString());

    private async Task FanOutAsync(Fact fact, CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "Routing fact {FactId} for part {Serial} to {BackendCount} backends",
            fact.FactId, fact.Serial, _backends.Count);

        var tasks = new Task[_backends.Count];
        for (var i = 0; i < _backends.Count; i++)
        {
            tasks[i] = _backends[i].EmitAsync(fact, cancellationToken);
        }
        await Task.WhenAll(tasks);
    }

    private async Task ReleaseAsync(IReadOnlyList<Fact> facts, CancellationToken cancellationToken)
    {
        if (facts.Count == 0)
        {
            return;
        }

        // Released facts bypass the site grain (which would re-queue them if
        // paused again) and fan out directly to every backend.
        var tasks = new Task[facts.Count];
        for (var i = 0; i < facts.Count; i++)
        {
            tasks[i] = FanOutAsync(facts[i], cancellationToken);
        }
        await Task.WhenAll(tasks);
    }
}
