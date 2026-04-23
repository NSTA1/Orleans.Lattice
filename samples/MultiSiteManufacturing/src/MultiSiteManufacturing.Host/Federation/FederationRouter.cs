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

    /// <summary>
    /// Raised after a fact has been fanned out to every backend. Used
    /// by the in-process dashboard broadcaster (<c>DashboardBroadcaster</c>)
    /// to push live part-summary updates to the Blazor UI without
    /// polling. Handlers must be non-blocking; exceptions are swallowed
    /// and logged.
    /// </summary>
    public event EventHandler<Fact>? FactRouted;

    /// <summary>
    /// Raised after any change to site or backend chaos configuration
    /// (direct configure, preset, backend config). Subscribers that
    /// render "active chaos" UI refresh their snapshots on this signal.
    /// </summary>
    public event EventHandler? ChaosConfigChanged;

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
    /// <returns>
    /// <see langword="true"/> if the fact was forwarded through the
    /// backends (fan-out ran and <see cref="FactRouted"/> was raised);
    /// <see langword="false"/> if the site grain held it (paused or
    /// buffered for reorder) — in which case no downstream side effects
    /// occur until a later unpause or reorder flush.
    /// </returns>
    public async Task<bool> EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        var admission = await SiteGrain(fact.Site).AdmitAsync(fact);

        // Tier 3 (plan §4.3): a reorder flush returns a shuffled batch of
        // previously-buffered facts for the router to release.
        if (admission.ShuffledDrain is { Count: > 0 } drained)
        {
            await ReleaseAsync(drained, cancellationToken);
        }

        if (!admission.Forward)
        {
            logger.LogInformation(
                "Fact {FactId} held at {Site} (paused or buffered for reorder)",
                fact.FactId, fact.Site);
            return false;
        }

        if (admission.DelayMs > 0)
        {
            await Task.Delay(admission.DelayMs, cancellationToken);
        }

        await FanOutAsync(fact, cancellationToken);
        return true;
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
        var snapshot = await SiteGrain(site).GetStateAsync();
        RaiseChaosConfigChanged();
        return snapshot;
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
        var snapshot = await Registry.ListSitesAsync();
        RaiseChaosConfigChanged();
        return snapshot;
    }

    /// <summary>Returns a snapshot of every site's configuration + counters.</summary>
    public Task<IReadOnlyList<SiteState>> ListSitesAsync() => Registry.ListSitesAsync();

    /// <summary>
    /// Returns a snapshot of every registered backend's chaos
    /// configuration (plan §4.3 Tier 2). One entry per
    /// <see cref="IFactBackend.Name"/>, in registration order.
    /// </summary>
    public async Task<IReadOnlyList<BackendChaosState>> ListBackendChaosAsync()
    {
        var tasks = new Task<BackendChaosConfig>[_backends.Count];
        for (var i = 0; i < _backends.Count; i++)
        {
            tasks[i] = grains.GetGrain<IBackendChaosGrain>(_backends[i].Name).GetConfigAsync();
        }
        var configs = await Task.WhenAll(tasks);

        var states = new BackendChaosState[_backends.Count];
        for (var i = 0; i < _backends.Count; i++)
        {
            states[i] = new BackendChaosState { Name = _backends[i].Name, Config = configs[i] };
        }
        return states;
    }

    /// <summary>
    /// Updates the chaos configuration for the backend with the given
    /// name and returns its fresh snapshot. Throws if
    /// <paramref name="backendName"/> is not registered.
    /// </summary>
    public async Task<BackendChaosState> ConfigureBackendChaosAsync(
        string backendName,
        BackendChaosConfig config)
    {
        // Validates the backend exists (throws otherwise).
        _ = GetBackend(backendName);
        var stored = await grains.GetGrain<IBackendChaosGrain>(backendName).ConfigureAsync(config);
        var snapshot = new BackendChaosState { Name = backendName, Config = stored };
        RaiseChaosConfigChanged();
        return snapshot;
    }

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

        // Await WhenAll but don't let a single backend failure suppress
        // the FactRouted signal for the others. Exceptions still bubble
        // (the ChaosFactBackend deliberately throws for fault injection),
        // but the event fires for partial success.
        try
        {
            await Task.WhenAll(tasks);
        }
        finally
        {
            RaiseFactRouted(fact);
        }
    }

    private void RaiseFactRouted(Fact fact)
    {
        var handler = FactRouted;
        if (handler is null)
        {
            return;
        }
        try
        {
            handler(this, fact);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "FactRouted subscriber threw for fact {FactId}", fact.FactId);
        }
    }

    private void RaiseChaosConfigChanged()
    {
        var handler = ChaosConfigChanged;
        if (handler is null)
        {
            return;
        }
        try
        {
            handler(this, EventArgs.Empty);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "ChaosConfigChanged subscriber threw");
        }
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
