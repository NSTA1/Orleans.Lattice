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
/// <para>
/// When the M12c silo-partition preset is active the router also
/// consults <see cref="IPartitionChaosGrain"/>: each silo accepts only
/// the half of the serial-hash space assigned to it (silo A keeps
/// <c>hash % 2 == 0</c>, silo B keeps <c>hash % 2 == 1</c>). Dropped
/// facts are logged but otherwise invisible to the caller — the
/// <see cref="EmitAsync"/> return value is <c>false</c> just as it
/// would be for a fact held at a paused site.
/// </para>
/// </remarks>
public sealed class FederationRouter(
    IEnumerable<IFactBackend> backends,
    IGrainFactory grains,
    ILogger<FederationRouter> logger,
    SiloIdentity siloIdentity)
{
    private readonly IReadOnlyList<IFactBackend> _backends = [.. backends];
    private readonly SiloIdentity _silo = siloIdentity ?? throw new ArgumentNullException(nameof(siloIdentity));

    /// <summary>Backends that receive every emitted fact, indexed by <see cref="IFactBackend.Name"/>.</summary>
    public IReadOnlyList<IFactBackend> Backends => _backends;

    /// <summary>Identity of the silo hosting this router instance.</summary>
    public SiloIdentity Silo => _silo;

    /// <summary>
    /// Raised after a fact has been fanned out to every backend. Used
    /// by the in-process dashboard broadcaster (<c>DashboardBroadcaster</c>)
    /// to push live part-summary updates to the Blazor UI without
    /// polling. Handlers must be non-blocking; exceptions are swallowed
    /// and logged.
    /// </summary>
    public event EventHandler<Fact>? FactRouted;

    /// <summary>
    /// Raised after a fact applied by the inbound replication handler
    /// has been replayed into the local <c>baseline</c> backend. Lets
    /// the dashboard broadcaster push live part-summary updates on the
    /// peer cluster without polling, so replicated facts appear in the
    /// UI as they land. Handlers must be non-blocking; exceptions are
    /// swallowed by the raiser.
    /// </summary>
    /// <remarks>
    /// Distinct from <see cref="FactRouted"/>: locally-emitted facts
    /// raise <c>FactRouted</c> (after fan-out through the router);
    /// replicated facts raise <c>FactReplicated</c> (after the inbound
    /// endpoint applied them to the local lattice and re-emitted the
    /// payload into the local baseline). Subscribers that only care
    /// about UI freshness typically wire both events to the same
    /// handler.
    /// </remarks>
    public event EventHandler<Fact>? FactReplicated;

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
    /// buffered for reorder), the silo-partition preset dropped it, or
    /// any other ingress filter suppressed fan-out — in which case no
    /// downstream side effects occur until a later unpause or reorder
    /// flush.
    /// </returns>
    public async Task<bool> EmitAsync(Fact fact, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fact);

        // M12c: silo-partition filter. Dropped facts never touch the
        // site grain or the backends so they produce no visible side
        // effects on the current silo; the other silo (which owns the
        // opposite hash bucket) accepts the write.
        if (await IsDroppedByPartitionAsync(fact))
        {
            logger.LogInformation(
                "Fact {FactId} dropped on silo {Silo} by partition filter (serial {Serial})",
                fact.FactId, _silo.Id, fact.Serial.Value);
            return false;
        }

        var admission = await SiteGrain(fact.Site).AdmitAsync(fact);

        // Tier 3: a reorder flush returns a shuffled batch of
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
    /// Returns whether the silo-partition preset is currently active on
    /// the cluster.
    /// </summary>
    public Task<bool> IsPartitionedAsync() =>
        grains.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey).IsPartitionedAsync();

    /// <summary>
    /// Toggles the silo-partition preset. Returns the resulting value.
    /// </summary>
    public async Task<bool> ConfigurePartitionAsync(bool partitioned)
    {
        var value = await grains.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey).SetPartitionedAsync(partitioned);
        RaiseChaosConfigChanged();
        return value;
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
        // Chaos toggles:
        //  * ClusterSplit         → intra-cluster hash filter on
        //  * ReplicationDisconnect → cross-cluster HTTP replication paused
        //  * ClearAll             → both flags cleared
        if (preset == ChaosPreset.ClusterSplit)
        {
            await grains.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey).SetPartitionedAsync(true);
        }
        else if (preset == ChaosPreset.ReplicationDisconnect)
        {
            await grains.GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey).SetDisconnectedAsync(true);
        }
        else if (preset == ChaosPreset.ClearAll)
        {
            await grains.GetGrain<IPartitionChaosGrain>(IPartitionChaosGrain.SingletonKey).SetPartitionedAsync(false);
            await grains.GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey).SetDisconnectedAsync(false);
        }

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
    /// configuration. One entry per <see cref="IFactBackend.Name"/>,
    /// in registration order.
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

    private async Task<bool> IsDroppedByPartitionAsync(Fact fact)
    {
        if (!await IsPartitionedAsync())
        {
            return false;
        }

        // M12c partitioning: deterministic hash-based filter. Silo A
        // (IsPrimary == true) keeps even-hash serials; silo B keeps
        // odd. Using string.GetHashCode is fine for a demo — we only
        // need the same answer on every call, not cryptographic
        // quality. Ordinal hash keeps the split stable across runs
        // (string.GetHashCode() is randomized per process).
        var hash = OrdinalHash(fact.Serial.Value);
        var evenBucket = (hash & 1) == 0;
        return evenBucket != _silo.IsPrimary;
    }

    /// <summary>
    /// Process-stable FNV-1a hash. Used so the M12c partition filter
    /// produces the same "which silo owns this serial" answer on every
    /// silo (unlike <see cref="string.GetHashCode()"/>, which is
    /// randomized per process).
    /// </summary>
    private static uint OrdinalHash(string value)
    {
        const uint offset = 2166136261;
        const uint prime = 16777619;
        var hash = offset;
        foreach (var c in value)
        {
            hash ^= c;
            hash *= prime;
        }
        return hash;
    }

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

    /// <summary>
    /// Raises <see cref="FactReplicated"/> after a peer-originated
    /// fact has been applied to the local lattice and replayed into
    /// the local baseline. Called by the inbound replication endpoint.
    /// Exceptions thrown by subscribers are swallowed and logged.
    /// </summary>
    internal void RaiseFactReplicated(Fact fact)
    {
        ArgumentNullException.ThrowIfNull(fact);
        var handler = FactReplicated;
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
            logger.LogWarning(ex, "FactReplicated subscriber threw for fact {FactId}", fact.FactId);
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
