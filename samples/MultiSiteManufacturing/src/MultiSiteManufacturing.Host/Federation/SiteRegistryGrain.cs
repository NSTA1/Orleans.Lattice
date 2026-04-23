using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Stateless registry grain implementation. Holds no persistent state of
/// its own — it's purely a fan-out coordinator over the seven
/// <see cref="IProcessSiteGrain"/> instances.
/// </summary>
internal sealed class SiteRegistryGrain(IGrainFactory grains) : Grain, ISiteRegistryGrain
{
    private static readonly ProcessSite[] AllSites = Enum.GetValues<ProcessSite>();

    public async Task<IReadOnlyList<SiteState>> ListSitesAsync()
    {
        var tasks = new Task<SiteState>[AllSites.Length];
        for (var i = 0; i < AllSites.Length; i++)
        {
            tasks[i] = SiteGrain(AllSites[i]).GetStateAsync();
        }
        return await Task.WhenAll(tasks);
    }

    public Task<SiteConfigureResult> ConfigureSiteAsync(ProcessSite site, SiteConfig config) =>
        SiteGrain(site).ConfigureAsync(config);

    public async Task<IReadOnlyList<Fact>> ApplyPresetAsync(ChaosPreset preset)
    {
        (ProcessSite Site, SiteConfig Config)[] siteTargets;
        IReadOnlyDictionary<string, BackendChaosConfig> backendTargets;

        switch (preset)
        {
            case ChaosPreset.ClearAll:
                siteTargets = AllSites.Select(s => (s, SiteConfig.Nominal)).ToArray();
                backendTargets = new Dictionary<string, BackendChaosConfig>
                {
                    ["baseline"] = BackendChaosConfig.Nominal,
                    ["lattice"] = BackendChaosConfig.Nominal,
                };
                break;
            case ChaosPreset.TransoceanicBackhaulOutage:
                siteTargets =
                [
                    (ProcessSite.StuttgartCmmLab, new SiteConfig { IsPaused = true, DelayMs = 4_000 }),
                    (ProcessSite.ToulouseNdtLab, new SiteConfig { IsPaused = true, DelayMs = 4_000 }),
                ];
                backendTargets = EmptyBackendTargets;
                break;
            case ChaosPreset.CustomsHold:
                siteTargets =
                [
                    (ProcessSite.NagoyaHeatTreat, new SiteConfig { DelayMs = 8_000 }),
                ];
                backendTargets = EmptyBackendTargets;
                break;
            case ChaosPreset.MrbWeekend:
                siteTargets =
                [
                    (ProcessSite.CincinnatiMrb, new SiteConfig { IsPaused = true }),
                ];
                backendTargets = EmptyBackendTargets;
                break;
            case ChaosPreset.LatticeStorageFlakes:
                siteTargets = [];
                backendTargets = new Dictionary<string, BackendChaosConfig>
                {
                    ["lattice"] = new BackendChaosConfig
                    {
                        JitterMsMin = 50,
                        JitterMsMax = 250,
                        TransientFailureRate = 0.10,
                    },
                };
                break;
            case ChaosPreset.BaselineReorderStorm:
                siteTargets = [];
                backendTargets = new Dictionary<string, BackendChaosConfig>
                {
                    ["baseline"] = new BackendChaosConfig
                    {
                        ReorderWindowMs = 300,
                    },
                };
                break;
            case ChaosPreset.SiloPartition:
                // No site / backend knobs — the router handles the
                // partition flag directly via IPartitionChaosGrain.
                siteTargets = [];
                backendTargets = EmptyBackendTargets;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(preset), preset, "Unknown chaos preset.");
        }

        var siteTasks = siteTargets.Select(t => SiteGrain(t.Site).ConfigureAsync(t.Config)).ToArray();
        var backendTasks = backendTargets
            .Select(kv => grains.GetGrain<IBackendChaosGrain>(kv.Key).ConfigureAsync(kv.Value))
            .ToArray();

        var siteResults = await Task.WhenAll(siteTasks);
        await Task.WhenAll(backendTasks);

        var drained = new List<Fact>();
        foreach (var result in siteResults)
        {
            drained.AddRange(result.Drained);
        }
        return drained;
    }

    private static readonly IReadOnlyDictionary<string, BackendChaosConfig> EmptyBackendTargets
        = new Dictionary<string, BackendChaosConfig>();

    private IProcessSiteGrain SiteGrain(ProcessSite site) =>
        grains.GetGrain<IProcessSiteGrain>(site.ToString());
}
