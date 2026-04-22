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
        var target = preset switch
        {
            ChaosPreset.ClearAll => AllSites.Select(s => (s, SiteConfig.Nominal)).ToArray(),
            ChaosPreset.TransoceanicBackhaulOutage =>
            [
                (ProcessSite.StuttgartCmmLab, new SiteConfig { IsPaused = true, DelayMs = 4_000 }),
                (ProcessSite.ToulouseNdtLab, new SiteConfig { IsPaused = true, DelayMs = 4_000 }),
            ],
            ChaosPreset.CustomsHold =>
            [
                (ProcessSite.NagoyaHeatTreat, new SiteConfig { DelayMs = 8_000 }),
            ],
            ChaosPreset.MrbWeekend =>
            [
                (ProcessSite.CincinnatiMrb, new SiteConfig { IsPaused = true }),
            ],
            _ => throw new ArgumentOutOfRangeException(nameof(preset), preset, "Unknown chaos preset."),
        };

        var tasks = target.Select(t => SiteGrain(t.Item1).ConfigureAsync(t.Item2)).ToArray();
        var results = await Task.WhenAll(tasks);

        var drained = new List<Fact>();
        foreach (var result in results)
        {
            drained.AddRange(result.Drained);
        }
        return drained;
    }

    private IProcessSiteGrain SiteGrain(ProcessSite site) =>
        grains.GetGrain<IProcessSiteGrain>(site.ToString());
}
