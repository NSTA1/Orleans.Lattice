using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;

namespace MultiSiteManufacturing.Tests.Federation;

[TestFixture]
public class SiteRegistryGrainTests
{
    private FederationTestClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ISiteRegistryGrain Registry =>
        _fixture.GrainFactory.GetGrain<ISiteRegistryGrain>(ISiteRegistryGrain.SingletonKey);

    private IProcessSiteGrain Site(ProcessSite site) =>
        _fixture.GrainFactory.GetGrain<IProcessSiteGrain>(site.ToString());

    [Test]
    public async Task ListSitesAsync_returns_every_site()
    {
        var states = await Registry.ListSitesAsync();
        var sites = states.Select(s => s.Site).ToArray();

        Assert.That(sites, Is.EquivalentTo(Enum.GetValues<ProcessSite>()));
    }

    [Test]
    public async Task ApplyPresetAsync_MrbWeekend_pauses_cincinnati()
    {
        await Registry.ApplyPresetAsync(ChaosPreset.MrbWeekend);

        var state = await Site(ProcessSite.CincinnatiMrb).GetStateAsync();
        Assert.That(state.Config.IsPaused, Is.True);

        await Registry.ApplyPresetAsync(ChaosPreset.ClearAll);
    }

    [Test]
    public async Task ApplyPresetAsync_TransoceanicBackhaulOutage_pauses_and_delays_cmm_and_ndt()
    {
        await Registry.ApplyPresetAsync(ChaosPreset.TransoceanicBackhaulOutage);

        var cmm = await Site(ProcessSite.StuttgartCmmLab).GetStateAsync();
        var ndt = await Site(ProcessSite.ToulouseNdtLab).GetStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(cmm.Config.IsPaused, Is.True);
            Assert.That(cmm.Config.DelayMs, Is.EqualTo(4_000));
            Assert.That(ndt.Config.IsPaused, Is.True);
            Assert.That(ndt.Config.DelayMs, Is.EqualTo(4_000));
        });

        await Registry.ApplyPresetAsync(ChaosPreset.ClearAll);
    }

    [Test]
    public async Task ApplyPresetAsync_ClearAll_resets_every_site()
    {
        await Registry.ApplyPresetAsync(ChaosPreset.MrbWeekend);
        await Registry.ApplyPresetAsync(ChaosPreset.CustomsHold);

        await Registry.ApplyPresetAsync(ChaosPreset.ClearAll);

        var states = await Registry.ListSitesAsync();
        Assert.That(states.All(s => s.Config == SiteConfig.Nominal), Is.True);
    }
}
