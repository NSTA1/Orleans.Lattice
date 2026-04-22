using Google.Protobuf.WellKnownTypes;
using MultiSiteManufacturing.Contracts.V1;

namespace MultiSiteManufacturing.Tests.Grpc;

[TestFixture]
public sealed class SiteControlContractTests
{
    private GrpcContractFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new GrpcContractFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    [Test]
    public async Task ListSites_returns_seven_sites_with_nominal_config()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var response = await client.ListSitesAsync(new Empty());

        Assert.That(response.Sites, Has.Count.EqualTo(7));
        foreach (var site in response.Sites)
        {
            Assert.That(site.Config.IsPaused, Is.False);
            Assert.That(site.Config.DelayMs, Is.Zero);
            Assert.That(site.PendingCount, Is.Zero);
        }
    }

    [Test]
    public async Task ConfigureSite_pauses_specified_site()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var state = await client.ConfigureSiteAsync(new ConfigureSiteRequest
        {
            Site = ProcessSite.ToulouseNdtLab,
            Config = new SiteConfig { IsPaused = true, DelayMs = 1500 },
        });

        Assert.That(state.Site, Is.EqualTo(ProcessSite.ToulouseNdtLab));
        Assert.That(state.Config.IsPaused, Is.True);
        Assert.That(state.Config.DelayMs, Is.EqualTo(1500));
    }

    [Test]
    public async Task TriggerPreset_transoceanic_pauses_cmm_and_ndt()
    {
        using var channel = _fixture.CreateChannel();
        var client = new SiteControlService.SiteControlServiceClient(channel);

        var result = await client.TriggerPresetAsync(new TriggerPresetRequest
        {
            Preset = ChaosPreset.TransoceanicBackhaulOutage,
        });

        var cmm = result.Sites.Single(s => s.Site == ProcessSite.StuttgartCmmLab);
        var ndt = result.Sites.Single(s => s.Site == ProcessSite.ToulouseNdtLab);
        Assert.That(cmm.Config.IsPaused, Is.True);
        Assert.That(ndt.Config.IsPaused, Is.True);
        Assert.That(cmm.Config.DelayMs, Is.EqualTo(4000));
        Assert.That(ndt.Config.DelayMs, Is.EqualTo(4000));
    }
}
