using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

[TestFixture]
public class ProcessSiteGrainTests
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

    private IProcessSiteGrain Grain(ProcessSite site) =>
        _fixture.GrainFactory.GetGrain<IProcessSiteGrain>(site.ToString());

    [Test]
    public async Task Default_state_is_nominal_with_no_pending()
    {
        var state = await Grain(ProcessSite.OhioForge).GetStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.Site, Is.EqualTo(ProcessSite.OhioForge));
            Assert.That(state.Config, Is.EqualTo(SiteConfig.Nominal));
            Assert.That(state.PendingCount, Is.Zero);
        });
    }

    [Test]
    public async Task AdmitAsync_returns_Pass_when_nominal()
    {
        var grain = Grain(ProcessSite.NagoyaHeatTreat);
        var admission = await grain.AdmitAsync(
            Step(new PartSerialNumber("HPT-BLD-S1-2028-93001"), 1, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat));

        Assert.Multiple(() =>
        {
            Assert.That(admission.Forward, Is.True);
            Assert.That(admission.DelayMs, Is.Zero);
        });
    }

    [Test]
    public async Task AdmitAsync_returns_Delayed_when_configured_with_delay()
    {
        var grain = Grain(ProcessSite.StuttgartMachining);
        await grain.ConfigureAsync(new SiteConfig { DelayMs = 250 });

        var admission = await grain.AdmitAsync(
            Step(new PartSerialNumber("HPT-BLD-S1-2028-93002"), 1, ProcessStage.Machining, ProcessSite.StuttgartMachining));

        Assert.Multiple(() =>
        {
            Assert.That(admission.Forward, Is.True);
            Assert.That(admission.DelayMs, Is.EqualTo(250));
        });

        await grain.ConfigureAsync(SiteConfig.Nominal);
    }

    [Test]
    public async Task AdmitAsync_holds_fact_when_paused()
    {
        var grain = Grain(ProcessSite.StuttgartCmmLab);
        await grain.ConfigureAsync(new SiteConfig { IsPaused = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-93003");
        var admission = await grain.AdmitAsync(
            Step(serial, 1, ProcessStage.Machining, ProcessSite.StuttgartCmmLab));

        var state = await grain.GetStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(admission.Forward, Is.False);
            Assert.That(state.PendingCount, Is.EqualTo(1));
        });

        await grain.ConfigureAsync(SiteConfig.Nominal);
    }

    [Test]
    public async Task ConfigureAsync_drains_pending_facts_on_pause_to_unpause_transition()
    {
        var grain = Grain(ProcessSite.ToulouseNdtLab);
        await grain.ConfigureAsync(new SiteConfig { IsPaused = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-93004");
        var nc = Nc(serial, 5, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var mrb = Mrb(serial, 10, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.ToulouseNdtLab);
        await grain.AdmitAsync(nc);
        await grain.AdmitAsync(mrb);

        var result = await grain.ConfigureAsync(SiteConfig.Nominal);

        Assert.Multiple(() =>
        {
            Assert.That(result.Drained.Select(f => f.FactId), Is.EqualTo(new[] { nc.FactId, mrb.FactId }));
            Assert.That(result.Config.IsPaused, Is.False);
        });

        var state = await grain.GetStateAsync();
        Assert.That(state.PendingCount, Is.Zero);
    }

    [Test]
    public async Task ConfigureAsync_does_not_drain_when_still_paused()
    {
        var grain = Grain(ProcessSite.CincinnatiMrb);
        await grain.ConfigureAsync(new SiteConfig { IsPaused = true });

        await grain.AdmitAsync(
            Nc(new PartSerialNumber("HPT-BLD-S1-2028-93005"), 1, "NC-9", NcSeverity.Major, ProcessSite.CincinnatiMrb));

        var result = await grain.ConfigureAsync(new SiteConfig { IsPaused = true, DelayMs = 1_000 });

        Assert.That(result.Drained, Is.Empty);

        await grain.ConfigureAsync(SiteConfig.Nominal);
    }
}
