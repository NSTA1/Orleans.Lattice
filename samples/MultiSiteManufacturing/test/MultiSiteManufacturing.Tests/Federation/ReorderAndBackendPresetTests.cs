using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Verifies the reorder buffer on <see cref="ProcessSiteGrain"/>
/// (plan §4.3 Tier 3) and the <c>LatticeStorageFlakes</c> preset fan-out
/// into <see cref="IBackendChaosGrain"/>.
/// </summary>
[TestFixture]
public class ReorderAndBackendPresetTests
{
    private FederationTestClusterFixture _fixture = null!;
    private FederationRouter _router = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
        (_router, _, _) = _fixture.NewRouter();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public async Task ResetChaos() => await _router.ApplyPresetAsync(ChaosPreset.ClearAll);

    [Test]
    public async Task Reorder_buffer_holds_facts_until_window_fills_then_flushes_all()
    {
        await _router.ConfigureSiteAsync(ProcessSite.OhioForge, new SiteConfig { ReorderEnabled = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-97001");

        // Three facts: below the window size (ReorderWindowSize = 4) — held, no delivery yet.
        for (var i = 1; i <= 3; i++)
        {
            await _router.EmitAsync(Step(serial, i, ProcessStage.Forge, ProcessSite.OhioForge));
        }

        var latticeFactsBefore = await _router.GetBackend("lattice").GetFactsAsync(serial);
        Assert.That(latticeFactsBefore, Is.Empty, "Reorder buffer should hold facts below window size.");

        // Fourth fact triggers the flush.
        await _router.EmitAsync(Step(serial, 4, ProcessStage.Forge, ProcessSite.OhioForge));

        var latticeFactsAfter = await _router.GetBackend("lattice").GetFactsAsync(serial);
        Assert.That(latticeFactsAfter, Has.Count.EqualTo(4),
            "All four facts must reach the backends once the window fills.");
    }

    [Test]
    public async Task LatticeStorageFlakes_preset_configures_only_the_lattice_backend()
    {
        await _router.ApplyPresetAsync(ChaosPreset.LatticeStorageFlakes);

        var backends = await _router.ListBackendChaosAsync();
        var baseline = backends.Single(b => b.Name == "baseline");
        var lattice = backends.Single(b => b.Name == "lattice");

        Assert.Multiple(() =>
        {
            Assert.That(baseline.Config, Is.EqualTo(BackendChaosConfig.Nominal),
                "Baseline backend must remain nominal under the LatticeStorageFlakes preset.");
            Assert.That(lattice.Config.TransientFailureRate, Is.EqualTo(0.10).Within(0.0001));
            Assert.That(lattice.Config.JitterMsMin, Is.EqualTo(50));
            Assert.That(lattice.Config.JitterMsMax, Is.EqualTo(250));
        });
    }

    [Test]
    public async Task ClearAll_resets_backend_chaos_after_LatticeStorageFlakes()
    {
        await _router.ApplyPresetAsync(ChaosPreset.LatticeStorageFlakes);
        await _router.ApplyPresetAsync(ChaosPreset.ClearAll);

        var backends = await _router.ListBackendChaosAsync();
        Assert.That(
            backends.All(b => b.Config == BackendChaosConfig.Nominal),
            Is.True,
            "ClearAll must reset every backend chaos grain to nominal.");
    }

    [Test]
    public async Task ConfigureBackendChaosAsync_roundtrips_through_ListBackendChaosAsync()
    {
        var applied = await _router.ConfigureBackendChaosAsync(
            "lattice",
            new BackendChaosConfig { JitterMsMin = 5, JitterMsMax = 25 });

        Assert.That(applied.Name, Is.EqualTo("lattice"));

        var backends = await _router.ListBackendChaosAsync();
        var lattice = backends.Single(b => b.Name == "lattice");
        Assert.That(lattice.Config.JitterMsMax, Is.EqualTo(25));
    }

    [Test]
    public void ConfigureBackendChaosAsync_throws_for_unknown_backend()
    {
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            _router.ConfigureBackendChaosAsync("does-not-exist", BackendChaosConfig.Nominal));
    }
}
