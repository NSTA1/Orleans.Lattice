using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// Verifies that the router honours per-site chaos configuration: paused
/// sites hold facts, reconfiguring to unpaused releases them through
/// every backend, and clean sites still pass through.
/// </summary>
[TestFixture]
public class FederationRouterChaosTests
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
    public async Task Paused_site_holds_fact_so_backends_see_nothing()
    {
        await _router.ConfigureSiteAsync(ProcessSite.ToulouseNdtLab, new SiteConfig { IsPaused = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-94001");
        await _router.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab));

        var baselineFacts = await _router.GetBackend("baseline").GetFactsAsync(serial);
        var latticeFacts = await _router.GetBackend("lattice").GetFactsAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(baselineFacts, Is.Empty);
            Assert.That(latticeFacts, Is.Empty);
        });
    }

    [Test]
    public async Task Unpausing_a_site_releases_held_facts_to_every_backend()
    {
        await _router.ConfigureSiteAsync(ProcessSite.ToulouseNdtLab, new SiteConfig { IsPaused = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-94002");
        await _router.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab));

        await _router.ConfigureSiteAsync(ProcessSite.ToulouseNdtLab, SiteConfig.Nominal);

        var baselineState = await _router.GetBackend("baseline").GetStateAsync(serial);
        var latticeState = await _router.GetBackend("lattice").GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(baselineState, Is.EqualTo(ComplianceState.FlaggedForReview));
            Assert.That(latticeState, Is.EqualTo(ComplianceState.FlaggedForReview));
        });
    }

    [Test]
    public async Task Unpaused_site_forwards_fact_immediately()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-94003");
        await _router.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab));

        var state = await _router.GetBackend("lattice").GetStateAsync(serial);
        Assert.That(state, Is.EqualTo(ComplianceState.Rework));
    }

    [Test]
    public async Task ApplyPresetAsync_returns_snapshots_and_releases_facts()
    {
        // Pause a site, queue a fact, then apply ClearAll — the preset should
        // unpause the site and release the queued fact through the backends.
        await _router.ConfigureSiteAsync(ProcessSite.CincinnatiMrb, new SiteConfig { IsPaused = true });

        var serial = new PartSerialNumber("HPT-BLD-S1-2028-94004");
        await _router.EmitAsync(Mrb(serial, 2, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb));

        var snapshots = await _router.ApplyPresetAsync(ChaosPreset.ClearAll);

        var latticeFacts = await _router.GetBackend("lattice").GetFactsAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(snapshots.Select(s => s.Site), Is.EquivalentTo(Enum.GetValues<ProcessSite>()));
            Assert.That(snapshots.All(s => s.Config == SiteConfig.Nominal), Is.True);
            Assert.That(latticeFacts, Has.Count.EqualTo(1));
        });
    }
}
