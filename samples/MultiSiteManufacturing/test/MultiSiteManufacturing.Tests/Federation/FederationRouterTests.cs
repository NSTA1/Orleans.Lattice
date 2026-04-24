using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

/// <summary>
/// The baseline backend (arrival-order fold) and the lattice backend
/// (HLC-ordered fold) must disagree when facts arrive in non-HLC order.
/// This is the divergence the dashboard feed surfaces.
/// </summary>
[TestFixture]
public class FederationRouterTests
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

    [Test]
    public void Router_exposes_both_backends_by_name()
    {
        Assert.Multiple(() =>
        {
            Assert.That(_router.Backends.Select(b => b.Name), Is.EquivalentTo(new[] { "baseline", "lattice" }));
            Assert.That(_router.GetBackend("baseline"), Is.Not.Null);
            Assert.That(_router.GetBackend("lattice"), Is.Not.Null);
        });
    }

    [Test]
    public void GetBackend_throws_for_unknown_name()
    {
        Assert.Throws<InvalidOperationException>(() => _router.GetBackend("missing"));
    }

    [Test]
    public async Task EmitAsync_fans_out_to_every_backend()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92001");
        await _router.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab));

        var baselineState = await _router.GetBackend("baseline").GetStateAsync(serial);
        var latticeState = await _router.GetBackend("lattice").GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(baselineState, Is.EqualTo(ComplianceState.FlaggedForReview));
            Assert.That(latticeState, Is.EqualTo(ComplianceState.FlaggedForReview));
        });
    }

    [Test]
    public async Task Reorder_causes_baseline_and_lattice_to_diverge()
    {
        // Scenario: NC-Minor at HLC=10, MRB UseAsIs at HLC=20. Under HLC order
        // (lattice) UseAsIs demotes Flagged back to Nominal. Delivered reversed,
        // the arrival-order baseline sees UseAsIs-against-nothing (no-op) and
        // then the NC-Minor, so it stays Flagged.
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-92002");
        var nc = Nc(serial, 10, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var mrb = Mrb(serial, 20, "NC-1", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);

        await _router.EmitAsync(mrb);   // arrives first
        await _router.EmitAsync(nc);    // arrives second

        var baselineState = await _router.GetBackend("baseline").GetStateAsync(serial);
        var latticeState = await _router.GetBackend("lattice").GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(baselineState, Is.EqualTo(ComplianceState.FlaggedForReview),
                "Baseline applies in arrival order; early UseAsIs is a no-op, so the later NC sticks.");
            Assert.That(latticeState, Is.EqualTo(ComplianceState.Nominal),
                "Lattice re-orders by HLC; NC(10) then UseAsIs(20) demotes Flagged→Nominal.");
            Assert.That(baselineState, Is.Not.EqualTo(latticeState), "Divergence is the whole point.");
        });
    }
}
