using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

[TestFixture]
public class BaselineFactBackendTests
{
    private FederationTestClusterFixture _fixture = null!;
    private BaselineFactBackend _backend = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
        _backend = _fixture.NewBaselineBackend();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Ingested_fact_is_visible_in_state_and_log()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90001");
        await _backend.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab));

        var state = await _backend.GetStateAsync(serial);
        var facts = await _backend.GetFactsAsync(serial);
        var parts = await _backend.ListPartsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state, Is.EqualTo(ComplianceState.FlaggedForReview));
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(parts, Contains.Item(serial));
            Assert.That(_backend.Name, Is.EqualTo("baseline"));
        });
    }

    [Test]
    public async Task Arrival_order_is_preserved_in_log_even_when_HLC_is_reversed()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-90002");
        // Emit MRB UseAsIs first (later HLC), then NC (earlier HLC).
        var mrb = Mrb(serial, 20, "NC-2", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);
        var nc = Nc(serial, 10, "NC-2", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        await _backend.EmitAsync(mrb);
        await _backend.EmitAsync(nc);

        var facts = await _backend.GetFactsAsync(serial);

        Assert.That(facts.Select(f => f.FactId), Is.EqualTo(new[] { mrb.FactId, nc.FactId }));
    }
}
