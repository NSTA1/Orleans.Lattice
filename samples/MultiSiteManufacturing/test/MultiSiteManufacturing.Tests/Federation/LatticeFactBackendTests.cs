using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Federation;
using MultiSiteManufacturing.Host.Lattice;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Federation;

[TestFixture]
public class LatticeFactBackendTests
{
    private FederationTestClusterFixture _fixture = null!;
    private LatticeFactBackend _backend = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
        _backend = _fixture.NewLatticeBackend();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Ingested_fact_is_visible_in_state_and_log()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91001");
        await _backend.EmitAsync(Nc(serial, 1, "NC-1", NcSeverity.Major, ProcessSite.ToulouseNdtLab));

        var state = await _backend.GetStateAsync(serial);
        var facts = await _backend.GetFactsAsync(serial);
        var parts = await _backend.ListPartsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state, Is.EqualTo(ComplianceState.Rework));
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(parts, Contains.Item(serial));
            Assert.That(_backend.Name, Is.EqualTo("lattice"));
        });
    }

    [Test]
    public async Task Range_scan_returns_facts_in_HLC_order_regardless_of_arrival()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-91002");
        var first = Nc(serial, 10, "NC-A", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var second = Mrb(serial, 20, "NC-A", MrbDispositionKind.UseAsIs, ProcessSite.CincinnatiMrb);

        // Arrive reversed — lattice key bakes in HLC so the scan still orders correctly.
        await _backend.EmitAsync(second);
        await _backend.EmitAsync(first);

        var facts = await _backend.GetFactsAsync(serial);
        var state = await _backend.GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(facts.Select(f => f.FactId), Is.EqualTo(new[] { first.FactId, second.FactId }));
            Assert.That(state, Is.EqualTo(ComplianceState.Nominal));
        });
    }

    [Test]
    public async Task Scan_isolates_facts_for_the_requested_part()
    {
        var a = new PartSerialNumber("HPT-BLD-S1-2028-91010");
        var b = new PartSerialNumber("HPT-BLD-S1-2028-91011");
        await _backend.EmitAsync(Nc(a, 1, "NC-A", NcSeverity.Minor, ProcessSite.ToulouseNdtLab));
        await _backend.EmitAsync(Nc(b, 1, "NC-B", NcSeverity.Major, ProcessSite.ToulouseNdtLab));

        var factsA = await _backend.GetFactsAsync(a);
        var factsB = await _backend.GetFactsAsync(b);

        Assert.Multiple(() =>
        {
            Assert.That(factsA, Has.Count.EqualTo(1));
            Assert.That(factsA[0].Serial, Is.EqualTo(a));
            Assert.That(factsB, Has.Count.EqualTo(1));
            Assert.That(factsB[0].Serial, Is.EqualTo(b));
        });
    }
}
