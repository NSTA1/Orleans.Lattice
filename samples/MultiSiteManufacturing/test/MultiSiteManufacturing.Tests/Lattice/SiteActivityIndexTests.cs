using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Covers <see cref="SiteActivityIndex"/> (plan §M12d): range scans
/// over the B+ tree index that powers the "parts at $site" view, for
/// every fact type (not just <see cref="ProcessStepCompleted"/>).
/// </summary>
[TestFixture]
public class SiteActivityIndexTests
{
    private FederationTestClusterFixture _fixture = null!;
    private SiteActivityIndex _index = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();

        // The router is only needed so the index can subscribe to
        // FactRouted; tests use AppendAsync directly so the router
        // stays idle.
        var (router, _, _) = _fixture.NewRouter();
        _index = new SiteActivityIndex(_fixture.GrainFactory, router, NullLogger<SiteActivityIndex>.Instance);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ListAtSite_returns_empty_when_no_facts_recorded()
    {
        var result = await _index.ListAtSiteAsync(ProcessSite.BristolFai);
        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task AppendAsync_then_ListAtSite_returns_the_entry()
    {
        var serial = new PartSerialNumber("HPT-IDX-2028-92100");
        await _index.AppendAsync(Step(serial, 100, ProcessStage.Forge, ProcessSite.OhioForge));

        var result = await _index.ListAtSiteAsync(ProcessSite.OhioForge);

        Assert.That(
            result.Any(e => e.Serial == serial && e.Activity.Contains("Forge")),
            Is.True,
            "OhioForge scan should surface the appended entry with a Forge activity label");
    }

    [Test]
    public async Task ListAtSite_only_returns_entries_for_that_site()
    {
        var a = new PartSerialNumber("HPT-IDX-2028-92101");
        var b = new PartSerialNumber("HPT-IDX-2028-92102");
        await _index.AppendAsync(Step(a, 200, ProcessStage.HeatTreat, ProcessSite.NagoyaHeatTreat));
        await _index.AppendAsync(Step(b, 201, ProcessStage.Machining, ProcessSite.StuttgartMachining));

        var nagoya = await _index.ListAtSiteAsync(ProcessSite.NagoyaHeatTreat);
        var stuttgart = await _index.ListAtSiteAsync(ProcessSite.StuttgartMachining);

        Assert.Multiple(() =>
        {
            Assert.That(nagoya.Select(e => e.Serial), Does.Contain(a));
            Assert.That(nagoya.Select(e => e.Serial), Does.Not.Contain(b));
            Assert.That(stuttgart.Select(e => e.Serial), Does.Contain(b));
            Assert.That(stuttgart.Select(e => e.Serial), Does.Not.Contain(a));
        });
    }

    [Test]
    public async Task ListAtSite_returns_entries_in_HLC_descending_order()
    {
        var s1 = new PartSerialNumber("HPT-IDX-2028-92103");
        var s2 = new PartSerialNumber("HPT-IDX-2028-92104");
        // Distinct site so we don't observe entries from other tests.
        var site = ProcessSite.ToulouseNdtLab;
        await _index.AppendAsync(Step(s2, 500, ProcessStage.NDT, site));
        await _index.AppendAsync(Step(s1, 300, ProcessStage.NDT, site));

        var result = await _index.ListAtSiteAsync(site);
        var serials = result.Where(e => e.Serial == s1 || e.Serial == s2)
                            .Select(e => e.Serial.Value)
                            .ToList();

        // Most-recent first: s2 (tick 500) before s1 (tick 300).
        Assert.That(serials, Is.EqualTo(new[] { s2.Value, s1.Value }));
    }

    [Test]
    public async Task ListAtSite_indexes_non_process_step_facts()
    {
        // CMM lab never emits a ProcessStepCompleted — only InspectionRecorded,
        // NonConformanceRaised, MrbDisposition etc. Before generalization the
        // site panel was always empty; now it shows the latest activity.
        var site = ProcessSite.CincinnatiMrb;
        var serial = new PartSerialNumber("HPT-IDX-2028-92110");
        await _index.AppendAsync(Nc(serial, 600, "NCR-ABC123", NcSeverity.Major, site));

        var result = await _index.ListAtSiteAsync(site);

        Assert.That(
            result.Any(e => e.Serial == serial && e.Activity.Contains("NCR-ABC123")),
            Is.True,
            "CincinnatiMrb scan should surface the NonConformanceRaised entry");
    }

    [Test]
    public async Task ListAtSite_dedups_by_serial_keeping_most_recent_activity()
    {
        // Same serial records two activities at the same site; the scan
        // should return only the most recent one.
        var site = ProcessSite.CincinnatiMrb;
        var serial = new PartSerialNumber("HPT-IDX-2028-92111");
        await _index.AppendAsync(Nc(serial, 700, "NCR-OLD", NcSeverity.Minor, site));
        await _index.AppendAsync(Mrb(serial, 800, "NCR-OLD", MrbDispositionKind.UseAsIs, site));

        var result = await _index.ListAtSiteAsync(site);
        var rows = result.Where(e => e.Serial == serial).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1), "serial should appear exactly once");
            Assert.That(rows[0].Activity, Does.Contain("MRB"), "most-recent activity (MRB) should win");
        });
    }
}

