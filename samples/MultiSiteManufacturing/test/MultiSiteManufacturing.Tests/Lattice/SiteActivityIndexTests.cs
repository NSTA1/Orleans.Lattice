using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Tests.Federation;
using Orleans.Lattice;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Lattice;

/// <summary>
/// Covers <see cref="SiteActivityIndex"/>: the tag-index-backed
/// "parts at $site" view, for every fact type (not just
/// <see cref="ProcessStepCompleted"/>).
/// </summary>
[TestFixture]
public class SiteActivityIndexTests
{
    private FederationTestClusterFixture _fixture = null!;
    private SiteActivityIndex _index = null!;
    private ILatticeTagIndexFactory _tagIndexFactory = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();

        // The tag-index factory is resolved from the silo container, so the
        // index opens through the same replication-configured entry point the
        // host uses. The in-memory test path configures no replication, so the
        // factory reports replication disabled and the index uses the
        // last-writer-wins membership path.
        _tagIndexFactory = _fixture.SiloServices.GetRequiredService<ILatticeTagIndexFactory>();

        // The router is only needed so the index can subscribe to
        // FactRouted; tests use AppendAsync directly so the router
        // stays idle.
        var (router, _, _) = _fixture.NewRouter();
        _index = new SiteActivityIndex(_fixture.GrainFactory, _tagIndexFactory, router, NullLogger<SiteActivityIndex>.Instance);
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
        // CMM lab never emits a ProcessStepCompleted - only InspectionRecorded,
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

    [Test]
    public async Task AppendAsync_tags_the_part_key_with_its_site_in_the_tag_index()
    {
        // The new mechanism: the part-major subject key {serial}/{site}
        // is the genuine access path only because the tag index carries
        // a posting row for the site. Assert the tag index itself - not a
        // range scan - resolves the key.
        var serial = new PartSerialNumber("HPT-IDX-2028-92120");
        var site = ProcessSite.OhioForge;
        await _index.AppendAsync(Step(serial, 900, ProcessStage.FAI, site));

        var subjectTree = _fixture.GrainFactory.GetGrain<ILattice>(SiteActivityIndex.TreeId);
        var tagIndex = _tagIndexFactory.Create(subjectTree, SiteActivityIndex.IndexName);
        var expectedKey = $"{serial.Value}/{site}";

        var keys = new List<string>();
        await foreach (var key in tagIndex.WithAnyTags(site.ToString()))
        {
            keys.Add(key);
        }

        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain(expectedKey), "tag index should resolve the part-major key by its site tag");
            Assert.That(
                tagIndex.Key(expectedKey).GetAsync().GetAwaiter().GetResult(),
                Does.Contain(site.ToString()),
                "the key should carry exactly its site tag");
        });
    }

    [Test]
    public async Task AppendAsync_keeps_most_recent_activity_when_facts_arrive_out_of_order()
    {
        // A stale (older-HLC) fact must not clobber a newer one for the
        // same part at the same site: the HLC guard skips the regression.
        var site = ProcessSite.StuttgartCmmLab;
        var serial = new PartSerialNumber("HPT-IDX-2028-92130");

        await _index.AppendAsync(Mrb(serial, 1100, "NCR-NEW", MrbDispositionKind.Scrap, site));
        await _index.AppendAsync(Nc(serial, 1000, "NCR-OLD", NcSeverity.Minor, site));

        var result = await _index.ListAtSiteAsync(site);
        var rows = result.Where(e => e.Serial == serial).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(rows, Has.Count.EqualTo(1), "serial should appear exactly once");
            Assert.That(rows[0].Activity, Does.Contain("MRB"), "the newer activity must survive the stale append");
            Assert.That(rows[0].Hlc.WallClockTicks, Is.EqualTo(1100), "the surviving row should keep the newer HLC");
        });
    }

    [Test]
    public async Task ListAtSite_preserves_the_activity_label_through_value_round_trip()
    {
        // The HLC now lives in the value, not the key; confirm the label
        // survives the encode/decode and the HLC is recovered intact.
        var site = ProcessSite.OhioForge;
        var serial = new PartSerialNumber("HPT-IDX-2028-92140");
        await _index.AppendAsync(Nc(serial, 1234, "NCR-XYZ789", NcSeverity.Critical, site));

        var result = await _index.ListAtSiteAsync(site);
        var row = result.Single(e => e.Serial == serial);

        Assert.Multiple(() =>
        {
            Assert.That(row.Activity, Does.Contain("NCR-XYZ789"));
            Assert.That(row.Activity, Does.Contain("Critical"));
            Assert.That(row.Hlc.WallClockTicks, Is.EqualTo(1234));
            Assert.That(row.Site, Is.EqualTo(site));
        });
    }
}

