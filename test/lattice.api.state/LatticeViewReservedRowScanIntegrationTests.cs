using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage that the state API entry scan hides an aggregation
/// view's internal reserved rows. A grouped-reduce (or custom-fold) view keeps
/// its accumulator / inverse / membership rows under the reserved NUL prefix, so
/// a scan of the view tree must surface only the materialised group values -
/// never the internal plumbing rows, which sort before every group value and
/// would otherwise dominate the first page.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeViewReservedRowScanIntegrationTests
{
    private CatalogClusterFixture _fixture = null!;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new CatalogClusterFixture();
        await _fixture.InitializeAsync();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task ScanEntriesAsync_aggregation_view_excludes_internal_reserved_rows()
    {
        await _fixture.CreatePopulatedTreeAsync("agg-scan-source", keyCount: 3);
        var view = _fixture.CreateAggregationView("agg-scan-source", "agg-scan-view");
        await view.RebuildAsync();

        var page = await _fixture.Query.ScanEntriesAsync(
            new EntryScanRequest { TreeId = "view-agg-scan-view", PageSize = 100 });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Is.Not.Empty);

        // No entry may fall in the reserved region: the internal accumulator /
        // inverse / membership rows all begin with the reserved NUL prefix.
        Assert.That(page.Entries.Select(e => e.Key), Has.All.Matches<string>(
            key => key.Length > 0 && key[0] != '\u0000'),
            "the scan must not surface any reserved NUL-prefixed internal row");

        // The materialised group value (this Count view groups every source key
        // into the single group "all") must still be returned, with its bytes.
        var group = page.Entries.SingleOrDefault(e => e.Key == "all");
        Assert.That(group, Is.Not.Null, "the materialised group value must remain visible");
        Assert.That(group!.ValueLength, Is.GreaterThan(0));
    }

    [Test]
    public async Task ScanEntriesAsync_aggregation_view_live_mode_also_excludes_reserved_rows()
    {
        await _fixture.CreatePopulatedTreeAsync("agg-live-source", keyCount: 3);
        var view = _fixture.CreateAggregationView("agg-live-source", "agg-live-view");
        await view.RebuildAsync();

        var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = "view-agg-live-view",
            PageSize = 100,
            Mode = EntryScanMode.Live,
        });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries.Select(e => e.Key), Has.All.Matches<string>(
            key => key.Length > 0 && key[0] != '\u0000'),
            "the reserved floor applies to every scan mode, not just the snapshot default");
        Assert.That(page.Entries.Any(e => e.Key == "all"), Is.True);
    }

    [Test]
    public async Task ScanEntriesAsync_predicate_view_is_unaffected_by_the_reserved_floor()
    {
        await _fixture.CreatePopulatedTreeAsync("predicate-floor-source", keyCount: 3);
        var view = _fixture.CreateView("predicate-floor-source", "predicate-floor-view");
        await view.RebuildAsync();

        var page = await _fixture.Query.ScanEntriesAsync(
            new EntryScanRequest { TreeId = "view-predicate-floor-view", PageSize = 100 });

        // A predicate / key-preserving view keeps no reserved rows, so every
        // source key is mirrored verbatim and the floor changes nothing.
        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Has.Count.EqualTo(3));
    }
}
