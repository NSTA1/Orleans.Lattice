using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Integration coverage for CRDT current-state rendering of materialised-view and
/// tag-index trees. A predicate / key-preserving view stores its source tree's
/// value verbatim, so a view over an OR-Set source must surface the source's live
/// members through the view-tree id; an aggregation or history view, and a tag
/// index whose membership tree is opaque, must degrade to a blob without
/// crashing.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeViewCrdtRenderingIntegrationTests
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
    public async Task ScanEntriesAsync_predicate_view_over_orset_reports_source_shape_and_members()
    {
        await _fixture.CreateOrSetSourceTreeAsync("orset-view-source", "focus", "live-element");
        var view = _fixture.CreateView("orset-view-source", "orset-mirror");
        await view.RebuildAsync();

        var page = await _fixture.Query.ScanEntriesAsync(
            new EntryScanRequest { TreeId = "view-orset-mirror", PageSize = 100 });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Has.Count.EqualTo(1));

        // A key-preserving predicate view stores the source OR-Set value verbatim,
        // so the view tree must mirror the source's OR-Set shape and decode the
        // same live members rather than rendering raw hex.
        var entry = page.Entries[0];
        Assert.That(entry.CrdtShape, Is.EqualTo("OrSet"),
            "a predicate view over an OR-Set source must mirror the source's CRDT shape");
        Assert.That(entry.CurrentMembers, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(entry.CurrentMembers[0].Element), Is.EqualTo("live-element"));
    }

    [Test]
    public async Task GetEntryAsync_predicate_view_over_orset_decodes_live_members()
    {
        await _fixture.CreateOrSetSourceTreeAsync("orset-view-detail-source", "focus", "kept");
        var view = _fixture.CreateView("orset-view-detail-source", "orset-detail-mirror");
        await view.RebuildAsync();

        var result = await _fixture.Query.GetEntryAsync("view-orset-detail-mirror", "focus");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CrdtShape, Is.EqualTo("OrSet"));
        Assert.That(result.Entry.CurrentMembers, Has.Count.EqualTo(1));
        Assert.That(Encoding.UTF8.GetString(result.Entry.CurrentMembers[0].Element), Is.EqualTo("kept"));
    }

    [Test]
    public async Task ScanEntriesAsync_predicate_view_over_lww_source_reports_no_shape()
    {
        await _fixture.CreatePopulatedTreeAsync("lww-view-source", keyCount: 3);
        var view = _fixture.CreateView("lww-view-source", "lww-mirror");
        await view.RebuildAsync();

        var page = await _fixture.Query.ScanEntriesAsync(
            new EntryScanRequest { TreeId = "view-lww-mirror", PageSize = 100 });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Is.Not.Empty);

        // A view over an opaque last-writer-wins source has no member CRDT shape,
        // so its entries stay blobs and surface no current members.
        Assert.That(page.Entries.Select(e => e.CrdtShape), Is.All.Null,
            "a view over a last-writer-wins source carries no CRDT shape tag");
        Assert.That(page.Entries.SelectMany(e => e.CurrentMembers), Is.Empty);
    }
}
