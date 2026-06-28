using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Coverage for <see cref="EntryRecord.CrdtShape"/>: the read facade stamps the
/// declared per-tree CRDT merge mode onto every entry it returns, so a consumer
/// can tell a typed CRDT (an OR-Set here) apart from opaque last-writer-wins
/// bytes. The shape is sourced from the merge-mode resolver, which is the only
/// thing the system knows about a value's shape.
/// </summary>
public sealed partial class LatticeStateQueryIntegrationTests
{
    [Test]
    public async Task ScanEntriesAsync_orset_tree_reports_orset_shape()
    {
        const string treeId = "orset-scan-shape";
        await _fixture.CreateOrSetTreeAsync(treeId, "alpha", "bravo", "charlie");

        var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = treeId,
            PageSize = 100,
        });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Has.Count.EqualTo(3));
        Assert.That(page.Entries.Select(e => e.CrdtShape),
            Is.All.EqualTo("OrSet"),
            "every entry on an OR-Set tree must carry the OrSet shape tag");
    }

    [Test]
    public async Task GetEntryAsync_orset_tree_reports_orset_shape()
    {
        const string treeId = "orset-get-shape";
        await _fixture.CreateOrSetTreeAsync(treeId, "focus");

        var result = await _fixture.Query.GetEntryAsync(treeId, "focus");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CrdtShape, Is.EqualTo("OrSet"));
    }

    [Test]
    public async Task ScanEntriesAsync_lww_tree_reports_null_shape()
    {
        const string treeId = "lww-scan-shape";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 5);

        var page = await _fixture.Query.ScanEntriesAsync(new EntryScanRequest
        {
            TreeId = treeId,
            PageSize = 100,
        });

        Assert.That(page.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(page.Entries, Is.Not.Empty);
        Assert.That(page.Entries.Select(e => e.CrdtShape),
            Is.All.Null,
            "opaque last-writer-wins entries carry no CRDT shape tag");
    }

    [Test]
    public async Task GetEntryAsync_lww_tree_reports_null_shape()
    {
        const string treeId = "lww-get-shape";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 3);

        var result = await _fixture.Query.GetEntryAsync(treeId, "key-00001");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CrdtShape, Is.Null);
    }
}
