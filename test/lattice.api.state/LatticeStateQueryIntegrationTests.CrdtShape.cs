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

    [Test]
    public async Task GetEntryAsync_orset_tree_excludes_removed_element_from_current_members()
    {
        const string treeId = "orset-get-removed-excluded";
        await _fixture.CreateOrSetTreeWithRemovalAsync(treeId, "focus", liveElement: "keep", removedElement: "drop");

        var result = await _fixture.Query.GetEntryAsync(treeId, "focus");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);

        // The removed element's add dot survives under a tombstone in the folded
        // OR-Set, but the current-state projection must surface only live members.
        var members = result.Entry!.CurrentMembers;
        var elements = members.Select(m => System.Text.Encoding.UTF8.GetString(m.Element)).ToArray();
        Assert.That(elements, Is.EqualTo(new[] { "keep" }),
            "a fully-removed OR-Set element must not appear in the current folded state");
    }

    [Test]
    public async Task GetEntryAsync_orset_tree_decodes_current_members()
    {
        const string treeId = "orset-get-current-state";
        await _fixture.CreateOrSetTreeAsync(treeId, "focus");

        var result = await _fixture.Query.GetEntryAsync(treeId, "focus");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);

        // The current folded state surfaces the single live element with its
        // originating replica, decoded server-side rather than left as an opaque
        // serialized blob. No add/remove distinction: only present members.
        var members = result.Entry!.CurrentMembers;
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(System.Text.Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("member-of-focus"));
        Assert.That(members[0].ReplicaId, Is.EqualTo("replica-a"));
    }

    [Test]
    public async Task GetEntryAsync_pncounter_tree_decodes_net_value_member()
    {
        const string treeId = "pncounter-get-current-state";
        await _fixture.CreatePnCounterTreeAsync(treeId, "votes", increment: 5, decrement: 2);

        var result = await _fixture.Query.GetEntryAsync(treeId, "votes");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CrdtShape, Is.EqualTo("PnCounter"));

        // The folded PN-counter exposes a single current-state member carrying its
        // net value (increment 5 minus decrement 2 = 3), not per-replica
        // contributions. The net total is both the element text and the ordinal.
        var members = result.Entry.CurrentMembers;
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(System.Text.Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("3"));
        Assert.That(members[0].Ordinal, Is.EqualTo(3));
        Assert.That(members[0].ReplicaId, Is.Empty);
    }

    [Test]
    public async Task GetEntryAsync_lww_tree_has_no_current_members()
    {
        const string treeId = "lww-get-no-members";
        await _fixture.CreatePopulatedTreeAsync(treeId, keyCount: 3);

        var result = await _fixture.Query.GetEntryAsync(treeId, "key-00001");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CurrentMembers, Is.Empty,
            "an opaque last-writer-wins value decodes to no CRDT members");
    }
}
