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
    public async Task GetEntryAsync_orset_tree_decodes_current_members()
    {
        const string treeId = "orset-get-current-state";
        await _fixture.CreateOrSetTreeAsync(treeId, "focus");

        var result = await _fixture.Query.GetEntryAsync(treeId, "focus");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);

        // The current folded state surfaces the single live element with its
        // originating replica, decoded server-side rather than left as an opaque
        // serialized blob.
        var members = result.Entry!.CurrentMembers;
        Assert.That(members, Has.Count.EqualTo(1));
        Assert.That(members[0].Kind, Is.EqualTo(CrdtMemberChangeKind.Added));
        Assert.That(System.Text.Encoding.UTF8.GetString(members[0].Element), Is.EqualTo("member-of-focus"));
        Assert.That(members[0].ReplicaId, Is.EqualTo("replica-a"));
    }

    [Test]
    public async Task GetEntryAsync_pncounter_tree_decodes_current_members()
    {
        const string treeId = "pncounter-get-current-state";
        await _fixture.CreatePnCounterTreeAsync(treeId, "votes", increment: 5, decrement: 2);

        var result = await _fixture.Query.GetEntryAsync(treeId, "votes");

        Assert.That(result.Status, Is.EqualTo(StateQueryStatus.Found));
        Assert.That(result.Entry, Is.Not.Null);
        Assert.That(result.Entry!.CrdtShape, Is.EqualTo("PnCounter"));

        // The folded PN-counter exposes one positive per-replica contribution and
        // one negative one, surfaced as Added / Removed members carrying the
        // contribution magnitude as the ordinal.
        var members = result.Entry.CurrentMembers;
        Assert.That(members, Has.Count.EqualTo(2));

        var added = members.Single(m => m.Kind == CrdtMemberChangeKind.Added);
        var removed = members.Single(m => m.Kind == CrdtMemberChangeKind.Removed);
        Assert.That(added.ReplicaId, Is.EqualTo("replica-a"));
        Assert.That(added.Ordinal, Is.EqualTo(5));
        Assert.That(removed.ReplicaId, Is.EqualTo("replica-b"));
        Assert.That(removed.Ordinal, Is.EqualTo(2));
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
