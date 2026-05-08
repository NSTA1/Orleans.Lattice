namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── SnapshotAsync (Online) ──────────────────────────────────────────

    [Test]
    public async Task SnapshotAsync_online_copies_entries_to_destination()
    {
        var sourceId = "pac-snap-online-src-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-snap-online-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);

        for (var i = 0; i < 10; i++)
        {
            await src.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        await src.SnapshotAsync(destId, SnapshotMode.Online);
        await PollUntilAsync(async () => await src.IsSnapshotCompleteAsync(), TimeSpan.FromSeconds(30));

        var dst = Tree(destId);
        for (var i = 0; i < 10; i++)
        {
            Assert.That(Str(await dst.GetAsync($"k{i:D2}")), Is.EqualTo($"v{i}"));
        }
    }

    [Test]
    public async Task SnapshotAsync_online_keeps_source_writeable()
    {
        var sourceId = "pac-snap-online-srcwrite-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-snap-online-dstwrite-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        await src.SetAsync("k", Bytes("v"));

        await src.SnapshotAsync(destId, SnapshotMode.Online);
        await PollUntilAsync(async () => await src.IsSnapshotCompleteAsync(), TimeSpan.FromSeconds(30));

        // Source remains writable post-snapshot.
        await src.SetAsync("k2", Bytes("v2"));
        Assert.That(Str(await src.GetAsync("k2")), Is.EqualTo("v2"));
    }

    // ── SnapshotAsync (Offline) ─────────────────────────────────────────

    [Test]
    public async Task SnapshotAsync_offline_copies_entries_to_destination()
    {
        var sourceId = "pac-snap-offline-src-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-snap-offline-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);

        for (var i = 0; i < 5; i++)
        {
            await src.SetAsync($"k{i}", Bytes($"v{i}"));
        }

        await src.SnapshotAsync(destId, SnapshotMode.Offline);
        await PollUntilAsync(async () => await src.IsSnapshotCompleteAsync(), TimeSpan.FromSeconds(30));

        var dst = Tree(destId);
        for (var i = 0; i < 5; i++)
        {
            Assert.That(Str(await dst.GetAsync($"k{i}")), Is.EqualTo($"v{i}"));
        }
    }

    // ── Sizing override ────────────────────────────────────────────────

    [Test]
    public async Task SnapshotAsync_with_sizing_override_applies_to_destination()
    {
        var sourceId = "pac-snap-resize-src-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-snap-resize-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);

        for (var i = 0; i < 10; i++)
        {
            await src.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        await src.SnapshotAsync(destId, SnapshotMode.Online, maxLeafKeys: 16, maxInternalChildren: 8);
        await PollUntilAsync(async () => await src.IsSnapshotCompleteAsync(), TimeSpan.FromSeconds(30));

        var dst = Tree(destId);
        for (var i = 0; i < 10; i++)
        {
            Assert.That(Str(await dst.GetAsync($"k{i:D2}")), Is.EqualTo($"v{i}"));
        }
    }

    // ── Failure modes ──────────────────────────────────────────────────

    [Test]
    public async Task SnapshotAsync_to_existing_destination_throws()
    {
        var sourceId = "pac-snap-existing-src-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-snap-existing-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        await src.SetAsync("k", Bytes("v"));

        // Make the destination tree exist by writing to it directly.
        await Tree(destId).SetAsync("k", Bytes("preexisting"));

        Assert.That(
            async () => await src.SnapshotAsync(destId, SnapshotMode.Online),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ── IsSnapshotCompleteAsync ─────────────────────────────────────────

    [Test]
    public async Task IsSnapshotCompleteAsync_returns_true_when_no_snapshot_in_progress()
    {
        var treeId = "pac-snap-noop-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(await tree.IsSnapshotCompleteAsync(), Is.True);
    }
}
