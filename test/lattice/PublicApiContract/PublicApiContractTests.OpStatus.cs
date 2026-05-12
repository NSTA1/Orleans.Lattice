namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // The Is*CompleteAsync surface returns `true` when the named operation
    // is not currently in progress for the tree - either it has never been
    // initiated, or the most recent invocation has completed. Each method
    // is independent - completion of a resize does not affect the
    // merge / snapshot / reshard "done" answer, and so on.
    //
    // The "in progress" state of these flags is exercised by the dedicated
    // Resize / Merge / Snapshot partials (which poll on the same flags
    // until the coordinator marks itself complete). This file pins the
    // "no operation initiated" answer and the "operation completed,
    // returns true again" answer.

    [Test]
    public async Task IsResizeCompleteAsync_on_pristine_tree_returns_true()
    {
        var treeId = "pac-opstatus-resize-pristine-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.IsResizeCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsReshardCompleteAsync_on_pristine_tree_returns_true()
    {
        var treeId = "pac-opstatus-reshard-pristine-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.IsReshardCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsMergeCompleteAsync_on_pristine_tree_returns_true()
    {
        var treeId = "pac-opstatus-merge-pristine-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.IsMergeCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsSnapshotCompleteAsync_on_pristine_tree_returns_true()
    {
        var treeId = "pac-opstatus-snap-pristine-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.IsSnapshotCompleteAsync(), Is.True);
    }

    [Test]
    public async Task Is_complete_flags_remain_true_after_their_operation_terminates()
    {
        var sourceId = "pac-opstatus-after-merge-src-" + Guid.NewGuid().ToString("N")[..8];
        var destId = "pac-opstatus-after-merge-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(destId, shardCount: 1);
        await src.SetAsync("k", Bytes("v"));

        await dst.MergeAsync(sourceId);
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        // Calling Is*CompleteAsync repeatedly after the operation has
        // terminated must consistently return true.
        Assert.That(await dst.IsMergeCompleteAsync(), Is.True);
        Assert.That(await dst.IsMergeCompleteAsync(), Is.True);
        Assert.That(await dst.IsMergeCompleteAsync(), Is.True);
    }
}
