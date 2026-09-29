using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for issue #3940: after a tree is deleted and purged, the
/// next write under its id registers a new, live tree, but the purged tree's
/// deletion record used to go on describing it - reporting it deleted and
/// purged, so a resize was refused ("recover it first") while recovery was
/// refused too ("already purged"), wedging the id for good.
/// </summary>
public partial class TreeResizeAliasLifecycleIntegrationTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task A_tree_created_again_under_a_purged_id_can_be_resized(bool aliasedBeforePurge)
    {
        var name = $"purged-reuse-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(name);
        await tree.SetAsync("old", [1]);
        if (aliasedBeforePurge) await ResizeAsync(name);
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();
        Assert.That(await tree.TreeExistsAsync(), Is.False);

        // The next write registers a new tree under the same id.
        await tree.SetAsync("new", [2]);
        Assert.That(await tree.TreeExistsAsync(), Is.True);

        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(name);
        var status = await deletion.GetDeletionStatusAsync();
        Assert.That(status.IsDeleted, Is.False, "tree_exists and tree_deletion_status disagree");
        Assert.That(status.PurgeComplete, Is.False);
        Assert.That(await deletion.IsDeletedAsync(), Is.False);

        await ResizeAsync(name);

        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        Assert.That(await registry.ResolveAsync(name), Is.Not.EqualTo(name), "the resize did not alias the tree");
        Assert.That((await registry.GetEntryAsync(name))?.MaxLeafKeys, Is.EqualTo(64));
        Assert.That(await tree.GetAsync("new"), Is.EqualTo(new byte[] { 2 }));
        Assert.That(await tree.GetAsync("old"), Is.Null, "a purged tree's data must not come back");
    }

    [Test]
    public async Task A_tree_created_again_under_a_purged_id_can_be_deleted_and_recovered()
    {
        var name = $"purged-relife-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(name);
        await tree.SetAsync("old", [1]);
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();
        await tree.SetAsync("new", [2]);

        // Used to be a silent no-op that left the new tree serving traffic.
        await tree.DeleteTreeAsync();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await tree.GetAsync("new"));
        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(name).GetDeletionStatusAsync();
        Assert.That(status.CanRecover, Is.True);

        await tree.RecoverTreeAsync();
        Assert.That(await tree.GetAsync("new"), Is.EqualTo(new byte[] { 2 }));
    }
}
