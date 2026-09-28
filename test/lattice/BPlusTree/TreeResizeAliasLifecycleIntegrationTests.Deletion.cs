using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

public partial class TreeResizeAliasLifecycleIntegrationTests
{
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    public async Task Logical_lifecycle_after_resize_targets_the_live_copy(bool purgeRetirement, bool secondResize)
    {
        var name = $"logical-delete-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(name);
        await tree.SetAsync("key", [42]);
        var events = new List<(string Tree, string Kind)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.TreeLifecycle,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? id = null, kind = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) id = tag.Value?.ToString();
                    if (tag.Key == LatticeMetrics.TagKind) kind = tag.Value?.ToString();
                }
                if (id is not null && id.StartsWith(name, StringComparison.Ordinal) && kind is not null)
                    lock (events) events.Add((id, kind));
            }));
        await ResizeAsync(name);
        Assert.That(await tree.GetAsync("key"), Is.EqualTo(new byte[] { 42 }));
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(name);
        Assert.ThrowsAsync<InvalidOperationException>(() => tree.RecoverTreeAsync());
        if (purgeRetirement) await deletion.PurgePhysicalAsync();
        if (secondResize) await ResizeAsync(name);
        Assert.That((await deletion.GetDeletionStatusAsync()).IsDeleted, Is.False);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var physical = await registry.ResolveAsync(name);

        await tree.DeleteTreeAsync();
        Assert.ThrowsAsync<InvalidOperationException>(async () => await tree.GetAsync("key"));
        Assert.ThrowsAsync<InvalidOperationException>(() => tree.SetAsync("other", [1]));
        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(name);
        Assert.ThrowsAsync<InvalidOperationException>(() => resize.ResizeAsync(32, 32));
        Assert.ThrowsAsync<InvalidOperationException>(() => resize.UndoResizeAsync());
        await tree.RecoverTreeAsync();
        Assert.That(await tree.GetAsync("key"), Is.EqualTo(new byte[] { 42 }));
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();

        Assert.That(await tree.TreeExistsAsync(), Is.False);
        Assert.That(await registry.ExistsAsync(physical), Is.False);
        Assert.That(await registry.ExistsAsync(name), Is.False);
        lock (events)
            Assert.That(events, Is.EqualTo(new[]
            {
                (name, "deleted"), (name, "recovered"), (name, "deleted"), (name, "purged"),
            }));
    }

    [Test]
    public async Task Admin_alias_does_not_grant_permission_to_delete_an_independent_tree()
    {
        var physical = $"independent-{Guid.NewGuid():N}";
        var logical = $"admin-alias-{Guid.NewGuid():N}";
        var target = _cluster.GrainFactory.GetGrain<ILattice>(physical);
        await target.SetAsync("key", [7]);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        await registry.SetAliasAsync(logical, physical);

        Assert.ThrowsAsync<InvalidOperationException>(() =>
            _cluster.GrainFactory.GetGrain<ILattice>(logical).DeleteTreeAsync());
        Assert.ThrowsAsync<InvalidOperationException>(() => target.DeleteTreeAsync());
        Assert.That(await target.GetAsync("key"), Is.EqualTo(new byte[] { 7 }));
        Assert.That(await registry.ExistsAsync(physical), Is.True);
    }

    [Test]
    public async Task Delete_during_resize_refuses_and_undo_releases_the_reservation()
    {
        var name = $"delete-during-resize-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(name);
        await tree.SetAsync("key", [1]);
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(name);
        await deletion.BeginAliasChangeAsync("resize");
        Assert.ThrowsAsync<InvalidOperationException>(() => tree.DeleteTreeAsync());
        await deletion.EndAliasChangeAsync("resize");
        await ResizeAsync(name);
        await _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(name).UndoResizeAsync();
        Assert.That(await tree.GetAsync("key"), Is.EqualTo(new byte[] { 1 }));
        await tree.DeleteTreeAsync();
        Assert.That(await deletion.IsDeletedAsync(), Is.True);
    }
}
