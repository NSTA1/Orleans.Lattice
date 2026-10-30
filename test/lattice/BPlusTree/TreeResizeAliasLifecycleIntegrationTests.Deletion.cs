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
    public async Task Explicit_alias_while_an_alias_operation_holds_the_reservation_is_refused_naming_it()
    {
        // Issue #5289: an explicit alias change ignored the alias reservation a
        // resize, restore, revert or schema remediation holds, which delete honours.
        var name = $"alias-during-reservation-{Guid.NewGuid():N}";
        var other = $"alias-during-reservation-target-{Guid.NewGuid():N}";
        await _cluster.GrainFactory.GetGrain<ILattice>(name).SetAsync("key", [1]);
        await _cluster.GrainFactory.GetGrain<ILattice>(other).SetAsync("key", [2]);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var lineage = (await registry.GetEntryAsync(name))?.Lineage;
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(name);
        await deletion.BeginAliasChangeAsync("resize:held");

        var carried = Assert.ThrowsAsync<InvalidOperationException>(() =>
            AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(_cluster.GrainFactory, name, other));
        var direct = Assert.ThrowsAsync<InvalidOperationException>(() => registry.SetAliasAsync(name, other));

        Assert.Multiple(async () =>
        {
            Assert.That(carried!.Message, Does.Contain(name).And.Contain("resize:held"), carried.Message);
            Assert.That(direct!.Message, Does.Contain(name).And.Contain("resize:held"), direct.Message);
            Assert.That(await registry.ResolveAsync(name), Is.EqualTo(name), "a refused alias moves nothing");
            Assert.That((await registry.GetEntryAsync(name))?.Lineage, Is.EqualTo(lineage),
                "the refusal precedes the lineage re-stamp");
        });

        await deletion.EndAliasChangeAsync("resize:held");
        await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(_cluster.GrainFactory, name, other);
        Assert.That(await registry.ResolveAsync(name), Is.EqualTo(other), "the alias is accepted once the reservation is released");
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
