using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed partial class MutationObserverIntegrationTests
{
    [Test]
    public async Task Cold_alias_routes_replace_inherited_identity_and_share_no_leaf_identity()
    {
        var physical = $"observer-physical-{Guid.NewGuid():N}";
        var direct = await _fixture.CreateTreeAsync(physical);
        var first = $"{physical}-first";
        var second = $"{physical}-second";
        var registry = _fixture.Cluster.Client.GetLatticeRegistry();
        await registry.SetAliasAsync(first, physical);
        await registry.SetAliasAsync(second, physical);
        MutationObserverClusterFixture.Drain();
        var contextKey = LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey;
        var previous = RequestContext.Get(contextKey);
        try
        {
            RequestContext.Set(contextKey, "inherited-unrelated-tree");
            await Task.WhenAll(
                _fixture.Cluster.Client.GetGrain<ILattice>(first).SetAsync("first", [1]),
                _fixture.Cluster.Client.GetGrain<ILattice>(second).SetAsync("second", [2]));
            await direct.SetAsync("direct", [3]);
        }
        finally
        {
            if (previous is null) RequestContext.Remove(contextKey);
            else RequestContext.Set(contextKey, previous);
        }

        var mutations = MutationObserverClusterFixture.Drain().ToDictionary(m => m.Key);
        Assert.That(mutations["first"].TreeId, Is.EqualTo(first));
        Assert.That(mutations["second"].TreeId, Is.EqualTo(second));
        Assert.That(mutations["direct"].TreeId, Is.EqualTo(physical));
    }

    [Test]
    public async Task Aliased_bulk_load_and_terminal_records_preserve_observer_coverage()
    {
        var physical = $"observer-bulk-{Guid.NewGuid():N}";
        await _fixture.CreateTreeAsync(physical);
        var logical = $"{physical}-logical";
        await _fixture.Cluster.Client.GetLatticeRegistry().SetAliasAsync(logical, physical);
        var tree = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        MutationObserverClusterFixture.Drain();
        await tree.BulkLoadAsync([new("seed", [1])]);
        await tree.BulkAppendChunkAsync("observer-chunk", [new("tail", [2])]);
        Assert.That(MutationObserverClusterFixture.Drain(), Is.Empty);
        Assert.That(await tree.GetAsync("seed"), Is.EqualTo(new byte[] { 1 }));
        Assert.That(await tree.GetAsync("tail"), Is.EqualTo(new byte[] { 2 }));

        var shard = _fixture.Cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0");
        var terminal = await shard.AppendTxTerminalAsync(Guid.NewGuid(), committed: true, inlineWalAppend: false);
        Assert.That(terminal, Is.Not.Null);
        Assert.That(terminal!.Value.TreeId, Is.EqualTo(physical));
        Assert.That(terminal.Value.Op, Is.EqualTo(MutationKind.TxCommit));
        Assert.That(MutationObserverClusterFixture.Drain(), Is.Empty);
    }

    [TestCase("set")]
    [TestCase("delete")]
    [TestCase("range")]
    [TestCase("crdt")]
    [TestCase("many")]
    [TestCase("atomic")]
    [TestCase("replication-set")]
    [TestCase("replication-many")]
    [TestCase("replication-range")]
    public async Task Write_after_resize_publishes_logical_tree_id(string path)
    {
        var treeId = $"observer-resize-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("before", [1]);
        var resize = _fixture.Cluster.Client.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        var registry = _fixture.Cluster.Client.GetLatticeRegistry();
        Assert.That(await registry.ResolveAsync(treeId), Is.Not.EqualTo(treeId));

        MutationObserverClusterFixture.Drain();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);
        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        switch (path)
        {
            case "set":
                await tree.SetAsync("after", [2]);
                break;
            case "delete":
                await tree.DeleteAsync("before");
                break;
            case "range":
                await tree.DeleteRangeAsync("a", "z");
                break;
            case "crdt":
                await tree.PnCounter("after").IncrementAsync("local");
                break;
            case "many":
                await tree.SetManyAsync([new("after", [2]), new("after2", [3])]);
                break;
            case "atomic":
                await tree.SetManyAtomicAsync([new("after", [2]), new("after2", [3])]);
                break;
            case "replication-set":
                await apply.ApplySetAsync("after", [2], hlc, "peer", null, 0);
                break;
            case "replication-many":
                await apply.ApplyMergeManyAsync([
                    new ApplyMergeItem { Key = "after", Value = [2], SourceHlc = hlc, OriginClusterId = "peer" },
                    new ApplyMergeItem { Key = "after2", Value = [3], SourceHlc = hlc, OriginClusterId = "peer" }]);
                break;
            case "replication-range":
                await apply.ApplyDeleteRangeAsync("a", "z", hlc, "peer", null);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(path));
        }

        var mutations = MutationObserverClusterFixture.Drain();
        Assert.That(mutations, Is.Not.Empty);
        Assert.That(mutations.Select(m => m.TreeId), Is.All.EqualTo(treeId));
    }
}
