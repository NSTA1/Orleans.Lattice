using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // Regression: a WAL shard can activate after a tree has been purged,
    // because the purge's own writes keep the WAL alive past the registry
    // unregister. That activation previously called the full
    // LatticeOptionsResolver.ResolveAsync, whose lazy first-use seeding
    // re-created the registry row and resurrected the purged tree.
    [Test]
    public async Task PurgeTreeAsync_leaves_the_registry_row_absent()
    {
        var treeId = "pac-tree-purge-no-resurrect-" + Guid.NewGuid().ToString("N")[..8];
        var tree = Tree(treeId);
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();

        var registry = Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeId), Is.False,
                "the purged tree's registry row must not be re-created");
            Assert.That(await registry.GetEntryAsync(treeId), Is.Null,
                "the purged tree must have no registry entry");
            Assert.That(await tree.TreeExistsAsync(), Is.False,
                "TreeExistsAsync must report a purged tree as absent");
        });
    }

    [Test]
    public async Task PurgeTreeAsync_keeps_the_tree_absent_after_wal_shards_activate()
    {
        var treeId = "pac-tree-purge-settled-" + Guid.NewGuid().ToString("N")[..8];
        var tree = Tree(treeId);
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();

        // Give any deferred WAL-shard activation time to land; a resurrecting
        // activation re-registers the tree asynchronously.
        await Task.Delay(TimeSpan.FromSeconds(3));

        Assert.That(await tree.TreeExistsAsync(), Is.False);
    }
}
