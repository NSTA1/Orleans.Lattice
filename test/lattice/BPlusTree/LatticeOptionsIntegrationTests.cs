using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(SmallLeafClusterCollection.Name)]
public class LatticeOptionsIntegrationTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Per_tree_options_override_triggers_earlier_split()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(SmallLeafClusterFixture.TreeName);

        // Insert more keys than the small MaxLeafKeys limit so a split must occur.
        var keyCount = SmallLeafClusterFixture.SmallMaxLeafKeys * 2;
        for (int i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"value-{i}"));
        }

        // All keys should still be readable after the split.
        for (int i = 0; i < keyCount; i++)
        {
            var result = await tree.GetAsync($"key-{i:D4}");
            Assert.NotNull(result);
            Assert.Equal($"value-{i}", Encoding.UTF8.GetString(result));
        }
    }

    [Fact]
    public async Task Default_tree_uses_global_options()
    {
        // A tree with no per-name override should still work at default capacity.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("default-options-tree");

        // Insert a handful of keys — well under the default 128 limit, no split expected.
        for (int i = 0; i < 10; i++)
        {
            await tree.SetAsync($"d-{i}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        for (int i = 0; i < 10; i++)
        {
            var result = await tree.GetAsync($"d-{i}");
            Assert.NotNull(result);
            Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }
}
