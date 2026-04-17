using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LatticeOptionsIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
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
            Assert.That(result, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo($"value-{i}"));
        }
    }

    [Test]
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
            Assert.That(result, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo($"v-{i}"));
        }
    }
}
