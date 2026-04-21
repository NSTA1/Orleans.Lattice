using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression: <c>DeleteRangeAsync</c> on a sparse multi-shard tree
/// must walk past empty leaves whose live keys are all less than
/// <c>startInclusive</c> rather than short-circuiting on the first
/// zero-delete leaf.
/// </summary>
[TestFixture]
public class MultiShardDeleteRangeRegressionTests
{
    private MultiPageFourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new MultiPageFourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task DeleteRange_walks_past_empty_leaves_on_multi_shard_tree()
    {
        var treeId = $"range-delete-sparse-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        for (int i = 0; i < 200; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));

        var deleted = await tree.DeleteRangeAsync("k-0050", "k-0060");

        Assert.That(deleted, Is.EqualTo(10), "should delete exactly 10 keys in the half-open range");

        for (int i = 50; i < 60; i++)
            Assert.That(await tree.GetAsync($"k-{i:D4}"), Is.Null, $"k-{i:D4} should be tombstoned");

        Assert.That(await tree.GetAsync("k-0049"), Is.Not.Null);
        Assert.That(await tree.GetAsync("k-0060"), Is.Not.Null);
    }

    [Test]
    public void RangeDeleteResult_reports_past_range_flag()
    {
        var result = new RangeDeleteResult { Deleted = 3, PastRange = true };
        Assert.That(result.Deleted, Is.EqualTo(3));
        Assert.That(result.PastRange, Is.True);
    }
}
