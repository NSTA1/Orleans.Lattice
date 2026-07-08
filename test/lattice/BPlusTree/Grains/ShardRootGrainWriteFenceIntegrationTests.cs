using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Integration coverage for the durable write fence (issue #1173) over a real
/// cluster. Proves the acceptance property: while the target tree is fenced,
/// mutations are refused cluster-wide with the retryable
/// <see cref="LatticeWriteFencedException"/> while reads continue unaffected,
/// and once the fence lifts writes are admitted again.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardRootGrainWriteFenceIntegrationTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task EngageFenceOnAllShardsAsync(string treeId, string sagaId, long deadlineTicks)
    {
        // The tree spans DefaultShardCount shards; engage every one so any key's
        // write is fenced regardless of which shard it hashes to.
        for (var i = 0; i < LatticeConstants.DefaultShardCount; i++)
        {
            await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}")
                .EngageWriteFenceAsync(sagaId, deadlineTicks);
        }
    }

    private async Task LiftFenceOnAllShardsAsync(string treeId, string sagaId)
    {
        for (var i = 0; i < LatticeConstants.DefaultShardCount; i++)
        {
            await _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}")
                .LiftWriteFenceAsync(sagaId);
        }
    }

    [Test]
    public async Task Fenced_write_is_refused_but_read_still_succeeds()
    {
        const string treeId = "wf-int-refuse";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        // Seed a value before fencing so the read path has something to return.
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        var deadline = DateTime.UtcNow.AddMinutes(5).Ticks;
        await EngageFenceOnAllShardsAsync(treeId, "saga-refuse", deadline);

        // Write is refused with the retryable fence exception.
        Assert.That(
            () => tree.SetAsync("k2", Encoding.UTF8.GetBytes("v2")),
            Throws.InstanceOf<LatticeWriteFencedException>());

        // Read is unaffected: the previously-written value is still visible.
        var read = await tree.GetAsync("k1");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Write_succeeds_again_after_the_fence_is_lifted()
    {
        const string treeId = "wf-int-lift";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));

        var deadline = DateTime.UtcNow.AddMinutes(5).Ticks;
        await EngageFenceOnAllShardsAsync(treeId, "saga-lift", deadline);

        Assert.That(
            () => tree.SetAsync("blocked", Encoding.UTF8.GetBytes("x")),
            Throws.InstanceOf<LatticeWriteFencedException>());

        await LiftFenceOnAllShardsAsync(treeId, "saga-lift");

        // After the lift the same write is admitted and read back.
        await tree.SetAsync("after", Encoding.UTF8.GetBytes("ok"));
        var read = await tree.GetAsync("after");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("ok"));
    }

    [Test]
    public async Task Fence_self_lifts_once_the_deadline_passes()
    {
        const string treeId = "wf-int-selflift";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));

        // Engage with a deadline already in the past: the gate treats the fence
        // as lifted so a stranded coordinator never blocks writes forever.
        var pastDeadline = DateTime.UtcNow.AddSeconds(-1).Ticks;
        await EngageFenceOnAllShardsAsync(treeId, "saga-stranded", pastDeadline);

        // The write is admitted despite the durable fence flag being present.
        await tree.SetAsync("after-deadline", Encoding.UTF8.GetBytes("ok"));
        var read = await tree.GetAsync("after-deadline");
        Assert.That(read, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(read!), Is.EqualTo("ok"));
    }
}
