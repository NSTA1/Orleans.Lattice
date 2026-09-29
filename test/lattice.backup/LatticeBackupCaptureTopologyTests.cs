using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// End-to-end regression coverage for the structural topology a capture records.
/// The capture used to count the tree's shards and then digest shards
/// <c>0..count-1</c>, treating a list position as a physical shard index. Once
/// shard healing folds a shard away the physical indices are no longer
/// contiguous, so the capture addressed a shard the routing map no longer held,
/// the digest read refused it with <see cref="ArgumentOutOfRangeException"/>, and
/// every backup of the tree failed. It also recorded the default 4096 virtual
/// slots whatever slot count the tree's map held.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupCaptureTopologyTests
{
    private const string Tree = "folded";
    private const int VirtualShardCount = 64;

    private CaptureClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new CaptureClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task CaptureAsync_records_the_routing_map_topology_when_a_fold_left_a_gap_in_the_shard_indices()
    {
        await _fixture.InitializeAsync();

        // Physical shards {0, 2, 3} over 64 slots: the slots shard 1 owned route to
        // shard 0, which is the shape a shard-healing fold leaves behind. Pinned
        // before any write so every key lands on a shard the map names.
        var slots = new int[VirtualShardCount];
        for (var i = 0; i < slots.Length; i++)
        {
            var owner = i % 4;
            slots[i] = owner == 1 ? 0 : owner;
        }

        await _fixture.GrainFactory.GetLatticeRegistry().RegisterAsync(Tree, new TreeRegistryEntry
        {
            ShardCount = 4,
            ShardMap = new ShardMap { Slots = slots, Version = 1 },
        });

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 24; i++)
        {
            await tree.SetAsync($"k{i:D2}", Encoding.UTF8.GetBytes("v"));
        }

        var result = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest("folded", BackupScopeSelector.WholeTree(Tree)));

        var topology = result.Manifest.Topology;
        Assert.Multiple(() =>
        {
            Assert.That(topology.ShardCount, Is.EqualTo(3));
            Assert.That(topology.VirtualShardCount, Is.EqualTo(VirtualShardCount));
            Assert.That(topology.ShardRootDigests, Has.Count.EqualTo(3));
            Assert.That(topology.ShardRootDigests, Has.None.StartsWith("nodigest-"));
        });
    }
}
