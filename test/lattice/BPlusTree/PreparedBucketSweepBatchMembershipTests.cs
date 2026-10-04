using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4499: a prepared bucket that a split's or a resize's retroactive
/// sweep copies onto another shard must keep its saga's atomic-batch
/// membership. Without it the copy lands in the destination's write-ahead log
/// with <c>AtomicBatchSize = 0</c>, so a replicating peer neither tallies it
/// nor exempts it from the causal-apply gate, and can apply the saga's terminal
/// ahead of it. Runs the real leaf, shard-root and WAL shard grains and the
/// real sweep replay.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PreparedBucketSweepBatchMembershipTests
{
    private const string Origin = "sweep-origin";

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Swept_prepare_reaches_the_destination_wal_with_its_atomic_batch_membership()
    {
        var source = $"sweep-src-{Guid.NewGuid():N}";
        var destination = $"sweep-dst-{Guid.NewGuid():N}";
        const string key = "k";
        var txid = Guid.NewGuid();
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(source).ApplyPreparedSetAsync(
            key, [7], new HybridLogicalClock { WallClockTicks = 5_000 }, Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 3, atomicBatchIndex: 1);

        var snapshot = (await PendingSnapshotsAsync(source)).Single(s => s.TransactionId == txid);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.AtomicBatchSize, Is.EqualTo(3), "the leaf must snapshot the prepare's batch size");
            Assert.That(snapshot.AtomicBatchIndex, Is.EqualTo(1), "the leaf must snapshot the prepare's batch index");
        });

        await _cluster.Client.GetGrain<ILattice>(destination).SetAsync("seed", [1]);
        var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        await PreparedBucketSweep.ReplayPreparedSnapshotAsync(
            _cluster.Client.GetGrain<IShardRootGrain>($"{destination}/{shard}"), snapshot, destination);

        var page = await _cluster.Client.GetGrain<IWalShardGrain>($"{destination}/0")
            .ReadAsync(0, 256, CancellationToken.None);
        var swept = page.Entries.Select(e => e.Entry).Single(e => e.TransactionId == txid && e.IsPrepared);

        Assert.Multiple(() =>
        {
            Assert.That(swept.AtomicBatchSize, Is.EqualTo(3), "the swept copy must carry the batch size");
            Assert.That(swept.AtomicBatchIndex, Is.EqualTo(1), "the swept copy must carry the batch index");
        });
    }

    [Test]
    public async Task Prepare_without_batch_membership_is_swept_without_one()
    {
        var source = $"sweep-plain-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(source).ApplyPreparedSetAsync(
            "k", [7], new HybridLogicalClock { WallClockTicks = 5_000 }, Origin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 0, atomicBatchIndex: 0);

        var snapshot = (await PendingSnapshotsAsync(source)).Single(s => s.TransactionId == txid);

        Assert.That(snapshot.AtomicBatchSize, Is.Zero);
    }

    private async Task<List<PendingMutationSnapshot>> PendingSnapshotsAsync(string tree)
    {
        var map = ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var result = new List<PendingMutationSnapshot>();
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{tree}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                result.AddRange(await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount));
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return result;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
