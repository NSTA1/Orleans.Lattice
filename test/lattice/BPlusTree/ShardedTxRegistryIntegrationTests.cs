using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

// Integration coverage for the sharded saga decision registry (issue #3501). One
// registry shard admits ShardedTxRegistryClusterFixture.TombstonesPerShard
// retained decisions, and retention is long enough that none ages out, so a
// single registry would refuse the tree's next saga once that many had
// completed. With the registry sharded, the same tree completes twice that many
// sagas, run concurrently, without a single TxRegistryCapacity refusal.
[TestFixture]
[Category("Integration")]
public class ShardedTxRegistryIntegrationTests
{
    private const int SagaCount = ShardedTxRegistryClusterFixture.TombstonesPerShard * 2;
    private const int Concurrency = 32;

    private ShardedTxRegistryClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ShardedTxRegistryClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task One_registry_refuses_before_admitting_the_saga_count_the_sharded_tree_sustains()
    {
        // Control: drive saga lifecycles straight at one unsharded registry
        // (the legacy bare-tree key) under the same budget and retention. It
        // must refuse before reaching SagaCount, which is what makes the
        // sharded run below a real proof rather than an under-budget run.
        var registry = _cluster.GrainFactory.GetGrain<ITxRegistryGrain>("sharded-registry-control");
        var admitted = 0;
        LatticeSaturatedException? refusal = null;
        for (var i = 0; i < SagaCount; i++)
        {
            try
            {
                await registry.EnsureSagaAdmissionAsync();
            }
            catch (LatticeSaturatedException ex)
            {
                refusal = ex;
                break;
            }

            var txid = Guid.NewGuid();
            await registry.MarkCommittedAsync(txid);
            await registry.ForgetAsync(txid);
            admitted++;
        }

        Assert.Multiple(() =>
        {
            Assert.That(refusal, Is.Not.Null, "One registry must refuse inside SagaCount under this budget.");
            Assert.That(refusal?.SaturationSource, Is.EqualTo(LatticeSaturationSource.TxRegistryCapacity));
            Assert.That(admitted, Is.EqualTo(ShardedTxRegistryClusterFixture.TombstonesPerShard));
        });
    }

    [Test]
    public async Task A_sharded_tree_sustains_more_concurrent_sagas_than_one_shard_admits_without_refusal()
    {
        const string treeId = "sharded-registry-tree";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);

        var refusals = 0;
        for (var wave = 0; wave < SagaCount / Concurrency; wave++)
        {
            var sagas = new Task[Concurrency];
            for (var i = 0; i < Concurrency; i++)
            {
                var n = (wave * Concurrency) + i;
                sagas[i] = tree.SetManyAtomicAsync(
                [
                    new KeyValuePair<string, byte[]>($"k{n:D4}-a", [(byte)n]),
                    new KeyValuePair<string, byte[]>($"k{n:D4}-b", [(byte)n]),
                ]);
            }

            try
            {
                await Task.WhenAll(sagas);
            }
            catch
            {
                // Inspect every task below; WhenAll surfaces only the first fault.
            }

            foreach (var saga in sagas)
            {
                if (saga.Exception?.InnerException is LatticeSaturatedException)
                {
                    refusals++;
                }
                else if (saga.Exception is { } fault)
                {
                    Assert.Fail($"Saga faulted with a non-saturation error: {fault.InnerException}");
                }
            }
        }

        var perShard = new List<int>();
        foreach (var key in TxRegistryRouting.EnumerateKeys(treeId, ShardedTxRegistryClusterFixture.ShardCount))
        {
            perShard.Add((await _cluster.GrainFactory.GetGrain<ITxRegistryGrain>(key).SnapshotAsync()).Count);
        }

        Assert.Multiple(() =>
        {
            Assert.That(refusals, Is.Zero, "No saga may be refused by TxRegistryCapacity on a sharded tree.");
            Assert.That(perShard.Count(c => c > 0), Is.GreaterThan(1), "Sagas must spread across registry shards.");
            Assert.That(perShard.Max(), Is.LessThanOrEqualTo(ShardedTxRegistryClusterFixture.TombstonesPerShard),
                "No one shard may hold more decisions than its budget admits.");
            Assert.That(perShard[^1], Is.Zero, "A sharded tree records no decision on the legacy registry.");
        });

        var sample = await tree.GetAsync($"k{SagaCount - 1:D4}-b");
        Assert.That(sample, Is.EqualTo(new[] { (byte)(SagaCount - 1) }));
    }
}
