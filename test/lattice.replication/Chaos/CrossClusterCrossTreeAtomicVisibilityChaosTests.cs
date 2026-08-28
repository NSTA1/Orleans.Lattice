using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Chaos test for <b>cross-tree</b> atomic visibility on the receiver side.
/// Two sites concurrently author cross-tree
/// <see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync(IGrainFactory, System.Collections.Generic.IReadOnlyList{LatticeTreeBatch}, string, System.Threading.CancellationToken)"/>
/// batches that span <em>two</em> distinct replicated trees, while the
/// inter-site delivery topology for each tree is partitioned and healed
/// mid-workload. Because each tree's terminal replicates on its own WAL feed,
/// a receiver can observe one tree's terminal long before the sibling tree's
/// terminal arrives. The receiver-side cross-tree barrier
/// (<see cref="Orleans.Lattice.BPlusTree.ILatticeCrossTreeReceiverGrain"/>) must hold <em>every</em>
/// participating tree invisible until all of the batch's replicated terminals
/// have arrived, then flip them together.
/// <para>
/// The post-drain predicate is the cross-tree all-or-nothing invariant: on
/// every receiver site, for every authored batch, tree A's keys and tree B's
/// keys must have <em>identical</em> presence - either both fully visible or
/// both fully absent. Tree A visible while tree B is absent (or vice versa) is
/// a cross-tree atomicity violation that the receiver barrier exists to
/// prevent. Two independent <see cref="ChaosDeliveryPump"/> instances (one per
/// tree) ship the two trees' feeds with independent partition cycles, so the
/// receiver routinely sees one tree's terminal before the other's.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public class CrossClusterCrossTreeAtomicVisibilityChaosTests
{
    private const string TreeA = "chaos-xt-a";
    private const string TreeB = "chaos-xt-b";
    private const int SiteCount = 2;
    private const int SagasPerSite = 5;
    private const int KeysPerSaga = 3;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

    [Test]
    public async Task Concurrent_cross_tree_batches_under_partition_remain_atomically_visible_across_both_trees()
    {
        await using var runner = new TestRunner();
        await runner.InitializeAsync();
        var fixture = runner.Fixture;
        var pumpA = runner.PumpA;
        var pumpB = runner.PumpB;

        // Per (site, saga) deterministic key namespaces, one set per tree, so
        // the post-drain assertion can recover batch membership from keys.
        var keysA = new string[SiteCount, SagasPerSite][];
        var keysB = new string[SiteCount, SagasPerSite][];
        for (var site = 0; site < SiteCount; site++)
        {
            for (var n = 0; n < SagasPerSite; n++)
            {
                var a = new string[KeysPerSaga];
                var b = new string[KeysPerSaga];
                for (var k = 0; k < KeysPerSaga; k++)
                {
                    a[k] = $"s{site}-xt{n:D2}-a{k}";
                    b[k] = $"s{site}-xt{n:D2}-b{k}";
                }
                keysA[site, n] = a;
                keysB[site, n] = b;
            }
        }

        var workloadTasks = new Task[SiteCount];
        for (var site = 0; site < SiteCount; site++)
        {
            var siteIdx = site;
            var factory = fixture.ClientOf(siteIdx);
            workloadTasks[siteIdx] = Task.Run(async () =>
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var entriesA = new List<KeyValuePair<string, byte[]>>(KeysPerSaga);
                    var entriesB = new List<KeyValuePair<string, byte[]>>(KeysPerSaga);
                    for (var k = 0; k < KeysPerSaga; k++)
                    {
                        entriesA.Add(new KeyValuePair<string, byte[]>(
                            keysA[siteIdx, n][k], Encoding.UTF8.GetBytes($"a-{siteIdx}-{n}-{k}")));
                        entriesB.Add(new KeyValuePair<string, byte[]>(
                            keysB[siteIdx, n][k], Encoding.UTF8.GetBytes($"b-{siteIdx}-{n}-{k}")));
                    }

                    var batches = new List<LatticeTreeBatch>
                    {
                        new(TreeA, entriesA),
                        new(TreeB, entriesB),
                    };
                    var opId = $"op-s{siteIdx}-{n:D2}";
                    await factory.SetManyAtomicAsync(batches, opId);

                    // Mid-workload partition cycle driven from site 0. Each
                    // tree's pump is cycled independently so the receiver
                    // routinely observes one tree's terminal before the other.
                    if (siteIdx == 0 && n == SagasPerSite / 3)
                    {
                        pumpA.IsolateSite(1);
                    }
                    if (siteIdx == 0 && n == SagasPerSite / 2)
                    {
                        pumpB.IsolateSite(0);
                    }
                    if (siteIdx == 0 && n == (2 * SagasPerSite) / 3)
                    {
                        pumpA.HealSite(1);
                        pumpB.HealSite(0);
                    }
                }
            });
        }

        await Task.WhenAll(workloadTasks);
        await pumpA.HealAllAndDrainAsync(DrainTimeout);
        await pumpB.HealAllAndDrainAsync(DrainTimeout);

        // Cross-tree all-or-nothing invariant: on every receiver site, every
        // batch's tree-A keys and tree-B keys must have identical presence.
        for (var receiver = 0; receiver < SiteCount; receiver++)
        {
            var latticeA = fixture.ClientOf(receiver).GetGrain<ILattice>(TreeA);
            var latticeB = fixture.ClientOf(receiver).GetGrain<ILattice>(TreeB);
            for (var author = 0; author < SiteCount; author++)
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    var presentA = await CountPresentAsync(latticeA, keysA[author, n]);
                    var presentB = await CountPresentAsync(latticeB, keysB[author, n]);

                    var aWhole = presentA is 0 or KeysPerSaga;
                    var bWhole = presentB is 0 or KeysPerSaga;
                    Assert.That(aWhole && bWhole, Is.True,
                        $"Site {receiver} observed a PARTIAL single-tree view for author={author} saga={n}: A={presentA}/{KeysPerSaga}, B={presentB}/{KeysPerSaga}.");

                    var aVisible = presentA == KeysPerSaga;
                    var bVisible = presentB == KeysPerSaga;
                    Assert.That(aVisible, Is.EqualTo(bVisible),
                        $"Site {receiver} observed a CROSS-TREE partial commit for author={author} saga={n}: A visible={aVisible}, B visible={bVisible}. The receiver barrier must flip both trees together.");
                }
            }
        }

        // After a full drain every batch is a commit, so every site must
        // observe every batch's keys on both trees.
        for (var receiver = 0; receiver < SiteCount; receiver++)
        {
            var latticeA = fixture.ClientOf(receiver).GetGrain<ILattice>(TreeA);
            var latticeB = fixture.ClientOf(receiver).GetGrain<ILattice>(TreeB);
            for (var author = 0; author < SiteCount; author++)
            {
                for (var n = 0; n < SagasPerSite; n++)
                {
                    Assert.That(await CountPresentAsync(latticeA, keysA[author, n]), Is.EqualTo(KeysPerSaga),
                        $"Site {receiver} missing tree-A keys for author={author} saga={n} after drain.");
                    Assert.That(await CountPresentAsync(latticeB, keysB[author, n]), Is.EqualTo(KeysPerSaga),
                        $"Site {receiver} missing tree-B keys for author={author} saga={n} after drain.");
                }
            }
        }
    }

    private static async Task<int> CountPresentAsync(ILattice lattice, string[] keys)
    {
        var present = 0;
        foreach (var key in keys)
        {
            if (await lattice.GetAsync(key) is not null)
            {
                present++;
            }
        }
        return present;
    }

    private sealed class TestRunner : IAsyncDisposable
    {
        public MultiSiteClusterFixture Fixture { get; } = new(
            LatticeMergeMode.LwwRegister,
            SiteCount,
            configureClient: static o => o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                [TreeA] = LatticeMergeMode.LwwRegister,
                [TreeB] = LatticeMergeMode.LwwRegister,
            });

        public ChaosDeliveryPump PumpA { get; private set; } = null!;
        public ChaosDeliveryPump PumpB { get; private set; } = null!;

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            PumpA = new ChaosDeliveryPump(Fixture, TreeA);
            PumpB = new ChaosDeliveryPump(Fixture, TreeB);
            PumpA.Start();
            PumpB.Start();
        }

        public async ValueTask DisposeAsync()
        {
            if (PumpA is not null)
            {
                await PumpA.DisposeAsync();
            }
            if (PumpB is not null)
            {
                await PumpB.DisposeAsync();
            }
            await Fixture.DisposeAsync();
        }
    }
}
