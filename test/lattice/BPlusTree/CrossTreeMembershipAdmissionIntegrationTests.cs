using System.Diagnostics;
using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for issue #5073: on a host without replication a
/// cross-tree sub-saga records no cross-tree membership in its tree's
/// transaction registry, so the registry's admission budget is spent on the
/// decision tombstones alone and a single registry shard sustains the
/// cross-tree saga rate it did before memberships were introduced (#4683).
/// Runs on the default <see cref="LatticeOptions"/>: one registry shard, the
/// default admission budget and the default decision retention.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrossTreeMembershipAdmissionIntegrationTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private IGrainFactory Grains => _fixture.Cluster.GrainFactory;

    private static List<KeyValuePair<string, byte[]>> Entry(string key) =>
        [new(key, Encoding.UTF8.GetBytes("v"))];

    [Test]
    public async Task Cross_tree_sub_saga_on_a_host_without_replication_records_no_membership()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var (t1, t2) = ($"ctm-a-{suffix}", $"ctm-b-{suffix}");

        var outcome = await Grains.SetManyAtomicAsync(
            [new LatticeTreeBatch(t1, Entry("k")), new LatticeTreeBatch(t2, Entry("k"))],
            operationId: $"ctm-op-{suffix}");

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        foreach (var tree in new[] { t1, t2 })
        {
            var decided = 0;
            var memberships = 0;
            foreach (var key in TxRegistryRouting.EnumerateKeys(tree, LatticeOptions.DefaultTxRegistryShardCount))
            {
                var registry = Grains.GetGrain<ITxRegistryGrain>(key);
                var decisions = await registry.SnapshotAsync();
                decided += decisions.Count;
                memberships += (await registry.GetCrossTreeMembershipsAsync([.. decisions.Keys])).Count;
            }

            Assert.That(decided, Is.GreaterThan(0), $"tree '{tree}' must still store its sub-saga's decision for the probe to mean anything");
            Assert.That(memberships, Is.Zero, $"tree '{tree}' recorded a cross-tree membership on a host without replication");
            Assert.That(await TxRegistryFanOut.CrossTreeSequenceFloorAsync(Grains, tree), Is.Null);
        }
    }

    [Test]
    public async Task One_registry_shard_admits_twenty_cross_tree_sagas_a_second_for_a_whole_retention_window()
    {
        // 20 sagas/s retained for the 60 s default decision retention is 1,200
        // retained cross-tree decisions per tree. Committing them faster than
        // the retention keeps every one of them in the row at once, which is
        // exactly the steady state of a sustained 20 sagas/s on one shard.
        const int sagas = 20 * 60;
        const int concurrency = 16;
        var options = new LatticeOptions();
        Assume.That(options.TxRegistryShardCount, Is.EqualTo(1));
        Assume.That(options.TxRegistryAdmissionBudgetBytes, Is.EqualTo(LatticeOptions.DefaultTxRegistryAdmissionBudgetBytes));

        var suffix = Guid.NewGuid().ToString("N");
        var (t1, t2) = ($"ctm-rate-a-{suffix}", $"ctm-rate-b-{suffix}");
        var next = -1;
        var refused = 0;
        var stopwatch = Stopwatch.StartNew();

        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async _ =>
        {
            int i;
            while ((i = Interlocked.Increment(ref next)) < sagas)
            {
                try
                {
                    var outcome = await Grains.SetManyAtomicAsync(
                        [new LatticeTreeBatch(t1, Entry($"k{i}")), new LatticeTreeBatch(t2, Entry($"k{i}"))],
                        operationId: $"ctm-rate-{suffix}-{i}");
                    Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
                }
                catch (LatticeSaturatedException ex) when (ex.SaturationSource == LatticeSaturationSource.TxRegistryCapacity)
                {
                    Interlocked.Increment(ref refused);
                }
            }
        }));

        stopwatch.Stop();
        TestContext.Out.WriteLine($"{sagas} cross-tree sagas in {stopwatch.Elapsed.TotalSeconds:F1} s; {refused} refused for TxRegistryCapacity.");
        Assert.That(stopwatch.Elapsed, Is.LessThan(options.TxDecisionRetention),
            "every saga's tombstone must still be retained at the end for the window to be a whole retention window");
        Assert.That(refused, Is.Zero);
    }
}
