using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4473, on real grains: the prepared-bucket sweep an adaptive split and an
/// online resize snapshot share (<see cref="PreparedBucketSweep"/>) must settle a
/// prepared mutation whose saga decided but whose registry row is masked
/// (<see cref="TxStatus.Indeterminate"/>, retention elapsed, not yet pruned) by its
/// recorded decision. Treating the masked row as in flight replayed the prepare,
/// which the target refuses for a decided saga (#4445), so the target held only
/// an activation-scoped shadow marker and never the committed value.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PreparedBucketSweepIndeterminateIntegrationTests
{
    private static readonly TimeSpan Retention = TimeSpan.FromMilliseconds(200);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task A_sweep_settles_a_prepare_whose_committed_decision_the_registry_masks()
    {
        var grains = (IGrainFactory)_cluster.Client;
        var sourceTree = $"sweep-masked-src-{Guid.NewGuid():N}";
        var targetTree = $"sweep-masked-dst-{Guid.NewGuid():N}";
        var source = grains.GetGrain<IShardRootGrain>($"{sourceTree}/0");
        var target = grains.GetGrain<IShardRootGrain>($"{targetTree}/0");
        var txid = Guid.NewGuid();
        var committedValue = Encoding.UTF8.GetBytes("saga");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await source.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));
            await target.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));

            // The saga's prepare sits as a bucket on the source shard.
            var previous = LatticeTransactionContext.Current;
            LatticeTransactionContext.Set(txid);
            try
            {
                using var prepared = LatticePreparedContext.BeginScope();
                await source.SetAsync("k", committedValue);
            }
            finally
            {
                LatticeTransactionContext.Set(previous);
            }

            // The saga commits and is forgotten; once the retention window has
            // passed with no registry write to prune it, the row is masked.
            var registry = TxRegistryRouting.GetRegistry(grains, sourceTree, txid);
            await registry.MarkCommittedAsync(txid);
            await registry.ForgetAsync(txid);
            await Task.Delay(Retention + TimeSpan.FromMilliseconds(300));
            Assert.That(await registry.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate),
                "precondition: the registry masks the committed decision");
            Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.Committed),
                "precondition: the decision is still recorded behind the mask");

            var firstLeaf = await source.GetLeftmostLeafIdAsync();
            Assert.That(firstLeaf, Is.Not.Null);
            var slots = Enumerable.Range(0, LatticeConstants.DefaultVirtualShardCount).ToArray();
            var progress = new PreparedBucketSweepProgress();

            await PreparedBucketSweep.RunAsync(
                grains, sourceTree, firstLeaf!.Value, target, slots, LatticeConstants.DefaultVirtualShardCount, progress);

            Assert.Multiple(async () =>
            {
                Assert.That(progress.Replayed, Is.EqualTo(1), "precondition: the sweep found the prepared mutation");
                Assert.That(await target.GetAsync("k"), Is.EqualTo(committedValue),
                    "the target must hold the committed value, not a marker over nothing");
            });
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.TxDecisionRetention = Retention);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
