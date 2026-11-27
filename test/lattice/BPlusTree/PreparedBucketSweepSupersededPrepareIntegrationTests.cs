using System.Text;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #6206, on real grains: a committed saga whose terminal never reached a
/// leaf leaves its prepare buffered there, and a later plain write to the same
/// key supersedes it (the source's read gate already hides it). The
/// prepared-bucket sweep a split or resize runs must not resurrect that stale
/// prepare on the destination: backstopping it as a committed value restamped it
/// above the newer row, so the key read the superseded value until a later
/// write overwrote it - a transient torn atomic view.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PreparedBucketSweepSupersededPrepareIntegrationTests
{
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

    [TestCase(true)]
    [TestCase(false)]
    public async Task A_sweep_does_not_resurrect_a_committed_prepare_a_newer_row_superseded(bool committed)
    {
        var grains = (IGrainFactory)_cluster.Client;
        var sourceTree = $"sweep-superseded-src-{Guid.NewGuid():N}";
        var targetTree = $"sweep-superseded-dst-{Guid.NewGuid():N}";
        var source = grains.GetGrain<IShardRootGrain>($"{sourceTree}/0");
        var target = grains.GetGrain<IShardRootGrain>($"{targetTree}/0");
        var txid = Guid.NewGuid();
        var stale = Encoding.UTF8.GetBytes("round-7");
        var newer = Encoding.UTF8.GetBytes("round-42");

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await source.SetAsync("seed", Encoding.UTF8.GetBytes("seed"));

            var previous = LatticeTransactionContext.Current;
            LatticeTransactionContext.Set(txid);
            try
            {
                using var prepared = LatticePreparedContext.BeginScope();
                await source.SetAsync("k", stale);
            }
            finally
            {
                LatticeTransactionContext.Set(previous);
            }

            // The saga decides, but its terminal never reaches the source leaf
            // (the coordinator's broadcast was cut short by a silo restart).
            if (committed)
                await TxRegistryRouting.GetRegistry(grains, sourceTree, txid).MarkCommittedAsync(txid);

            // A later plain write supersedes the stranded prepare.
            await source.SetAsync("k", newer);
            Assert.That(await source.GetAsync("k"), Is.EqualTo(newer),
                "precondition: the source reads the newer row over the superseded prepare");

            // The destination holds the row the split's bulk copy carried over.
            await target.SetAsync("k", newer);

            var firstLeaf = await source.GetLeftmostLeafIdAsync();
            Assert.That(firstLeaf, Is.Not.Null);
            var slots = Enumerable.Range(0, LatticeConstants.DefaultVirtualShardCount).ToArray();
            var progress = new PreparedBucketSweepProgress();

            await PreparedBucketSweep.RunAsync(
                grains, sourceTree, firstLeaf!.Value, target, slots, LatticeConstants.DefaultVirtualShardCount, progress);

            if (!committed)
            {
                // The saga commits after the sweep; its terminal must not land
                // the superseded value over the newer row either.
                await TxRegistryRouting.GetRegistry(grains, sourceTree, txid).MarkCommittedAsync(txid);
                await target.AppendTxTerminalAsync(txid, committed: true);
            }

            Assert.That(await target.GetAsync("k"), Is.EqualTo(newer),
                "the destination must keep the newer row, not the superseded prepare");
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
