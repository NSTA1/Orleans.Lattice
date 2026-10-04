using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4508 control: on a host that does not run replication, nothing can
/// re-ship a prepare to a peer, so the transaction registry keeps purging an
/// aged-out decision on its retention alone, whatever the WAL still retains.
/// Runs the real registry against real WAL shards under the core default
/// replication context.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TxRegistryWalPurgeGuardSingleClusterTests
{
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
    public async Task Single_cluster_host_purges_an_aged_out_decision_on_retention_alone()
    {
        var tree = $"single-{Guid.NewGuid():N}";
        var txid = Guid.NewGuid();
        await _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/0").AppendAsync(
            new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = "k",
                Value = [1],
                Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
                TransactionId = txid,
                IsPrepared = true,
            },
            CancellationToken.None);
        var registry = TxRegistryRouting.GetRegistry(_cluster.Client, tree, txid);
        await registry.MarkCommittedAsync(txid);
        await registry.ForgetAsync(txid);

        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var other = Guid.NewGuid();
        await registry.MarkCommittedAsync(other);
        await registry.ForgetAsync(other);

        Assert.That(await registry.GetRecordedStatusAsync(txid), Is.EqualTo(TxStatus.InFlight),
            "without replication the WAL cannot re-ship the prepare, so retention alone governs the purge");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TxDecisionRetention = TimeSpan.FromMilliseconds(300);
                o.WalPartitions = 2;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
