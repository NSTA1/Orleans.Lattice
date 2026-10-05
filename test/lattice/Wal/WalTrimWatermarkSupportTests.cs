using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Issue #4621: a WAL reader trusts the trim watermark only when every silo in the
/// cluster manifest hosts the capability marker. Runs a two-silo cluster of the
/// current build, so every silo maintains the watermark and the gate opens.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class WalTrimWatermarkSupportTests
{
    private const string Tree = "trim-watermark-support";

    private static readonly InMemoryWalStorageProvider SharedWal = new();

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public void Every_silo_of_the_current_build_maintains_the_trim_watermark()
    {
        foreach (var silo in _cluster.GetActiveSilos().Cast<InProcessSiloHandle>())
        {
            Assert.That(WalTrimWatermarkSupport.AllSilosMaintain(silo.SiloHost.Services), Is.True,
                $"silo {silo.SiloAddress} sees a silo without the trim watermark capability");
        }
    }

    [Test]
    public void A_host_without_runtime_services_has_no_other_silo_and_maintains_it()
        => Assert.That(WalTrimWatermarkSupport.AllSilosMaintain(null), Is.True);

    [Test]
    public async Task A_shard_grain_reports_its_providers_trim_watermark_once_every_silo_maintains_it()
    {
        await _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            Tree,
            new TreeRegistryEntry { ShardCount = 1, WalPartitions = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(Tree);
        await tree.SetAsync("a", [1]);
        await tree.SetAsync("b", [2]);
        var shard = _cluster.Client.GetGrain<IWalShardGrain>($"{Tree}/0");
        Assert.That(await shard.GetTrimWatermarkAsync(CancellationToken.None), Is.EqualTo(-1L), "nothing has been trimmed");

        await SharedWal.TrimAsync(Tree, 0, 0, CancellationToken.None);

        Assert.That(await shard.GetTrimWatermarkAsync(CancellationToken.None), Is.EqualTo(0L));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddWalStorage(_ => SharedWal);
        }
    }
}
