using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4615: tombstone compaction reaped on the wall clock alone, so a write
/// older than a delete, delivered after the delete's tombstone was reaped, found
/// no tombstone and resurrected the key on that replica only. A replicated tree
/// now reaps a tombstone only below the replication frontier. Runs the real
/// compaction grain, leaves, receiver tree frontier and applier in a real
/// cluster that receives the tree from one origin.
/// </summary>
[TestFixture]
[Category("Integration")]
public class TombstoneReapGateIntegrationTests
{
    private const string LocalClusterId = "trg-site-b";
    private const string OriginClusterId = "trg-site-a";
    private const string Tree = "trg-tree";

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

    private IReplicationApplier Applier =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    private Task CompactAsync() =>
        _cluster.Client.GetGrain<ITombstoneCompactionGrain>(Tree).RunCompactionPassAsync();

    private Task ApplyLateWriteAsync(string key, HybridLogicalClock stamp) =>
        Applier.ApplyAsync(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = [7],
            Timestamp = stamp,
            OriginClusterId = OriginClusterId,
        });

    private static HybridLogicalClock Older(TimeSpan by) =>
        new() { WallClockTicks = (DateTime.UtcNow - by).Ticks, Counter = 0 };

    [Test]
    public async Task A_late_write_older_than_a_delete_does_not_resurrect_the_key_while_its_origin_is_not_covered()
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(Tree);
        const string key = "deleted-while-origin-pending";
        await lattice.SetAsync(key, [1]);

        // The origin has pushed the tree here but shipped no applied low
        // watermark, so a write of it older than the delete may still arrive.
        var frontier = _cluster.Client.GetGrain<IReplicationTreeFrontierGrain>(Tree);
        await frontier.ObserveAsync(OriginClusterId, shipped: null);

        await lattice.DeleteAsync(key);
        await Task.Delay(50);
        await CompactAsync();

        // The origin's write, authored before the delete, is delivered late.
        await ApplyLateWriteAsync(key, Older(TimeSpan.FromHours(1)));

        Assert.That(await lattice.GetAsync(key), Is.Null,
            "a write the tombstone beats, delivered after the compaction pass, must still find the tombstone");
    }

    [Test]
    public async Task A_tombstone_is_reaped_once_every_origin_covers_it()
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(Tree);
        const string key = "deleted-then-covered";
        await lattice.SetAsync(key, [1]);
        var frontier = _cluster.Client.GetGrain<IReplicationTreeFrontierGrain>(Tree);
        var epoch = await frontier.ObserveAsync(OriginClusterId, shipped: null);
        Assert.That(epoch, Is.Not.EqualTo(Guid.Empty), "precondition: the tree tracks a lineage, so its frontier is exact");

        await lattice.DeleteAsync(key);
        await Task.Delay(50);
        await CompactAsync();
        var keptBeforeCover = await ProbeReapedAsync(lattice, key + "-probe-a");

        // The origin vouches that every write of it below now is applied here.
        var covered = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        await frontier.ObserveAsync(OriginClusterId, new ReplicationSourceFrontier
        {
            ReceiverLineage = epoch,
            TreeLowWatermark = covered,
            OriginLowWatermark = covered,
            OriginGeneration = 1,
        });
        await CompactAsync();

        // Observed through the tombstone's absence: an older write now lands.
        // (The origin could not legitimately send one below its watermark.)
        await ApplyLateWriteAsync(key, Older(TimeSpan.FromHours(1)));

        Assert.Multiple(async () =>
        {
            Assert.That(keptBeforeCover, Is.True, "precondition: the tombstone was kept while the origin was not covered");
            Assert.That(await lattice.GetAsync(key), Is.EqualTo(new byte[] { 7 }),
                "once the origin's watermark passes the tombstone, compaction reaps it");
        });
    }

    // A sibling key deleted and compacted alongside the subject proves nothing on
    // its own; this reads the subject's tombstone through a dominated write.
    private async Task<bool> ProbeReapedAsync(ILattice lattice, string probeKey)
    {
        await lattice.SetAsync(probeKey, [1]);
        await lattice.DeleteAsync(probeKey);
        await Task.Delay(50);
        await CompactAsync();
        await ApplyLateWriteAsync(probeKey, Older(TimeSpan.FromHours(1)));
        return await lattice.GetAsync(probeKey) is null;
    }

    [Test]
    public async Task A_tree_not_replicated_here_reaps_on_the_grace_period_alone()
    {
        const string tree = "trg-not-replicated";
        var lattice = _cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", [1]);
        await lattice.DeleteAsync("k");
        await Task.Delay(50);
        await _cluster.Client.GetGrain<ITombstoneCompactionGrain>(tree).RunCompactionPassAsync();

        await Applier.ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "k",
            Value = [7],
            Timestamp = Older(TimeSpan.FromHours(1)),
            OriginClusterId = OriginClusterId,
        });

        Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 7 }),
            "an ungated tree reaps as before, so the older write lands where the tombstone was");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.TombstoneGracePeriod = TimeSpan.FromMilliseconds(1));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = LocalClusterId;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode> { [Tree] = LatticeMergeMode.LwwRegister };
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
