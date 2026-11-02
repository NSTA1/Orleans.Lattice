using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Cluster-level regression for #5462 on a real two-silo <see cref="TestCluster"/>.
/// A runtime replication enable is a write to the config tree, and the core change
/// feed reports it only on the silo hosting the committing leaf, so before the
/// cross-silo config epoch the other silo kept the enrollment it compiled at
/// warm-up. In the Explorer journeys run that silo refused the peer's re-seed
/// export of the tree ("not enrolled"), leaving the peer's link waiting for a
/// re-seed that could never complete, and declined to coordinate a restore of the
/// tree, which it then replaced locally while the peer kept the discarded writes.
/// The test enables a tree through silo 0 after every silo has compiled its
/// snapshot, then asks every silo's own membership and merge-mode resolver - the
/// seams the restore dispatcher and the snapshot-export gate read - so whichever
/// silo hosts the leaf, the other is exercised as the stale one.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ReplicationConfigCrossSiloIntegrationTests
{
    private const string Tree = "factory-orders";

    // One epoch poll plus slack for publishing the advance and a loaded CI host.
    private static readonly TimeSpan Budget =
        ReplicationConfigEpochSynchronizer.DefaultPollInterval + TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;
    private IServiceProvider[] _silos = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(2);
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_cluster);
        _silos = _cluster.Silos.OfType<InProcessSiloHandle>().Select(s => s.SiloHost.Services).ToArray();
        Assert.That(_silos, Has.Length.EqualTo(2), "precondition: a two-silo cluster");
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
    public async Task A_runtime_enable_committed_through_one_silo_reaches_every_silo_within_one_poll()
    {
        // Every silo compiles its snapshot BEFORE the enable, so the silo that does
        // not host the committing leaf starts stale rather than cold.
        foreach (var silo in _silos)
        {
            await silo.GetRequiredService<CompiledReplicationConfigSnapshotMaintainer>().EnsureWarmAsync();
            Assert.That(
                silo.GetRequiredService<IReplicatedTreeMembership>().IsReplicated(Tree),
                Is.False,
                "precondition: every silo compiled the tree as not replicated");
        }

        using (LatticeSystemOrigin.Enter())
        {
            await _silos[0].GetRequiredService<ILatticeReplicationConfigAuthority>()
                .EnableReplicationAsync(Tree, LatticeMergeMode.LwwRegister);
        }

        for (var i = 0; i < _silos.Length; i++)
        {
            var membership = _silos[i].GetRequiredService<IReplicatedTreeMembership>();
            var context = _silos[i].GetRequiredService<ILatticeReplicationContext>();
            var elapsed = await ConvergeAsync(() =>
                membership.IsReplicated(Tree) && context.ResolveMergeMode(Tree) == LatticeMergeMode.LwwRegister);
            Assert.That(
                elapsed,
                Is.Not.Null,
                $"silo {i} must see a tree enabled through silo 0 as replicated within {Budget}: the restore dispatcher "
                + "and the snapshot-export gate both read this answer");
        }
    }

    [Test]
    public async Task Epoch_grain_advances_monotonically_within_one_incarnation()
    {
        var grain = _cluster.GrainFactory.GetGrain<IReplicationConfigEpochGrain>("xsilo-epoch-probe");

        var before = await grain.GetAsync();
        var advanced = await grain.AdvanceAsync();
        var after = await grain.GetAsync();

        Assert.That(advanced.Incarnation, Is.EqualTo(before.Incarnation));
        Assert.That(advanced.Version, Is.EqualTo(before.Version + 1));
        Assert.That(after, Is.EqualTo(advanced));
    }

    private static async Task<TimeSpan?> ConvergeAsync(Func<bool> converged)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            if (converged())
            {
                return clock.Elapsed;
            }

            if (clock.Elapsed > Budget)
            {
                return null;
            }

            await Task.Delay(50);
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(options => options.ClusterId = "west", enableRuntimeConfig: true);
        }
    }
}
