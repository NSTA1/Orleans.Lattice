using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5689. The receiver's exact-identity dedupe is scoped by path. A live
/// re-delivery of an identity is still deduped. A bootstrap drain dedupes within
/// the copy it writes into, so a re-driven import into a new copy applies every
/// row again, and trusts a live-cache hit only when its copy holds the row.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ReplicationApplierDrainDedupeScopeIntegrationTests
{
    private const string Origin = "dds-origin";

    private TestCluster _cluster = null!;
    private ReplicationApplier _applier = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _applier = _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services
            .GetRequiredService<ReplicationApplier>();
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

    private static WalRecord Row(string tree, string key, byte value) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [value],
        Timestamp = new HybridLogicalClock { WallClockTicks = 5_000, Counter = 0 },
        OriginClusterId = Origin,
    };

    private async Task<(string Tree, string CopyA, string CopyB)> TreeWithCopiesAsync(string name)
    {
        var tree = $"{name}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(tree, new TreeRegistryEntry());
        await registry.RegisterAsync(tree + "/resized/a", new TreeRegistryEntry());
        await registry.RegisterAsync(tree + "/resized/b", new TreeRegistryEntry());
        return (tree, tree + "/resized/a", tree + "/resized/b");
    }

    private async Task<ApplyResult> DrainAsync(string tree, string copy, WalRecord row)
    {
        using var drain = LatticeBootstrapApplyContext.BeginScope();
        using var route = LatticeBootstrapShadowRouteContext.BeginScope(tree, copy);
        return await _applier.ApplyAsync(row);
    }

    [Test]
    public async Task A_live_re_delivery_of_an_identity_is_deduped()
    {
        var (tree, _, _) = await TreeWithCopiesAsync("dds-live");
        var row = Row(tree, "k", 1);

        var first = await _applier.ApplyAsync(row);
        var second = await _applier.ApplyAsync(row);

        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.True);
            Assert.That(second.Applied, Is.False, "a live re-delivery of an applied identity is deduped");
            Assert.That(second.Deferred, Is.False);
        });
    }

    [Test]
    public async Task A_drain_re_delivery_into_the_same_copy_is_deduped()
    {
        var (tree, copyA, _) = await TreeWithCopiesAsync("dds-same");
        var row = Row(tree, "k", 1);

        var first = await DrainAsync(tree, copyA, row);
        var second = await DrainAsync(tree, copyA, row);

        Assert.Multiple(() =>
        {
            Assert.That(first.Applied, Is.True);
            Assert.That(second.Applied, Is.False, "a transient re-drain into the same copy dedupes");
        });
    }

    [Test]
    public async Task A_drain_into_a_new_copy_applies_a_row_an_aborted_copy_already_applied()
    {
        var (tree, copyA, copyB) = await TreeWithCopiesAsync("dds-redrive");
        var row = Row(tree, "k", 7);

        await DrainAsync(tree, copyA, row);
        var redriven = await DrainAsync(tree, copyB, row);

        Assert.Multiple(async () =>
        {
            Assert.That(redriven.Applied, Is.True, "a re-driven import into a new copy applies the row again");
            Assert.That(await _cluster.Client.GetGrain<ILattice>(copyB).GetAsync("k"), Is.EqualTo(new byte[] { 7 }),
                "the new copy holds the row");
        });
    }

    [Test]
    public async Task A_live_re_delivery_after_a_drain_applied_the_identity_is_deduped()
    {
        var (tree, copyA, _) = await TreeWithCopiesAsync("dds-drain-then-live");
        var row = Row(tree, "k", 1);

        await DrainAsync(tree, copyA, row);
        var live = await _applier.ApplyAsync(row);

        Assert.That(live.Applied, Is.False, "a drain records its identity for the live path, as before");
    }

    [Test]
    public async Task A_drain_row_the_live_path_applied_to_another_copy_is_applied_to_this_copy()
    {
        var (tree, copyA, _) = await TreeWithCopiesAsync("dds-live-elsewhere");
        var row = Row(tree, "k", 3);

        await _applier.ApplyAsync(row);
        var drained = await DrainAsync(tree, copyA, row);

        Assert.Multiple(async () =>
        {
            Assert.That(drained.Applied, Is.True, "a live-cache hit the drain's copy does not hold is applied");
            Assert.That(await _cluster.Client.GetGrain<ILattice>(copyA).GetAsync("k"), Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public async Task A_drain_row_the_live_path_applied_to_this_copy_is_deduped()
    {
        var (tree, copyA, _) = await TreeWithCopiesAsync("dds-live-here");
        var row = Row(tree, "k", 4);

        // The live stream delivered the row into the held copy: recorded in the
        // live cache, and present in the copy at the row's version.
        await _applier.ApplyAsync(row);
        await _cluster.Client.GetGrain<IReplicationApplyGrain>(copyA)
            .ApplySetAsync("k", [4], row.Timestamp, Origin, sourceVectorClock: null, expiresAtTicks: 0);
        var drained = await DrainAsync(tree, copyA, row);

        Assert.That(drained.Applied, Is.False, "a live-cache hit the drain's copy confirms is not applied again");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = "dds-receiver";
                opts.AutoBootstrapOnFallOffLog = false;
            });
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
