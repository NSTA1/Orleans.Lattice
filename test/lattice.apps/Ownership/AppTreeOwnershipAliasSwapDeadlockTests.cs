using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Regression for #4128: an alias swap in a host with Lattice Apps registered must not deadlock
/// when the ownership ledger (<c>sys-app-trees</c>) is registered but the shard its read routes to
/// has never been seeded. The registry's <c>SetAliasAsync</c> holds the singleton's non-interleaved
/// turn while it awaits the app ownership guard; the guard's ledger read seeds that cold shard,
/// and the seed used to self-register the tree through <c>RegisterAsync</c>, which queued behind
/// the very <c>SetAliasAsync</c> waiting on it until the seed deadline expired.
/// </summary>
/// <remarks>
/// Each test gets a fresh cluster so every ledger shard starts unseeded, which is the state the
/// Explorer sample reached (a registered ledger holding few keys). The ledger is registered
/// directly rather than through an install, because an install's activation pipeline scans the
/// ledger and so seeds every shard, hiding the defect.
/// </remarks>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class AppTreeOwnershipAliasSwapDeadlockTests
{
    // Well under the 15 s shard-root seed deadline and the 30 s response timeout, so the
    // pre-fix deadlock surfaces as this bound expiring rather than as a slow pass.
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;

    private IServiceProvider Silo => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private IGrainFactory Grains => Silo.GetRequiredService<IGrainFactory>();

    [SetUp]
    public async Task SetUpAsync()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // The ledger exists, so the guard reads it, but no shard of it has been seeded.
        await Grains.GetLatticeRegistry().RegisterAsync(AppRegistryTreeNames.TreeLedgerTree);
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task SetAlias_completes_when_the_ownership_ledger_shard_is_unseeded()
    {
        var registry = Grains.GetLatticeRegistry();
        var logical = $"logical-{Guid.NewGuid():N}";
        var physical = $"physical-{Guid.NewGuid():N}";
        using (LatticeSystemOrigin.Enter())
        {
            await Grains.GetGrain<ILattice>(logical).SetAsync("k", [1]);
            await Grains.GetGrain<ILattice>(physical).SetAsync("k", [2]);

            await registry.SetAliasAsync(logical, physical).WaitAsync(Bound);
        }

        Assert.That(await registry.ResolveAsync(logical), Is.EqualTo(physical));
    }

    [Test]
    public async Task Resize_completes_its_swap_when_the_ownership_ledger_shard_is_unseeded()
    {
        var treeId = $"resized-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        var resize = Grains.GetGrain<ITreeResizeGrain>(treeId);
        using (LatticeSystemOrigin.Enter())
        {
            await tree.SetAsync("k", [7]);
            await resize.ResizeAsync(64, 64);
            await resize.RunResizePassAsync().WaitAsync(Bound);
        }

        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(treeId), Does.StartWith(treeId + "/resized/"));
        Assert.That(await resize.IsIdleAsync(), Is.True, "the resize ran to completion");
        using (LatticeSystemOrigin.Enter())
        {
            Assert.That(await tree.GetAsync("k"), Is.EqualTo(new byte[] { 7 }));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeApps();
        }
    }
}
