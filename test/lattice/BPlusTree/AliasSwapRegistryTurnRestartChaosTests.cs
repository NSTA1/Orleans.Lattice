using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.BPlusTree.PublicApiContract;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chaos regression for #4128: a resize whose swap step failed - as the registry-turn deadlock
/// made it fail, by timing out - is left in its swap phase, a silo restarts between the copy and
/// the swap, and the resize must then finish when it is driven again. The host's ownership guard
/// reads a registered tree none of whose shards has been seeded, from inside the registry's
/// non-interleaved <c>SetAliasAsync</c> turn, which is the shape of the apps ownership ledger
/// read that deadlocked the Explorer sample.
/// </summary>
/// <remarks>
/// Two silos over <see cref="ProcessScopeMemoryGrainStorage"/> and a shared WAL provider, so
/// durable state survives the secondary's restart (see <see cref="MultiSiloRestartChaosTests"/>
/// for why both are needed). The swap's first <c>SetAliasAsync</c> is failed by an incoming-call
/// filter rather than by the real deadlock, so the stuck state is reached deterministically.
/// </remarks>
[TestFixture]
[NonParallelizable]
[Category("Chaos")]
public sealed class AliasSwapRegistryTurnRestartChaosTests
{
    private const string LedgerTree = "sys-test-ownership-ledger";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static readonly InMemoryWalStorageProvider WalProvider = new();

    private static int _failNextSetAlias;

    private TestCluster _cluster = null!;

    private IGrainFactory Grains => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services.GetRequiredService<IGrainFactory>();

    [SetUp]
    public async Task SetUpAsync()
    {
        ProcessScopeMemoryGrainStorage.Reset();
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        // The guard's tree is registered, so the guard reads it, but no shard of it is seeded.
        await Grains.GetLatticeRegistry().RegisterAsync(LedgerTree);
    }

    [TearDown]
    public async Task TearDownAsync()
    {
        Volatile.Write(ref _failNextSetAlias, 0);
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        ProcessScopeMemoryGrainStorage.Reset();
    }

    [Test]
    public async Task A_resize_stuck_in_its_swap_completes_after_a_silo_restart()
    {
        var treeId = $"chaos-resized-{Guid.NewGuid():N}";
        var tree = Grains.GetGrain<ILattice>(treeId);
        var resize = Grains.GetGrain<ITreeResizeGrain>(treeId);
        using (LatticeSystemOrigin.Enter())
        {
            for (var i = 0; i < 20; i++)
            {
                await tree.SetAsync($"k{i:D2}", [(byte)i]);
            }

            await resize.ResizeAsync(64, 64);
            Volatile.Write(ref _failNextSetAlias, 1);
            Assert.CatchAsync<Exception>(() => resize.RunResizePassAsync().WaitAsync(Bound), "the swap step fails, as the deadlock made it");
        }

        Assert.That(Volatile.Read(ref _failNextSetAlias), Is.Zero, "the injected failure was consumed by the swap");
        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(treeId), Is.EqualTo(treeId), "the alias was not swapped");

        await _cluster.RestartSiloAsync(_cluster.SecondarySilos[0]);

        using (LatticeSystemOrigin.Enter())
        {
            await Grains.GetGrain<ITreeResizeGrain>(treeId).RunResizePassAsync().WaitAsync(Bound);
        }

        Assert.That(await Grains.GetLatticeRegistry().ResolveAsync(treeId), Does.StartWith(treeId + "/resized/"));
        Assert.That(await Grains.GetGrain<ITreeResizeGrain>(treeId).IsIdleAsync(), Is.True, "the resize ran to completion");
        using (LatticeSystemOrigin.Enter())
        {
            for (var i = 0; i < 20; i++)
            {
                Assert.That(await tree.GetAsync($"k{i:D2}"), Is.EqualTo(new[] { (byte)i }));
            }
        }
    }

    /// <summary>
    /// Reads the logical tree and the alias target from <see cref="LedgerTree"/>, as the apps
    /// ownership ledger does, and allows every alias.
    /// </summary>
    private sealed class LedgerReadingOwnershipGuard(IGrainFactory grainFactory) : ITreeOwnershipGuard
    {
        public async ValueTask<TreeOwnershipDecision> AuthorizeAliasAsync(
            string logicalTreeId,
            string physicalTreeId,
            string? derivedFrom,
            CancellationToken cancellationToken = default)
        {
            var ledger = grainFactory.GetGrain<ILattice>(LedgerTree);
            using (LatticeSystemOrigin.Enter())
            {
                await ledger.GetAsync(logicalTreeId, cancellationToken);
                await ledger.GetAsync(derivedFrom ?? physicalTreeId, cancellationToken);
            }

            return TreeOwnershipDecision.Allow();
        }
    }

    private sealed class FailFirstSetAliasFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            if (context.InterfaceMethod?.Name == nameof(ILatticeRegistry.SetAliasAsync)
                && context.InterfaceMethod.DeclaringType == typeof(ILatticeRegistry)
                && Interlocked.Exchange(ref _failNextSetAlias, 0) == 1)
            {
                throw new TimeoutException("Injected: the alias swap timed out (#4128).");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddWalStorage(_ => WalProvider);
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new ProcessScopeMemoryGrainStorage()));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddIncomingGrainCallFilter<FailFirstSetAliasFilter>();
            siloBuilder.Services.Replace(ServiceDescriptor.Singleton<ITreeOwnershipGuard, LedgerReadingOwnershipGuard>());
        }
    }
}
