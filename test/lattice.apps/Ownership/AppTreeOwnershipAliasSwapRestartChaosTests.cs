using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Chaos variant of the #4128 regression: a resize whose swap step failed (as the deadlock made
/// it fail, by timing out) is left in its swap phase, a silo restarts between the copy and the
/// swap, and the resize must then finish when it is driven again - with every shard of the
/// ownership ledger the guard reads still unseeded.
/// </summary>
/// <remarks>
/// Two silos over process-scope grain storage and a shared WAL provider, so durable state
/// survives the secondary's restart. The swap's first <c>SetAliasAsync</c> is failed by an
/// incoming-call filter rather than by the real deadlock, so the stuck state is reached
/// deterministically and without waiting out a timeout.
/// </remarks>
[TestFixture]
[Category("Chaos")]
[NonParallelizable]
public sealed class AppTreeOwnershipAliasSwapRestartChaosTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static readonly InMemoryWalStorageProvider WalProvider = new();

    private static int _failNextSetAlias;

    private TestCluster _cluster = null!;

    private IServiceProvider Primary => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    private IGrainFactory Grains => Primary.GetRequiredService<IGrainFactory>();

    [SetUp]
    public async Task SetUpAsync()
    {
        SharedGrainStorage.Reset();
        var builder = new TestClusterBuilder(2);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        await Grains.GetLatticeRegistry().RegisterAsync(AppRegistryTreeNames.TreeLedgerTree);
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

        SharedGrainStorage.Reset();
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

    /// <summary>Grain storage shared by every silo in the process, so state survives a silo restart.</summary>
    private sealed class SharedGrainStorage : IGrainStorage
    {
        private static readonly ConcurrentDictionary<string, (string ETag, object State)> Store = new();

        public static void Reset() => Store.Clear();

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (Store.TryGetValue($"{stateName}/{grainId}", out var entry))
            {
                grainState.State = (T)entry.State;
                grainState.ETag = entry.ETag;
                grainState.RecordExists = true;
            }
            else
            {
                grainState.RecordExists = false;
            }

            return Task.CompletedTask;
        }

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var etag = Guid.NewGuid().ToString("N");
            Store[$"{stateName}/{grainId}"] = (etag, grainState.State!);
            grainState.ETag = etag;
            grainState.RecordExists = true;
            return Task.CompletedTask;
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            Store.TryRemove($"{stateName}/{grainId}", out _);
            grainState.ETag = null!;
            grainState.RecordExists = false;
            return Task.CompletedTask;
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddWalStorage(_ => WalProvider);
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new SharedGrainStorage()));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddIncomingGrainCallFilter<FailFirstSetAliasFilter>();
            siloBuilder.AddLatticeApps();
        }
    }
}
