using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Counts incoming grain calls whose interface is <see cref="ILattice"/> so
/// tests can assert the read facade issues a bounded number of grain calls
/// per request, independent of shard count.
/// </summary>
internal sealed class LatticeCallCounter
{
    private long _count;

    public long Count => Interlocked.Read(ref _count);

    public void Reset() => Interlocked.Exchange(ref _count, 0);

    public void Increment() => Interlocked.Increment(ref _count);
}

internal sealed class LatticeCallCountingFilter(LatticeCallCounter counter) : IIncomingGrainCallFilter
{
    public Task Invoke(IIncomingGrainCallContext context)
    {
        if (context.InterfaceMethod?.DeclaringType == typeof(ILattice))
        {
            counter.Increment();
        }

        return context.Invoke();
    }
}

/// <summary>
/// Test <see cref="ILatticeMergeModeResolver"/> that declares any tree whose id
/// starts with <c>"orset"</c> as an <see cref="LatticeMergeMode.OrSet"/> tree
/// and leaves every other tree undeclared (last-writer-wins). Lets the read
/// facade observe a per-tree CRDT merge mode without standing up the full
/// replication package.
/// </summary>
internal sealed class OrSetPrefixMergeModeResolver : ILatticeMergeModeResolver
{
    public LatticeMergeMode? Resolve(string treeId) =>
        treeId.StartsWith("orset", StringComparison.Ordinal)
            ? LatticeMergeMode.OrSet
            : null;
}

/// <summary>
/// Single-silo fixture that registers the core lattice, the state API, and a
/// grain-call-counting filter, and pre-creates multi-shard trees via the
/// registry. Shared by the read-facade integration tests.
/// </summary>
internal sealed class StateQueryClusterFixture
{
    public const int ShardCount = 4;
    public const int MaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    public LatticeCallCounter CallCounter => SiloServices.GetRequiredService<LatticeCallCounter>();

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// Registers <paramref name="treeId"/> with the given shard count and
    /// writes <paramref name="keyCount"/> keys, returning its grain reference.
    /// </summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(
        string treeId,
        int keyCount,
        int shardCount = ShardCount)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = shardCount,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"key-{i:D5}", new byte[] { (byte)i });
        }

        return tree;
    }

    /// <summary>
    /// Registers <paramref name="treeId"/> (declared as an OR-Set tree by the
    /// fixture's merge-mode resolver) and writes one OR-Set element per key in
    /// <paramref name="keys"/>, returning its grain reference.
    /// </summary>
    public async Task<ILattice> CreateOrSetTreeAsync(string treeId, params string[] keys)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = ShardCount,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        foreach (var key in keys)
        {
            await tree.OrSet(key).AddAsync(System.Text.Encoding.UTF8.GetBytes($"member-of-{key}"), "replica-a");
        }

        return tree;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeStateApi();
            siloBuilder.Services.AddSingleton<LatticeCallCounter>();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, LatticeCallCountingFilter>();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, OrSetPrefixMergeModeResolver>();
        }
    }
}
