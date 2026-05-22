using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class FourShardClusterFixture
{
    public const string TreeName = "four-shard-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(TreeName, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Pre-registers <paramref name="treeId"/> in the tree registry with the fixture's
    /// pinned structural layout (<see cref="TestShardCount"/> shards, <see cref="SmallMaxLeafKeys"/>
    /// keys per leaf) and returns a grain reference to it. Tests that need a fresh per-test
    /// tree ID must call this instead of <c>GetGrain&lt;ILattice&gt;(...)</c> directly, otherwise
    /// the tree lazy-seeds from <see cref="LatticeConstants"/> defaults (64 shards / 128 keys
    /// per leaf) and subsequent <c>ReshardAsync</c> / <c>ResizeAsync</c> calls that target
    /// smaller values are rejected as shrinks.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = TestShardCount,
        });
        RegisterDefaultOrMapShapes(treeId);
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Registers the OR-Map <c>(string, OrSet)</c> shape on every silo's
    /// <see cref="CrdtShapeRegistry"/> for the supplied tree id. The
    /// CRDT-accessor integration tests use this shape exclusively, and
    /// the producer-side delta-apply seam requires the descriptor to be
    /// resolvable at write time. Other OR-Map shapes can be registered
    /// per-test via <see cref="RegisterOrMapShape{TKey, TValue}(string)"/>.
    /// </summary>
    private void RegisterDefaultOrMapShapes(string treeId)
        => RegisterOrMapShape<string, OrSet>(treeId);

    /// <summary>
    /// Registers a per-test OR-Map shape on every silo's
    /// <see cref="CrdtShapeRegistry"/>. Mirrors the host-side
    /// <c>AddOrMapShape&lt;TKey, TValue&gt;</c> but runs after silo
    /// startup so tests can pick fresh tree ids per test method.
    /// </summary>
    public void RegisterOrMapShape<TKey, TValue>(string treeId)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        foreach (var silo in Cluster.Silos.OfType<InProcessSiloHandle>())
        {
            var registry = silo.SiloHost.Services.GetRequiredService<CrdtShapeRegistry>();
            registry.Register(treeId, CrdtShape.ForOrMap<TKey, TValue>());
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
