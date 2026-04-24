using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test cluster fixture that registers a process-global
/// <see cref="IMutationObserver"/> in the silo DI container so integration
/// tests can assert end-to-end delivery from
/// <c>ILattice.SetAsync</c> / <c>DeleteAsync</c> / <c>DeleteRangeAsync</c>
/// through the full grain pipeline down to the observer.
/// </summary>
public sealed class MutationObserverClusterFixture
{
    public const string TreeName = "obs-integ-tree";
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;

    /// <summary>
    /// Process-global sink populated by <see cref="CapturingMutationObserver"/>.
    /// Drained on each test via <see cref="Drain"/>.
    /// </summary>
    public static readonly ConcurrentQueue<LatticeMutation> Captured = new();

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
    /// Pre-registers a tree with the fixture's pinned layout and returns a
    /// grain reference. Mirrors <see cref="FourShardClusterFixture.CreateTreeAsync"/>.
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
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>Drains all captured mutations — call between tests.</summary>
    public static IReadOnlyList<LatticeMutation> Drain()
    {
        var list = new List<LatticeMutation>();
        while (Captured.TryDequeue(out var m)) list.Add(m);
        return list;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IMutationObserver, CapturingMutationObserver>();
        }
    }

    /// <summary>
    /// Silo-side observer that forwards every mutation into the process-global
    /// <see cref="Captured"/> queue so the test process can assert delivery.
    /// </summary>
    internal sealed class CapturingMutationObserver : IMutationObserver
    {
        public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
        {
            Captured.Enqueue(mutation);
            return Task.CompletedTask;
        }
    }
}
