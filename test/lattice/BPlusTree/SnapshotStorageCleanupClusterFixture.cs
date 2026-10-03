using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;
using Orleans.TestingHost;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// A single-silo cluster whose lattice grain storage is an
/// <see cref="EnumerableMemoryGrainStorage"/>, so a test can count the snapshot
/// rows a leaf removal leaves behind (issue #4383). The segment window is the
/// smallest the options permit, so a modest leaf captures a segmented snapshot
/// and both kinds of snapshot row - manifest and segment - exist to be checked.
/// </summary>
public sealed class SnapshotStorageCleanupClusterFixture
{
    /// <summary>Leaf key limit for the fixture's trees: small, so a short seed splits into many leaves.</summary>
    public const int MaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The silo's lattice grain storage, enumerable by state name.</summary>
    internal EnumerableMemoryGrainStorage Storage { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.Options.InitialSilosCount = 1;
        builder.UseSharedInMemoryWal();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        Storage = (EnumerableMemoryGrainStorage)((InProcessSiloHandle)Cluster.Primary).SiloHost.Services
            .GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>Registers a single-shard tree with <see cref="MaxLeafKeys"/>.</summary>
    public async Task<ILattice> CreateTreeAsync(string treeName)
    {
        var registry = Cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = MaxLeafKeys });
        return Cluster.GrainFactory.GetGrain<ILattice>(treeName);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new EnumerableMemoryGrainStorage()));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.LeafSnapshotSegmentBytes = LatticeOptions.MinimumLeafSnapshotSegmentBytes;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
