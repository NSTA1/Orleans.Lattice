using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test cluster fixture where the silo-wide
/// <see cref="LatticeOptions.MaintainProjectionDigest"/> is set to
/// <c>false</c>. Used to verify that the opt-out propagates through the
/// resolver to the leaf, internal, and shard-root grains and that
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/> fast-fails at the
/// public surface for digest-quiescent trees.
/// </summary>
public sealed class DigestDisabledClusterFixture
{
    public const int TestShardCount = 4;
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

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

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            // Silo-wide opt-out: every tree on this cluster turns off
            // projection-digest maintenance unless explicitly re-enabled
            // by a per-tree ConfigureLattice override.
            siloBuilder.ConfigureLattice(opts => opts.MaintainProjectionDigest = false);
        }
    }
}
