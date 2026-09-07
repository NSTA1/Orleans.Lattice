using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

public sealed class SmallLeafClusterFixture
{
    public const string TreeName = "small-leaf-tree";
    public const string CompactionTreeName = "compaction-tree";
    public const int SmallMaxLeafKeys = 4;

    /// <summary>
    /// A tree configured with a deliberately short
    /// <see cref="LatticeOptions.LeafRetirementRetryDeadline"/>, so a test can
    /// assert that the configured value governs how long a write waits on a
    /// retirement latch rather than the default. Named rather than generated
    /// because per-tree options are bound at silo build time.
    /// </summary>
    public const string ShortRetirementDeadlineTreeName = "short-retirement-deadline-tree";

    /// <summary>
    /// The deadline configured for <see cref="ShortRetirementDeadlineTreeName"/>.
    /// Far enough below <see cref="LatticeOptions.DefaultLeafRetirementRetryDeadline"/>
    /// that a test can tell the two apart from elapsed time alone.
    /// </summary>
    public static readonly TimeSpan ShortRetirementDeadline = TimeSpan.FromMilliseconds(250);

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();

        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var pin = new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = 1,
        };
        await registry.RegisterAsync(TreeName, pin);
        await registry.RegisterAsync(CompactionTreeName, pin);
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
            });
            siloBuilder.ConfigureLattice(ShortRetirementDeadlineTreeName, o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.LeafRetirementRetryDeadline = ShortRetirementDeadline;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
