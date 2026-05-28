using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the c2-xxviii leaf-side digest-coalescing
/// integration tests. Pins
/// <see cref="LatticeOptions.DigestCoalescingWindowMs"/> to a small
/// positive value (5 ms) and a small leaf-key cap (4) so a handful of
/// writes per shard exercises both the coalesced-publish path on the
/// per-write hot loop and the inline-publish path on structural events
/// (leaf split, rebuild, saga terminal, etc.).
/// <para>
/// Sibling fixtures - <see cref="FourShardClusterFixture"/> and
/// <see cref="PublicApiContract.PublicApiContractClusterFixture"/> -
/// deliberately pin the window to <c>0</c> so their pre-coalescing
/// oracle tests observe the synchronous-publish shape they were
/// authored against. The coalescing-shape invariants live exclusively
/// in this fixture so the two shapes are tested independently.
/// </para>
/// </summary>
public sealed class CoalescingClusterFixture
{
    /// <summary>The configured coalescing window, in milliseconds.</summary>
    public const int CoalescingWindowMs = 5;

    /// <summary>Default per-tree leaf-key cap used by <see cref="CreateTreeAsync"/>.</summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Default shard count used by <see cref="CreateTreeAsync"/>.</summary>
    public const int TestShardCount = 1;

    /// <summary>The currently-active test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Deploys the test cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Tears down the test cluster.</summary>
    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Pre-registers <paramref name="treeId"/> in the tree registry with
    /// the fixture's pinned small-leaf layout and returns a grain
    /// reference to it.
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

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            // Pin the c2-xxviii coalescing window deliberately so the
            // tests using this fixture exercise the
            // publish-deferred-then-fires path. ConfigureLattice (no
            // tree name) uses ConfigureAll, which applies to every
            // named options instance the resolver pulls from
            // IOptionsMonitor.
            siloBuilder.ConfigureLattice(o => o.DigestCoalescingWindowMs = CoalescingWindowMs);
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
