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
        => await CreateTreeAsync(treeId, TestShardCount);

    /// <summary>
    /// Pre-registers <paramref name="treeId"/> in the tree registry with
    /// the fixture's pinned small-leaf layout but the caller-supplied
    /// shard count and returns a grain reference to it. Used by the
    /// chained-fold-under-coalescing integration test that needs to
    /// exercise the per-shard fan-out path with more than one shard.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId, int shardCount)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = shardCount,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            // The coalescing-metrics tests assert per-publish mechanics
            // (scheduled / skipped / fired / inline counts) under the
            // single-partition shape these tests were written against.
            // Pin WalPartitions=1 so the tests stay deterministic after
            // the silo-wide default flipped to 8 - multi-partition fan-
            // out is exercised by its own dedicated MultiPartition*
            // integration suite.
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = CoalescingWindowMs;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
