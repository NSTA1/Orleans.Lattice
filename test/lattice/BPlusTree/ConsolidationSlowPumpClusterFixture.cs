using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Four-shard cluster whose online shard-consolidation coordinator drains
/// <b>one donor leaf per background pass</b>, so a started fold stays in its
/// drain phase long enough for a test to observe and act on it deterministically.
/// <para>
/// <b>Why this exists.</b> <c>TreeShardConsolidationGrain</c> arms a
/// reminder-anchored phase timer whose first tick is due immediately, and at the
/// default <see cref="LatticeOptions.ConsolidationDrainLeavesPerPass"/> of 16 a
/// small test tree drains in a single pass. The fold therefore reaches
/// <c>Swap</c> - its point of no return - within the first pump tick, which can
/// easily land before a test's next client call. Any test that starts a fold and
/// then depends on it still being pre-Swap is racing that timer, and loses on a
/// loaded machine while passing on an idle one: the worst possible failure
/// distribution, because CI writes it off as flake.
/// </para>
/// <para>
/// Draining one leaf per pass turns that into a structural margin rather than a
/// coincidence. A donor holding N leaves now needs N pump ticks at the
/// coordinator's two-second cadence before it can reach <c>Swap</c>, so a test
/// that populates a few hundred keys has tens of seconds in which the fold is
/// provably still cancellable. Tests using this fixture must still assert the
/// phase they observed rather than the phase they assumed - the margin makes the
/// scenario reachable, the assertion is what makes it proven.
/// </para>
/// <para>
/// Everything else matches <see cref="FourShardClusterFixture"/>. This is a
/// cadence-only override, so it exercises exactly the same consolidation code
/// path as production.
/// </para>
/// </summary>
public sealed class ConsolidationSlowPumpClusterFixture
{
    /// <summary>Physical shard count every tree in this fixture is registered with.</summary>
    public const int TestShardCount = 4;

    /// <summary>
    /// Deliberately tiny leaf fan-out, so a modest key population still produces
    /// a long donor leaf chain and therefore a long bounded drain.
    /// </summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Donor leaves the consolidation coordinator drains per background pass.</summary>
    public const int DrainLeavesPerPass = 1;

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

    /// <summary>
    /// Registers <paramref name="treeId"/> with this fixture's pinned structural
    /// layout and returns a grain reference to it.
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
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.ConsolidationDrainLeavesPerPass = DrainLeavesPerPass;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
