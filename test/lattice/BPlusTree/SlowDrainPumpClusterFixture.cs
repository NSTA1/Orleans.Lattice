using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Four-shard cluster whose background coordinator drains visit <b>one source
/// leaf per pass</b>, so a started shard split - and any reshard built on top
/// of one - stays in flight long enough for a test to observe and act on it
/// deterministically.
/// <para>
/// <b>Why this exists.</b> <c>CoordinatorGrain</c> arms its phase timer with a
/// due time of zero, so the first tick lands essentially as the start call
/// returns: the race window is the client round trip, not the two-second
/// period. <c>TreeShardSplitGrain.SplitAsync</c> compounds that by persisting
/// its intent already advanced to the <c>Drain</c> phase, so that immediate
/// first tick runs a drain pass rather than merely opening the shadow window. At
/// the default <see cref="LatticeOptions.BackgroundDrainLeavesPerPass"/> of 64 a
/// small test tree's entire drain completes inside that one pass, so the split
/// reaches <c>Swap</c> - the point at which moved slots stop routing to the
/// source and live writes stop being shadow-forwarded - on the very next tick.
/// A test that starts a split and then depends on it still being pre-swap is
/// therefore working inside roughly one timer period: comfortable on an idle
/// machine, and not a guarantee anywhere else.
/// </para>
/// <para>
/// Draining one leaf per pass turns that into a structural margin rather than a
/// coincidence. The drain itself now needs one tick per source leaf, so a source
/// shard holding N leaves cannot reach <c>Swap</c> for N ticks at the
/// coordinator's two-second cadence: a test that seeds a few hundred keys has
/// tens of seconds in which the split is provably still pre-swap. Explicitly
/// driving a coordinator with <c>RunSplitPassAsync</c> is unaffected: that API
/// drives its bounded drain through to completion inside the one call, so tests
/// still finish promptly.
/// </para>
/// <para>
/// The margin only makes the scenario <em>reachable</em>. Tests using this
/// fixture must still assert the state they observed rather than the state they
/// assumed - see <see cref="ShardMigrationInFlightIntegrationTests"/>.
/// </para>
/// <para>
/// Everything else matches <see cref="FourShardClusterFixture"/>. This is a
/// cadence-only override, so it exercises exactly the same split and reshard
/// code paths as production.
/// </para>
/// </summary>
public sealed class SlowDrainPumpClusterFixture
{
    /// <summary>Physical shard count every tree in this fixture is registered with.</summary>
    public const int TestShardCount = 4;

    /// <summary>
    /// Deliberately tiny leaf fan-out, so a modest key population still produces
    /// a long source leaf chain and therefore a long bounded drain.
    /// </summary>
    public const int SmallMaxLeafKeys = 4;

    /// <summary>Source leaves a background coordinator drain visits per pass.</summary>
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
                o.BackgroundDrainLeavesPerPass = DrainLeavesPerPass;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
