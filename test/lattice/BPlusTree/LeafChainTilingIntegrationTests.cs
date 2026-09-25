using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Proves the shared <see cref="LeafChainTiling"/> check can fail (issue
/// #2125, acceptance 6), by driving a leaf fold that declines at its
/// compare-and-swap.
/// <para>
/// The current fold ordering (#2099) widens the predecessor in the
/// compare-and-swap <b>before</b> it retires the folded leaf's routing, so a
/// fold that declines at the swap has changed nothing and the tree tiles
/// exactly as it did. The pre-#2099 ordering retired routing first, so the same
/// decline left the folded leaf's range routed to a predecessor whose declared
/// span had never been widened: a routing gap, invisible to a span-only walk
/// because both leaves still sit in the chain with matching bounds, and the
/// shape that silently loses writes on the next projection rebuild. The
/// routing arm of the shared check is what turns that ordering red.
/// </para>
/// <para>
/// The decline is injected deterministically by an incoming call filter that
/// answers the predecessor's <c>TryUnlinkSuccessorAsync</c> with <c>false</c>
/// - the exact observable of a split landing underneath the fold - without
/// needing to win that race for real.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafChainTilingIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        UnlinkDeclineGate.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => UnlinkDeclineGate.Disarm();

    [Test]
    public async Task A_fold_declined_at_its_compare_and_swap_leaves_routing_agreeing_with_the_chain()
    {
        var treeName = $"tiling-declined-fold-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");

        for (var i = 0; i < 120; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        await router.DeleteRangeAsync("k030", "k090");

        await LeafChainTiling.AssertTilesAsync(_cluster.GrainFactory, shard, "before the declined fold");

        UnlinkDeclineGate.Arm();
        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);
        UnlinkDeclineGate.Disarm();

        Assert.That(UnlinkDeclineGate.Declines, Is.GreaterThan(0),
            "precondition: the pass must have reached the compare-and-swap of at least one fold");
        Assert.That(reclaimed, Is.Zero, "every fold was declined at its compare-and-swap");

        await LeafChainTiling.AssertTilesAsync(_cluster.GrainFactory, shard, "after a fold declined at its compare-and-swap");

        // And the property the routing arm stands for: a write into the range
        // the declined fold would have moved still survives a full rebuild.
        await router.SetAsync("k050", Encoding.UTF8.GetBytes("after-decline"));
        for (var cursor = await shard.GetLeftmostLeafIdAsync(); cursor is { } id;)
        {
            var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);
            await leaf.RebuildProjectionFromWalAsync();
            cursor = await leaf.GetNextSiblingAsync();
        }

        Assert.That(await router.GetAsync("k050"), Is.EqualTo(Encoding.UTF8.GetBytes("after-decline")),
            "a write into the range of a declined fold must survive a projection rebuild");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o => o.TombstoneGracePeriod = TimeSpan.Zero);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, UnlinkDeclineFilter>();
        }
    }

    /// <summary>
    /// Control state for the injected decline. Static because the TestingHost
    /// silo runs in-process; disarmed after every test so it cannot leak.
    /// </summary>
    private static class UnlinkDeclineGate
    {
        private static int _armed;
        private static int _declines;

        internal static int Declines => Volatile.Read(ref _declines);

        internal static bool Armed => Volatile.Read(ref _armed) == 1;

        internal static void Arm()
        {
            Volatile.Write(ref _declines, 0);
            Volatile.Write(ref _armed, 1);
        }

        internal static void Disarm() => Volatile.Write(ref _armed, 0);

        internal static void CountDecline() => Interlocked.Increment(ref _declines);
    }

    /// <summary>
    /// Answers <c>TryUnlinkSuccessorAsync</c> with <c>false</c> while armed,
    /// without invoking the leaf, so the predecessor is left exactly as it was.
    /// </summary>
    private sealed class UnlinkDeclineFilter : IIncomingGrainCallFilter
    {
        public Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (UnlinkDeclineGate.Armed
                && context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.InterfaceMethod?.Name == nameof(IBPlusLeafGrain.TryUnlinkSuccessorAsync))
            {
                UnlinkDeclineGate.CountDecline();
                context.Result = false;
                return Task.CompletedTask;
            }

            return context.Invoke();
        }
    }
}
