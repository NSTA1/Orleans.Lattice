using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the system-tree bypass on
/// <see cref="LatticeOptionsResolver.GetHistoryRetentionAsync"/>.
/// <para>
/// The resolver has five entry points that a grain activation can reach, and
/// four of them - <c>ResolveAsync</c>, <c>GetWalPartitionsAsync</c>,
/// <c>GetMaxCacheValueBytesAsync</c> and <c>GetWalPlacementSnapshotAsync</c> -
/// short-circuit a system tree to its defaults without touching
/// <see cref="ILatticeRegistry"/>. The guard exists to keep option resolution
/// for the registry's own backing trees from calling back into the registry
/// singleton that is trying to resolve them.
/// <c>GetHistoryRetentionAsync</c> was the one entry point missing it
/// (issue #3180, AC3).
/// </para>
/// <para>
/// This is an inconsistency in a cycle-avoidance guard, not a demonstrated
/// live cycle: the trees the view maintainers pass here are the <c>sys-*</c>
/// projection trees, which do not carry the reserved
/// <see cref="LatticeConstants.SystemTreePrefix"/>, so the missing branch was
/// not reachable from the observed wedge. It is still worth closing, because a
/// guard that four of five call sites have is one a reader will assume the
/// fifth has too.
/// </para>
/// <para>
/// The bypass is behaviour-preserving as well as cycle-avoiding, and the tests
/// below assert exactly that: a system tree can never be registered (the
/// registry excludes the reserved prefix from self-registration and
/// <c>RegisterAsync</c> rejects it outright), so the registry read the bypass
/// replaces could only ever have returned <c>null</c> and fallen through to the
/// same defaults. Asserting the returned policy rather than only the absence of
/// the call is what makes that claim testable rather than asserted.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeOptionsResolverHistoryRetentionTests
{
    private static readonly TimeSpan HybridWindow = TimeSpan.FromMinutes(7);

    [SetUp]
    public void Setup() => LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();

    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry) Build()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        // A retention policy that is deliberately *not* the default, so a test
        // that expects the bypass cannot pass by coincidence if the registry is
        // consulted after all.
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                MaxLeafKeys = LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = LatticeConstants.DefaultMaxInternalChildren,
                ShardCount = LatticeConstants.DefaultShardCount,
                HistoryRetentionMode = HistoryRetentionMode.FullValue,
                HistoryRetentionWindowTicks = TimeSpan.FromHours(3).Ticks,
            }));

        return (new LatticeOptionsResolver(factory, monitor), registry);
    }

    [TestCase(LatticeConstants.RegistryTreeId)]
    [TestCase(LatticeConstants.SystemTreePrefix + "wal")]
    [TestCase(LatticeConstants.SystemTreePrefix + "replog_x")]
    public async Task GetHistoryRetentionAsync_bypasses_the_registry_for_a_system_tree(string treeId)
    {
        var (resolver, registry) = Build();

        var policy = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);

        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
        Assert.Multiple(() =>
        {
            Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly),
                "A system tree must resolve to the same defaults an unregistered tree would, so the " +
                "bypass changes only which calls are made, never the answer.");
            Assert.That(policy.Window, Is.EqualTo(TimeSpan.Zero));
            Assert.That(policy.HybridFullValueWindow, Is.EqualTo(HybridWindow),
                "The caller-supplied hybrid window must be carried through unchanged by the bypass.");
        });
    }

    [Test]
    public async Task GetHistoryRetentionAsync_still_reads_the_registry_for_a_user_tree()
    {
        var (resolver, registry) = Build();

        var policy = await resolver.GetHistoryRetentionAsync("ordinary-tree", HybridWindow);

        await registry.Received(1).GetEntryAsync("ordinary-tree");
        Assert.Multiple(() =>
        {
            Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.FullValue),
                "The bypass must be scoped to the reserved prefix; a user tree still resolves its " +
                "retention from the registry entry.");
            Assert.That(policy.Window, Is.EqualTo(TimeSpan.FromHours(3)));
            Assert.That(policy.HybridFullValueWindow, Is.EqualTo(HybridWindow));
        });
    }

    [Test]
    public void GetHistoryRetentionAsync_rejects_a_null_tree_id()
    {
        var (resolver, _) = Build();

        Assert.That(
            async () => await resolver.GetHistoryRetentionAsync(null!, HybridWindow),
            Throws.TypeOf<ArgumentNullException>(),
            "The null guard must stay ahead of the system-tree branch so a null id still throws rather " +
            "than faulting inside StartsWith.");
    }
}
