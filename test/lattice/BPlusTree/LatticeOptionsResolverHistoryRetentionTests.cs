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

    /// <summary>
    /// Pins the deliberate <em>absence</em> of a cache on the user-tree path
    /// (issue #3181, AC4).
    /// <para>
    /// A "no cache" decision is invisible in the source - it looks exactly like
    /// nobody having got round to adding one - so the only way to stop a future
    /// reader from "optimising" it back is to make a cache fail a test. The
    /// decision is deliberate: the resolver is a per-silo <c>AddSingleton</c>
    /// and <c>SetHistoryRetentionAsync</c> performs no local invalidation, so a
    /// memo would go stale without bound, and stale retention silently drops or
    /// retains value bytes against the operator's instruction.
    /// </para>
    /// </summary>
    [Test]
    public async Task GetHistoryRetentionAsync_reads_the_registry_afresh_on_every_call()
    {
        var (resolver, registry) = Build();

        await resolver.GetHistoryRetentionAsync("ordinary-tree", HybridWindow);
        await resolver.GetHistoryRetentionAsync("ordinary-tree", HybridWindow);
        await resolver.GetHistoryRetentionAsync("ordinary-tree", HybridWindow);

        await registry.Received(3).GetEntryAsync("ordinary-tree");
    }

    /// <summary>
    /// The history-retention read must stay a <em>pure</em> read. The resolve
    /// path's fetch lazily calls <c>RegisterAsync</c> when a structural pin is
    /// missing; routing this read through it would let a view maintainer
    /// draining an unregistered source tree silently register that tree as a
    /// side effect of reading its retention policy.
    /// </summary>
    [Test]
    public async Task GetHistoryRetentionAsync_does_not_seed_a_row_for_an_unregistered_tree()
    {
        var (resolver, registry) = Build();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(null));

        var policy = await resolver.GetHistoryRetentionAsync("never-registered", HybridWindow);

        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!, default);
        Assert.Multiple(() =>
        {
            Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly),
                "An unregistered tree falls through to the documented defaults rather than being seeded.");
            Assert.That(policy.Window, Is.EqualTo(TimeSpan.Zero));
        });
    }
}
