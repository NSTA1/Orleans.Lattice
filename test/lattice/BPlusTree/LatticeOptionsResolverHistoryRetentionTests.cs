using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The durable-history retention resolution surface of
/// <see cref="LatticeOptionsResolver"/> (issue #3181): the system-tree
/// short-circuit, the registry mapping it short-circuits past, and the
/// coalesced-but-uncached shape of the read.
/// <para>
/// <b>Why the bypass clause asserts on the substitute and not on the value.</b>
/// A system tree's defaults are <em>result-identical</em> to what the registry
/// path composes for it - a system tree carries no registry entry by
/// construction, so the read it replaces returns <c>null</c> and yields exactly
/// <see cref="HistoryRetentionMode.MetadataOnly"/> with no age bound. A clause
/// that only inspects the returned policy therefore passes equally against an
/// implementation with no bypass at all, which is a false green rather than a
/// weak test. The discriminating observation is that the registry is never
/// touched, so that is what is asserted.
/// </para>
/// <para>
/// <b>Both directions of the not-a-cache property are pinned</b>
/// (<see cref="Concurrent_history_reads_of_one_tree_share_a_single_registry_read"/>
/// and
/// <see cref="Sequential_history_reads_each_read_the_registry_afresh"/>),
/// matching <c>LatticeOptionsResolverRegistryCoalescingTests</c>. Only the pair
/// distinguishes coalescing from memoisation, and the distinction is
/// load-bearing here: the policy is runtime-mutable through
/// <see cref="ILattice.SetHistoryRetentionAsync"/>, which writes the registry
/// from the source tree's grain with no channel to invalidate a per-silo memo.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeOptionsResolverHistoryRetentionTests
{
    private static readonly TimeSpan HybridWindow = TimeSpan.FromMinutes(17);

    [SetUp]
    public void Setup() => LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();

    private static string UniqueTree() => $"tree-history-{Guid.NewGuid():N}";

    private static (LatticeOptionsResolver Resolver, ILatticeRegistry Registry, IGrainFactory Factory) Build(
        TreeRegistryEntry? entry = null)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult(entry));

        return (new LatticeOptionsResolver(factory, monitor), registry, factory);
    }

    // ---- The system-tree bypass (AC 1 and AC 3) --------------------------

    /// <summary>
    /// THE load-bearing clause. A <c>_lattice_</c>-prefixed id must resolve
    /// without any registry interaction whatsoever - not a
    /// <see cref="ILatticeRegistry.GetEntryAsync"/> call, not any other call,
    /// and not even a grain reference acquired for one.
    /// </summary>
    [TestCase(LatticeConstants.RegistryTreeId)]
    [TestCase(LatticeConstants.SystemTreePrefix + "views")]
    [TestCase(LatticeConstants.SystemTreePrefix)]
    public async Task System_tree_resolves_without_any_registry_call(string treeId)
    {
        var (resolver, registry, factory) = Build(new TreeRegistryEntry
        {
            // Deliberately a NON-default entry. Were the bypass absent, this is
            // what the registry would hand back, so the clause below that checks
            // the returned policy discriminates too.
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = TimeSpan.FromDays(9).Ticks,
        });

        var policy = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);

        Assert.That(registry.ReceivedCalls(), Is.Empty,
            "A system-tree resolution must not touch the registry at all: the registry's own backing "
            + "tree is _lattice_trees, so consulting it re-enters the grain being resolved.");
        await registry.DidNotReceiveWithAnyArgs().GetEntryAsync(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeRegistry>(default!);

        Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        Assert.That(policy.Window, Is.EqualTo(TimeSpan.Zero));
    }

    /// <summary>
    /// The bypass must never seed a structural pin either. The resolve path's
    /// coalesced fetch lazily registers a tree with a missing pin; this is a
    /// pure read and must not acquire that side effect, least of all for a tree
    /// that is not supposed to be in the registry.
    /// </summary>
    [Test]
    public async Task System_tree_does_not_seed_a_registry_entry()
    {
        var (resolver, registry, _) = Build(entry: null);

        _ = await resolver.GetHistoryRetentionAsync(LatticeConstants.RegistryTreeId, HybridWindow);

        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!);
    }

    /// <summary>
    /// The bypass is a short-circuit, not a different answer: for a tree with
    /// no registry entry - which is what a system tree is, by construction -
    /// the two paths must compose the same policy. If this ever diverges, the
    /// bypass has become a behaviour change rather than an elision.
    /// </summary>
    [Test]
    public async Task System_tree_bypass_agrees_with_the_registry_path_for_an_unregistered_tree()
    {
        var (resolver, _, _) = Build(entry: null);

        var bypassed = await resolver.GetHistoryRetentionAsync(
            LatticeConstants.SystemTreePrefix + "trees", HybridWindow);
        var viaRegistry = await resolver.GetHistoryRetentionAsync(UniqueTree(), HybridWindow);

        Assert.That(bypassed, Is.EqualTo(viaRegistry));
    }

    /// <summary>
    /// The caller-supplied hybrid window is carried through the bypass
    /// unchanged, exactly as the registry path carries it, so the short-circuit
    /// cannot silently zero a caller's configuration.
    /// </summary>
    [Test]
    public async Task System_tree_carries_the_caller_supplied_hybrid_window_through()
    {
        var (resolver, _, _) = Build(entry: null);

        var policy = await resolver.GetHistoryRetentionAsync(
            LatticeConstants.SystemTreePrefix + "trees", HybridWindow);

        Assert.That(policy.HybridFullValueWindow, Is.EqualTo(HybridWindow));
    }

    // ---- The registry path is untouched ---------------------------------

    [Test]
    public async Task User_tree_maps_the_registry_mode_and_window()
    {
        var (resolver, registry, _) = Build(new TreeRegistryEntry
        {
            HistoryRetentionMode = HistoryRetentionMode.Hybrid,
            HistoryRetentionWindowTicks = TimeSpan.FromHours(6).Ticks,
        });
        var treeId = UniqueTree();

        var policy = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);

        Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.Hybrid));
        Assert.That(policy.Window, Is.EqualTo(TimeSpan.FromHours(6)));
        Assert.That(policy.HybridFullValueWindow, Is.EqualTo(HybridWindow));
        await registry.Received(1).GetEntryAsync(treeId);
    }

    [Test]
    public async Task User_tree_with_no_entry_resolves_the_documented_defaults()
    {
        var (resolver, registry, _) = Build(entry: null);
        var treeId = UniqueTree();

        var policy = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);

        Assert.That(policy.Mode, Is.EqualTo(HistoryRetentionMode.MetadataOnly));
        Assert.That(policy.Window, Is.EqualTo(TimeSpan.Zero));
        await registry.Received(1).GetEntryAsync(treeId);
    }

    /// <summary>
    /// The user-tree path is a pure read too: an unpinned entry is reported as
    /// it stands rather than lazily registered, matching
    /// <c>GetMaxCacheValueBytesAsync</c>. A view maintainer draining a source
    /// tree must not be the thing that registers it.
    /// </summary>
    [Test]
    public async Task User_tree_read_does_not_seed_a_missing_structural_pin()
    {
        var (resolver, registry, _) = Build(entry: null);

        _ = await resolver.GetHistoryRetentionAsync(UniqueTree(), HybridWindow);

        await registry.DidNotReceiveWithAnyArgs().RegisterAsync(default!);
    }

    [Test]
    public void Throws_on_a_null_tree_id()
    {
        var (resolver, _, _) = Build();

        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await resolver.GetHistoryRetentionAsync(null!, HybridWindow));
    }

    // ---- Coalesced, and deliberately not cached (AC 4) -------------------

    private static (LatticeOptionsResolver Resolver, Func<int> Calls, Func<int> Entered) BuildGated(
        TaskCompletionSource release)
    {
        var calls = 0;
        var entered = 0;
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ =>
        {
            Interlocked.Increment(ref calls);
            return ServeAsync();
        });

        async Task<TreeRegistryEntry?> ServeAsync()
        {
            Interlocked.Increment(ref entered);
            await release.Task;
            return new TreeRegistryEntry { HistoryRetentionMode = HistoryRetentionMode.FullValue };
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        return (new LatticeOptionsResolver(factory, monitor),
            () => Volatile.Read(ref calls),
            () => Volatile.Read(ref entered));
    }

    /// <summary>
    /// The registry is a non-reentrant cluster singleton, so N concurrent
    /// readers cost N serialised turns. Readers that arrive while a read is in
    /// flight must join it instead.
    /// </summary>
    [Test]
    public async Task Concurrent_history_reads_of_one_tree_share_a_single_registry_read()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (resolver, calls, entered) = BuildGated(release);
        var treeId = UniqueTree();

        var first = resolver.GetHistoryRetentionAsync(treeId, HybridWindow).AsTask();

        var spun = 0;
        while (entered() == 0 && spun++ < 500)
            await Task.Delay(10);
        Assert.That(entered(), Is.EqualTo(1), "the first read must be inside the registry call");

        var followers = Enumerable.Range(0, 16)
            .Select(_ => resolver.GetHistoryRetentionAsync(treeId, HybridWindow).AsTask())
            .ToArray();

        Assert.That(calls(), Is.EqualTo(1),
            "16 history reads arriving while a read is in flight must join it, not queue 16 more "
            + "turns behind a non-reentrant registry singleton.");

        release.SetResult();
        var policies = await Task.WhenAll(followers.Prepend(first));

        Assert.That(calls(), Is.EqualTo(1), "joining must not issue a second read on completion.");
        Assert.That(policies, Has.All.Matches<HistoryRetentionPolicy>(
                p => p.Mode == HistoryRetentionMode.FullValue),
            "every joined caller must receive the shared result.");
    }

    /// <summary>
    /// The other direction, and the reason coalescing was chosen over a cache.
    /// Nothing is retained between reads, so the policy stays honestly
    /// runtime-mutable: a memo here would be per-silo, and
    /// <see cref="ILattice.SetHistoryRetentionAsync"/> - which writes from the
    /// source tree's own grain - has no channel to invalidate it.
    /// </summary>
    [Test]
    public async Task Sequential_history_reads_each_read_the_registry_afresh()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        release.SetResult();
        var (resolver, calls, _) = BuildGated(release);
        var treeId = UniqueTree();

        for (var i = 0; i < 8; i++)
            _ = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);

        Assert.That(calls(), Is.EqualTo(8),
            "Eight sequential reads must cost eight round trips; any smaller number means a value "
            + "was cached, which would freeze a policy documented as runtime-mutable.");
    }

    /// <summary>
    /// Each caller composes its own policy from the shared entry, so joining a
    /// flight must not hand one caller another's hybrid window.
    /// </summary>
    [Test]
    public async Task Joined_readers_each_keep_their_own_hybrid_window()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (resolver, _, entered) = BuildGated(release);
        var treeId = UniqueTree();

        var first = resolver.GetHistoryRetentionAsync(treeId, TimeSpan.FromMinutes(1)).AsTask();
        var spun = 0;
        while (entered() == 0 && spun++ < 500)
            await Task.Delay(10);

        var second = resolver.GetHistoryRetentionAsync(treeId, TimeSpan.FromMinutes(2)).AsTask();
        release.SetResult();

        Assert.That((await first).HybridFullValueWindow, Is.EqualTo(TimeSpan.FromMinutes(1)));
        Assert.That((await second).HybridFullValueWindow, Is.EqualTo(TimeSpan.FromMinutes(2)));
    }

    /// <summary>
    /// A failed read faults its joiners - which is what each of them would have
    /// observed on its own - and must not poison the tree, or one transient
    /// registry fault would become a permanent per-tree history outage healed
    /// only by a restart.
    /// </summary>
    [Test]
    public async Task A_failed_flight_faults_its_joiners_and_does_not_poison_the_next_read()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var entered = 0;

        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ =>
        {
            var n = Interlocked.Increment(ref calls);
            return ServeAsync(n);
        });

        async Task<TreeRegistryEntry?> ServeAsync(int n)
        {
            if (n == 1)
            {
                Interlocked.Increment(ref entered);
                await gate.Task;
                throw new InvalidOperationException("registry unavailable");
            }

            return new TreeRegistryEntry { HistoryRetentionMode = HistoryRetentionMode.FullValue };
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var resolver = new LatticeOptionsResolver(factory, monitor);
        var treeId = UniqueTree();

        var first = resolver.GetHistoryRetentionAsync(treeId, HybridWindow).AsTask();
        var spun = 0;
        while (Volatile.Read(ref entered) == 0 && spun++ < 500)
            await Task.Delay(10);

        var joiner = resolver.GetHistoryRetentionAsync(treeId, HybridWindow).AsTask();
        gate.SetResult();

        Assert.That(async () => await first, Throws.InstanceOf<InvalidOperationException>());
        Assert.That(async () => await joiner, Throws.InstanceOf<InvalidOperationException>(),
            "a joiner must observe the same fault it would have observed on its own read.");

        var recovered = await resolver.GetHistoryRetentionAsync(treeId, HybridWindow);
        Assert.That(recovered.Mode, Is.EqualTo(HistoryRetentionMode.FullValue),
            "one failed read must not poison the tree's subsequent history resolutions.");
    }
}
