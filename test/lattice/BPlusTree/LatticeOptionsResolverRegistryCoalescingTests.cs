using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Single-flight coalescing of the registry read
/// <see cref="LatticeOptionsResolver.ResolveAsync"/> performs for a non-system
/// tree (issue #2768).
/// <para>
/// <b>The measurement that produced this fixture.</b> Every cold leaf
/// activation resolves options exactly once, and that resolve was an
/// uncoalesced <see cref="ILatticeRegistry.GetEntryAsync"/> call against a
/// non-reentrant cluster singleton. N concurrent cold activations therefore
/// took N turns in series inside a fixed activation deadline. Reproduced in
/// process against a serialised registry, 200 concurrent leaves produced 200
/// registry calls, 16 successes and 184 cancellations - the field signature
/// exactly, including its 100%/0% split - while the replay concurrency gate sat
/// completely uncontended throughout.
/// </para>
/// <para>
/// <b>Why this is a fix and not a tuning.</b> Nothing here is sized, capped, or
/// fitted to a host: the coalescing window is the duration of a round trip that
/// is already happening, so it adapts to whatever the registry's latency turns
/// out to be, on any host, with no constant to choose.
/// </para>
/// <para>
/// <b>Both directions of the not-a-cache property are pinned below</b>
/// (<see cref="Concurrent_resolves_of_one_tree_share_a_single_registry_read"/>
/// and
/// <see cref="Sequential_resolves_each_read_the_registry_afresh"/>), because
/// only the pair distinguishes coalescing from memoisation. Coalescing has no
/// staleness window and no expiry constant; a cache would have both, and would
/// additionally freeze
/// <see cref="TreeRegistryEntry.MaxCacheValueBytes"/>, which is documented as
/// runtime-mutable.
/// </para>
/// </summary>
[TestFixture]
public class LatticeOptionsResolverRegistryCoalescingTests
{
    [SetUp]
    public void Setup() => LatticeOptionsResolver.ResetWarnedLatchedTreesForTests();

    private static string UniqueTree() => $"tree-coalesce-{Guid.NewGuid():N}";

    private static TreeRegistryEntry PinnedEntry() => new()
    {
        MaxLeafKeys = 128,
        MaxInternalChildren = 128,
        ShardCount = 1,
    };

    /// <summary>
    /// Builds a resolver over a registry whose <c>GetEntryAsync</c> is gated on
    /// <paramref name="release"/>, so a test can hold every caller inside the
    /// round trip and observe how many round trips there are.
    /// </summary>
    private static (LatticeOptionsResolver Resolver, Func<int> Calls, Func<int> Entered)
        BuildGated(TaskCompletionSource release)
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
            return PinnedEntry();
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
    /// THE load-bearing property. Callers that arrive while a read is in flight
    /// join it instead of issuing their own, which is what collapses a cold
    /// start's N serialised round trips to one.
    /// </summary>
    [Test]
    public async Task Concurrent_resolves_of_one_tree_share_a_single_registry_read()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (resolver, calls, entered) = BuildGated(release);
        var treeId = UniqueTree();

        var first = resolver.ResolveAsync(treeId);

        // Wait until the first read is genuinely inside the registry call, so a
        // pass cannot come from the followers merely being scheduled late.
        var spun = 0;
        while (entered() == 0 && spun++ < 500)
            await Task.Delay(10);
        Assert.That(entered(), Is.EqualTo(1), "the first resolve must be inside the registry call");

        var followers = Enumerable.Range(0, 32)
            .Select(_ => resolver.ResolveAsync(treeId))
            .ToArray();

        Assert.That(calls(), Is.EqualTo(1),
            "32 resolves arriving while a read is in flight must join it, not queue 32 more turns "
            + "behind a non-reentrant registry singleton.");

        release.SetResult();
        var resolved = await Task.WhenAll(followers.Prepend(first));

        Assert.That(calls(), Is.EqualTo(1), "joining must not issue a second read on completion.");
        Assert.That(resolved, Has.All.Not.Null,
            "every joined caller must receive the shared result, not a null it would have to re-read.");
    }

    /// <summary>
    /// The other direction, and the reason coalescing was chosen over a cache.
    /// A caller arriving after a flight has finished must start a fresh read.
    /// <para>
    /// Without this assertion the fixture above is equally satisfied by a
    /// permanent memo, which would be a behavioural change to every consumer of
    /// a runtime-mutable registry entry rather than the latency fix intended.
    /// </para>
    /// </summary>
    [Test]
    public async Task Sequential_resolves_each_read_the_registry_afresh()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        release.SetResult();
        var (resolver, calls, _) = BuildGated(release);
        var treeId = UniqueTree();

        for (var i = 0; i < 8; i++)
            _ = await resolver.ResolveAsync(treeId);

        Assert.That(calls(), Is.EqualTo(8),
            "Nothing is retained between resolves. Eight sequential resolves must cost eight reads; "
            + "any smaller number means a value was cached, which would freeze a registry entry that "
            + "is documented as runtime-mutable.");
    }

    /// <summary>
    /// Coalescing is keyed by tree, so two trees resolving at once must not
    /// share a result. A shared flight across trees would hand one tree the
    /// other's structural pin.
    /// </summary>
    [Test]
    public async Task Concurrent_resolves_of_different_trees_do_not_share_a_read()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var (resolver, calls, entered) = BuildGated(release);

        var a = resolver.ResolveAsync(UniqueTree());
        var b = resolver.ResolveAsync(UniqueTree());

        var spun = 0;
        while (entered() < 2 && spun++ < 500)
            await Task.Delay(10);

        Assert.That(calls(), Is.EqualTo(2),
            "two distinct trees must each get their own read.");

        release.SetResult();
        await Task.WhenAll(a, b);
    }

    /// <summary>
    /// A failing read faults every caller that joined it - which is what each
    /// of them would have observed anyway - and must not poison the tree. The
    /// next resolve starts a clean flight.
    /// <para>
    /// This is the clause that keeps coalescing self-healing. A retained failed
    /// flight would convert one transient registry fault into a permanent
    /// resolve failure for that tree, requiring a process restart - exactly the
    /// operator intervention this work exists to remove.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_failed_flight_faults_its_joiners_and_does_not_poison_the_next_resolve()
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

            return PinnedEntry();
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var resolver = new LatticeOptionsResolver(factory, monitor);
        var treeId = UniqueTree();

        var first = resolver.ResolveAsync(treeId);
        var spun = 0;
        while (Volatile.Read(ref entered) == 0 && spun++ < 500)
            await Task.Delay(10);

        var joiner = resolver.ResolveAsync(treeId);
        gate.SetResult();

        Assert.That(async () => await first, Throws.InstanceOf<InvalidOperationException>());
        Assert.That(async () => await joiner, Throws.InstanceOf<InvalidOperationException>(),
            "a joiner must observe the same fault it would have observed on its own read.");

        var recovered = await resolver.ResolveAsync(treeId);
        Assert.That(recovered, Is.Not.Null,
            "The tree must not be poisoned by one failed read. A retained failure would turn a "
            + "transient registry fault into a permanent per-tree outage healed only by a restart.");
    }
}
