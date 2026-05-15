using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TerminalFanOutResolver"/>: BFS-expansion
/// of a seed shard set against each shard's
/// <see cref="IShardRootGrain.GetSplitForwardTargetsAsync"/>. The
/// resolver replaces the previous recursive
/// <c>ForwardSplitTerminalAsync</c> hop on the receiving shard root,
/// flattening the transitive closure of split destinations into a
/// single parallel fan-out at the saga (or replication-apply) layer.
/// </summary>
[TestFixture]
public class TerminalFanOutResolverTests
{
    private const string TreeId = "resolver-tree";

    /// <summary>
    /// Wires up an <see cref="IGrainFactory"/> mock whose
    /// <c>GetGrain&lt;IShardRootGrain&gt;("{TreeId}/{idx}")</c>
    /// returns a substitute with
    /// <see cref="IShardRootGrain.GetSplitForwardTargetsAsync"/>
    /// stubbed to <paramref name="topology"/>'s entry for that index
    /// (or an empty list when no entry exists).
    /// </summary>
    private static IGrainFactory WireTopology(Dictionary<int, List<int>> topology)
    {
        var factory = Substitute.For<IGrainFactory>();
        foreach (var idx in topology.Keys.Concat(topology.Values.SelectMany(v => v)).Distinct())
        {
            var shard = Substitute.For<IShardRootGrain>();
            var targets = topology.TryGetValue(idx, out var t) ? t : new List<int>();
            shard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>(targets)));
            factory.GetGrain<IShardRootGrain>($"{TreeId}/{idx}").Returns(shard);
        }
        return factory;
    }

    [Test]
    public async Task ResolveTransitiveAsync_returns_seed_when_no_shard_forwards()
    {
        // No shard has any split destination → resolver returns the
        // seed verbatim (sorted ascending) with no new shards.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [],
            [1] = [],
            [2] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 2, 0, 1 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task ResolveTransitiveAsync_expands_single_hop_split_destination()
    {
        // Shard 0 has split to 1 (in-flight or completed) - resolver
        // expands {0} to {0, 1}.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [1],
            [1] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task ResolveTransitiveAsync_expands_multi_hop_cascading_chain()
    {
        // Cascading split: 0 → 1 → 2 → 3. The seed {0} must reach
        // every transitive destination - this is the exact scenario
        // that compounded into Orleans' 30s response timeout when the
        // forward was recursive per-shard. The resolver flattens it
        // into a wavefront BFS.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [1],
            [1] = [2],
            [2] = [3],
            [3] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public async Task ResolveTransitiveAsync_expands_branching_chain()
    {
        // Branching: 0 → {1, 2}; 1 → 3; 2 → 4. The resolver must reach
        // every leaf of the expansion tree.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [1, 2],
            [1] = [3],
            [2] = [4],
            [3] = [],
            [4] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1, 2, 3, 4 }));
    }

    [Test]
    public async Task ResolveTransitiveAsync_terminates_on_cycle()
    {
        // Pathological topology: 0 → 1 → 0 (a stale forward record
        // pointing back at the source). The visited-set cycle guard
        // must terminate the BFS without re-queueing 0, so the call
        // returns rather than looping forever.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [1],
            [1] = [0],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task ResolveTransitiveAsync_deduplicates_shared_destinations()
    {
        // Two seed shards both forward to the same destination. The
        // visited-set must collapse the duplicate so the destination
        // appears exactly once in the result and is queried for its
        // own forward targets exactly once (not twice).
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [2],
            [1] = [2],
            [2] = [3],
            [3] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0, 1 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 0, 1, 2, 3 }));

        // Verify shard 2 was queried only once even though both seed
        // shards forwarded to it (cycle/dedup guard works on shared
        // destinations as well as cycles).
        var shard2 = factory.GetGrain<IShardRootGrain>($"{TreeId}/2");
        await shard2.Received(1).GetSplitForwardTargetsAsync();
    }

    [Test]
    public async Task ResolveTransitiveAsync_returns_empty_for_empty_seed()
    {
        var factory = WireTopology(new Dictionary<int, List<int>>());

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, Array.Empty<int>(), CancellationToken.None);

        Assert.That(targets, Is.Empty);
    }

    [Test]
    public async Task ResolveTransitiveAsync_returns_sorted_ascending()
    {
        // Result list must be sorted ascending so caller iteration is
        // order-deterministic (assertion-friendly, persistence-stable).
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [5] = [3],
            [3] = [1],
            [1] = [],
            [10] = [],
        });

        var targets = await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 10, 5 }, CancellationToken.None);

        Assert.That(targets, Is.EqualTo(new[] { 1, 3, 5, 10 }));
    }

    [Test]
    public void ResolveTransitiveAsync_throws_on_null_grain_factory()
    {
        Assert.That(() => TerminalFanOutResolver.ResolveTransitiveAsync(
            grainFactory: null!, TreeId, new[] { 0 }, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ResolveTransitiveAsync_throws_on_null_tree_id()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, physicalTreeId: null!, new[] { 0 }, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ResolveTransitiveAsync_throws_on_null_seed()
    {
        var factory = Substitute.For<IGrainFactory>();
        Assert.That(() => TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, seed: null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ResolveTransitiveAsync_observes_cancellation_between_wavefronts()
    {
        // Cancellation is checked at the top of each wavefront, so a
        // multi-hop topology with a pre-cancelled token observes the
        // throw before the first RPC fires. A single-shard call with
        // a pre-cancelled token must throw OperationCanceledException
        // (or one of its subclasses, e.g. TaskCanceledException) and
        // never return a result.
        var factory = WireTopology(new Dictionary<int, List<int>>
        {
            [0] = [1],
            [1] = [],
        });
        var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(async () => await TerminalFanOutResolver.ResolveTransitiveAsync(
            factory, TreeId, new[] { 0 }, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
