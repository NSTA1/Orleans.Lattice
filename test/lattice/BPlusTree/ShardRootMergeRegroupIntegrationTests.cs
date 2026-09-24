using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for the shard root's batch-merge re-grouping (issue
/// #2125): a merge batch that was grouped by leaf <b>before</b> a concurrent
/// split of one of its target leaves, and dispatched <b>after</b> it, must land
/// every key on the leaf that declares it - with no leaf-to-leaf span forward
/// and no fail-open local commit - and every key must survive a cold rebuild.
/// <para>
/// The window is opened deterministically. <c>MergeManyAsync</c> groups the
/// whole batch up front and then dispatches the groups one at a time, so an
/// incoming call filter parks the <b>first</b> group's leaf call. While the
/// shard root's merge turn is suspended on it, an interleaving
/// <c>SetManyAsync</c> (the shard root's only <c>[AlwaysInterleave]</c> write)
/// splits the <b>second</b> group's target leaf several times. Releasing the
/// gate then dispatches that second group against routing that has moved
/// through it. Before #2125 the group went whole to its pivot key's leaf and
/// survived only because the leaf span-forwarded every key past the new
/// boundaries; the forward observation below is what turns that red.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class ShardRootMergeRegroupIntegrationTests
{
    private const int MaxLeafKeys = 4;

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
        MergeGate.Reset();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [TearDown]
    public void Disarm() => MergeGate.Reset();

    private IBPlusLeafGrain Leaf(GrainId id) => _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);

    [Test]
    public async Task Batch_grouped_before_a_concurrent_split_lands_every_key_on_its_declaring_leaf_and_survives_rebuild()
    {
        var treeName = $"merge-regroup-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = MaxLeafKeys });
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");

        for (var i = 0; i < 20; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"seed{i}"));

        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Has.Count.GreaterThanOrEqualTo(2),
            "precondition: the tree must hold several leaves, so the merge takes the grouped path");
        var firstLeaf = chain[0];
        var lastLeaf = chain[^1];

        // Group 1 lands on the leftmost leaf (ordinally below every seed key);
        // group 2 is interleaved, key by key, with the keys the concurrent
        // writer uses to split the rightmost leaf, so the new boundaries
        // necessarily fall inside group 2.
        const string firstGroupKey = "a-first";
        var secondGroupKeys = Enumerable.Range(0, 20).Select(i => $"z{2 * i + 1:D3}").ToArray();
        var splitKeys = Enumerable.Range(0, 20).Select(i => $"z{2 * i:D3}").ToArray();

        var stamp = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.Ticks, Counter = 0 };
        var batch = new Dictionary<string, LwwValue<byte[]>>
        {
            [firstGroupKey] = new() { Value = Encoding.UTF8.GetBytes("v-" + firstGroupKey), Timestamp = stamp },
        };
        foreach (var key in secondGroupKeys)
            batch[key] = new() { Value = Encoding.UTF8.GetBytes("v-" + key), Timestamp = stamp };

        var failOpenCommits = 0L;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.LeafSpanFailOpenCommits,
            l => l.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref failOpenCommits, value)));

        MergeGate.Arm(firstLeaf, shard.GetGrainId(), secondGroupKeys);
        var mergeTask = shard.MergeManyAsync(batch);

        await MergeGate.Parked.WaitAsync(TimeSpan.FromSeconds(30));

        for (var i = 0; i < splitKeys.Length; i += 2)
        {
            await shard.SetManyAsync(
            [
                new KeyValuePair<string, byte[]>(splitKeys[i], Encoding.UTF8.GetBytes("s-" + splitKeys[i])),
                new KeyValuePair<string, byte[]>(splitKeys[i + 1], Encoding.UTF8.GetBytes("s-" + splitKeys[i + 1])),
            ]).WaitAsync(TimeSpan.FromSeconds(30));
        }

        // Walked leaf to leaf, not through the shard root, whose merge turn is
        // still parked on the first group.
        var declaringLeaves = new HashSet<GrainId>();
        foreach (var key in secondGroupKeys)
            declaringLeaves.Add(await ResolveDeclaringLeafAsync(lastLeaf, key));
        Assert.That(declaringLeaves, Has.Count.GreaterThanOrEqualTo(2),
            "precondition: the concurrent split must move a boundary through the already-grouped second group");

        MergeGate.Release();
        await mergeTask.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Multiple(() =>
        {
            Assert.That(MergeGate.ForwardHops, Is.Empty,
                "no key of the re-grouped batch may reach its leaf through a leaf-to-leaf span forward: "
                + "the shard root must re-route a group dispatched after routing moved");
            Assert.That(Interlocked.Read(ref failOpenCommits), Is.Zero,
                "no key of the batch may be committed fail-open on a leaf that does not declare it");
        });

        await AssertEveryKeyOnItsDeclaringLeafAsync(shard, router, batch, "after the merge");

        foreach (var leafId in await WalkChainAsync(shard))
            await Leaf(leafId).RebuildProjectionFromWalAsync();
        await AssertEveryKeyOnItsDeclaringLeafAsync(shard, router, batch, "after a full WAL rebuild of every leaf");

        foreach (var leafId in await WalkChainAsync(shard))
            await Leaf(leafId).ForceDeactivateAsync();
        await Task.Delay(500);
        await AssertEveryKeyOnItsDeclaringLeafAsync(shard, router, batch, "after every leaf reactivated cold");

        await LeafChainTiling.AssertTilesAsync(_cluster.GrainFactory, shard, "after the re-grouped merge");
    }

    private async Task AssertEveryKeyOnItsDeclaringLeafAsync(
        IShardRootGrain shard, ILattice router, Dictionary<string, LwwValue<byte[]>> batch, string when)
    {
        var head = (await shard.GetLeftmostLeafIdAsync())!.Value;
        foreach (var (key, lww) in batch)
        {
            var declaring = await ResolveDeclaringLeafAsync(head, key);
            Assert.That(await Leaf(declaring).GetAsync(key), Is.EqualTo(lww.Value),
                $"{when}: '{key}' must be held by the leaf whose declared span owns it");
            Assert.That(await router.GetAsync(key), Is.EqualTo(lww.Value),
                $"{when}: '{key}' must be readable end to end through the router");
        }
    }

    private async Task<List<GrainId>> WalkChainAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var current = await shard.GetLeftmostLeafIdAsync();
        while (current is not null && chain.Count < 256)
        {
            chain.Add(current.Value);
            current = await Leaf(current.Value).GetNextSiblingAsync();
        }

        return chain;
    }

    private async Task<GrainId> ResolveDeclaringLeafAsync(GrainId from, string key)
    {
        var current = from;
        for (var hop = 0; hop < 256; hop++)
        {
            var range = await Leaf(current).GetKeyRangeAsync();
            if (SplitBoundary.Owns(key, range.LowKeyInclusive, range.HighKeyExclusive))
                return current;

            var next = await Leaf(current).GetNextSiblingAsync();
            Assert.That(next, Is.Not.Null, $"the chain ended before any leaf declared '{key}'");
            current = next!.Value;
        }

        Assert.Fail($"no leaf in the chain declares '{key}' within a bounded walk");
        return default;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, MergeGateFilter>();
        }
    }

    /// <summary>
    /// Control state for the parked first-group call and the forward
    /// observation. Static because the TestingHost silo runs in-process;
    /// disarmed after every test so it cannot leak.
    /// </summary>
    private static class MergeGate
    {
        private static TaskCompletionSource _parked = NewTcs();
        private static TaskCompletionSource _release = NewTcs();
        private static GrainId? _parkLeaf;
        private static GrainId? _shardRoot;
        private static HashSet<string> _watchedKeys = [];
        private static int _parkedOnce;

        internal static ConcurrentBag<string> ForwardHops { get; private set; } = [];

        internal static Task Parked => _parked.Task;

        internal static Task ReleaseTask => _release.Task;

        internal static void Arm(GrainId parkLeaf, GrainId shardRoot, IEnumerable<string> watchedKeys)
        {
            _parked = NewTcs();
            _release = NewTcs();
            ForwardHops = [];
            _watchedKeys = new HashSet<string>(watchedKeys, StringComparer.Ordinal);
            _shardRoot = shardRoot;
            Volatile.Write(ref _parkedOnce, 0);
            _parkLeaf = parkLeaf;
        }

        internal static void Release() => _release.TrySetResult();

        internal static void Reset()
        {
            _parkLeaf = null;
            _shardRoot = null;
            _release.TrySetResult();
        }

        internal static bool ShouldPark(GrainId target) =>
            _parkLeaf is { } leaf && leaf == target && Interlocked.Exchange(ref _parkedOnce, 1) == 0;

        internal static void SignalParked() => _parked.TrySetResult();

        internal static void ObserveMerge(GrainId? source, GrainId target, Dictionary<string, LwwValue<byte[]>>? entries)
        {
            if (_shardRoot is not { } root || entries is null || source == root)
                return;

            foreach (var key in entries.Keys)
            {
                if (_watchedKeys.Contains(key))
                    ForwardHops.Add($"{key}: {source} -> {target}");
            }
        }

        private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    /// Parks the first leaf <c>MergeManyAsync</c> addressed to the armed leaf,
    /// and records every leaf <c>MergeManyAsync</c> carrying a watched key that
    /// did not come from the shard root - a span forward.
    /// </summary>
    private sealed class MergeGateFilter : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (context.InterfaceMethod?.Name == nameof(IBPlusLeafGrain.MergeManyAsync)
                && context.InterfaceMethod?.DeclaringType == typeof(IBPlusLeafGrain)
                && context.TargetContext is { } target)
            {
                var entries = context.Request.GetArgumentCount() > 0
                    ? context.Request.GetArgument(0) as Dictionary<string, LwwValue<byte[]>>
                    : null;
                MergeGate.ObserveMerge(context.SourceId, target.GrainId, entries);

                if (MergeGate.ShouldPark(target.GrainId))
                {
                    MergeGate.SignalParked();
                    await MergeGate.ReleaseTask.WaitAsync(TimeSpan.FromSeconds(90));
                }
            }

            await context.Invoke();
        }
    }
}
