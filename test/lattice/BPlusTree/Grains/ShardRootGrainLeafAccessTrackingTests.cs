using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the shard root's opt-in leaf-access tracking and post-restart
/// leaf-cache pre-warm (issue #332). Covers the default-off posture, the
/// read-path recording hook, the throttled model persistence, and the bounded
/// best-effort pre-warm fan-out.
/// </summary>
[TestFixture]
public class ShardRootGrainLeafAccessTrackingTests
{
    private const string ShardKey = "prewarm-tree/0";

    private sealed record Harness(
        ShardRootGrain Grain,
        FakePersistentState<ShardRootState> State,
        ILeafCacheGrain Cache,
        GrainId LeafId);

    private static Harness CreateGrain(
        int preWarmCount = 0,
        int flushIntervalMs = LatticeOptions.DefaultLeafAccessModelFlushIntervalMs,
        FakePersistentState<ShardRootState>? state = null,
        ILeafCacheGrain? cache = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        state ??= new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "prewarm-tree-leaf-0");
        state.State.RootNodeId ??= leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.CountAsync().Returns(Task.FromResult(0));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        cache ??= Substitute.For<ILeafCacheGrain>();
        cache.PreWarmAsync().Returns(Task.CompletedTask);
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        cache.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var options = new LatticeOptions
        {
            LeafCachePreWarmCount = preWarmCount,
            LeafAccessModelFlushIntervalMs = flushIntervalMs,
        };
        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: factory);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness(grain, state, cache, leafId);
    }

    // ---- default-off posture

    [Test]
    public void LeafCachePreWarmCount_defaults_to_zero()
    {
        Assert.That(new LatticeOptions().LeafCachePreWarmCount, Is.Zero);
    }

    [Test]
    public void LeafAccessModelFlushIntervalMs_defaults_to_thirty_seconds()
    {
        Assert.That(new LatticeOptions().LeafAccessModelFlushIntervalMs, Is.EqualTo(30_000));
    }

    [Test]
    public void Default_constants_match_the_declared_defaults()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeOptions.DefaultLeafCachePreWarmCount, Is.Zero);
            Assert.That(LatticeOptions.DefaultLeafAccessModelFlushIntervalMs, Is.EqualTo(30_000));
            Assert.That(LatticeOptions.MaxLeafCachePreWarmCount, Is.EqualTo(64));
        });
    }

    [Test]
    public async Task Reads_record_nothing_when_pre_warm_is_disabled()
    {
        var harness = CreateGrain(preWarmCount: 0);

        for (var i = 0; i < 10; i++) await harness.Grain.GetAsync($"k{i}");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel, Is.Null,
            "a disabled shard must never persist an access model");
    }

    [Test]
    public async Task WarmUp_issues_no_pre_warm_calls_when_disabled()
    {
        var harness = CreateGrain(preWarmCount: 0);
        harness.State.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [harness.LeafId.ToString()],
            Visits = [10L],
        };

        await harness.Grain.WarmUpAsync();

        await harness.Cache.DidNotReceive().PreWarmAsync();
    }

    // ---- recording on the read path

    [Test]
    public async Task Reads_accumulate_an_access_model_when_enabled()
    {
        var harness = CreateGrain(preWarmCount: 4);

        await harness.Grain.GetAsync("k1");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        var snapshot = harness.State.State.LeafAccessModel;
        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Is.Not.Null);
            Assert.That(snapshot!.Leaves, Has.Count.EqualTo(1));
            Assert.That(snapshot.Leaves[0], Is.EqualTo(harness.LeafId.ToString()));
            Assert.That(snapshot.Visits[0], Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ExistsAsync_also_feeds_the_access_model()
    {
        var harness = CreateGrain(preWarmCount: 4);

        await harness.Grain.ExistsAsync("k1");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel!.Visits[0], Is.EqualTo(1));
    }

    [Test]
    public async Task GetManyAsync_also_feeds_the_access_model()
    {
        var harness = CreateGrain(preWarmCount: 4);
        harness.Cache.GetManyAsync(Arg.Any<List<string>>())
            .Returns(Task.FromResult(new Dictionary<string, byte[]>()));

        await harness.Grain.GetManyAsync(["k1", "k2"]);
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel!.Visits[0], Is.EqualTo(1));
    }

    [Test]
    public async Task Repeated_reads_accumulate_visits_on_the_same_leaf()
    {
        var harness = CreateGrain(preWarmCount: 4);

        for (var i = 0; i < 7; i++) await harness.Grain.GetAsync($"k{i}");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel!.Visits[0], Is.EqualTo(7));
    }

    // ---- throttled persistence

    [Test]
    public async Task Reads_do_not_persist_the_model_synchronously()
    {
        // The read path must never await a storage write for the access model.
        // In the test harness the grain-runtime timer cannot register against a
        // substituted IGrainContext, so the only remaining flush is the one on
        // deactivation - which is exactly the coalescing contract.
        var harness = CreateGrain(preWarmCount: 4);

        var writesBefore = harness.State.WriteCount;
        for (var i = 0; i < 25; i++) await harness.Grain.GetAsync($"k{i}");

        Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore),
            "reads must not trigger a shard-root state write for the access model");
    }

    [Test]
    public async Task Deactivation_flushes_the_pending_model_exactly_once()
    {
        var harness = CreateGrain(preWarmCount: 4);
        await harness.Grain.GetAsync("k1");

        var writesBefore = harness.State.WriteCount;
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);
        var afterFirst = harness.State.WriteCount;

        // A second deactivation has nothing dirty left to write.
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(writesBefore + 1));
            Assert.That(harness.State.WriteCount, Is.EqualTo(afterFirst));
        });
    }

    [Test]
    public async Task Deactivation_writes_nothing_when_no_read_was_observed()
    {
        var harness = CreateGrain(preWarmCount: 4);

        var writesBefore = harness.State.WriteCount;
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore));
            Assert.That(harness.State.State.LeafAccessModel, Is.Null);
        });
    }

    [Test]
    public async Task A_zero_flush_interval_still_persists_on_deactivation()
    {
        var harness = CreateGrain(preWarmCount: 4, flushIntervalMs: 0);

        await harness.Grain.GetAsync("k1");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel, Is.Not.Null);
    }

    // ---- pre-warm fan-out

    [Test]
    public async Task WarmUp_primes_the_ranked_leaves_from_the_persisted_model()
    {
        var otherLeaf = GrainId.Create("leaf", "prewarm-tree-leaf-1");
        var state = new FakePersistentState<ShardRootState>();
        state.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [GrainId.Create("leaf", "prewarm-tree-leaf-0").ToString(), otherLeaf.ToString()],
            Visits = [30L, 20L],
        };

        var harness = CreateGrain(preWarmCount: 2, state: state);

        await harness.Grain.WarmUpAsync();

        await harness.Cache.Received(2).PreWarmAsync();
    }

    [Test]
    public async Task WarmUp_primes_at_most_the_configured_number_of_leaves()
    {
        var leaves = Enumerable.Range(0, 10)
            .Select(i => GrainId.Create("leaf", $"prewarm-tree-leaf-{i}").ToString())
            .ToList();
        var state = new FakePersistentState<ShardRootState>();
        state.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = leaves,
            Visits = [.. Enumerable.Range(0, 10).Select(i => (long)(100 - i))],
        };

        var harness = CreateGrain(preWarmCount: 3, state: state);

        await harness.Grain.WarmUpAsync();

        await harness.Cache.Received(3).PreWarmAsync();
    }

    [Test]
    public async Task WarmUp_issues_no_pre_warm_calls_when_the_model_is_empty()
    {
        var harness = CreateGrain(preWarmCount: 8);

        await harness.Grain.WarmUpAsync();

        await harness.Cache.DidNotReceive().PreWarmAsync();
    }

    [Test]
    public async Task WarmUp_survives_a_failing_pre_warm_call()
    {
        // Pre-warm is best-effort by contract: a leaf that has been merged away
        // or a transient storage fault must not fail WarmUpAsync.
        var cache = Substitute.For<ILeafCacheGrain>();
        cache.PreWarmAsync().Returns(Task.FromException(new InvalidOperationException("boom")));

        var state = new FakePersistentState<ShardRootState>();
        state.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [GrainId.Create("leaf", "prewarm-tree-leaf-0").ToString()],
            Visits = [30L],
        };

        var harness = CreateGrain(preWarmCount: 4, state: state, cache: cache);

        Assert.DoesNotThrowAsync(async () => await harness.Grain.WarmUpAsync());
        await cache.Received().PreWarmAsync();
    }

    [Test]
    public async Task WarmUp_leaves_the_restored_model_clean_so_it_is_not_rewritten()
    {
        var state = new FakePersistentState<ShardRootState>();
        state.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [GrainId.Create("leaf", "prewarm-tree-leaf-0").ToString()],
            Visits = [30L],
        };
        var harness = CreateGrain(preWarmCount: 4, state: state);

        await harness.Grain.WarmUpAsync();
        var writesAfterWarmUp = harness.State.WriteCount;
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.WriteCount, Is.EqualTo(writesAfterWarmUp),
            "a warm-up that only read the model must not dirty it");
    }

    [Test]
    public async Task A_restart_cycle_carries_the_ranking_across_activations()
    {        // The whole point of the feature: the model an activation builds from
        // live reads must be the model the *next* activation pre-warms from.
        var state = new FakePersistentState<ShardRootState>();
        var first = CreateGrain(preWarmCount: 4, state: state);
        await first.Grain.GetAsync("k1");
        await ((IGrainBase)first.Grain).OnDeactivateAsync(default, default);

        Assert.That(state.State.LeafAccessModel, Is.Not.Null, "the first activation must persist a model");

        // A fresh grain over the same durable state stands in for the restart.
        var second = CreateGrain(preWarmCount: 4, state: state);
        await second.Grain.WarmUpAsync();

        await second.Cache.Received(1).PreWarmAsync();
    }

    [Test]
    public async Task Warm_up_leaves_tracking_online_and_additive_over_the_restored_counts()
    {
        // Warm-up is normally the FIRST call on a fresh activation, so it - not
        // the first read - is what brings tracking online. Pin that the reads
        // which follow are still recorded, and that they ADD to the restored
        // counts rather than replacing them; a restore that reset the counters
        // would quietly turn the chain into a single-activation recency list.
        //
        // Note this fixture cannot observe the coalescing flush timer: a
        // substituted IGrainContext has no grain runtime to register one
        // against. That warm-up also arms the timer (so a warm-up-first
        // activation does not depend solely on a clean deactivation to persist)
        // is covered by LeafCachePreWarmIntegrationTests against a real silo.
        var state = new FakePersistentState<ShardRootState>();
        state.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [GrainId.Create("leaf", "prewarm-tree-leaf-0").ToString()],
            Visits = [5L],
        };
        var harness = CreateGrain(preWarmCount: 4, state: state);

        await harness.Grain.WarmUpAsync();
        for (var i = 0; i < 3; i++) await harness.Grain.GetAsync($"k{i}");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel!.Visits[0], Is.EqualTo(8),
            "the 5 restored visits plus the 3 observed after warm-up");
    }
}
