using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Coverage for the per-tick folds <see cref="SharedMetricsSampler"/> runs
/// before it walks anything: the ordinal de-duplication of an explicit tree-id
/// list (which switches strategy above a threshold), the view-lag roll-up over
/// views whose lag was not sampled, and the degenerate arm of the saturated-tree
/// short circuit where the routing map cannot name a shard count. A dashboard
/// request names a handful of distinct trees, so none of these arms is on the
/// path an ordinary subscription takes.
/// </summary>
public partial class SharedMetricsSamplerTests
{
    [Test]
    public async Task Sample_de_duplicates_a_short_tree_id_list_ordinally()
    {
        // Below the threshold the de-duplication is a linear scan of the
        // survivors. A repeated id must be folded out, so each distinct tree is
        // walked exactly once and appears once in the map.
        var query = new RecordingStateQuery
        {
            Shards =
            {
                ["a"] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) },
                ["b"] = new[] { Shard(index: 0, depth: 1, liveKeys: 2, tombstones: 0, opsPerSecond: 1.0, splitting: false) },
            },
        };

        var sampler = CreateSampler(query, signal: null);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { "a", "b", "a", "b", "a" } },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Keys, Is.EquivalentTo(new[] { "a", "b" }));
            Assert.That(query.ShardSummaryCalls, Is.EqualTo(2),
                "a repeated tree id costs no extra shard walk");
        });
    }

    [Test]
    public async Task Sample_de_duplicates_a_long_tree_id_list_through_the_hash_path()
    {
        // Above the threshold the sampler switches from the linear survivor scan
        // to a hash set, so a pathologically long list cannot go quadratic. The
        // observable contract is identical: first-seen order, one walk per
        // distinct id.
        var query = new RecordingStateQuery();
        var distinct = Enumerable.Range(0, 20).Select(i => $"tree-{i:D2}").ToArray();
        foreach (var id in distinct)
        {
            query.Shards[id] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) };
        }

        // 40 ids, every one repeated once, so the request is comfortably above
        // the threshold both before and after de-duplication.
        var requested = distinct.Concat(distinct).ToArray();

        var sampler = CreateSampler(query, signal: null);
        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = requested },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(requested, Has.Length.EqualTo(40), "the request really is above the small-list threshold");
            Assert.That(result.Keys, Is.EquivalentTo(distinct));
            Assert.That(query.ShardSummaryCalls, Is.EqualTo(distinct.Length),
                "each distinct tree is walked once despite every id appearing twice");
        });
    }

    [Test]
    public async Task Sample_skips_a_saturated_tree_whose_routing_map_names_no_shard_count()
    {
        // The saturated short circuit serves a degraded row built from the
        // routing read. When routing cannot name a shard count (the tree is gone,
        // or its map is not resolvable) there is no degraded row to serve, so the
        // tree is omitted rather than reported with a fabricated count.
        var query = new RecordingStateQuery
        {
            Shards = { [TreeId] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) } },
        };

        var signal = new StubSaturationSignal();
        signal.Set(TreeId, WalSaturationState.Saturated);

        var sampler = CreateSampler(query, signal);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { TreeId } },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Empty, "no shard count means no degraded row");
            Assert.That(query.ShardCountCalls, Is.EqualTo(1), "the routing read was attempted");
            Assert.That(query.ShardSummaryCalls, Is.Zero, "and the per-shard walk stayed skipped");
        });
    }

    [Test]
    public async Task Sample_view_lag_roll_up_counts_a_view_whose_lag_was_not_sampled()
    {
        // Lag is nullable: the catalog reports null when it did not sample the
        // view. Such a view still counts towards the tree's view count, but
        // contributes nothing to the lag total - and a tree whose every view is
        // unsampled reports a null total rather than a confident zero.
        var query = new RecordingStateQuery
        {
            Shards =
            {
                ["measured"] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) },
                ["unmeasured"] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) },
            },
            Views =
            {
                new ViewStateSummary { ViewName = "v1", SourceTreeId = "measured", Lag = 5 },
                new ViewStateSummary { ViewName = "v2", SourceTreeId = "measured", Lag = null },
                new ViewStateSummary { ViewName = "v3", SourceTreeId = "unmeasured", Lag = null },
            },
        };

        var sampler = CreateSampler(query, signal: null);

        var result = await sampler.SampleOnceAsync(
            new TreeMetricsRequest { TreeIds = new[] { "measured", "unmeasured" }, IncludeViewLag = true },
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result["measured"].ViewCount, Is.EqualTo(2), "the unsampled view is still a view");
            Assert.That(result["measured"].ViewLagTotal, Is.EqualTo(5), "only the sampled lag is summed");
            Assert.That(result["unmeasured"].ViewCount, Is.EqualTo(1));
            Assert.That(result["unmeasured"].ViewLagTotal, Is.Null,
                "a tree with no sampled lag reports no total, not a zero total");
        });
    }

    [Test]
    public async Task Subscribe_keys_the_signature_on_every_request_flag()
    {
        // The signature renders the three request flags, so two subscriptions that
        // differ only in a flag must not coalesce. Exercises the set arm of each
        // flag, which the identity-focused subscription tests leave unset.
        var query = new RecordingStateQuery
        {
            Shards = { [TreeId] = new[] { Shard(index: 0, depth: 1, liveKeys: 1, tombstones: 0, opsPerSecond: 1.0, splitting: false) } },
            Views = { new ViewStateSummary { ViewName = "v1", SourceTreeId = TreeId, Lag = 1 } },
        };

        var sampler = CreateSampler(query, signal: null);

        await using var bare = new SubscriptionProbe(sampler, FlagRequest(false, false, false), token: "t");
        await bare.FirstAsync();

        await using var all = new SubscriptionProbe(sampler, FlagRequest(true, true, true), token: "t");
        var allMap = await all.FirstAsync();

        Assert.Multiple(() =>
        {
            Assert.That(allMap.ContainsKey(TreeId), Is.True);
            Assert.That(sampler.ActiveSamplerCount, Is.EqualTo(2),
                "requests differing only in their flags must run on separate sampling loops");
        });
    }

    [Test]
    public void Constructing_the_sampler_rejects_a_missing_dependency()
    {
        // The sampler is a singleton resolved once at host start, so a missing
        // dependency has to fail there and name itself. Resolving to a half-built
        // sampler instead would surface much later, as an absent metric feed.
        //
        // Scope of this guard, measured rather than assumed: replacing the
        // sampler's own `services` null-check with `services!` leaves this test
        // green, because the visibility filter it builds re-guards the same
        // argument and throws the identical ArgumentNullException. So the null
        // service provider is double-guarded and this test pins the observable
        // contract, not that one particular line is the thing enforcing it. The
        // query and options arms are singly guarded and are detected.
        var query = new RecordingStateQuery();
        var options = Options.Create(new LatticeApiStateOptions());
        var services = new ServiceCollection().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new SharedMetricsSampler(null!, options, services),
                Throws.ArgumentNullException.With.Property("ParamName").EqualTo("query"));
            Assert.That(
                () => new SharedMetricsSampler(query, null!, services),
                Throws.ArgumentNullException.With.Property("ParamName").EqualTo("apiOptions"));
            Assert.That(
                () => new SharedMetricsSampler(query, options, null!),
                Throws.ArgumentNullException.With.Property("ParamName").EqualTo("services"));
        });
    }

    private static TreeMetricsRequest FlagRequest(bool hotness, bool viewLag, bool systemTrees) => new()    {
        TreeIds = new[] { TreeId },
        IncludeShardHotness = hotness,
        IncludeViewLag = viewLag,
        IncludeSystemTrees = systemTrees,
        SampleInterval = TimeSpan.FromMilliseconds(40),
    };
}
