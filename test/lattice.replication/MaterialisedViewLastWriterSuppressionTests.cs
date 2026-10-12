using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for issue #4885. The commit log is appended in arrival order, not
/// HLC order: a replicated write that is older than one the source already holds
/// can be logged after it. The source resolves the pair by last-writer-wins and
/// keeps the newer write; the view must agree rather than install whichever write
/// it drained last. Covers point sets and deletes, filter exit, a re-keyed
/// projection, matched and unconstrained range deletes, aggregation membership and
/// retraction, and an accumulative history view, which records every revision.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewLastWriterSuppressionTests
{
    private const string Origin = "remote-origin";
    private const string OtherOrigin = "remote-origin-b";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    public enum LateWrite
    {
        OlderSetAfterNewerSet,
        OlderSetAfterNewerBatch,
        OlderDeleteAfterNewerSet,
        OlderSetAfterNewerDelete,
        OlderSetAfterNewerFilterExit,
        OlderMatchedRangeAfterNewerSet,
        OlderUnconstrainedRangeAfterNewerSet,
    }

    private sealed record ViewPerson(int Age, string Tag);

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Person(int age, string tag) =>
        JsonLatticeSerializer<ViewPerson>.Default.Serialize(new ViewPerson(age, tag));

    private static byte[] Score(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static int ShardOf(string key) =>
        LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private async Task DrainToZeroAsync(string view)
    {
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"View '{view}' did not catch up to the source head.");
    }

    private async Task CreateAsync(ILattice source, string view, LatticeViewDefinition definition)
    {
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        await _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();
    }

    [Test]
    public async Task Filter_view_keeps_the_source_last_writer(
        [Values] LateWrite scenario,
        [Values] bool rekey)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = $"{(int)scenario}{(rekey ? "r" : "p")}";
        var tree = $"mv-lws-{suffix}-src";
        var view = $"mv-lws-{suffix}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        Func<string, string> viewKey = rekey ? static k => "v/" + k : static k => k;
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 18),
            keySelector: rekey ? viewKey : null,
            keySelectorVersion: rekey ? "v1" : null);
        await CreateAsync(source, view, new LatticeViewDefinition(view, projection));

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var key = $"lws-{suffix}-k";
        var sibling = $"lws-{suffix}-l";
        var rangeEnd = $"lws-{suffix}-z";

        // An older sibling inside the range, so the range delete still removes
        // what it genuinely supersedes.
        await apply.ApplySetAsync(sibling, Person(50, "sibling"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        if (scenario == LateWrite.OlderSetAfterNewerDelete)
        {
            await apply.ApplySetAsync(key, Person(50, "first"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        }

        await DrainToZeroAsync(view);

        switch (scenario)
        {
            case LateWrite.OlderSetAfterNewerBatch:
                var tx = Guid.NewGuid();
                await apply.ApplyPreparedSetAsync(
                    key, Person(40, "newer"), Hlc(200), OtherOrigin,
                    sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 1, atomicBatchIndex: 0);
                await apply.ApplyTxTerminalAsync(tx, committed: true, ShardOf(key), Hlc(201), OtherOrigin, atomicShardCount: 1);
                break;
            case LateWrite.OlderSetAfterNewerDelete:
                await apply.ApplyDeleteAsync(key, Hlc(200), OtherOrigin, sourceVectorClock: null);
                break;
            case LateWrite.OlderSetAfterNewerFilterExit:
                await apply.ApplySetAsync(key, Person(10, "minor"), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
                break;
            default:
                await apply.ApplySetAsync(key, Person(40, "newer"), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
                break;
        }

        await DrainToZeroAsync(view);

        switch (scenario)
        {
            case LateWrite.OlderDeleteAfterNewerSet:
                await apply.ApplyDeleteAsync(key, Hlc(100), Origin, sourceVectorClock: null);
                break;
            case LateWrite.OlderMatchedRangeAfterNewerSet:
                await apply.ApplyDeleteRangeAsync(key, rangeEnd, Hlc(100), Origin, sourceVectorClock: null, explicitMatchedKeys: [key, sibling]);
                break;
            case LateWrite.OlderUnconstrainedRangeAfterNewerSet:
                await apply.ApplyDeleteRangeAsync(key, rangeEnd, Hlc(100), Origin, sourceVectorClock: null);
                break;
            default:
                await apply.ApplySetAsync(key, Person(30, "older"), Hlc(100), Origin, sourceVectorClock: null, expiresAtTicks: 0);
                break;
        }

        await DrainToZeroAsync(view);

        var expected = scenario switch
        {
            LateWrite.OlderSetAfterNewerDelete or LateWrite.OlderSetAfterNewerFilterExit => null,
            _ => Person(40, "newer"),
        };
        var siblingSurvives = scenario is not (LateWrite.OlderMatchedRangeAfterNewerSet or LateWrite.OlderUnconstrainedRangeAfterNewerSet);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        var sourceValue = await source.GetAsync(key);
        var sourceSibling = await source.GetAsync(sibling);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(scenario == LateWrite.OlderSetAfterNewerFilterExit ? Person(10, "minor") : expected),
                "The source keeps the newer write by last-writer-wins.");
            Assert.That(await viewTree.GetAsync(viewKey(key)), Is.EqualTo(expected),
                "An older write logged after a newer one must not replace it in the view.");
            Assert.That(sourceSibling is not null, Is.EqualTo(siblingSurvives), "The source range delete removes the older sibling.");
            Assert.That(await viewTree.GetAsync(viewKey(sibling)) is not null, Is.EqualTo(siblingSurvives),
                "The view range delete still removes what it supersedes.");
        });
    }

    [Test]
    public async Task Unconstrained_range_delete_pages_listed_keys_exactly_under_slow_source_reads()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-lws-page-src";
        const string view = "mv-lws-page-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await CreateAsync(source, view, new LatticeViewDefinition(
            view,
            new PredicateLatticeViewProjection(LatticePredicateTranslator.Translate<ViewPerson>(p => p.Age >= 0))));

        // Three view pages and a short tail, with source-newer survivors straddling
        // both page boundaries the pass resumes across.
        const int count = 1100;
        int[] survivors = [0, 510, 511, 512, 513, 1022, 1023, 1024, 1025, count - 1];
        static string KeyOf(int i) => $"lws-page-{i:D5}";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        for (var offset = 0; offset < count; offset += 64)
        {
            await Task.WhenAll(Enumerable.Range(offset, Math.Min(64, count - offset)).Select(i =>
                apply.ApplySetAsync(KeyOf(i), Person(50, "old"), Hlc(50), Origin, sourceVectorClock: null, expiresAtTicks: 0)));
        }

        await DrainToZeroAsync(view);
        foreach (var i in survivors)
        {
            await apply.ApplySetAsync(KeyOf(i), Person(40, "newer"), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
        }

        await DrainToZeroAsync(view);
        await apply.ApplyDeleteRangeAsync(KeyOf(0), "lws-page-~", Hlc(100), Origin, sourceVectorClock: null);

        // Hold the first page's source read for a listed key, then land a row the
        // pass did not list between two listed keys it is about to delete. The
        // source holds that key at a version newer than the range delete, so it must
        // never leave the view: here it arrives in the view ahead of its own logged
        // write, as a row from another view writer (the cross-tree coordinator, or
        // out-of-band drift) would. A delete spanning the gap between listed keys
        // would remove it without confirming it against the source. The source logs
        // the unconstrained range once per shard, so later copies of it list the row
        // and must spare it on its newer source version.
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        const string unlisted = "lws-page-00100~";
        var gate = RawVersionReadHoldFilter.Arm(tree, KeyOf(100), TimeSpan.FromMilliseconds(40));
        var everAbsent = false;
        using var stopPolling = new CancellationTokenSource();
        Task? poller = null;
        try
        {
            var drain = DrainToZeroAsync(view);
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

            // Every page's per-shard reads are dispatched through a bounded fan-out:
            // with the held read outstanding, no more than one width of reads is
            // ever in flight however many shards the page spans.
            await Task.Delay(300);
            var peak = gate.MaxInFlight;
            Assert.That(peak, Is.InRange(1, BoundedFanOut.DefaultWidth),
                "Source version reads are bounded by the fan-out width.");
            await apply.ApplySetAsync(unlisted, Person(1, "unlisted"), Hlc(300), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
            using (ViewWriteContext.BeginScope())
            {
                await viewTree.SetAsync(unlisted, Person(1, "unlisted"));
            }

            poller = Task.Run(async () =>
            {
                using var pollScope = ViewReadContext.BeginScope();
                while (!stopPolling.IsCancellationRequested)
                {
                    if (await viewTree.GetAsync(unlisted) is null)
                    {
                        everAbsent = true;
                    }

                    await Task.Delay(5);
                }
            });

            gate.Release();
            await drain;
        }
        finally
        {
            RawVersionReadHoldFilter.Disarm(tree);
            stopPolling.Cancel();
            if (poller is not null)
            {
                await poller;
            }
        }

        var remaining = new List<string>();
        await foreach (var key in viewTree.ScanKeysAsync())
        {
            remaining.Add(key);
        }

        var expected = survivors.Select(KeyOf).Append(unlisted).Order(StringComparer.Ordinal).ToList();
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(gate.Reads, Is.GreaterThan(1), "Every page's source reads ran through the hold.");
            Assert.That(everAbsent, Is.False, "A row the pass did not list and confirm is never deleted.");
            Assert.That(remaining, Is.EqualTo(expected),
                "Every listed key the source range delete removed is gone, across every page boundary; "
                + "source-newer survivors and the unlisted row remain.");
            Assert.That(await source.ScanKeysAsync().CountAsync(), Is.EqualTo(survivors.Length + 1), "The source keeps only the newer writes.");
        });
    }

    [Test]
    public async Task Aggregation_membership_follows_the_source_last_writer([Values] bool lateDelete)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = lateDelete ? "d" : "s";
        var tree = $"mv-lws-agg-{suffix}-src";
        var view = $"mv-lws-agg-{suffix}-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new AggregationLatticeViewProjection(
            AggregationKind.Sum,
            static v => JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(v)!.Team,
            "v1",
            valueSelector: static v => JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(v)!.Score);
        await CreateAsync(source, view, new LatticeViewDefinition(view, projection));

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var key = $"lws-agg-{suffix}-k";
        await apply.ApplySetAsync(key, Score("blue", 7), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        if (lateDelete)
        {
            await apply.ApplyDeleteAsync(key, Hlc(100), Origin, sourceVectorClock: null);
        }
        else
        {
            await apply.ApplySetAsync(key, Score("red", 5), Hlc(100), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        }

        await DrainToZeroAsync(view);

        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        var red = await viewTree.GetAsync("red");
        var blue = await viewTree.GetAsync("blue");
        await Assert.MultipleAsync(() =>
        {
            Assert.That(blue is null ? (double?)null : LatticeAggregationValue.DecodeDouble(blue), Is.EqualTo(7),
                "The older write must not retract the newer membership.");
            Assert.That(red is null ? 0 : LatticeAggregationValue.DecodeDouble(red), Is.EqualTo(0),
                "The older write must not add a stale membership.");
            return Task.CompletedTask;
        });
    }

    [Test]
    public async Task History_view_still_records_an_older_revision_logged_later()
    {
        var tree = "mv-lws-hist-src";
        var view = "mv-lws-hist-view";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, view, LatticeHistoryView.Definition(view, _fixture.SiloServices));
        await _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view).EnsureActiveAsync();

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        const string key = "lws-hist-k";
        await apply.ApplySetAsync(key, Person(40, "newer"), Hlc(200), OtherOrigin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);
        await apply.ApplySetAsync(key, Person(30, "older"), Hlc(100), Origin, sourceVectorClock: null, expiresAtTicks: 0);
        await DrainToZeroAsync(view);

        var codec = _fixture.SiloServices.GetRequiredService<HistoryRowCodec>();
        var timestamps = new List<HybridLogicalClock>();
        using (ViewReadContext.BeginScope())
        {
            var viewTree = await _fixture.ActiveViewTreeAsync(view);
            await foreach (var entry in viewTree.ScanEntriesAsync())
            {
                var row = codec.Decode(entry.Value);
                if (row.SourceKey == key)
                {
                    timestamps.Add(row.Timestamp);
                }
            }
        }

        Assert.That(timestamps, Is.EquivalentTo(new[] { Hlc(200), Hlc(100) }),
            "A history view records every revision, including one the source superseded.");
    }
}