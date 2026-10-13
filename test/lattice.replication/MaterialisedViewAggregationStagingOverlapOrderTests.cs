using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression for overlapping atomic batches on one source key feeding an
/// aggregation view. The source resolves two batches that both prepared the
/// same key by last-writer-wins on the prepared HLC, whichever commits first.
/// The aggregation flush must agree: a batch that prepared earlier but
/// completes later must not re-group the key, or the group accumulators stay
/// permanently diverged from the source.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewAggregationStagingOverlapOrderTests
{
    private const string Origin = "remote-origin";

    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed record ScoreRecord(string Team, double Score);

    private static byte[] Record(string team, double score) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Serialize(new ScoreRecord(team, score));

    private static string Team(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Team;

    private static double Score(byte[] value) =>
        JsonLatticeSerializer<ScoreRecord>.Default.Deserialize(value)!.Score;

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static double? SumValue(byte[]? bytes) =>
        bytes is null ? null : LatticeAggregationValue.DecodeDouble(bytes);

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail("View did not catch up to the source head.");
    }

    [Test]
    public async Task An_older_batch_completing_after_a_newer_batch_does_not_regroup_the_source_key()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        const string tree = "mv-agg-overlap-src";
        const string view = "mv-agg-overlap-view";
        const string key = "agg-overlap-k";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new AggregationLatticeViewProjection(AggregationKind.Sum, Team, "v1", valueSelector: Score);
        var definition = new LatticeViewDefinition(view, projection);
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        await source.SetAsync("anchor", Record("blue", 1));
        await DrainToZeroAsync(maintainer);

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        var older = Guid.NewGuid();
        var newer = Guid.NewGuid();
        await apply.ApplyPreparedSetAsync(
            key, Record("red", 10), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, older, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            key, Record("blue", 20), Hlc(200), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, newer, atomicBatchSize: 1, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(newer, committed: true, shard, Hlc(1000), Origin, atomicShardCount: 1);
        await maintainer.DrainAsync();

        await apply.ApplyTxTerminalAsync(older, committed: true, shard, Hlc(1001), Origin, atomicShardCount: 1);
        await DrainToZeroAsync(maintainer);

        var sourceValue = await source.GetAsync(key);
        var viewTree = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Record("blue", 20)),
                "The source resolves overlapping prepares by last-writer-wins on the prepared HLC.");
            Assert.That(SumValue(await viewTree.GetAsync("blue")), Is.EqualTo(21),
                "The surviving source value must be aggregated into its group.");
            Assert.That(SumValue(await viewTree.GetAsync("red")), Is.Null,
                "The batch that completed last but prepared earlier must not re-group the key.");
        });
    }

    [Test]
    public async Task Omitted_batch_member_flushes_only_after_its_newer_ordinary_replacement_is_visible() =>
        await AssertOmittedBatchClosureAsync(splitPublication: false);

    [Test]
    public void Public_snapshot_detector_rejects_split_omitted_member_numeric_publication()
    {
        var failure = Assert.ThrowsAsync<AssertionException>(
            () => AssertOmittedBatchClosureAsync(splitPublication: true));
        TestContext.Progress.WriteLine($"Expected split-publication violation: {failure!.Message}");
        Assert.That(failure!.Message, Does.Contain("public snapshot split the omitted-member numeric closure"));
    }

    private async Task AssertOmittedBatchClosureAsync(bool splitPublication)
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var suffix = splitPublication ? "split" : "atomic";
        var tree = $"mv-agg-omitted-visible-{suffix}-src";
        var view = splitPublication ? "mv-agg-omitted-visible-split-view" : "mv-agg-omitted-visible-view";
        const string key = "agg-omitted-visible-k";
        const string other = "agg-omitted-visible-other";
        const string final = "agg-omitted-visible-final";
        const string firstGroup = "green";
        const string secondGroup = "yellow";
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var projection = new AggregationLatticeViewProjection(AggregationKind.Sum, Team, "v1", valueSelector: Score);
        var definition = new LatticeViewDefinition(view, projection);
        _ = factory.Create(source, view, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(view);
        await maintainer.EnsureActiveAsync();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var physical = await _fixture.Cluster.Client
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .ResolveAsync(tree);
        var walPartitions = await _fixture.SiloServices
            .GetRequiredService<LatticeOptionsResolver>()
            .GetWalPartitionsAsync(physical);
        var keyWalPartition = WalPartitionHash.Compute(key, walPartitions);
        var walShards = Enumerable.Range(0, walPartitions)
            .Select(partition => _fixture.Cluster.Client.GetGrain<IWalShardGrain>($"{physical}/{partition}"))
            .ToArray();
        var keyWal = walShards[keyWalPartition];

        await apply.ApplySetAsync(key, Record("red", 3), Hlc(10), Origin, null, 0);
        await DrainToZeroAsync(maintainer);

        var tx = Guid.NewGuid();
        var beforePrepare = await keyWal.GetNextSequenceAsync(CancellationToken.None);
        await apply.ApplyPreparedSetAsync(
            key, Record("red", 10), Hlc(100), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 3, atomicBatchIndex: 0);
        var afterPrepare = await keyWal.GetNextSequenceAsync(CancellationToken.None);
        await apply.ApplyPreparedSetAsync(
            other, Record("green", 5), Hlc(101), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 3, atomicBatchIndex: 1);
        await apply.ApplyPreparedSetAsync(
            final, Record("yellow", 2), Hlc(102), Origin,
            sourceVectorClock: null, expiresAtTicks: 0, tx, atomicBatchSize: 3, atomicBatchIndex: 2);
        var beforeReplacement = await keyWal.GetNextSequenceAsync(CancellationToken.None);
        await apply.ApplySetAsync(key, Record("blue", 7), Hlc(200), Origin, null, 0);
        var afterReplacement = await keyWal.GetNextSequenceAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(afterPrepare, Is.GreaterThan(beforePrepare),
                "the prepared mutation must append to the source key's WAL partition");
            Assert.That(beforeReplacement, Is.GreaterThanOrEqualTo(afterPrepare),
                "the ordinary replacement follows the prepare in the same partition's WAL");
            Assert.That(afterReplacement, Is.GreaterThan(beforeReplacement),
                "the ordinary replacement must also append to the source key's WAL partition");
        });
        TestContext.Progress.WriteLine(
            $"WAL partition {keyWalPartition}: prepare HLC=100 offsets [{beforePrepare},{afterPrepare}); "
            + $"ordinary HLC=200 offsets [{beforeReplacement},{afterReplacement})");

        // Drain the replacement while the atomic batch is still incomplete.
        // These are real group rows from the live maintainer, not final-state
        // reconstruction after the committed batch has flushed.
        await maintainer.DrainAsync();
        var stagedView = await _fixture.ActiveViewTreeAsync(view);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0),
                "the unresolved prepare must still hold the source checkpoint");
            Assert.That(SumValue(await stagedView.GetAsync("blue")), Is.EqualTo(7),
                "the newer ordinary replacement must be published before the batch can flush");
            Assert.That(SumValue(await stagedView.GetAsync("red")), Is.Null,
                "the old membership must already be retracted before the omitted member is skipped");
            Assert.That(SumValue(await stagedView.GetAsync("green")), Is.Null,
                "the unaffected prepared member is not visible before its batch commits");
            Assert.That(SumValue(await stagedView.GetAsync(secondGroup)), Is.Null,
                "the second unaffected prepared member is not visible before its batch commits");
        });

        var shards = new[] { key, other, final }
            .Select(k => LatticeSharding.GetShardIndex(k, LatticeConstants.DefaultShardCount))
            .Distinct()
            .ToArray();
        var viewTreeId = await maintainer.GetActiveTreeIdAsync();
        var completedView = _fixture.Cluster.Client.GetGrain<ILattice>(viewTreeId);
        var closureKeys = new List<string> { firstGroup, secondGroup };
        var beforeTerminalHeads = new long[walPartitions];
        for (var partition = 0; partition < walPartitions; partition++)
        {
            beforeTerminalHeads[partition] = await walShards[partition].GetNextSequenceAsync(CancellationToken.None);
        }

        var previousTerminalHeads = beforeTerminalHeads;
        var terminalRanges = new List<string>();
        for (var i = 0; i < shards.Length; i++)
        {
            await apply.ApplyTxTerminalAsync(tx, committed: true, shards[i], Hlc(1000 + i), Origin,
                atomicShardCount: shards.Length);

            var currentTerminalHeads = new long[walPartitions];
            for (var partition = 0; partition < walPartitions; partition++)
            {
                currentTerminalHeads[partition] = await walShards[partition].GetNextSequenceAsync(CancellationToken.None);
            }

            var appended = Enumerable.Range(0, walPartitions)
                .Where(partition => currentTerminalHeads[partition] > previousTerminalHeads[partition])
                .Select(partition => $"{partition}:[{previousTerminalHeads[partition]},{currentTerminalHeads[partition]})");
            terminalRanges.Add($"shard {shards[i]} HLC {1000 + i}: {string.Join(",", appended)}");
            previousTerminalHeads = currentTerminalHeads;
        }

        TestContext.Progress.WriteLine(
            $"terminal WAL appends={string.Join("; ", terminalRanges)}");

        var gate = HistoryPublicationHoldFilter.Arm(viewTreeId, closureKeys, splitPublication);
        var drain = DrainToZeroAsync(maintainer);
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            AssertNumericClosureSnapshot(await completedView.GetManyAsync(closureKeys), firstGroup, secondGroup);
            gate.Release();
            while (!drain.IsCompleted)
            {
                AssertNumericClosureSnapshot(await completedView.GetManyAsync(closureKeys), firstGroup, secondGroup);
                await Task.Delay(1);
            }

            await drain;
        }
        finally
        {
            gate.Release();
            HistoryPublicationHoldFilter.Disarm(viewTreeId);
            await drain.WaitAsync(TimeSpan.FromSeconds(30));
        }

        var sourceValue = await source.GetAsync(key);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(sourceValue, Is.EqualTo(Record("blue", 7)),
                "the source's higher-HLC ordinary write supersedes the prepared member");
            Assert.That(SumValue(await completedView.GetAsync("blue")), Is.EqualTo(7),
                "flushing the completed batch must retain the already-visible replacement");
            Assert.That(SumValue(await completedView.GetAsync("red")), Is.Null,
                "the superseded prepared contribution must not be published into its old group");
            Assert.That(SumValue(await completedView.GetAsync("green")), Is.EqualTo(5),
                "the unaffected member of the same committed batch must still be published");
            Assert.That(SumValue(await completedView.GetAsync(secondGroup)), Is.EqualTo(2),
                "the second unaffected member of the same committed batch must still be published");
        });
    }

    private static void AssertNumericClosureSnapshot(Dictionary<string, byte[]> snapshot, string firstGroup, string secondGroup)
    {
        Assert.That(snapshot.ContainsKey(firstGroup), Is.EqualTo(snapshot.ContainsKey(secondGroup)),
            "The public snapshot split the omitted-member numeric closure.");
        if (snapshot.TryGetValue(firstGroup, out var first))
        {
            Assert.Multiple(() =>
            {
                Assert.That(SumValue(first), Is.EqualTo(5));
                Assert.That(SumValue(snapshot[secondGroup]), Is.EqualTo(2));
            });
        }
    }
}