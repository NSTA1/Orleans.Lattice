using Microsoft.Extensions.DependencyInjection;
using Orleans.Core.Internal;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Exercises the post-capture WAL suffix of a rebuilt history view through the
/// real maintainer and public history reader.
/// </summary>
[TestFixture]
[Category("Integration")]
[NonParallelizable]
public sealed class MaterialisedViewHistoryRebuildCompositionTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Rebuild_captured_floor_keeps_post_floor_revisions_after_lost_response_and_reactivation()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var scenario = await CreateScenarioAsync();
        var gate = HistoryPublicationHoldFilter.Arm(
            scenario.ActiveTreeId, scenario.AtomicRevisionKeys, splitPublication: false, loseResponse: true);
        var drain = scenario.Maintainer.DrainAsync();
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.That(gate.Committed, Is.True, "The fault must happen after the atomic history rows commit.");
            Assert.That(
                await scenario.HistoryTree.GetManyAsync(scenario.AtomicRevisionKeys),
                Has.Count.EqualTo(scenario.AtomicRevisionKeys.Count),
                "The uncertain response must leave the complete committed revision pair visible.");
            gate.Release();
            Assert.ThrowsAsync<TimeoutException>(async () => await drain);
        }
        finally
        {
            gate.Release();
            HistoryPublicationHoldFilter.Disarm(scenario.ActiveTreeId);
            if (!drain.IsCompleted)
            {
                await drain.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        await scenario.Maintainer.AsReference<IGrainManagementExtension>().DeactivateOnIdle();
        await Task.Delay(500);
        await DrainToZeroAsync(scenario.Maintainer);

        var keyHistory = await scenario.Source.ScanEntryHistoryAsync(scenario.Key, null, null, 100, null);
        var firstMemberHistory = await scenario.Source.ScanEntryHistoryAsync(scenario.FirstMember, null, null, 100, null);
        var secondMemberHistory = await scenario.Source.ScanEntryHistoryAsync(scenario.SecondMember, null, null, 100, null);
        var currentSourceValue = await scenario.Source.GetAsync(scenario.Key);

        Assert.Multiple(() =>
        {
            Assert.That(keyHistory.Source, Is.EqualTo(EntryHistorySource.View));
            Assert.That(keyHistory.Truncated, Is.False);
            Assert.That(keyHistory.Revisions.Select(revision => revision.Hlc),
                Is.EquivalentTo(scenario.KeyRevisionClocks));
            Assert.That(keyHistory.Revisions.Select(revision => revision.ValuePreview),
                Is.EqualTo(scenario.KeyRevisionValues),
                "Rebuild may seed the current pre-floor value, but must not claim older pre-floor revisions.");
            Assert.That(firstMemberHistory.Revisions.Select(revision => revision.Hlc),
                Is.EqualTo(new[] { scenario.FirstMemberClock }));
            Assert.That(firstMemberHistory.Revisions[0].ValuePreview, Is.EqualTo(new byte[] { 11 }));
            Assert.That(secondMemberHistory.Revisions.Select(revision => revision.Hlc),
                Is.EqualTo(new[] { scenario.SecondMemberClock }));
            Assert.That(secondMemberHistory.Revisions[0].ValuePreview, Is.EqualTo(new byte[] { 21 }));
            Assert.That(currentSourceValue, Is.EqualTo(new byte[] { 30 }),
                "The ordinary write remains the source winner while all committed history revisions survive.");
        });
    }

    [Test]
    public async Task Rebuild_history_detector_rejects_a_split_post_floor_atomic_revision_pair()
    {
        using var viewReadScope = ViewReadContext.BeginScope();
        var scenario = await CreateScenarioAsync();
        var failure = Assert.ThrowsAsync<AssertionException>(async () =>
            await AssertAtomicHistoryPublicationAsync(scenario, splitPublication: true));
        Assert.That(failure!.Message, Does.Contain("captured-floor history publication exposed a partial atomic revision pair"));
    }

    private async Task<Scenario> CreateScenarioAsync()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var treeId = $"history-floor-source-{suffix}";
        var viewName = $"history-floor-view-{suffix}";
        var key = $"history-floor-key-{suffix}";

        var source = _fixture.Cluster.Client.GetGrain<ILattice>(treeId);
        await source.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, null);
        await source.SetAsync(key, [0]);
        await source.SetAsync(key, [1]);

        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, viewName, LatticeHistoryView.Definition(viewName, _fixture.SiloServices));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();

        // The explicit rebuild captures and durably installs the floor before the
        // following mutations are appended. Revisions before this floor are not
        // recoverable from the history view; only the current source seed survives.
        await maintainer.RebuildAsync();
        await DrainToZeroAsync(maintainer);
        var seed = await source.ScanEntryHistoryAsync(key, null, null, 100, null);
        Assert.That(seed.Revisions, Has.Count.EqualTo(1));
        Assert.That(seed.Revisions[0].ValuePreview, Is.EqualTo(new byte[] { 1 }));

        var keyShard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        var firstMember = FindDifferentShardKey($"history-floor-first-{suffix}", keyShard);
        var secondMember = FindDifferentShardKey($"history-floor-second-{suffix}", keyShard);
        var firstMemberShard = LatticeSharding.GetShardIndex(firstMember, LatticeConstants.DefaultShardCount);
        var secondMemberShard = LatticeSharding.GetShardIndex(secondMember, LatticeConstants.DefaultShardCount);
        var firstClock = After(seed.Revisions[0].Hlc, 10);
        var secondClock = After(seed.Revisions[0].Hlc, 20);
        var ordinaryClock = After(seed.Revisions[0].Hlc, 30);
        var firstMemberClock = After(seed.Revisions[0].Hlc, 40);
        var secondMemberClock = After(seed.Revisions[0].Hlc, 50);
        var firstTerminalClock = After(seed.Revisions[0].Hlc, 60);
        var secondTerminalClock = After(seed.Revisions[0].Hlc, 70);
        var firstTransaction = Guid.NewGuid();
        var secondTransaction = Guid.NewGuid();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);

        await apply.ApplyPreparedSetAsync(key, [10], firstClock, "history-floor-origin", null, 0,
            firstTransaction, atomicBatchSize: 2, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(firstMember, [11], firstMemberClock, "history-floor-origin", null, 0,
            firstTransaction, atomicBatchSize: 2, atomicBatchIndex: 1);
        await apply.ApplyPreparedSetAsync(key, [20], secondClock, "history-floor-origin", null, 0,
            secondTransaction, atomicBatchSize: 2, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(secondMember, [21], secondMemberClock, "history-floor-origin", null, 0,
            secondTransaction, atomicBatchSize: 2, atomicBatchIndex: 1);
        await apply.ApplySetAsync(key, [30], ordinaryClock, "history-floor-origin", null, 0);
        await CommitAsync(apply, firstTransaction, keyShard, firstMemberShard, firstTerminalClock);
        await CommitAsync(apply, secondTransaction, keyShard, secondMemberShard, secondTerminalClock);

        var firstRevisionKeys = new List<string>
        {
            HistoryKey.Encode(key, firstClock),
            HistoryKey.Encode(firstMember, firstMemberClock),
        };
        var activeTreeId = await maintainer.GetActiveTreeIdAsync();
        var historyTree = _fixture.Cluster.Client.GetGrain<ILattice>(activeTreeId);
        Assert.That(await historyTree.GetManyAsync(firstRevisionKeys), Is.Empty);

        return new Scenario(
            source,
            maintainer,
            historyTree,
            activeTreeId,
            key,
            firstMember,
            secondMember,
            firstRevisionKeys,
            [seed.Revisions[0].Hlc, firstClock, secondClock, ordinaryClock],
            [new byte[] { 1 }, new byte[] { 10 }, new byte[] { 20 }, new byte[] { 30 }],
            firstMemberClock,
            secondMemberClock);
    }

    private static async Task CommitAsync(
        IReplicationApplyGrain apply,
        Guid transactionId,
        int firstShard,
        int secondShard,
        HybridLogicalClock terminalClock)
    {
        await apply.ApplyTxTerminalAsync(
            transactionId, committed: true, firstShard, terminalClock, "history-floor-origin", atomicShardCount: 2);
        await apply.ApplyTxTerminalAsync(
            transactionId, committed: true, secondShard, terminalClock, "history-floor-origin", atomicShardCount: 2);
    }

    private static async Task AssertAtomicHistoryPublicationAsync(Scenario scenario, bool splitPublication)
    {
        var gate = HistoryPublicationHoldFilter.Arm(
            scenario.ActiveTreeId, scenario.AtomicRevisionKeys, splitPublication);
        var drain = scenario.Maintainer.DrainAsync();
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            AssertAtomicSnapshot(
                await scenario.HistoryTree.GetManyAsync(scenario.AtomicRevisionKeys),
                scenario.AtomicRevisionKeys);
            gate.Release();
            await drain.WaitAsync(TimeSpan.FromSeconds(30));
            AssertAtomicSnapshot(
                await scenario.HistoryTree.GetManyAsync(scenario.AtomicRevisionKeys),
                scenario.AtomicRevisionKeys);
        }
        finally
        {
            gate.Release();
            HistoryPublicationHoldFilter.Disarm(scenario.ActiveTreeId);
            if (!drain.IsCompleted)
            {
                await drain.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    private static void AssertAtomicSnapshot(Dictionary<string, byte[]> snapshot, IReadOnlyList<string> keys)
    {
        var visible = keys.Count(snapshot.ContainsKey);
        Assert.That(
            visible,
            Is.EqualTo(0).Or.EqualTo(keys.Count),
            "The captured-floor history publication exposed a partial atomic revision pair.");
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
        long lag;
        do
        {
            await maintainer.DrainAsync();
            lag = await maintainer.GetLagAsync();
            if (lag == 0)
            {
                return;
            }

            await Task.Delay(20);
        }
        while (Environment.TickCount64 < deadline);

        Assert.Fail($"History view did not converge to its captured source head (residual lag {lag}).");
    }

    private static string FindDifferentShardKey(string prefix, int shard)
        => Enumerable.Range(0, 64)
            .Select(index => $"{prefix}-{index}")
            .First(key => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount) != shard);

    private static HybridLogicalClock After(HybridLogicalClock clock, long ticks)
        => new() { WallClockTicks = clock.WallClockTicks + ticks, Counter = clock.Counter };

    private sealed record Scenario(
        ILattice Source,
        IViewMaintainerGrain Maintainer,
        ILattice HistoryTree,
        string ActiveTreeId,
        string Key,
        string FirstMember,
        string SecondMember,
        List<string> AtomicRevisionKeys,
        HybridLogicalClock[] KeyRevisionClocks,
        byte[][] KeyRevisionValues,
        HybridLogicalClock FirstMemberClock,
        HybridLogicalClock SecondMemberClock);
}
