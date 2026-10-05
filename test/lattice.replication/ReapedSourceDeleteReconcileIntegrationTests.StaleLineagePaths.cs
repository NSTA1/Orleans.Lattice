using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4707: the source-lineage refusal of issue #4673 holds on every apply
/// entry, not only on a pushed one-entry batch. A multi-entry push takes the
/// applier's batched run path; an entry that parks in the causal-apply buffer
/// before the receiver realigns is applied later by the buffer's drain; and an
/// entry dead-lettered before the realign is applied later by an operator
/// replay. Each test delivers a write the source read under its pre-restore
/// lineage along one of those paths after the receiver drained the restored
/// lineage, and fails if the write lands. Each also carries a positive arm - the
/// same path under the drained lineage applies - so a refusal that comes from
/// anything but the lineage check cannot pass it.
/// </summary>
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private static ILatticeReplicationDeadLetters DeadLetters(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<ILatticeReplicationDeadLetters>();

    /// <summary>A write that cannot apply until <see cref="SiteCClusterId"/>'s write at <paramref name="dependency"/> has.</summary>
    private static WalRecord DependentWrite(string tree, string key, HybridLogicalClock dependency) =>
        StaleWrite(tree, key) with
        {
            VectorClock = new VersionVector { Entries = { [SiteCClusterId] = dependency } },
        };

    /// <summary>Third origin <see cref="SiteCClusterId"/>'s write at <paramref name="at"/>, relayed by the source.</summary>
    private static WalRecord ThirdOriginWrite(string tree, string key, HybridLogicalClock at) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [7],
        Timestamp = at,
        OriginClusterId = SiteCClusterId,
    };

    /// <summary>An HLC no write of the clusters under test reaches before the test ends.</summary>
    private static HybridLogicalClock FutureHlc() =>
        new() { WallClockTicks = DateTime.UtcNow.Ticks + TimeSpan.TicksPerHour, Counter = 0 };

    [Test]
    public async Task A_multi_entry_batch_read_under_the_pre_restore_lineage_is_refused_whole()
    {
        const string tree = "rsdr-stale-lineage-batch";
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        await RestampSourceLineageAsync(tree);
        await BootstrapSiteBAsync(tree);
        var restored = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // Two entries, so the push takes the applier's batched run path rather
        // than delegating to the per-entry one.
        var stale = await PushStampedAsync(_siteB, [StaleWrite(tree, "stale-1"), StaleWrite(tree, "stale-2")], preRestore);
        var current = await PushStampedAsync(_siteB, [StaleWrite(tree, "live-1"), StaleWrite(tree, "live-2")], restored);

        Assert.Multiple(async () =>
        {
            Assert.That(stale, Is.EqualTo(ReplicationSourceLineageGate.Verdict.RefuseLineage));
            Assert.That(await siteB.GetAsync("stale-1"), Is.Null,
                "a batched write the source read under the pre-restore lineage must not land");
            Assert.That(await siteB.GetAsync("stale-2"), Is.Null);
            Assert.That(current, Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply));
            Assert.That(await siteB.GetAsync("live-1"), Is.EqualTo(new byte[] { 9 }),
                "precondition: a batch of the drained lineage applies through the same path");
        });
    }

    [Test]
    public async Task An_entry_parked_under_the_pre_restore_lineage_is_discarded_when_the_drain_reaches_it_after_the_realign()
    {
        const string tree = "rsdr-stale-lineage-drain";
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        var buffer = _siteB.Client.GetGrain<ICausalApplyBufferGrain>(tree);
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // A write the source read before its restore arrives while a third
        // origin's write it depends on has not: it parks, stamped with the
        // pre-restore lineage it was pushed under.
        var dependency = FutureHlc();
        var parked = await PushStampedAsync(_siteB, DependentWrite(tree, "parked-before-restore", dependency), preRestore);
        var parkedCount = await buffer.CountAsync();

        // The source restores the tree, and the receiver drains the restored
        // lineage. The parked entry survives the realign.
        await RestampSourceLineageAsync(tree);
        await BootstrapSiteBAsync(tree);
        var restored = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;
        var parkedAfterRealign = await buffer.CountAsync();

        // Positive arm: a write pushed under the restored lineage parks on the
        // same dependency.
        await PushStampedAsync(_siteB, DependentWrite(tree, "parked-after-restore", dependency), restored);

        // The dependency arrives, under the restored lineage, and the drain
        // reaches both parked entries.
        var dependencyVerdict = await PushStampedAsync(_siteB, ThirdOriginWrite(tree, "dependency", dependency), restored);
        await buffer.DrainAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(parked, Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply),
                "precondition: the entry was admitted under the lineage drained at the time");
            Assert.That(parkedCount, Is.EqualTo(1), "precondition: the entry parked");
            Assert.That(parkedAfterRealign, Is.EqualTo(1), "precondition: the parked entry survived the realign");
            Assert.That(dependencyVerdict, Is.EqualTo(ReplicationSourceLineageGate.Verdict.Apply));
            Assert.That(await siteB.GetAsync("dependency"), Is.EqualTo(new byte[] { 7 }), "precondition: the dependency applied");
            Assert.That(await siteB.GetAsync("parked-before-restore"), Is.Null,
                "the drain must refuse an entry its sender read under a lineage this tree no longer holds");
            Assert.That(await siteB.GetAsync("parked-after-restore"), Is.EqualTo(new byte[] { 9 }),
                "an entry parked under the drained lineage applies when the drain reaches it");
            Assert.That(await buffer.CountAsync(), Is.Zero,
                "the refused entry is discarded rather than left parked forever");
        });
    }

    [Test]
    public async Task Concurrent_pushes_stamped_with_different_lineages_are_each_judged_on_their_own()
    {
        const string tree = "rsdr-stale-lineage-concurrent";
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;
        await RestampSourceLineageAsync(tree);
        await BootstrapSiteBAsync(tree);
        var restored = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;
        var epoch = await _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(tree)
            .ObserveAsync(SiteAClusterId, null, CancellationToken.None);

        // Both deliveries hold a live lineage scope before either is admitted, so
        // a carrier shared between flows would judge one by the other's stamp.
        var staleEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var currentEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task<ApplyResult> PushAsync(Guid? stamp, string key, TaskCompletionSource mine)
        {
            using var scope = ReplicationSourceLineageScope.Enter(SiteAClusterId, stamp, epoch);
            mine.SetResult();
            await Task.WhenAll(staleEntered.Task, currentEntered.Task);
            return await Applier(_siteB).ApplyBatchAsync([StaleWrite(tree, key)], CancellationToken.None);
        }

        var stale = Task.Run(() => PushAsync(preRestore, "concurrent-stale", staleEntered));
        var current = Task.Run(() => PushAsync(restored, "concurrent-current", currentEntered));
        await Task.WhenAll(stale, current);

        Assert.Multiple(async () =>
        {
            Assert.That((await stale).SourceLineageRefused, Is.True,
                "the stale delivery is judged by its own pre-restore stamp, not by the concurrent current one");
            Assert.That(await siteB.GetAsync("concurrent-stale"), Is.Null);
            Assert.That((await current).SourceLineageRefused, Is.False,
                "the current delivery is judged by its own stamp, not by the concurrent stale one");
            Assert.That(await siteB.GetAsync("concurrent-current"), Is.EqualTo(new byte[] { 9 }));
        });
    }

    [Test]
    public async Task A_dead_letter_read_under_the_pre_restore_lineage_is_not_applied_by_a_replay_after_the_realign()
    {
        const string tree = "rsdr-stale-lineage-replay";
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        var queue = _siteB.Client.GetGrain<IReplicationDeadLetterGrain>(tree);
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var preRestore = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // A write pushed under the pre-restore lineage is dead-lettered with the
        // stamp it arrived under.
        var staleId = await queue.EnqueueAsync(
            StaleWrite(tree, "dead-lettered-before-restore"),
            "test",
            retryCount: 0,
            LatticeReplicationMetrics.ReasonUnknown,
            CancellationToken.None,
            new ReplicationSourceLineageStamp(SiteAClusterId, preRestore!.Value));

        await RestampSourceLineageAsync(tree);
        await BootstrapSiteBAsync(tree);
        var restored = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // Positive arm: a dead letter of the restored lineage.
        var currentId = await queue.EnqueueAsync(
            StaleWrite(tree, "dead-lettered-after-restore"),
            "test",
            retryCount: 0,
            LatticeReplicationMetrics.ReasonUnknown,
            CancellationToken.None,
            new ReplicationSourceLineageStamp(SiteAClusterId, restored!.Value));

        var staleReplay = await DeadLetters(_siteB).ReplayAsync(tree, staleId);
        var currentReplay = await DeadLetters(_siteB).ReplayAsync(tree, currentId);

        Assert.Multiple(async () =>
        {
            Assert.That(staleReplay?.SourceLineageRefused, Is.True,
                "a replay must check the entry against the lineage this tree has drained since it was parked");
            Assert.That(await siteB.GetAsync("dead-lettered-before-restore"), Is.Null);
            Assert.That(await queue.TryGetAsync(staleId, CancellationToken.None), Is.Not.Null,
                "a refused replay is not terminal: the entry stays parked for the operator to discard");
            Assert.That(currentReplay?.Applied, Is.True, "precondition: a dead letter of the drained lineage replays");
            Assert.That(await siteB.GetAsync("dead-lettered-after-restore"), Is.EqualTo(new byte[] { 9 }));
            Assert.That(await queue.TryGetAsync(currentId, CancellationToken.None), Is.Null);
        });
    }

    [Test]
    public async Task An_entry_dead_lettered_during_a_stamped_push_keeps_the_stamp_it_arrived_under()
    {
        const string tree = "rsdr-stale-lineage-dlq-stamp";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("kept", [1]);
        await BootstrapSiteBAsync(tree);
        var current = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(tree))!.Lineage;

        // A wire merge mode the receiver does not resolve for the tree: the
        // applier dead-letters the entry inside the push.
        var mismatched = StaleWrite(tree, "mode-mismatch") with { Mode = LatticeMergeMode.PnCounter };
        await PushStampedAsync(_siteB, mismatched, current);

        var parked = await DeadLetters(_siteB).ListAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(parked, Has.Count.EqualTo(1), "precondition: the push dead-lettered the entry");
            Assert.That(parked[0].SourceLineageClusterId, Is.EqualTo(SiteAClusterId));
            Assert.That(parked[0].SourceLineage, Is.EqualTo(current),
                "a dead letter keeps the lineage its push was stamped with, so its replay can be checked");
        });
    }
}
