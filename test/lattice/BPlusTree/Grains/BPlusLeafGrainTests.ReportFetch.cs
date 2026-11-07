using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The read-only report fetch on the coverage-lag tick (#5851 (2), a5e2e2ef's R1-R4 and
/// F1-F7): a pre-arm leaf whose latest report does not cover the closes it carries now,
/// and which that missing report alone holds, asks the shard for the report a pre-arm
/// append would carry, recorded as that append's response, and advances to its own high
/// without a replay. Never on a write or within a drive, once per partition per tick, and
/// never above the own high.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static int ReportFetchCalls(LiveAdvanceRig rig) =>
        rig.Shard.ReceivedCalls().Count(c => c.GetMethodInfo().Name == nameof(IWalShardGrain.ReadLeafReportAsync));

    /// <summary>
    /// A one-partition pre-arm leaf with an own write reported before it learned of a
    /// witnessed donor close it now carries: its report no longer covers what it carries.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, LiveAdvanceRig Rig, long OwnHigh)> UncoveredReportLeafAsync(long fetchedBound)
    {
        var rig = CarryRig(partitions: 1);
        rig.Shard.ReadLeafReportAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(fetchedBound));
        var grain = CreateGrain(liveAdvance: rig);
        await InitialiseOwnMarkerAsync(rig);
        await SetStampedAsync(grain, "a", "1");
        var ownHigh = rig.Writer.NextOffset(0) - 1;
        await WitnessedCarriedCloseAsync(rig);
        return (grain, rig, ownHigh);
    }

    [Test]
    public async Task ReportFetch_an_idle_leaf_whose_report_predates_its_carried_close_advances_on_the_tick_to_its_own_high()
    {
        var (grain, rig, ownHigh) = await UncoveredReportLeafAsync(fetchedBound: -1);

        var heldWithoutFetch = !await grain.TryAdvanceLiveCheckpointAsync();
        var advanced = await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.Multiple(() =>
        {
            Assert.That(heldWithoutFetch, Is.True, "precondition: the report that predates the close holds the advance");
            Assert.That(advanced, Is.True, "the fetched report did not unblock the advance");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(ownHigh), "the advance did not reach the own high");
            Assert.That(ReportFetchCalls(rig), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task ReportFetch_never_raises_the_target_above_the_own_high()
    {
        // F1: other authors' records lie above the own high; the fetch removes only the
        // missing report, so the checkpoint stops at the own high.
        var (grain, rig, ownHigh) = await UncoveredReportLeafAsync(fetchedBound: -1);
        for (var i = 0; i < 5; i++)
        {
            rig.Writer.ForeignAppend();
        }

        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(ownHigh), "the fetch advanced the leaf past its own high");
    }

    [Test]
    public async Task ReportFetch_a_carried_stale_high_above_the_checkpoint_holds_the_advance()
    {
        // R4 (FetchReport's ReportStale): the fetched report carries the stale high of the
        // close the reader carries, and the reader carries it; a stale record above the
        // checkpoint keeps the advance held.
        var (grain, rig, ownHigh) = await UncoveredReportLeafAsync(fetchedBound: 0);
        var stale = rig.Writer.ForeignAppend();
        rig.Shard.ReadLeafReportAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(stale));

        var advanced = await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False, "the advance passed a carried close's stale record");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.LessThan(ownHigh));
        });
        await rig.Shard.Received(1).ReadLeafReportAsync(Arg.Is<WalAppendStamp>(s => s.Close1.IsSet && s.LeafId == rig.LeafId), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReportFetch_is_issued_only_on_the_tick_and_only_while_the_missing_report_binds()
    {
        // R3: never from a write's own path, nor within a drive; never when a close head
        // binds as well; at most once per partition per tick.
        var (grain, rig, _) = await UncoveredReportLeafAsync(fetchedBound: int.MaxValue);
        await grain.TryAdvanceLiveCheckpointAsync();
        var onWrite = ReportFetchCalls(rig);
        await grain.TryAdvanceLiveCheckpointAsync(withinDrive: true, fetchReports: true);
        var inDrive = ReportFetchCalls(rig);
        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);
        var oneTick = ReportFetchCalls(rig);

        var (closeHeld, closeRig, _) = await UncoveredReportLeafAsync(fetchedBound: -1);
        await closeRig.SetLeafMarkerAsync(m =>
        {
            HandOffMarkerCore.Open(m, "reclaim:x", null, null);
            return HandOffMarkerCore.Close(m, "reclaim:x", [closeRig.Writer.NextOffset(0) + 5]);
        });
        await closeHeld.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.Multiple(() =>
        {
            Assert.That(onWrite, Is.Zero, "a fetch was issued off the tick");
            Assert.That(inDrive, Is.Zero, "a fetch was issued within a drive");
            Assert.That(oneTick, Is.EqualTo(1), "a tick fetched more than once at a partition");
            Assert.That(ReportFetchCalls(closeRig), Is.Zero, "a fetch was issued while a close head bound the leaf as well");
        });
    }

    [Test]
    public async Task ReportFetch_a_report_from_before_a_newly_carried_close_is_fetched_again()
    {
        // F4: a fetched report counts only for the closes its reader carried. A close the
        // leaf carries since makes it not count, and the next tick fetches afresh.
        var (grain, rig, _) = await UncoveredReportLeafAsync(fetchedBound: int.MaxValue);
        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);
        var second = new Guid("d0000000-0000-0000-0000-00000000d0d2");
        await rig.SetLeafMarkerAsync(m =>
        {
            HandOffMarkerCore.Open(m, "split:donor2", null, null);
            return HandOffMarkerCore.CloseWithDeferredBound(m, "split:donor2", [0], second, ClaimlessDonorToken, 0, 1)
                | HandOffMarkerCore.WitnessLink(m, second, 0, CarryWitness);
        });

        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.That(ReportFetchCalls(rig), Is.EqualTo(2), "a report fetched for an earlier carried set was reused");
    }

    [Test]
    public async Task ReportFetch_a_shard_that_does_not_know_the_call_latches_the_fetch_off()
    {
        // F5 / S1: an earlier build's shard does not know the call, and refuses it with the
        // unresolved-alias TypeLoadException. The leaf stays held, nothing escapes the tick,
        // and it never asks again this activation.
        var (grain, rig, _) = await UncoveredReportLeafAsync(fetchedBound: -1);
        rig.Shard.ReadLeafReportAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(ShardBulkLoadCompletionTests.MethodUnknown());

        var first = await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);
        var second = await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.Multiple(() =>
        {
            Assert.That(first || second, Is.False, "an unanswered fetch advanced the leaf");
            Assert.That(ReportFetchCalls(rig), Is.EqualTo(1), "the fetch was asked again of a shard that does not know it");
        });
    }

    [TestCase(typeof(TimeoutException))]
    [TestCase(typeof(NotImplementedException))]
    public async Task ReportFetch_any_other_fetch_failure_is_retried_on_the_next_tick(Type failure)
    {
        // F5 / S1: only a definite unknown-method latches; a transient or non-definite failure does not.
        var (grain, rig, _) = await UncoveredReportLeafAsync(fetchedBound: -1);
        rig.Shard.ReadLeafReportAsync(Arg.Any<WalAppendStamp>(), Arg.Any<CancellationToken>())
            .ThrowsAsync((Exception)Activator.CreateInstance(failure)!);

        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);
        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.That(ReportFetchCalls(rig), Is.EqualTo(2), "a transient fetch failure latched the fetch off");
    }

    [Test]
    public async Task ReportFetch_counts_fetches_issued_and_those_that_unblocked_an_advance()
    {
        // F7.
        var issued = BPlusLeafGrain.ReportFetchesIssuedForProbe;
        var unblocked = BPlusLeafGrain.ReportFetchesUnblockedForProbe;
        var (grain, _, _) = await UncoveredReportLeafAsync(fetchedBound: -1);

        await grain.TryAdvanceLiveCheckpointAsync(fetchReports: true);

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.ReportFetchesIssuedForProbe, Is.GreaterThan(issued));
            Assert.That(BPlusLeafGrain.ReportFetchesUnblockedForProbe, Is.GreaterThan(unblocked));
        });
    }
}
