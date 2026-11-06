using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The fence's half of the report fetch (#5851 (2), a5e2e2ef's R2): the reader is the stamp
/// a pre-arm append at the partition would carry, and a fetched report counts only for the
/// closes its reader carried.
/// </summary>
public sealed partial class LeafAppendEpochFenceTests
{
    [Test]
    public async Task ReportFetch_the_reader_is_the_stamp_a_pre_arm_append_would_carry()
    {
        var fence = PreArmFence();
        fence.SetCarriedCloses([(CloseA, new[] { true, false })], boundPending: false);
        var shard = Substitute.For<IWalShardGrain>();

        var gotResolved = fence.TryGetReportReader(0, out var atResolved);
        var gotCarried = fence.TryGetReportReader(1, out var atCarried);
        var appendAtCarried = (await fence.StampForAsync(shard, partition: 1, CancellationToken.None))!.Value;

        Assert.Multiple(() =>
        {
            Assert.That(gotResolved && gotCarried, Is.True);
            Assert.That(atResolved.Close1.IsSet, Is.False, "the reader carried a close resolved at its partition");
            Assert.That(atCarried, Is.EqualTo(appendAtCarried), "the reader is not the stamp an append at the partition carries");
        });
    }

    [Test]
    public void ReportFetch_a_report_whose_reader_did_not_carry_a_close_carried_now_does_not_count()
    {
        // R2 (ReportPredatesCarry): recorded with a reader that predates the carriage, the
        // report gives no claimless view; with the reader carrying it, it does.
        var fence = PreArmFence();
        fence.TryGetReportReader(0, out var stale);
        fence.SetCarriedCloses([(CloseA, Array.Empty<bool>())], boundPending: false);
        fence.OnReportFetched(0, 3, in stale);
        var countedStale = fence.TryGetPreArmView(0, out _);

        fence.TryGetReportReader(0, out var fresh);
        fence.OnReportFetched(0, 3, in fresh);

        Assert.Multiple(() =>
        {
            Assert.That(countedStale, Is.False, "a report whose reader predates the carried close counted");
            Assert.That(fence.TryGetPreArmView(0, out var bound) && bound == 3, Is.True, "a report whose reader carried the close did not count");
        });
    }
}
