using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the coordinator-terminal stamp class on the WAL shard (epic #5605,
/// member 2; design note section 2, "Coordinator terminals", and review S2 of
/// revision 3): a saga terminal a coordinator appends under the class is admitted
/// with no entry check and never raises the partition's unstamped watermark, and a
/// record of any other kind sent with the class is admitted unstamped and reported,
/// so the class cannot bypass a fence or the watermark.
/// </summary>
public partial class WalShardGrainTests
{
    private static WalRecord MakeTerminal(MutationKind op) => new()
    {
        TreeId = TreeId,
        Op = op,
        Key = "0",
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        TransactionId = Guid.NewGuid(),
        ShardIndex = 0,
        OriginClusterId = "site-a",
    };

    [Test]
    public async Task A_terminal_class_commit_or_abort_never_raises_the_unstamped_watermark()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        var claim = await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);

        var single = await grain.AppendStampedAsync(MakeTerminal(MutationKind.TxCommit), WalAppendStamp.CoordinatorTerminal, CancellationToken.None);
        var batch = await grain.AppendBatchStampedAsync(
            new[] { MakeTerminal(MutationKind.TxAbort), MakeTerminal(MutationKind.TxCommit) },
            WalAppendStamp.CoordinatorTerminal,
            CancellationToken.None);
        var leafAppend = await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(claim.Accepted, Is.True);
            Assert.That(single.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "the class needs no claimed entry");
            Assert.That(batch.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted));
            Assert.That(batch.AdmittedCount, Is.EqualTo(2), "a multi-entry terminal batch has no leaf entry to hold back");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(-1),
                "a coordinator's TxCommit/TxAbort must not raise the watermark (member 2 ordering dependency)");
            Assert.That(leafAppend.UnstampedWatermark, Is.EqualTo(-1),
                "a leaf writing the partition after a saga is not reported an unstamped append");
        });
    }

    [Test]
    public async Task A_non_terminal_record_under_the_terminal_class_is_admitted_unstamped_and_reported()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());
        await grain.ClaimAppendEpochAsync(FenceLeaf, 1, CancellationToken.None);
        long misuses = 0;
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.WalAppendFenceUnstampedAppends,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagUnstampedSource && Equals(tag.Value, "terminal_class_misuse"))
                    {
                        Interlocked.Add(ref misuses, value);
                    }
                }
            }));

        var single = await grain.AppendStampedAsync(MakeEntry("smuggled"), WalAppendStamp.CoordinatorTerminal, CancellationToken.None);
        var afterSingle = grain.UnstampedWatermarkForTesting;
        var batch = await grain.AppendBatchStampedAsync(
            new[] { MakeTerminal(MutationKind.TxCommit), MakeEntry("smuggled-too") },
            WalAppendStamp.CoordinatorTerminal,
            CancellationToken.None);
        var leafAppend = await grain.AppendStampedAsync(MakeEntry("a"), WalAppendStamp.ForLeaf(FenceLeaf, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(single.Outcome, Is.EqualTo(WalAppendFenceOutcome.Admitted), "never rejected: admitted unstamped");
            Assert.That(afterSingle, Is.EqualTo(single.Offset), "a Set under the terminal class raises the watermark");
            Assert.That(single.UnstampedWatermark, Is.EqualTo(single.Offset), "and is reported in the same response");
            Assert.That(grain.UnstampedWatermarkForTesting, Is.EqualTo(batch.Offsets[1]),
                "only the batch's non-terminal entry raises the watermark");
            Assert.That(leafAppend.UnstampedWatermark, Is.EqualTo(batch.Offsets[1]),
                "a leaf writing the partition is reported the smuggled record");
            Assert.That(Interlocked.Read(ref misuses), Is.EqualTo(2));
        });
    }

    [Test]
    public void IsCoordinatorTerminalOp_admits_only_commit_and_abort()
    {
        foreach (var op in Enum.GetValues<MutationKind>())
        {
            Assert.That(
                WalAppendFenceCore.IsCoordinatorTerminalOp(op),
                Is.EqualTo(op is MutationKind.TxCommit or MutationKind.TxAbort),
                op.ToString());
        }
    }

    [Test]
    public async Task A_routing_generation_is_never_claimed_for_the_terminal_class()
    {
        var grain = await CreateFencedGrainAsync(new EnumerableMemoryGrainStorage());

        Assert.ThrowsAsync<ArgumentException>(() => grain.ClaimRoutingGenerationAsync(
            WalAppendStamp.CoordinatorTerminal.Initiator, 1, CancellationToken.None));
    }
}
