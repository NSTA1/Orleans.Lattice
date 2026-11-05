using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// #5843, a5e2e2ef's carried note on #5840 S4: the pre-arm split donor's re-claim allowance
/// is counted on <c>orleans.lattice.wal.append_fence.reclaim_allowance</c> - spent on a refusal
/// at the current epoch, spent on a bump's row conflict, or superseded_after_close when an
/// activation that counted a close is superseded with the allowance already spent (the liveness edge: a
/// duplicate's row write before the link spends it, and a re-arm that races the link fence
/// then deactivates the live donor).
/// </summary>
public partial class WalShardGrainTests
{
    private static MeterListener ListenForReclaimAllowance(string tree, ConcurrentQueue<string> outcomes) =>
        MeterListening.StartForInstrument(
            LatticeMetrics.WalAppendFenceReclaimAllowance,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? measuredTree = null, outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) measuredTree = tag.Value as string;
                    else if (tag.Key == LatticeMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (measuredTree == tree && outcome is not null)
                {
                    outcomes.Enqueue(outcome);
                }
            }));

    [Test]
    public async Task A_refusal_at_the_current_epoch_spends_the_allowance_and_a_second_counts_as_superseded_after_close()
    {
        var tree = "reclaim-allowance-" + Guid.NewGuid().ToString("N");
        var outcomes = new ConcurrentQueue<string>();
        using var listener = ListenForReclaimAllowance(tree, outcomes);
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: true);
        var donor = new DonorActivation(storage, live.Etag, preArm: true, metricTree: tree);
        await ArmAtAsync(donor, wal);
        await CloseSplitPreArmAsync(storage, donor);

        await ArmAtAsync(donor, wal);
        donor.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, donor.Fence.Epoch));
        await ArmAtAsync(donor, wal);
        donor.Fence.OnStale(WalAppendStamp.ForLeaf(FenceLeaf, donor.Fence.Epoch));

        Assert.That(outcomes, Is.EqualTo(new[] { "spent_stale_epoch", "superseded_after_close" }));
    }

    [Test]
    public async Task A_bumps_row_conflict_spends_the_allowance_and_a_second_counts_as_superseded_after_close()
    {
        // The carried edge: after the counted close the donor re-armed, and a duplicate's row
        // write before the link makes its next bump conflict - the allowance is spent there.
        var tree = "reclaim-allowance-" + Guid.NewGuid().ToString("N");
        var outcomes = new ConcurrentQueue<string>();
        using var listener = ListenForReclaimAllowance(tree, outcomes);
        var (storage, wal, live, _) = await TwoDonorActivationsAsync(preArm: false);
        var donor = new DonorActivation(storage, live.Etag, preArm: false, metricTree: tree);
        await donor.Fence.StampForAsync(wal, 0, CancellationToken.None);
        await CloseSplitPreArmAsync(storage, donor);
        await donor.Fence.StampForAsync(wal, 0, CancellationToken.None);

        await DuplicateRowWriteAsync(storage);
        await wal.ClaimAppendEpochAsync(FenceLeaf, 5, CancellationToken.None);
        var spent = Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(
            async () => await donor.Fence.StampForAsync(wal, 1, CancellationToken.None));
        var staleAfterSpend = donor.Fence.IsStale;

        await donor.Fence.StampForAsync(wal, 2, CancellationToken.None);
        await DuplicateRowWriteAsync(storage);
        await wal.ClaimAppendEpochAsync(FenceLeaf, 20, CancellationToken.None);
        Assert.ThrowsAsync<Orleans.Lattice.LeafRetiredException>(
            async () => await donor.Fence.StampForAsync(wal, 3, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(spent!.ActivationSuperseded, Is.True);
            Assert.That(staleAfterSpend, Is.False, "precondition: the first conflict spent the allowance rather than superseding");
            Assert.That(donor.Fence.IsStale, Is.True, "precondition: the second conflict superseded the live donor");
            Assert.That(outcomes, Is.EqualTo(new[] { "spent_row_conflict", "superseded_after_close" }));
        });
    }

    /// <summary>Another activation of the donor writes the epoch row, as a duplicate's claim does.</summary>
    private static async Task DuplicateRowWriteAsync(IGrainStorage storage)
    {
        var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, FenceLeaf);
        row.State.Epoch++;
        await WalAppendFenceRows.WriteAsync(storage, WalAppendFenceRows.LeafEpochGrainType, WalAppendFenceRows.LeafEpochKey(FenceLeaf), row);
    }
}
