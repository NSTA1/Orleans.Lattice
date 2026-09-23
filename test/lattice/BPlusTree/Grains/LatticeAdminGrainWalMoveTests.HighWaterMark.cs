using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The move and reclaim arms that only a trimmed shard reaches. A provider's
/// highest offset is a high-water mark that a trim never lowers (issue #3366),
/// so a shard can report a highest offset it no longer holds. Each case here is
/// shaped so the pre-fix code, which read that mark as "the target holds
/// everything up to here", produces a different verdict.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    private static bool PinWasFlipped(Harness harness) =>
        harness.Registry.ReceivedCalls().Any(c =>
            c.GetMethodInfo().Name == nameof(ILatticeRegistry.UpdateWalPlacementAsync));

    // ---- a target whose mark overlaps the source's retained range

    [Test]
    public void A_move_refuses_a_target_whose_high_water_mark_covers_offsets_it_no_longer_holds()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2, 3, 4, 5);
        harness.QuiesceScript.Add(() => Quiesced(highest: 5));

        // The target is a reclaimed former source: it once held 0..3, so its
        // high-water mark is 3, but it holds nothing now.
        harness.Target.Seed(0, 1, 2, 3);
        harness.Target.TrimAsync(TreeId, 0, 3, CancellationToken.None).GetAwaiter().GetResult();
        harness.Target.Trims.Clear();

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("no longer holds"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Target.AppendedOffsets, Is.Empty,
                "resuming past the mark would copy 4..5 and silently drop 0..3");
            Assert.That(PinWasFlipped(harness), Is.False, "an aborted move must never flip the durable pin");
            Assert.That(harness.DeactivateCalls, Is.GreaterThan(0),
                "the fenced source is released so it resumes service immediately");
        });
    }

    [Test]
    public async Task A_move_resumes_past_a_target_that_still_holds_the_overlap()
    {
        // Positive control for the refusal above: the same mark over the same
        // range is safe when the target's live entries reach down to the source's
        // retained floor, which is the ordinary resumable re-drive.
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2, 3, 4, 5);
        harness.QuiesceScript.Add(() => Quiesced(highest: 5));
        harness.Target.Seed(0, 1, 2, 3);

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(new[] { 4L, 5L }));
            Assert.That(harness.Target.Offsets, Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }));
        });
    }

    [Test]
    public async Task A_move_reserves_the_floor_on_a_trimmed_target_whose_mark_sits_below_the_source_range()
    {
        // The mark is below the source's retained floor, so nothing is skipped:
        // the target reserves the floor and the copy starts at the source's floor.
        var harness = CreateHarness();
        harness.Source.Seed(5, 6, 7);
        harness.QuiesceScript.Add(() => Quiesced(highest: 7));
        harness.Target.Seed(0, 1, 2);
        await harness.Target.TrimAsync(TreeId, 0, 2, CancellationToken.None);
        harness.Target.Trims.Clear();

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.Trims, Is.EqualTo(new[] { 4L }));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(new[] { 5L, 6L, 7L }));
        });
    }

    // ---- a source with no live entries

    [Test]
    public void A_move_of_a_fully_trimmed_source_refuses_a_target_that_cannot_record_its_high_water_mark()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.Source.TrimAsync(TreeId, 0, 2, CancellationToken.None).GetAwaiter().GetResult();
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("reuse offsets"));

        Assert.That(PinWasFlipped(harness), Is.False,
            "flipping would let the target allocate offset 0 again beneath consumers already past offset 2");
    }

    [Test]
    public void The_high_water_mark_guard_runs_even_when_content_verification_is_off()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.Source.TrimAsync(TreeId, 0, 2, CancellationToken.None).GetAwaiter().GetResult();
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(
                TreeId, Move(0, SecondaryKey), new WalMoveOptions { VerifyAfterCopy = false }),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("reuse offsets"));
    }

    [Test]
    public async Task A_move_of_a_fully_trimmed_source_carries_its_high_water_mark_to_a_target_that_records_it()
    {
        var harness = CreateHarness(targetTrimRaisesHighWaterMark: true);
        harness.Source.Seed(0, 1, 2);
        await harness.Source.TrimAsync(TreeId, 0, 2, CancellationToken.None);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.Trims, Is.EqualTo(new[] { 2L }),
                "the source's high-water mark is reserved on the target");
            Assert.That(harness.Target.AppendedOffsets, Is.Empty, "there is nothing live to copy");
            Assert.That(receipt.Moves[0].TargetHighestOffset, Is.EqualTo(2),
                "the target's next activation allocates from offset 3");
        });
    }

    // ---- reclaim

    [Test]
    public async Task ReclaimMovedWalSourceAsync_is_a_no_op_when_re_run_on_a_reclaimed_source()
    {
        var pin = WalPlacementPin.Create().WithPartition(0, SecondaryKey, 2);
        var harness = CreateHarness(pin);
        harness.Source.Seed(0, 1, 2, 3);

        var first = await Admin(harness).ReclaimMovedWalSourceAsync(
            TreeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);
        var second = await Admin(harness).ReclaimMovedWalSourceAsync(
            TreeId, 0, IWalStorageProviderCatalog.DefaultProviderKey);

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(WalMoveOutcome.SourceReclaimed));
            Assert.That(second.Outcome, Is.EqualTo(WalMoveOutcome.NoOp),
                "the reclaimed source still reports high-water mark 3 but holds nothing");
            Assert.That(harness.Source.Trims, Is.EqualTo(new[] { 3L }), "the second call trims nothing");
        });
    }
}
