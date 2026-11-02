namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5466: a move whose copy is, or is reported to be, short of the
/// quiesced source tail must never cut over. The partition shape is the one the
/// flake hit - 495 entries (offsets 0..494) copied to a target capped at 100
/// entries per append, so the final chunk is the 39 entries 456..494.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    private const long ShortCopyTail = 494;
    private const long ShortCopyFinalChunkStart = 456;

    [Test]
    public void A_target_reported_one_chunk_short_blocks_the_cutover()
    {
        // The #5466 symptom: the copy landed every entry, but the target's
        // highest-offset read missed the final chunk.
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, (int)ShortCopyTail + 1));
        harness.QuiesceScript.Add(() => Quiesced(highest: ShortCopyTail));
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(ShortCopyFinalChunkStart - 1);

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains("failed verification")
                .And.Message.Contains($"source highest offset {ShortCopyTail} but target highest offset {ShortCopyFinalChunkStart - 1}"));
        Assert.Multiple(() =>
        {
            Assert.That(PinWasFlipped(harness), Is.False, "a short verification must never flip the durable pin");
            Assert.That(harness.DeactivateCalls, Is.GreaterThan(0),
                "the fenced source is released so it resumes service at once");
        });
    }

    [Test]
    public async Task A_move_retried_after_a_short_verification_completes_without_duplicating_the_copy()
    {
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, (int)ShortCopyTail + 1));
        harness.QuiesceScript.Add(() => Quiesced(highest: ShortCopyTail));
        harness.Target.HighestOverrides.Enqueue(-1);
        harness.Target.HighestOverrides.Enqueue(ShortCopyFinalChunkStart - 1);
        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("failed verification"));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(Range(0, (int)ShortCopyTail + 1)),
                "the retry resumes past what landed: every offset appended exactly once");
            Assert.That(receipt.Moves[0].TargetHighestOffset, Is.EqualTo(ShortCopyTail));
        });
    }

    [Test]
    public void A_copy_that_cannot_read_the_quiesced_tail_aborts_even_with_verification_off()
    {
        // The source counts its final chunk in its highest offset but does not
        // yet serve it to reads. Without the copy's own completeness check the
        // move would flip the pin onto a target missing 456..494 whenever
        // content verification is switched off.
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, (int)ShortCopyTail + 1));
        harness.Source.ReadableThrough = ShortCopyFinalChunkStart - 1;
        harness.QuiesceScript.Add(() => Quiesced(highest: ShortCopyTail));

        Assert.That(
            async () => await Admin(harness).ExecuteWalMoveAsync(
                TreeId, Move(0, SecondaryKey), new WalMoveOptions { VerifyAfterCopy = false }),
            Throws.InstanceOf<InvalidOperationException>()
                .With.Message.Contains($"the copy reached offset {ShortCopyFinalChunkStart - 1}")
                .And.Message.Contains($"quiesced highest offset {ShortCopyTail}"));
        Assert.Multiple(() =>
        {
            Assert.That(PinWasFlipped(harness), Is.False, "a short copy must never flip the durable pin");
            Assert.That(harness.Target.Offsets, Is.EqualTo(Range(0, (int)ShortCopyFinalChunkStart)),
                "the readable prefix landed and is kept for a resumable retry");
            Assert.That(harness.DeactivateCalls, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task A_copy_short_of_the_tail_resumes_once_the_tail_is_readable()
    {
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, (int)ShortCopyTail + 1));
        harness.Source.ReadableThrough = ShortCopyFinalChunkStart - 1;
        harness.QuiesceScript.Add(() => Quiesced(highest: ShortCopyTail));
        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("returned no entries past it"));

        harness.Source.ReadableThrough = null;
        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(Range(0, (int)ShortCopyTail + 1)),
                "the retry appends exactly the missing final chunk");
            Assert.That(harness.Target.AppendBatchSizes[^1], Is.EqualTo(ShortCopyTail - ShortCopyFinalChunkStart + 1));
        });
    }
}
