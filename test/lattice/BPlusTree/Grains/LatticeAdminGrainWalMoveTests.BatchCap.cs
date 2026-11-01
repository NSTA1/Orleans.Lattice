namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5445: the copy phase writes each source page to the target in appends
/// no larger than the target's advertised per-call cap, so a move with default
/// options (256-entry pages) succeeds on a provider that caps an append at 100
/// entries, and an interrupted copy resumes past exactly the chunks that landed.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    private static long[] Range(long from, int count)
    {
        var offsets = new long[count];
        for (var i = 0; i < count; i++)
        {
            offsets[i] = from + i;
        }
        return offsets;
    }

    [Test]
    public async Task A_move_with_default_options_chunks_each_page_to_the_target_append_cap()
    {
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, 300));
        harness.QuiesceScript.Add(() => Quiesced(highest: 299));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendBatchSizes, Is.EqualTo(new[] { 100, 100, 56, 44 }),
                "each 256-entry page is written as appends of at most 100 entries");
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(Range(0, 300)));
            Assert.That(receipt.Moves[0].TargetHighestOffset, Is.EqualTo(299));
        });
    }

    [Test]
    public async Task A_move_to_an_uncapped_target_writes_each_page_in_one_append()
    {
        var harness = CreateHarness();
        harness.Source.Seed(Range(0, 300));
        harness.QuiesceScript.Add(() => Quiesced(highest: 299));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendBatchSizes, Is.EqualTo(new[] { 256, 44 }));
        });
    }

    [Test]
    public async Task A_copy_interrupted_between_chunks_resumes_past_the_chunks_that_landed()
    {
        var harness = CreateHarness(targetMaxAppendBatchEntries: 100);
        harness.Source.Seed(Range(0, 300));
        harness.QuiesceScript.Add(() => Quiesced(highest: 299));
        harness.Target.FailOnAppendCall = 2;

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.Contains("scripted append failure"));
        Assert.Multiple(() =>
        {
            Assert.That(PinWasFlipped(harness), Is.False, "an aborted move must never flip the durable pin");
            Assert.That(harness.Target.Offsets, Is.EqualTo(Range(0, 100)),
                "only the first chunk landed; the failed chunk left nothing behind");
        });

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.AppendedOffsets, Is.EqualTo(Range(0, 300)),
                "the re-drive appends exactly the missing suffix: no duplicate and no gap");
            Assert.That(harness.Target.AppendBatchSizes, Has.All.LessThanOrEqualTo(100));
        });
    }
}
