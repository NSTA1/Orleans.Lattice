namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5470: the coordinator wrote the target through its own provider
/// instance, so it waits for those appends to become readable before it
/// verifies the target and cuts over. The source's barrier is not the
/// coordinator's to call: only the silo whose WAL grain wrote the source knows
/// what it has yet to publish, so the quiesce does it there.
/// </summary>
public sealed partial class LatticeAdminGrainWalMoveTests
{
    [Test]
    public async Task A_move_waits_for_the_copy_to_be_readable_on_the_target_before_verifying_it()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));

        var receipt = await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey));

        Assert.Multiple(() =>
        {
            Assert.That(receipt.Outcome, Is.EqualTo(WalMoveOutcome.Moved));
            Assert.That(harness.Target.Calls, Does.Contain("barrier"));
            Assert.That(harness.Target.Calls.LastIndexOf("barrier"), Is.LessThan(harness.Target.Calls.LastIndexOf("highest")),
                "the verification reads the target tail only after the copy is readable");
            Assert.That(harness.Source.Calls, Does.Not.Contain("barrier"),
                "the source barrier belongs to the WAL grain's silo, inside the quiesce");
        });
    }

    [Test]
    public void A_move_whose_target_copy_cannot_be_made_readable_aborts_without_flipping()
    {
        var harness = CreateHarness();
        harness.Source.Seed(0, 1, 2);
        harness.QuiesceScript.Add(() => Quiesced(highest: 2));
        harness.Target.VisibilityFault = new InvalidOperationException("manifest commit failed");

        Assert.That(async () => await Admin(harness).ExecuteWalMoveAsync(TreeId, Move(0, SecondaryKey)),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("manifest commit failed"));
        Assert.Multiple(() =>
        {
            Assert.That(PinWasFlipped(harness), Is.False, "a copy that is not readable must never be cut over to");
            Assert.That(harness.DeactivateCalls, Is.GreaterThan(0), "the fenced source is released");
        });
    }
}
