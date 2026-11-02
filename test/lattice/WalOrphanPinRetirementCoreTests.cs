using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalOrphanPinRetirementCore"/>, the decision core
/// of the WAL GC's orphaned materialiser pin retirement (issues #3105, #4238,
/// #4246, #5358) that both retirement paths and the pin grain's removal fence
/// route through.
/// </summary>
[TestFixture]
public sealed class WalOrphanPinRetirementCoreTests
{
    private static readonly Guid Epoch = new("5358cafe-0000-0000-0000-000000000001");

    [TestCase(WalGcBlockingPinState.Orphaned, true)]
    [TestCase(WalGcBlockingPinState.NoDurableState, true)]
    [TestCase(WalGcBlockingPinState.Unreadable, false)]
    [TestCase(WalGcBlockingPinState.CheckpointedUncovered, false)]
    [TestCase(WalGcBlockingPinState.NeverCheckpointed, false)]
    [TestCase(WalGcBlockingPinState.CheckpointedCoverageUnknown, false)]
    public void IsHusk_is_true_only_for_a_row_with_no_record_or_no_tree_id(WalGcBlockingPinState state, bool husk)
    {
        Assert.That(WalOrphanPinRetirementCore.IsHusk(state), Is.EqualTo(husk));
    }

    [TestCase(WalGcBlockingPinState.CheckpointedUncovered, nameof(OrphanPinRemovalDecision.KeepLive))]
    [TestCase(WalGcBlockingPinState.NeverCheckpointed, nameof(OrphanPinRemovalDecision.KeepLive))]
    [TestCase(WalGcBlockingPinState.CheckpointedCoverageUnknown, nameof(OrphanPinRemovalDecision.KeepLive))]
    [TestCase(WalGcBlockingPinState.Unreadable, nameof(OrphanPinRemovalDecision.KeepUnreadable))]
    public void Decide_keeps_every_pin_whose_leaf_read_is_not_a_husk(
        WalGcBlockingPinState state, string expectedName)
    {
        var expected = Enum.Parse<OrphanPinRemovalDecision>(expectedName);
        foreach (var verdict in Enum.GetValues<ConsumerIdVerdict>())
        {
            foreach (var budget in new[] { true, false })
            {
                Assert.That(WalOrphanPinRetirementCore.Decide(state, verdict, budget), Is.EqualTo(expected),
                    $"verdict {verdict}, budget {budget}");
            }
        }
    }

    [TestCase(WalGcBlockingPinState.Orphaned)]
    [TestCase(WalGcBlockingPinState.NoDurableState)]
    public void Decide_retires_a_husk_only_for_a_leaf_published_id_within_budget(WalGcBlockingPinState state)
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                WalOrphanPinRetirementCore.Decide(state, ConsumerIdVerdict.LeafPublished, budgetAvailable: true),
                Is.EqualTo(OrphanPinRemovalDecision.Retire));
            Assert.That(
                WalOrphanPinRetirementCore.Decide(state, ConsumerIdVerdict.LeafPublished, budgetAvailable: false),
                Is.EqualTo(OrphanPinRemovalDecision.Defer));
            Assert.That(
                WalOrphanPinRetirementCore.Decide(state, ConsumerIdVerdict.MalformedId, budgetAvailable: true),
                Is.EqualTo(OrphanPinRemovalDecision.RefuseMalformedId));
            Assert.That(
                WalOrphanPinRetirementCore.Decide(state, ConsumerIdVerdict.AmbiguousPartition, budgetAvailable: true),
                Is.EqualTo(OrphanPinRemovalDecision.RefuseAmbiguousPartition));
        });
    }

    [Test]
    public void Decide_rejects_an_unmapped_verdict_rather_than_retiring()
    {
        Assert.That(
            () => WalOrphanPinRetirementCore.Decide(
                WalGcBlockingPinState.Orphaned, (ConsumerIdVerdict)99, budgetAvailable: true),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void MayRemove_admits_a_consumer_not_merged_since_the_token()
    {
        var token = new WalMaterialiserPinToken(Epoch, 7);

        Assert.Multiple(() =>
        {
            Assert.That(WalOrphanPinRetirementCore.MayRemove(token, Epoch, lastMergedSequence: null), Is.True);
            Assert.That(WalOrphanPinRetirementCore.MayRemove(token, Epoch, lastMergedSequence: 7), Is.True);
            Assert.That(WalOrphanPinRetirementCore.MayRemove(token, Epoch, lastMergedSequence: 3), Is.True);
        });
    }

    [Test]
    public void MayRemove_refuses_a_consumer_merged_after_the_token()
    {
        Assert.That(
            WalOrphanPinRetirementCore.MayRemove(new WalMaterialiserPinToken(Epoch, 7), Epoch, lastMergedSequence: 8),
            Is.False);
    }

    [Test]
    public void MayRemove_refuses_a_token_from_another_activation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                WalOrphanPinRetirementCore.MayRemove(new WalMaterialiserPinToken(Guid.NewGuid(), 7), Epoch, null),
                Is.False);
            Assert.That(
                WalOrphanPinRetirementCore.MayRemove(default, Epoch, null),
                Is.False,
                "a default token must never authorise a removal.");
        });
    }
}
