using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LeafLiveAdvanceCore"/>, the decision behind a leaf's
/// live checkpoint advance (issues #5572, #5593). Each condition is pinned on its
/// own, and the safety rule the WAL durability model's <c>LiveAdvance</c> relies on
/// is checked exhaustively over a small input grid.
/// </summary>
[TestFixture]
public sealed class LeafLiveAdvanceCoreTests
{
    [Test]
    public void A_quiescent_leaf_may_advance()
        => Assert.That(LeafLiveAdvanceCore.IsQuiescent(
            poisoned: false, unappliedAppends: 0, replayBarrierClear: true, replayInFlight: false, sagaPending: false), Is.True);

    [TestCase(true, 0, true, false, false, TestName = "A_poisoned_tracker_holds_the_advance")]
    [TestCase(false, 1, true, false, false, TestName = "An_unapplied_own_append_holds_the_advance")]
    [TestCase(false, 0, false, false, false, TestName = "An_unconverged_activation_replay_holds_the_advance")]
    [TestCase(false, 0, true, true, false, TestName = "A_replay_in_flight_holds_the_advance")]
    [TestCase(false, 0, true, false, true, TestName = "A_pending_saga_holds_the_advance")]
    public void Each_quiescence_condition_holds_the_advance(
        bool poisoned, int unapplied, bool barrierClear, bool replayInFlight, bool sagaPending)
        => Assert.That(LeafLiveAdvanceCore.IsQuiescent(poisoned, unapplied, barrierClear, replayInFlight, sagaPending), Is.False);

    [Test]
    public void A_born_leaf_advances_to_its_highest_own_offset_past_entries_it_never_read()
        => Assert.That(LeafLiveAdvanceCore.TargetFor(born: true, ownLow: 40, ownHigh: 42, current: -1, staleLatched: false),
            Is.EqualTo(42L));

    [Test]
    public void An_unborn_leaf_is_armed_only_once_its_checkpoint_reaches_its_first_own_offset()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafLiveAdvanceCore.TargetFor(born: false, ownLow: 40, ownHigh: 42, current: 38, staleLatched: false),
                Is.EqualTo(-1L), "offset 39 has not been read and may be the leaf's own.");
            Assert.That(LeafLiveAdvanceCore.TargetFor(born: false, ownLow: 40, ownHigh: 42, current: 39, staleLatched: false),
                Is.EqualTo(42L));
        });
    }

    [Test]
    public void A_stale_partition_or_one_already_past_its_own_appends_does_not_move()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LeafLiveAdvanceCore.TargetFor(born: true, ownLow: 0, ownHigh: 5, current: -1, staleLatched: true), Is.EqualTo(-1L));
            Assert.That(LeafLiveAdvanceCore.TargetFor(born: true, ownLow: 0, ownHigh: 5, current: 5, staleLatched: false), Is.EqualTo(-1L));
            Assert.That(LeafLiveAdvanceCore.TargetFor(born: true, ownLow: long.MaxValue, ownHigh: -1, current: -1, staleLatched: false),
                Is.EqualTo(-1L), "a partition with no own append has nothing to advance over.");
        });
    }

    /// <summary>
    /// The rule the model's <c>LiveAdvance</c> relies on, over every small input:
    /// an advance never moves backwards, never passes the highest own offset, and
    /// an unborn leaf never passes an offset below its first own append that its
    /// checkpoint has not reached.
    /// </summary>
    [Test]
    public void An_advance_never_passes_an_offset_the_leaf_has_not_read_or_written()
    {
        foreach (var born in new[] { false, true })
            for (long low = 0; low <= 4; low++)
                for (var high = low; high <= 4; high++)
                    for (long current = -1; current <= 4; current++)
                    {
                        var target = LeafLiveAdvanceCore.TargetFor(born, low, high, current, staleLatched: false);
                        if (target < 0)
                            continue;

                        Assert.Multiple(() =>
                        {
                            Assert.That(target, Is.GreaterThan(current));
                            Assert.That(target, Is.EqualTo(high));
                            if (!born)
                                Assert.That(current, Is.GreaterThanOrEqualTo(low - 1));
                        });
                    }
    }
}
