using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="LeafLiveAdvanceCore"/>, the decision behind a leaf's live
/// checkpoint advance (issues #5572, #5593; epic #5605, member 4). Each condition of the
/// design model's <c>Armed</c> predicate (<c>spec/wal/design/WalLiveAdvance.tla</c>) is
/// pinned on its own, and the predicate is checked exhaustively over a small input grid:
/// a target is returned exactly when every conjunct holds.
/// </summary>
[TestFixture]
public sealed class LeafLiveAdvanceCoreTests
{
    private static long Target(
        bool claimRaised = true,
        long claimHead = 10,
        bool handOffClear = true,
        long closeHead = -1,
        long unstampedSeen = -1,
        long ownHigh = 15,
        long current = 9,
        bool staleLatched = false) =>
        LeafLiveAdvanceCore.TargetFor(claimRaised, claimHead, handOffClear, closeHead, unstampedSeen, ownHigh, current, staleLatched);

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
    public void An_armed_leaf_advances_to_its_highest_own_offset()
        => Assert.That(Target(), Is.EqualTo(15L));

    [Test]
    public void A_new_trees_first_leaf_arms_with_no_replay()
        => Assert.That(Target(claimHead: 0, current: -1, ownHigh: 3), Is.EqualTo(3L),
            "the claim head of an empty partition is 0, so a checkpoint of -1 has read everything before it (#5570)");

    [Test]
    public void An_unclaimed_partition_holds_the_advance()
        => Assert.That(Target(claimRaised: false), Is.EqualTo(-1L),
            "cstage = 3: without the claim a duplicate activation's appends are not rejected");

    [Test]
    public void A_leaf_that_has_not_read_to_its_claim_head_holds_the_advance()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Target(claimHead: 10, current: 8), Is.EqualTo(-1L), "offset 9 predates the claim and has not been read");
            Assert.That(Target(claimHead: 10, current: 9), Is.EqualTo(15L));
        });
    }

    [Test]
    public void An_open_or_uninitialised_hand_off_holds_the_advance()
        => Assert.That(Target(handOffClear: false), Is.EqualTo(-1L), "~MarkerOpen and mInit");

    [Test]
    public void A_leaf_that_has_not_read_to_a_close_head_holds_the_advance()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Target(closeHead: 12, current: 10), Is.EqualTo(-1L), "a foreign record may lie at offset 11");
            Assert.That(Target(closeHead: 12, current: 11), Is.EqualTo(15L));
        });
    }

    [Test]
    public void A_leaf_that_has_not_read_past_a_reported_unstamped_offset_holds_the_advance()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Target(unstampedSeen: 12, current: 11), Is.EqualTo(-1L), "rp >= seenU");
            Assert.That(Target(unstampedSeen: 12, current: 12), Is.EqualTo(15L));
        });
    }

    [Test]
    public void A_stale_partition_or_one_already_past_its_own_appends_does_not_move()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Target(staleLatched: true), Is.EqualTo(-1L));
            Assert.That(Target(ownHigh: 9, current: 9), Is.EqualTo(-1L));
            Assert.That(Target(ownHigh: -1), Is.EqualTo(-1L), "a partition with no own append has nothing to advance over");
        });
    }

    /// <summary>
    /// The model's <c>Armed</c> predicate over every small input: a target is returned
    /// exactly when every conjunct holds, it is always the highest own offset, and it
    /// never moves the checkpoint backwards.
    /// </summary>
    [Test]
    public void The_advance_is_taken_exactly_when_the_leaf_is_armed()
    {
        var checkedCount = 0;
        foreach (var claimRaised in new[] { false, true })
            foreach (var handOffClear in new[] { false, true })
                for (long claimHead = 0; claimHead <= 3; claimHead++)
                    for (long closeHead = -1; closeHead <= 3; closeHead++)
                        for (long unstamped = -1; unstamped <= 3; unstamped++)
                            for (long current = -1; current <= 3; current++)
                                for (long ownHigh = -1; ownHigh <= 4; ownHigh++)
                                {
                                    var target = LeafLiveAdvanceCore.TargetFor(
                                        claimRaised, claimHead, handOffClear, closeHead, unstamped, ownHigh, current, staleLatched: false);
                                    var armed = claimRaised
                                        && handOffClear
                                        && current >= claimHead - 1
                                        && current >= closeHead - 1
                                        && current >= unstamped;
                                    var expected = armed && ownHigh > current ? ownHigh : -1L;
                                    checkedCount++;
                                    if (target != expected)
                                    {
                                        Assert.Fail($"claim={claimRaised}/{claimHead} clear={handOffClear} close={closeHead} unstamped={unstamped} current={current} own={ownHigh}: expected {expected}, was {target}");
                                    }
                                }

        Assert.That(checkedCount, Is.GreaterThan(1000), "the grid is not vacuous");
    }

    [Test]
    public void ChainHopsAreNeverExcluded()
    {
        // a5e2e2ef's S-b on #5837's C model: only the immediate split's tag may be excluded;
        // a chain-forwarded hop (k >= 2) counts in every reader's foreign high.
        var readerTag = new Guid("5b000000-0000-0000-0000-0000000005b1");
        var reader = new Guid("ac000000-0000-0000-0000-0000000000a1");
        var empty = WalShardGrain.LeafAuthors.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(empty.WithForwardHop(1, readerTag, 7).ForeignHigh(reader, readerTag), Is.LessThan(7),
                "precondition: the first hop is excluded for its own narrow's reader");
            for (var hop = 2; hop <= WalAppendStamp.MaxForwardHops; hop++)
            {
                var authors = empty.WithForwardHop(hop, readerTag, 7);
                Assert.That(authors.ForeignHigh(reader, readerTag), Is.EqualTo(7), $"chain hop {hop} was excluded by its own tag");
                Assert.That(authors.ForeignHigh(reader, Guid.NewGuid()), Is.EqualTo(7), $"chain hop {hop} was excluded by another tag");
                Assert.That(authors.StaleHigh(Guid.NewGuid(), 0, long.MaxValue, readerTag), Is.EqualTo(7),
                    $"chain hop {hop} was excluded from a receiver's stale high");
            }
        });
    }
}
