using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="WalDrainLagEligibility"/>, the single predicate shared by
/// the registry's drain-lag minimum and the sampler's lagging-consumer count
/// (issues #2446 and #3131).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class WalDrainLagEligibilityTests
{
    private const string Leaf = ILeafCursorReporter.MaterialiserConsumerIdPrefix + "tree_leaf-a";
    private const long Floor = 1_000_000;

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static WalCursorSnapshot Snapshot(
        string consumerId,
        long cursorTicks,
        long reportedAtTicks,
        long? advancedAtTicks)
        => new(consumerId, cursorTicks == 0 ? HybridLogicalClock.Zero : Hlc(cursorTicks), reportedAtTicks)
        {
            CursorAdvancedAtTicks = advancedAtTicks,
        };

    [Test]
    public void IsEligible_excludes_a_consumer_that_never_reported_a_cursor()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, 0, Floor + 1, Floor + 1), Floor), Is.False);

    [Test]
    public void IsEligible_excludes_a_cold_consumer()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot("view", Floor + 5, Floor - 1, Floor - 1), Floor), Is.False);

    [Test]
    public void IsEligible_excludes_a_leaf_whose_position_is_stale_despite_a_fresh_report()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, 10, Floor + 1, 0), Floor), Is.False);

    [Test]
    public void IsEligible_keeps_a_leaf_that_advanced_since_the_floor()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, 10, Floor + 1, Floor), Floor), Is.True);

    [Test]
    public void IsEligible_keeps_a_leaf_whose_cursor_is_inside_the_window()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, Floor, Floor + 1, 0), Floor), Is.True);

    [Test]
    public void IsEligible_judges_a_snapshot_without_position_age_on_report_age_alone()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, 10, Floor + 1, null), Floor), Is.True,
            "a registry that does not track position age must keep the #2446 behaviour");

    [Test]
    public void IsEligible_keeps_a_tree_wide_consumer_with_a_stale_position()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot("view-maintainer", 10, Floor + 1, 0), Floor), Is.True,
            "a stalled tree-wide tailer is genuine backlog and must stay visible");

    [Test]
    public void IsEligible_min_value_floor_keeps_every_positive_cursor()
        => Assert.That(WalDrainLagEligibility.IsEligible(Snapshot(Leaf, 10, 1, 0), long.MinValue), Is.True);

    [TestCase(Leaf, true)]
    [TestCase(ILeafCursorReporter.MaterialiserConsumerIdPrefix, true)]
    [TestCase("view-maintainer", false)]
    [TestCase("_LATTICE_MATERIALISER_tree_leaf", false)]
    [TestCase("", false)]
    public void IsRangeScoped_matches_only_the_leaf_materialiser_prefix(string consumerId, bool expected)
        => Assert.That(WalDrainLagEligibility.IsRangeScoped(consumerId), Is.EqualTo(expected));

    [Test]
    public void IsRangeScoped_null_consumer_id_is_not_range_scoped()
        => Assert.That(WalDrainLagEligibility.IsRangeScoped(null!), Is.False);
}
