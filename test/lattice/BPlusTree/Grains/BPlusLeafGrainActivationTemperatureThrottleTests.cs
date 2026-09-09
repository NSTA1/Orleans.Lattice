using System.Diagnostics;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Guard tests for the rate-limited activation-temperature sample line
/// (issue #2148).
/// <para>
/// Classified in advance as <b>guard</b> tests. The design's whole claim is
/// that rate-limiting a line which prints <b>cumulative totals</b> is lossless,
/// where rate-limiting a line that announced each activation would not be: the
/// suppressed activations are still in the totals the next emitted line prints,
/// so any single line yields the cold:warm ratio and any two yield the rate.
/// These tests assert both halves - that suppression really happens, and that
/// the next emitted line still accounts for every activation in the suppressed
/// window. Without the second half the design is intended but unverified.
/// </para>
/// <para>
/// The control against a vacuous pass is that every test asserts an emission as
/// well as a suppression: an implementation that suppressed <b>everything</b>
/// would satisfy the negative assertions alone.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainActivationTemperatureThrottleTests
{
    private static string UniqueTree() => $"temperature-throttle-{Guid.NewGuid():N}";

    /// <summary>
    /// A fresh leaf identity per call. These tests are about the throttle, not
    /// the distinct-cold-leaf population, so each observation is attributed to
    /// its own leaf and the distinct count simply tracks the cold total.
    /// </summary>
    private static GrainId NextLeaf() => GrainId.Create("leaf", Guid.NewGuid().ToString("N"));

    /// <summary>Projects a sample down to the two totals a test asserts on.</summary>
    private static (long Cold, long Warm) Totals(BPlusLeafGrain.ActivationTemperatureSample sample)
        => (sample.Cold, sample.Warm);

    /// <summary>
    /// A timestamp delta comfortably larger than any plausible throttle
    /// interval, used to step past it deterministically instead of sleeping.
    /// </summary>
    private static long OneHour => Stopwatch.Frequency * 3600;

    [Test]
    public void The_first_observation_for_a_tree_emits_a_line()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var sample = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), now);

        Assert.That(sample, Is.Not.Null,
            "The first replay a tree records must emit, or a short-lived process reports nothing at all.");
        Assert.That(Totals(sample!.Value), Is.EqualTo((1L, 0L)));
    }

    [Test]
    public void Repeats_inside_the_interval_are_suppressed()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), now);
        var second = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), now);
        var third = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), now);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null, "The first observation emits,");
            Assert.That(second, Is.Null, "the immediate repeat is suppressed,");
            Assert.That(third, Is.Null, "and so is the one after it, regardless of arm.");
        });
    }

    /// <summary>
    /// The assertion that makes the design true rather than merely intended: a
    /// suppressed activation is not a lost activation. If the totals were
    /// accumulated only when a line is emitted - or if the line reported "an
    /// activation happened" instead of the running totals - the count would be
    /// short by exactly the suppressed window, and silently so.
    /// </summary>
    [Test]
    public void The_next_emitted_line_accounts_for_every_activation_in_the_suppressed_window()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start);

        // Four more inside the interval: all suppressed, none may be lost.
        var suppressed = new[]
        {
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start),
        };

        // The interval has now elapsed, so the next observation emits.
        var next = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start + OneHour);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null, "The first observation emits,");
            Assert.That(suppressed, Is.All.Null, "the four inside the interval are suppressed,");
            Assert.That(next, Is.Not.Null, "and the next one past the interval emits again.");
        });

        // 6 observations total: 3 cold (1 emitted + 2 suppressed),
        // 3 warm (2 suppressed + 1 emitted).
        Assert.That(Totals(next!.Value), Is.EqualTo((3L, 3L)),
            "The emitted line's totals must include every suppressed activation. A line that reported "
            + "only what it saw would undercount by an unknown factor, because suppression is invisible "
            + "in the output - which is exactly why the line prints cumulative totals rather than "
            + "announcing an event.");
        Assert.That(next.Value.Cold + next.Value.Warm, Is.EqualTo(6L),
            "and the two arms must sum to every observation, so neither is dropped nor double-counted.");
    }

    [Test]
    public void Totals_stay_cumulative_across_successive_emitted_lines()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start);
        BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start);
        var second = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start + OneHour);
        BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start + OneHour);
        var third = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start + (2 * OneHour));

        Assert.Multiple(() =>
        {
            Assert.That(Totals(first!.Value), Is.EqualTo((1L, 0L)));
            Assert.That(Totals(second!.Value), Is.EqualTo((2L, 1L)));
            Assert.That(Totals(third!.Value), Is.EqualTo((3L, 2L)),
                "Totals must never reset between lines: two consecutive lines yield the rate only if "
                + "each one is a running total taken from the same origin.");
        });
    }

    /// <summary>
    /// The line is keyed by tree only - deliberately unlike the over-budget
    /// warning's (tree, leaf, partition) key (issue #2023). That key names the
    /// leaf because it reports a per-leaf checkpoint whose successive values
    /// must be comparable; this line reports a per-tree aggregate already summed
    /// over every leaf, so a per-leaf key would emit one line per leaf per
    /// interval - the flood the throttle exists to prevent.
    /// </summary>
    [Test]
    public void Each_tree_is_throttled_and_totalled_independently()
    {
        var treeA = UniqueTree();
        var treeB = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var a = BPlusLeafGrain.ObserveLeafActivationReplay(treeA, cold: true, NextLeaf(), now);
        var b = BPlusLeafGrain.ObserveLeafActivationReplay(treeB, cold: false, NextLeaf(), now);
        var aRepeat = BPlusLeafGrain.ObserveLeafActivationReplay(treeA, cold: true, NextLeaf(), now);

        Assert.Multiple(() =>
        {
            Assert.That(Totals(a!.Value), Is.EqualTo((1L, 0L)), "Tree A reports its own totals,");
            Assert.That(Totals(b!.Value), Is.EqualTo((0L, 1L)),
                "tree B reports separately and is not suppressed by tree A - the ratio is per tree,");
            Assert.That(aRepeat, Is.Null, "while tree A's own repeat inside the interval stays suppressed.");
        });
    }

    /// <summary>
    /// The discrimination the distinct-cold-leaf count exists to provide
    /// (issue #2278). A per-tree cold total alone cannot separate the two
    /// states below, and they have opposite remedies: a broad arm is a
    /// one-time first-activation cost that terminates on its own, whereas a
    /// narrow arm with the same total is the same few leaves failing to bank a
    /// snapshot and replaying forever. The deployed line that prompted the
    /// issue reported only the total, so the question was unanswerable from
    /// the logs.
    /// <para>
    /// Asserted as one test over both states deliberately: a test that only
    /// pinned the broad case would pass against an implementation that
    /// returned the cold total verbatim, which is precisely the signal being
    /// replaced.
    /// </para>
    /// </summary>
    [Test]
    public void The_distinct_count_separates_a_broad_cold_arm_from_a_repeating_one()
    {
        var start = Stopwatch.GetTimestamp();

        // A broad arm: five cold replays, five different leaves.
        var broadTree = UniqueTree();
        BPlusLeafGrain.ActivationTemperatureSample broad = default;
        for (var i = 0; i < 5; i++)
        {
            broad = BPlusLeafGrain.ObserveLeafActivationReplay(
                broadTree, cold: true, NextLeaf(), start + (i * OneHour))!.Value;
        }

        // A repeating arm: the same five cold replays, one leaf.
        var loopTree = UniqueTree();
        var stuckLeaf = NextLeaf();
        BPlusLeafGrain.ActivationTemperatureSample loop = default;
        for (var i = 0; i < 5; i++)
        {
            loop = BPlusLeafGrain.ObserveLeafActivationReplay(
                loopTree, cold: true, stuckLeaf, start + (i * OneHour))!.Value;
        }

        Assert.Multiple(() =>
        {
            Assert.That(broad.Cold, Is.EqualTo(5L), "Both arms report the same cold total,");
            Assert.That(loop.Cold, Is.EqualTo(5L), "which is why the total alone cannot tell them apart.");

            Assert.That(broad.DistinctColdLeaves, Is.EqualTo(5),
                "The broad arm spent its total across five leaves - a one-time cost per leaf,");
            Assert.That(loop.DistinctColdLeaves, Is.EqualTo(1),
                "while the repeating arm spent all five on one leaf, which is a rehydrate defect.");

            Assert.That(broad.DistinctColdLeavesSaturated, Is.False);
            Assert.That(loop.DistinctColdLeavesSaturated, Is.False,
                "Neither arm is near the cap, so both counts are exact rather than floors.");
        });
    }

    /// <summary>
    /// A warm replay must not enter the distinct-cold population. Were it to,
    /// the count would approximate the tree's whole leaf population rather than
    /// its cold arm, and the cold:distinct ratio - the entire diagnostic -
    /// would read as broad for every tree regardless of shape.
    /// </summary>
    [Test]
    public void Warm_replays_do_not_enter_the_distinct_cold_population()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();

        BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, NextLeaf(), start);
        for (var i = 0; i < 4; i++)
        {
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, NextLeaf(), start);
        }

        var sample = BPlusLeafGrain.ObserveLeafActivationReplay(
            tree, cold: false, NextLeaf(), start + OneHour)!.Value;

        Assert.Multiple(() =>
        {
            Assert.That(sample.Warm, Is.EqualTo(5L), "Five warm replays on five distinct leaves,");
            Assert.That(sample.Cold, Is.EqualTo(1L), "one cold replay,");
            Assert.That(sample.DistinctColdLeaves, Is.EqualTo(1),
                "and the distinct population counts only the cold arm's leaf.");
        });
    }

    /// <summary>
    /// Past the cap the set stops growing and the count latches as a floor. The
    /// saturation flag is what keeps the reported number honest: the line
    /// prefixes it with "at least", because a reader who took a saturated count
    /// as exact would compute a cold:distinct ratio that is too high and read a
    /// broad arm as a loop - the exact misreading the count exists to prevent.
    /// </summary>
    [Test]
    public void The_distinct_population_is_capped_and_says_so()
    {
        var tree = UniqueTree();
        var start = Stopwatch.GetTimestamp();
        const int cap = 512;

        BPlusLeafGrain.ActivationTemperatureSample sample = default;
        for (var i = 0; i < cap + 50; i++)
        {
            sample = BPlusLeafGrain.ObserveLeafActivationReplay(
                tree, cold: true, NextLeaf(), start + (i * OneHour))!.Value;
        }

        Assert.Multiple(() =>
        {
            Assert.That(sample.Cold, Is.EqualTo((long)cap + 50),
                "The cold total is uncapped - only the distinct set is bounded,");
            Assert.That(sample.DistinctColdLeaves, Is.EqualTo(cap),
                "the distinct count stops at the cap rather than growing per leaf,");
            Assert.That(sample.DistinctColdLeavesSaturated, Is.True,
                "and the sample says so, so the count is reported as a floor and not as an exact value.");
        });
    }
}
