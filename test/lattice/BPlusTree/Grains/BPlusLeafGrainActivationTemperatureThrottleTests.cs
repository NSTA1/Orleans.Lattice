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
    /// A timestamp delta comfortably larger than any plausible throttle
    /// interval, used to step past it deterministically instead of sleeping.
    /// </summary>
    private static long OneHour => Stopwatch.Frequency * 3600;

    [Test]
    public void The_first_observation_for_a_tree_emits_a_line()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var sample = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, now);

        Assert.That(sample, Is.Not.Null,
            "The first replay a tree records must emit, or a short-lived process reports nothing at all.");
        Assert.That(sample!.Value, Is.EqualTo((1L, 0L)));
    }

    [Test]
    public void Repeats_inside_the_interval_are_suppressed()
    {
        var tree = UniqueTree();
        var now = Stopwatch.GetTimestamp();

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, now);
        var second = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, now);
        var third = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, now);

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

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start);

        // Four more inside the interval: all suppressed, none may be lost.
        var suppressed = new[]
        {
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, start),
            BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, start),
        };

        // The interval has now elapsed, so the next observation emits.
        var next = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, start + OneHour);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null, "The first observation emits,");
            Assert.That(suppressed, Is.All.Null, "the four inside the interval are suppressed,");
            Assert.That(next, Is.Not.Null, "and the next one past the interval emits again.");
        });

        // 6 observations total: 3 cold (1 emitted + 2 suppressed),
        // 3 warm (2 suppressed + 1 emitted).
        Assert.That(next!.Value, Is.EqualTo((3L, 3L)),
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

        var first = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start);
        BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start);
        var second = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, start + OneHour);
        BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: false, start + OneHour);
        var third = BPlusLeafGrain.ObserveLeafActivationReplay(tree, cold: true, start + (2 * OneHour));

        Assert.Multiple(() =>
        {
            Assert.That(first!.Value, Is.EqualTo((1L, 0L)));
            Assert.That(second!.Value, Is.EqualTo((2L, 1L)));
            Assert.That(third!.Value, Is.EqualTo((3L, 2L)),
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

        var a = BPlusLeafGrain.ObserveLeafActivationReplay(treeA, cold: true, now);
        var b = BPlusLeafGrain.ObserveLeafActivationReplay(treeB, cold: false, now);
        var aRepeat = BPlusLeafGrain.ObserveLeafActivationReplay(treeA, cold: true, now);

        Assert.Multiple(() =>
        {
            Assert.That(a!.Value, Is.EqualTo((1L, 0L)), "Tree A reports its own totals,");
            Assert.That(b!.Value, Is.EqualTo((0L, 1L)),
                "tree B reports separately and is not suppressed by tree A - the ratio is per tree,");
            Assert.That(aRepeat, Is.Null, "while tree A's own repeat inside the interval stays suppressed.");
        });
    }
}
