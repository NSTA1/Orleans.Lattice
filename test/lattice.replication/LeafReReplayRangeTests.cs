using NUnit.Framework;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Tests for the targeted leaf re-replay public value types
/// (<see cref="LeafReReplayRange"/>, <see cref="LeafReReplayOutcome"/>, and
/// <see cref="LeafReReplaySkipReason"/>).
/// </summary>
[TestFixture]
public sealed class LeafReReplayRangeTests
{
    [Test]
    public void Range_properties_round_trip_through_init()
    {
        var range = new LeafReReplayRange { StartKey = "a", EndKey = "m" };

        Assert.Multiple(() =>
        {
            Assert.That(range.StartKey, Is.EqualTo("a"));
            Assert.That(range.EndKey, Is.EqualTo("m"));
        });
    }

    [Test]
    public void Range_default_is_unbounded_on_both_sides()
    {
        var range = default(LeafReReplayRange);

        Assert.Multiple(() =>
        {
            Assert.That(range.StartKey, Is.Null);
            Assert.That(range.EndKey, Is.Null);
        });
    }

    [Test]
    public void Range_value_equality_holds()
    {
        var a = new LeafReReplayRange { StartKey = "k", EndKey = null };
        var b = new LeafReReplayRange { StartKey = "k", EndKey = null };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Outcome_NotAttempted_is_default()
    {
        var outcome = LeafReReplayOutcome.NotAttempted;

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.False);
            Assert.That(outcome.RangesProcessed, Is.Zero);
            Assert.That(outcome.EntriesReReplayed, Is.Zero);
            Assert.That(outcome.SkipReason, Is.EqualTo(LeafReReplaySkipReason.None));
        });
    }

    [Test]
    public void Outcome_properties_round_trip_through_init()
    {
        var outcome = new LeafReReplayOutcome
        {
            Attempted = true,
            RangesProcessed = 2,
            EntriesReReplayed = 7,
            SkipReason = LeafReReplaySkipReason.None,
        };

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Attempted, Is.True);
            Assert.That(outcome.RangesProcessed, Is.EqualTo(2));
            Assert.That(outcome.EntriesReReplayed, Is.EqualTo(7));
        });
    }

    [Test]
    public void Skip_reason_enum_has_expected_numeric_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That((int)LeafReReplaySkipReason.None, Is.EqualTo(0));
            Assert.That((int)LeafReReplaySkipReason.Disabled, Is.EqualTo(1));
            Assert.That((int)LeafReReplaySkipReason.RangeEmpty, Is.EqualTo(2));
            Assert.That((int)LeafReReplaySkipReason.WalTrimmed, Is.EqualTo(3));
        });
    }

    [Test]
    public void Contains_unbounded_range_matches_every_key()
    {
        var range = default(LeafReReplayRange);

        Assert.Multiple(() =>
        {
            Assert.That(range.Contains(""), Is.True);
            Assert.That(range.Contains("a"), Is.True);
            Assert.That(range.Contains("zzz"), Is.True);
            Assert.That(range.Contains(null), Is.True);
        });
    }

    [Test]
    public void Contains_is_half_open_inclusive_start_exclusive_end()
    {
        var range = new LeafReReplayRange { StartKey = "b", EndKey = "m" };

        Assert.Multiple(() =>
        {
            Assert.That(range.Contains("a"), Is.False, "before start is excluded");
            Assert.That(range.Contains("b"), Is.True, "start is inclusive");
            Assert.That(range.Contains("f"), Is.True, "interior is included");
            Assert.That(range.Contains("m"), Is.False, "end is exclusive");
            Assert.That(range.Contains("z"), Is.False, "after end is excluded");
        });
    }

    [Test]
    public void Contains_null_start_is_unbounded_left()
    {
        var range = new LeafReReplayRange { StartKey = null, EndKey = "m" };

        Assert.Multiple(() =>
        {
            Assert.That(range.Contains(""), Is.True);
            Assert.That(range.Contains("a"), Is.True);
            Assert.That(range.Contains("m"), Is.False);
        });
    }

    [Test]
    public void Contains_null_end_is_unbounded_right()
    {
        var range = new LeafReReplayRange { StartKey = "m", EndKey = null };

        Assert.Multiple(() =>
        {
            Assert.That(range.Contains("a"), Is.False);
            Assert.That(range.Contains("m"), Is.True);
            Assert.That(range.Contains("zzz"), Is.True);
        });
    }

    [Test]
    public void Contains_treats_null_key_as_empty_string()
    {
        var leftmost = new LeafReReplayRange { StartKey = null, EndKey = "b" };
        var startsAtA = new LeafReReplayRange { StartKey = "a", EndKey = "b" };

        Assert.Multiple(() =>
        {
            // Empty string sorts before "a" ordinally, so it falls inside a
            // left-unbounded range but outside a range starting at "a".
            Assert.That(leftmost.Contains(null), Is.True);
            Assert.That(startsAtA.Contains(null), Is.False);
        });
    }

    [Test]
    public void Contains_uses_ordinal_comparison()
    {
        // Uppercase letters sort before lowercase ordinally (B = 0x42 < a = 0x61).
        var range = new LeafReReplayRange { StartKey = "B", EndKey = "a" };

        Assert.Multiple(() =>
        {
            Assert.That(range.Contains("B"), Is.True);
            Assert.That(range.Contains("Z"), Is.True);
            Assert.That(range.Contains("a"), Is.False);
        });
    }
}
