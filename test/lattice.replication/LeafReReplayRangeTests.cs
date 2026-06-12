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
}
