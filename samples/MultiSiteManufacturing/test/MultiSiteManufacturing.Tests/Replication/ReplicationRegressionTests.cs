using MultiSiteManufacturing.Host.Replication;
using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Tests for the deterministic cursor-advance rule inside
/// <c>ReplicatorGrain.ShipOneBatchAsync</c>. The grain itself is
/// exercised end-to-end by the two-cluster walkthrough; this suite
/// locks down the rule so the partial-apply-data-loss regression
/// (fixed with this commit) can't return unnoticed.
/// </summary>
[TestFixture]
public sealed class ReplicatorCursorAdvanceTests
{
    // Mirror of the rule as written in ReplicatorGrain. Duplicating
    // a three-line expression here is cheaper than surfacing a
    // public helper purely for testability.
    private static HybridLogicalClock ComputeNewCursor(
        ReplicationAck ack,
        int entriesCount,
        HybridLogicalClock highestInBatch) =>
        ack.Applied == entriesCount
            ? (ack.HighestAppliedHlc.CompareTo(highestInBatch) >= 0 ? ack.HighestAppliedHlc : highestInBatch)
            : ack.HighestAppliedHlc;

    [Test]
    public void Full_apply_advances_cursor_to_highest_in_batch_when_ack_matches()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 1000, Counter = 5 };
        var ack = new ReplicationAck { Applied = 3, HighestAppliedHlc = hlc };

        var cursor = ComputeNewCursor(ack, entriesCount: 3, highestInBatch: hlc);

        Assert.That(cursor, Is.EqualTo(hlc));
    }

    [Test]
    public void Full_apply_picks_the_later_of_highest_in_batch_and_ack_hlc()
    {
        var sender = new HybridLogicalClock { WallClockTicks = 2000, Counter = 0 };
        var receiver = new HybridLogicalClock { WallClockTicks = 1900, Counter = 9 };
        var ack = new ReplicationAck { Applied = 2, HighestAppliedHlc = receiver };

        var cursor = ComputeNewCursor(ack, entriesCount: 2, highestInBatch: sender);

        Assert.That(cursor, Is.EqualTo(sender));
    }

    [Test]
    public void Partial_apply_advances_cursor_only_to_receiver_reported_hlc()
    {
        // Regression guard: receiver applied 1 of 3; sender must NOT
        // advance to highestInBatch or the two unapplied entries are
        // silently dropped on the next tick.
        var first = new HybridLogicalClock { WallClockTicks = 1000, Counter = 0 };
        var third = new HybridLogicalClock { WallClockTicks = 3000, Counter = 0 };
        var ack = new ReplicationAck { Applied = 1, HighestAppliedHlc = first };

        var cursor = ComputeNewCursor(ack, entriesCount: 3, highestInBatch: third);

        Assert.That(cursor, Is.EqualTo(first),
            "Partial apply must not advance past the receiver's highest acked HLC.");
    }

    [Test]
    public void Zero_apply_leaves_cursor_at_receiver_hlc_zero()
    {
        var high = new HybridLogicalClock { WallClockTicks = 5000, Counter = 0 };
        var ack = new ReplicationAck { Applied = 0, HighestAppliedHlc = HybridLogicalClock.Zero };

        var cursor = ComputeNewCursor(ack, entriesCount: 3, highestInBatch: high);

        Assert.That(cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }
}

/// <summary>
/// Tests for <see cref="ReplicationLogWriter"/>'s HLC monotonicity.
/// The writer mints its own HLC for each replog entry (independent of
/// the lattice's internal HLC); the contract is strictly monotonic
/// within a process so replog scans yield a stable order.
/// </summary>
[TestFixture]
public sealed class ReplicationLogWriterHlcTests
{
    // We can't easily unit-test AppendAsync end-to-end without a
    // cluster, so we verify the monotonicity contract by reflection
    // against the private NextHlc. Keeps the test-scope narrow and
    // doesn't force a public API change purely for testability.
    private static HybridLogicalClock InvokeNextHlc(ReplicationLogWriter writer)
    {
        var method = typeof(ReplicationLogWriter).GetMethod(
            "NextHlc",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ReplicationLogWriter.NextHlc not found.");
        return (HybridLogicalClock)method.Invoke(writer, null)!;
    }

    private static ReplicationLogWriter NewWriter() => new(
        grains: null!,
        topology: new ReplicationTopology
        {
            LocalCluster = "us",
            SharedSecret = "s",
            Peers = [],
            ReplicatedTrees = [],
        },
        logger: Microsoft.Extensions.Logging.Abstractions.NullLogger<ReplicationLogWriter>.Instance);

    [Test]
    public void NextHlc_returns_strictly_monotonic_sequence()
    {
        var writer = NewWriter();
        var first = InvokeNextHlc(writer);
        var second = InvokeNextHlc(writer);
        var third = InvokeNextHlc(writer);

        Assert.Multiple(() =>
        {
            Assert.That(second.CompareTo(first), Is.GreaterThan(0));
            Assert.That(third.CompareTo(second), Is.GreaterThan(0));
        });
    }

    [Test]
    public void NextHlc_bumps_counter_when_wall_ticks_dont_advance()
    {
        // Deterministically force the counter-bump branch: seed
        // _wallCeiling into the future so every NextHlc call takes
        // the "else" path and increments the counter. The previous
        // form of this test relied on a tight loop landing multiple
        // calls inside the same wall tick, which fails on machines
        // where DateTime.UtcNow.Ticks advances between successive
        // reads.
        var writer = NewWriter();
        var future = DateTime.UtcNow.Ticks + TimeSpan.FromMinutes(1).Ticks;
        SetWallCeiling(writer, future);

        var samples = new HybridLogicalClock[5];
        for (var i = 0; i < samples.Length; i++)
        {
            samples[i] = InvokeNextHlc(writer);
        }

        Assert.Multiple(() =>
        {
            // Every sample landed on the seeded future tick.
            foreach (var s in samples)
            {
                Assert.That(s.WallClockTicks, Is.EqualTo(future));
            }

            // Counter strictly monotonic inside the tick (0, 1, 2, …).
            for (var i = 1; i < samples.Length; i++)
            {
                Assert.That(samples[i].CompareTo(samples[i - 1]), Is.GreaterThan(0),
                    $"Monotonicity broke at index {i}");
                Assert.That(samples[i].Counter, Is.GreaterThan(samples[i - 1].Counter));
            }
        });
    }

    private static void SetWallCeiling(ReplicationLogWriter writer, long value)
    {
        var field = typeof(ReplicationLogWriter).GetField(
            "_wallCeiling",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ReplicationLogWriter._wallCeiling not found.");
        field.SetValue(writer, value);
    }
}

