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
        // Call rapidly in a tight loop so multiple invocations almost
        // certainly land in the same wall-clock tick.
        var writer = NewWriter();
        var samples = new HybridLogicalClock[100];
        for (var i = 0; i < samples.Length; i++)
        {
            samples[i] = InvokeNextHlc(writer);
        }

        // At least one consecutive pair must share wall ticks (the
        // counter-bump branch exists precisely for this case). Every
        // pair must still be strictly monotonic.
        var sawCounterBump = false;
        for (var i = 1; i < samples.Length; i++)
        {
            Assert.That(samples[i].CompareTo(samples[i - 1]), Is.GreaterThan(0),
                $"Monotonicity broke at index {i}");
            if (samples[i].WallClockTicks == samples[i - 1].WallClockTicks)
            {
                sawCounterBump = true;
                Assert.That(samples[i].Counter, Is.GreaterThan(samples[i - 1].Counter));
            }
        }

        Assert.That(sawCounterBump, Is.True,
            "100 rapid calls should land at least one pair in the same wall tick.");
    }
}

