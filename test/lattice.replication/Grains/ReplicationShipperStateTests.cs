using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationShipperStateTests
{
    [Test]
    public void New_instance_has_zero_cursor()
    {
        var state = new ReplicationShipperState();
        Assert.That(state.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void New_instance_has_zero_consecutive_failures()
    {
        var state = new ReplicationShipperState();
        Assert.That(state.ConsecutiveFailures, Is.EqualTo(0));
    }

    [Test]
    public void Cursor_is_settable()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 };
        var state = new ReplicationShipperState { Cursor = hlc };
        Assert.That(state.Cursor, Is.EqualTo(hlc));
    }

    [Test]
    public void ConsecutiveFailures_is_settable()
    {
        var state = new ReplicationShipperState { ConsecutiveFailures = 3 };
        Assert.That(state.ConsecutiveFailures, Is.EqualTo(3));
    }

    // partition-resume: per-partition resume cursors --------------------------

    [Test]
    public void New_instance_has_empty_partition_cursors()
    {
        // The empty default is the wire-compat anchor: legacy persisted
        // state without an [Id(2)] slot decodes to this same shape.
        var state = new ReplicationShipperState();
        Assert.That(state.PartitionCursors, Is.Not.Null);
        Assert.That(state.PartitionCursors, Is.Empty);
    }

    [Test]
    public void PartitionCursors_is_settable()
    {
        var state = new ReplicationShipperState();
        state.PartitionCursors[0] = 42;
        Assert.That(state.PartitionCursors[0], Is.EqualTo(42L));
    }

    [Test]
    public void PartitionCursors_supports_multiple_partitions()
    {
        var state = new ReplicationShipperState();
        state.PartitionCursors[0] = 100;
        state.PartitionCursors[1] = 250;
        state.PartitionCursors[7] = 999;
        Assert.Multiple(() =>
        {
            Assert.That(state.PartitionCursors[0], Is.EqualTo(100L));
            Assert.That(state.PartitionCursors[1], Is.EqualTo(250L));
            Assert.That(state.PartitionCursors[7], Is.EqualTo(999L));
            Assert.That(state.PartitionCursors, Has.Count.EqualTo(3));
        });
    }

    /// <summary>
    /// Wire-compat anchor: a state value that round-trips through the
    /// Orleans serializer must come back byte-for-byte equivalent —
    /// proves <c>[Id(2)] PartitionCursors</c> participates in
    /// serialization without disturbing the existing slots.
    /// </summary>
    [Test]
    public void PartitionCursors_round_trips_through_orleans_serializer()
    {
        var serializer = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer<ReplicationShipperState>>();

        var original = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 },
            ConsecutiveFailures = 3,
        };
        original.PartitionCursors[0] = 100;
        original.PartitionCursors[2] = 4242;

        var bytes = serializer.SerializeToArray(original);
        var roundTripped = serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.Cursor, Is.EqualTo(original.Cursor));
            Assert.That(roundTripped.ConsecutiveFailures, Is.EqualTo(3));
            Assert.That(roundTripped.PartitionCursors, Has.Count.EqualTo(2));
            Assert.That(roundTripped.PartitionCursors[0], Is.EqualTo(100L));
            Assert.That(roundTripped.PartitionCursors[2], Is.EqualTo(4242L));
        });
    }

    /// <summary>
    /// Wire-compat anchor for the upgrade path: a freshly-constructed
    /// instance round-tripped through the serializer with no
    /// PartitionCursors entries written must come back with an empty
    /// (non-null) PartitionCursors. Combined with the
    /// <see cref="New_instance_has_empty_partition_cursors"/> initializer
    /// guarantee, this exercises the same code path Orleans takes when
    /// deserializing a legacy persisted blob that pre-dates the
    /// <c>[Id(2)]</c> slot: the parameterless constructor runs first
    /// (initializer populates an empty dictionary), then only the
    /// tagged slots present in the wire bytes are overwritten — a
    /// missing <c>[Id(2)]</c> tag therefore leaves PartitionCursors at
    /// the empty-dictionary default rather than null.
    /// </summary>
    [Test]
    public void Round_trip_with_no_partition_cursors_yields_empty_dictionary()
    {
        var serializer = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer<ReplicationShipperState>>();

        var original = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 999, Counter = 1 },
            ConsecutiveFailures = 5,
            // PartitionCursors deliberately untouched — exercises the
            // same wire shape a legacy blob produces.
        };
        var bytes = serializer.SerializeToArray(original);
        var roundTripped = serializer.Deserialize(bytes);

        Assert.Multiple(() =>
        {
            Assert.That(roundTripped.Cursor, Is.EqualTo(original.Cursor));
            Assert.That(roundTripped.ConsecutiveFailures, Is.EqualTo(5));
            Assert.That(roundTripped.PartitionCursors, Is.Not.Null,
                "Empty PartitionCursors must round-trip as the initializer default, never null.");
            Assert.That(roundTripped.PartitionCursors, Is.Empty,
                "Empty PartitionCursors must round-trip as empty so partition resume falls back to cold-start.");
        });
    }
}
