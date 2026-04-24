using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the <see cref="LwwEntry"/> raw DTO — focused on the
/// <see cref="LwwValue{T}"/> round-trip that underpins every bulk-load,
/// snapshot, and saga-pre-value path.
/// </summary>
[TestFixture]
public class LwwEntryTests
{
    private static readonly HybridLogicalClock Ts =
        new() { WallClockTicks = 12345L, Counter = 7 };

    [Test]
    public void Ctor_from_LwwValue_copies_every_field()
    {
        var lww = LwwValue<byte[]>.CreateWithExpiry([0xAA, 0xBB], Ts, expiresAtTicks: 999L)
            with { OriginClusterId = "peer-a" };

        var entry = new LwwEntry("k", lww);

        Assert.That(entry.Key, Is.EqualTo("k"));
        Assert.That(entry.Value, Is.EqualTo(new byte[] { 0xAA, 0xBB }));
        Assert.That(entry.Timestamp, Is.EqualTo(Ts));
        Assert.That(entry.IsTombstone, Is.False);
        Assert.That(entry.ExpiresAtTicks, Is.EqualTo(999L));
        Assert.That(entry.OriginClusterId, Is.EqualTo("peer-a"));
    }

    [Test]
    public void ToLwwValue_round_trips_every_field()
    {
        var lww = LwwValue<byte[]>.CreateWithExpiry([1, 2, 3], Ts, expiresAtTicks: 42L)
            with { OriginClusterId = "east" };

        var roundTripped = new LwwEntry("k", lww).ToLwwValue();

        Assert.That(roundTripped.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(roundTripped.Timestamp, Is.EqualTo(Ts));
        Assert.That(roundTripped.IsTombstone, Is.False);
        Assert.That(roundTripped.ExpiresAtTicks, Is.EqualTo(42L));
        Assert.That(roundTripped.OriginClusterId, Is.EqualTo("east"));
    }

    [Test]
    public void OriginClusterId_defaults_to_null()
    {
        var lww = LwwValue<byte[]>.Create([1], Ts);
        var entry = new LwwEntry("k", lww);
        Assert.That(entry.OriginClusterId, Is.Null);
        Assert.That(entry.ToLwwValue().OriginClusterId, Is.Null);
    }

    [Test]
    public void Tombstone_round_trips_OriginClusterId()
    {
        var tomb = LwwValue<byte[]>.Tombstone(Ts) with { OriginClusterId = "west" };
        var entry = new LwwEntry("k", tomb);

        Assert.That(entry.IsTombstone, Is.True);
        Assert.That(entry.OriginClusterId, Is.EqualTo("west"));
        Assert.That(entry.ToLwwValue().IsTombstone, Is.True);
        Assert.That(entry.ToLwwValue().OriginClusterId, Is.EqualTo("west"));
    }
}
