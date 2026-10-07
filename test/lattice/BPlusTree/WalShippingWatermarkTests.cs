using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Boundary tests for <see cref="WalShippingWatermark"/>, the rule that keeps a
/// cursor-advancing reader below the lowest still-in-flight append.
/// </summary>
[TestFixture]
public sealed class WalShippingWatermarkTests
{
    /// <summary>
    /// With an append in flight at offset k and a durable prefix through k - 1,
    /// the last exposable offset is k - 1, never k. The Coyote models flush a
    /// batch atomically, so the in-flight offset is never readable there and an
    /// off-by-one that exposes it ships nothing; this pins the boundary directly.
    /// </summary>
    [Test]
    public void The_first_in_flight_offset_is_never_exposable_and_the_one_below_it_is()
    {
        const long k = 5;
        var tail = WalShippingWatermark.DurableContiguousTail(
            hasInFlight: true, firstInFlightStartOffset: k, nextOffset: k + 3);

        Assert.Multiple(() =>
        {
            Assert.That(tail, Is.EqualTo(k), "the durable-contiguous tail is the first in-flight offset");
            Assert.That(WalShippingWatermark.IsOffsetExposable(k - 1, tail), Is.True,
                "the durable prefix through k - 1 is exposable");
            Assert.That(WalShippingWatermark.IsOffsetExposable(k, tail), Is.False,
                "the in-flight offset k must never be exposed");
            Assert.That(WalShippingWatermark.IsOffsetExposable(k + 1, tail), Is.False,
                "nothing above the hole is exposed");
        });
    }

    /// <summary>
    /// With nothing in flight the tail is the allocator's next offset, so every
    /// assigned offset is exposable and the next unassigned one is not.
    /// </summary>
    [Test]
    public void With_nothing_in_flight_every_assigned_offset_is_exposable()
    {
        var tail = WalShippingWatermark.DurableContiguousTail(
            hasInFlight: false, firstInFlightStartOffset: 0, nextOffset: 8);

        Assert.Multiple(() =>
        {
            Assert.That(tail, Is.EqualTo(8L));
            Assert.That(WalShippingWatermark.IsOffsetExposable(7, tail), Is.True);
            Assert.That(WalShippingWatermark.IsOffsetExposable(8, tail), Is.False);
        });
    }
}
