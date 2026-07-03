using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the <see cref="LeafCacheGrain.DebugFootprint"/> diagnostic seam
/// consumed by the <c>Bench.LeafCacheGrowth</c> memory probe and the future
/// per-activation budget regression test. The seam must report the live row
/// count and the summed non-null value-payload bytes of the cache mirror.
/// </summary>
public partial class LeafCacheGrainTests
{
    [Test]
    public void DebugFootprint_is_empty_before_any_read()
    {
        var (grain, _) = CreateGrain();

        var footprint = grain.DebugFootprint();

        Assert.That(footprint.EntryCount, Is.EqualTo(0));
        Assert.That(footprint.ValueBytes, Is.EqualTo(0));
    }

    [Test]
    public async Task DebugFootprint_counts_rows_and_sums_value_payload_bytes()
    {
        var (grain, leaf) = CreateGrain();
        var a = Encoding.UTF8.GetBytes("value-a");   // 7 bytes
        var b = Encoding.UTF8.GetBytes("bb");        // 2 bytes
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(("k1", a), ("k2", b)));

        await grain.GetAsync("k1");

        var footprint = grain.DebugFootprint();
        Assert.That(footprint.EntryCount, Is.EqualTo(2));
        Assert.That(footprint.ValueBytes, Is.EqualTo(a.Length + b.Length));
    }

    [Test]
    public async Task DebugFootprint_counts_tombstone_row_with_zero_value_bytes()
    {
        var (grain, leaf) = CreateGrain();

        // A tombstone carries a null Value, so it contributes to EntryCount but
        // adds zero to the summed payload bytes.
        var clock = HybridLogicalClock.Tick(default);
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(TombstoneDelta("gone", clock));

        await grain.GetAsync("gone");

        var footprint = grain.DebugFootprint();
        Assert.That(footprint.EntryCount, Is.EqualTo(1));
        Assert.That(footprint.ValueBytes, Is.EqualTo(0));
    }
}
