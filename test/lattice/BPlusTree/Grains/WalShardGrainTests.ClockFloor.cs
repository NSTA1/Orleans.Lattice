using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The WAL partition's clock floor (issue #4586): advanced and persisted on a
/// shipping read while the capability gate is open, published with the next
/// offset, and enforced on every fresh local append from then on.
/// </summary>
public partial class WalShardGrainTests
{
    private static WalRecord StampedEntry(string key, HybridLogicalClock stamp) => MakeEntry(key) with { Timestamp = stamp };

    private static HybridLogicalClock NowStamp() =>
        new() { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };

    private static HybridLogicalClock Ago(TimeSpan age) =>
        new() { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks - age.Ticks, Counter = 0 };

    [Test]
    public async Task A_closed_gate_publishes_no_floor_and_refuses_nothing()
    {
        var floorState = new FakePersistentState<WalShardFloorState>();
        var grain = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = false });

        var page = await grain.ReadShippingAsync(0, 10, CancellationToken.None);
        await grain.AppendAsync(StampedEntry("old", Ago(TimeSpan.FromHours(1))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ClockFloor, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(page.ClockFloorOffset, Is.Zero);
            Assert.That(floorState.WriteCount, Is.Zero, "a closed gate never persists a floor");
        });
    }

    [Test]
    public async Task An_open_gate_persists_the_floor_before_publishing_it_with_the_next_offset()
    {
        var floorState = new FakePersistentState<WalShardFloorState>();
        var grain = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = true });
        await grain.AppendAsync(StampedEntry("a", NowStamp()), CancellationToken.None);
        await grain.AppendAsync(StampedEntry("b", NowStamp()), CancellationToken.None);
        var before = DateTimeOffset.UtcNow.UtcTicks;

        var page = await grain.ReadShippingAsync(0, 10, CancellationToken.None);

        var lag = new LatticeOptions().ReplicationClockFloorLag;
        Assert.Multiple(() =>
        {
            Assert.That(floorState.WriteCount, Is.EqualTo(1));
            Assert.That(page.ClockFloor, Is.EqualTo(floorState.State.Floor), "only a persisted floor is published");
            Assert.That(page.ClockFloor.WallClockTicks, Is.InRange(before - lag.Ticks - TimeSpan.FromSeconds(5).Ticks, before - lag.Ticks + TimeSpan.FromSeconds(5).Ticks));
            Assert.That(page.ClockFloorOffset, Is.EqualTo(2), "the floor is paired with the next offset");
        });
    }

    [Test]
    public async Task A_published_floor_refuses_an_older_fresh_append_without_assigning_an_offset()
    {
        var grain = await CreateGrainAsync(floorGate: new TestWalClockFloorGate { IsOpen = true });
        var page = await grain.ReadShippingAsync(0, 10, CancellationToken.None);
        var below = page.ClockFloor with { Counter = 0, WallClockTicks = page.ClockFloor.WallClockTicks - 1 };

        var refusal = Assert.ThrowsAsync<WalStampBelowFloorException>(
            async () => await grain.AppendAsync(StampedEntry("late", below), CancellationToken.None));
        var batchRefusal = Assert.ThrowsAsync<WalStampBelowFloorException>(
            async () => await grain.AppendBatchAsync(new[] { StampedEntry("ok", NowStamp()), StampedEntry("late", below) }, CancellationToken.None));
        var next = await grain.GetNextSequenceAsync(CancellationToken.None);
        var admitted = await grain.AppendAsync(StampedEntry("fresh", page.ClockFloor), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(refusal!.Floor, Is.EqualTo(page.ClockFloor));
            Assert.That(refusal.Timestamp, Is.EqualTo(below));
            Assert.That(refusal.TreeId, Is.EqualTo(TreeId));
            Assert.That(batchRefusal!.Floor, Is.EqualTo(page.ClockFloor));
            Assert.That(next, Is.Zero, "a refused append or batch is assigned no offset");
            Assert.That(admitted, Is.Zero, "a stamp at the floor is admitted");
        });
    }

    [Test]
    public async Task Carried_and_foreign_stamps_below_the_floor_are_admitted()
    {
        var grain = await CreateGrainAsync(floorGate: new TestWalClockFloorGate { IsOpen = true });
        var page = await grain.ReadShippingAsync(0, 10, CancellationToken.None);
        var old = Ago(TimeSpan.FromHours(1));
        Assert.That(old, Is.LessThan(page.ClockFloor), "precondition");

        await grain.AppendAsync(StampedEntry("carried", old) with { IsCarriedStamp = true }, CancellationToken.None);
        await grain.AppendBatchAsync(new[] { StampedEntry("merge", old) with { IsMerge = true } }, CancellationToken.None);

        Assert.That(await grain.GetNextSequenceAsync(CancellationToken.None), Is.EqualTo(2));
    }

    [Test]
    public async Task A_persisted_floor_is_enforced_after_reactivation_even_with_the_gate_closed()
    {
        var floorState = new FakePersistentState<WalShardFloorState>();
        var first = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = true });
        var published = (await first.ReadShippingAsync(0, 10, CancellationToken.None)).ClockFloor;

        var reactivated = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = false });
        var page = await reactivated.ReadShippingAsync(0, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ClockFloor, Is.EqualTo(published), "a latched floor stays published while the gate is closed");
            Assert.That(
                async () => await reactivated.AppendAsync(StampedEntry("late", Ago(TimeSpan.FromHours(1))), CancellationToken.None),
                Throws.InstanceOf<WalStampBelowFloorException>());
        });
    }

    [Test]
    public async Task A_failed_floor_write_publishes_nothing_and_refuses_nothing()
    {
        var floorState = new FakePersistentState<WalShardFloorState> { ThrowOnWrite = new InvalidOperationException("storage down") };
        var grain = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = true });

        var page = await grain.ReadShippingAsync(0, 10, CancellationToken.None);
        await grain.AppendAsync(StampedEntry("old", Ago(TimeSpan.FromHours(1))), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.ClockFloor, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(floorState.State.Floor, Is.EqualTo(HybridLogicalClock.Zero), "a failed write is rolled back");
        });
    }

    [Test]
    public async Task The_floor_does_not_move_again_within_half_a_lag()
    {
        var floorState = new FakePersistentState<WalShardFloorState>();
        var grain = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = true });

        var first = await grain.ReadShippingAsync(0, 10, CancellationToken.None);
        var second = await grain.ReadShippingAsync(0, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.ClockFloor, Is.EqualTo(first.ClockFloor));
            Assert.That(floorState.WriteCount, Is.EqualTo(1), "one storage write per half lag");
        });
    }
}
