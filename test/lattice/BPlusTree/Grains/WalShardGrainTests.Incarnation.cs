using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The WAL partition's durable incarnation (issue #5896): advanced durably,
/// rolled back on a failed write, and written through the same gate as the
/// clock floor so the two writers of the floor state never overlap.
/// </summary>
public partial class WalShardGrainTests
{
    [Test]
    public async Task The_incarnation_advances_durably_and_reads_back_from_memory()
    {
        var floorState = new FakePersistentState<WalShardFloorState>();
        var grain = await CreateGrainAsync(floorState: floorState);

        Assert.That(await grain.GetIncarnationAsync(CancellationToken.None), Is.Zero);
        Assert.That(await grain.AdvanceIncarnationAsync(CancellationToken.None), Is.EqualTo(1));
        Assert.Multiple(async () =>
        {
            Assert.That(await grain.GetIncarnationAsync(CancellationToken.None), Is.EqualTo(1));
            Assert.That(floorState.State.Incarnation, Is.EqualTo(1));
            Assert.That(floorState.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_failed_incarnation_write_leaves_the_incarnation_where_it_was_and_throws()
    {
        var floorState = new FakePersistentState<WalShardFloorState> { ThrowOnWrite = new InvalidOperationException("storage down") };
        var grain = await CreateGrainAsync(floorState: floorState);

        Assert.That(async () => await grain.AdvanceIncarnationAsync(CancellationToken.None), Throws.InvalidOperationException);
        Assert.That(await grain.GetIncarnationAsync(CancellationToken.None), Is.Zero,
            "a value storage does not hold is never published");
    }

    [Test]
    public async Task An_incarnation_advance_and_a_clock_floor_persist_never_write_the_floor_state_at_once()
    {
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var floorState = new FakePersistentState<WalShardFloorState>();
        var grain = await CreateGrainAsync(floorState: floorState, floorGate: new TestWalClockFloorGate { IsOpen = true });
        await grain.AppendAsync(StampedEntry("a", NowStamp()), CancellationToken.None);

        var first = true;
        floorState.BeforeWrite = async () =>
        {
            if (!first)
                return;
            first = false;
            entered.TrySetResult();
            await parked.Task;
        };

        var advance = grain.AdvanceIncarnationAsync(CancellationToken.None);
        await entered.Task;
        var shipping = grain.ReadShippingAsync(0, 10, CancellationToken.None).AsTask();
        await Task.Delay(100);
        parked.SetResult();
        await Task.WhenAll(advance, shipping);

        Assert.Multiple(() =>
        {
            Assert.That(floorState.MaxConcurrentWrites, Is.EqualTo(1), "the two writers of the floor state are serialised");
            Assert.That(floorState.State.Incarnation, Is.EqualTo(1));
            Assert.That(floorState.State.Floor, Is.Not.EqualTo(HybridLogicalClock.Zero));
        });
    }
}
