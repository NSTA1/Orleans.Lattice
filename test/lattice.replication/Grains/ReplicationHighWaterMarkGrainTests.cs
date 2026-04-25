using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationHighWaterMarkGrainTests
{
    private static ReplicationHighWaterMarkGrain CreateGrain(
        FakePersistentState<ReplicationHighWaterMarkState>? state = null)
    {
        state ??= new FakePersistentState<ReplicationHighWaterMarkState>();
        return new ReplicationHighWaterMarkGrain(state);
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task GetAsync_returns_zero_for_fresh_grain()
    {
        var grain = CreateGrain();

        var hwm = await grain.GetAsync(CancellationToken.None);

        Assert.That(hwm, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void GetAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task TryAdvanceAsync_advances_when_candidate_is_strictly_greater()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);

        var advanced = await grain.TryAdvanceAsync(Hlc(10), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(10)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_returns_false_when_candidate_equals_current()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(Hlc(5), CancellationToken.None);

        var advanced = await grain.TryAdvanceAsync(Hlc(5), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(5)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_returns_false_when_candidate_is_less_than_current()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(Hlc(10), CancellationToken.None);

        var advanced = await grain.TryAdvanceAsync(Hlc(3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(10)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_is_monotonic_across_a_burst()
    {
        var grain = CreateGrain();

        await grain.TryAdvanceAsync(Hlc(1), CancellationToken.None);
        await grain.TryAdvanceAsync(Hlc(5), CancellationToken.None);
        await grain.TryAdvanceAsync(Hlc(2), CancellationToken.None);
        await grain.TryAdvanceAsync(Hlc(7), CancellationToken.None);
        await grain.TryAdvanceAsync(Hlc(7), CancellationToken.None);

        Assert.That(await grain.GetAsync(CancellationToken.None), Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task TryAdvanceAsync_rolls_back_on_storage_failure()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>
        {
            State = new ReplicationHighWaterMarkState { HighWaterMark = Hlc(3) },
            ThrowOnWrite = new InvalidOperationException("boom"),
        };
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.TryAdvanceAsync(Hlc(10), CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(3)));
    }

    [Test]
    public void TryAdvanceAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.TryAdvanceAsync(Hlc(1), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PinSnapshotAsync_sets_hwm_unconditionally()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(Hlc(10), CancellationToken.None);

        await grain.PinSnapshotAsync(Hlc(50), CancellationToken.None);

        Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task PinSnapshotAsync_can_lower_hwm_for_snapshot_handoff()
    {
        // PinSnapshotAsync is unconditional by design - the bootstrap
        // protocol may need to seed the HWM at the snapshot's authoring
        // HLC even if it is below the receiver's prior local cursor.
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(Hlc(100), CancellationToken.None);

        await grain.PinSnapshotAsync(Hlc(20), CancellationToken.None);

        Assert.That(await grain.GetAsync(CancellationToken.None), Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task PinSnapshotAsync_is_idempotent_when_already_at_target()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.PinSnapshotAsync(Hlc(7), CancellationToken.None);
        var writesBefore = state.WriteCount;

        await grain.PinSnapshotAsync(Hlc(7), CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task PinSnapshotAsync_rolls_back_on_storage_failure()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>
        {
            State = new ReplicationHighWaterMarkState { HighWaterMark = Hlc(3) },
            ThrowOnWrite = new InvalidOperationException("boom"),
        };
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.PinSnapshotAsync(Hlc(99), CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(state.State.HighWaterMark, Is.EqualTo(Hlc(3)));
    }

    [Test]
    public void PinSnapshotAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.PinSnapshotAsync(Hlc(1), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
