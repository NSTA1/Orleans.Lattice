using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationHighWaterMarkGrainTests
{
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";

    private static ReplicationHighWaterMarkGrain CreateGrain(
        FakePersistentState<ReplicationHighWaterMarkState>? state = null)
    {
        state ??= new FakePersistentState<ReplicationHighWaterMarkState>();
        return new ReplicationHighWaterMarkGrain(state);
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static VersionVector Vector(params (string Origin, HybridLogicalClock Clock)[] entries)
    {
        var v = new VersionVector();
        foreach (var (origin, clock) in entries)
        {
            v.Entries[origin] = clock;
        }
        return v;
    }

    [Test]
    public async Task GetAsync_returns_zero_for_fresh_grain()
    {
        var grain = CreateGrain();

        var hwm = await grain.GetAsync(OriginA, CancellationToken.None);

        Assert.That(hwm, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public void GetAsync_throws_when_origin_is_null()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.GetAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetAsync_throws_when_origin_is_empty()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.GetAsync("", CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void GetAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetAsync(OriginA, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetVectorAsync_returns_empty_clone_for_fresh_grain()
    {
        var grain = CreateGrain();

        var vector = await grain.GetVectorAsync(CancellationToken.None);

        Assert.That(vector.Entries, Is.Empty);
    }

    [Test]
    public async Task GetVectorAsync_returns_defensive_copy()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        var snapshot = await grain.GetVectorAsync(CancellationToken.None);
        snapshot.Entries[OriginA] = Hlc(999);
        snapshot.Entries[OriginB] = Hlc(42);

        var second = await grain.GetVectorAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(second.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(second.Entries.ContainsKey(OriginB), Is.False);
        });
    }

    [Test]
    public async Task GetVectorAsync_includes_every_origin_advanced_so_far()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginB, Hlc(7), CancellationToken.None);

        var vector = await grain.GetVectorAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(vector.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(vector.GetClock(OriginB), Is.EqualTo(Hlc(7)));
            Assert.That(vector.Entries, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void GetVectorAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetVectorAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task TryAdvanceAsync_advances_when_candidate_is_strictly_greater()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);

        var advanced = await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.True);
            Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_returns_false_when_candidate_equals_current()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(5), CancellationToken.None);

        var advanced = await grain.TryAdvanceAsync(OriginA, Hlc(5), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(5)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_returns_false_when_candidate_is_less_than_current()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        var advanced = await grain.TryAdvanceAsync(OriginA, Hlc(3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(advanced, Is.False);
            Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_is_monotonic_across_a_burst()
    {
        var grain = CreateGrain();

        await grain.TryAdvanceAsync(OriginA, Hlc(1), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginA, Hlc(5), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginA, Hlc(2), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginA, Hlc(7), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginA, Hlc(7), CancellationToken.None);

        Assert.That(await grain.GetAsync(OriginA, CancellationToken.None), Is.EqualTo(Hlc(7)));
    }

    [Test]
    public async Task TryAdvanceAsync_isolates_origins_within_the_same_tree()
    {
        var grain = CreateGrain();

        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginB, Hlc(3), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetAsync(OriginA, CancellationToken.None).Result, Is.EqualTo(Hlc(10)));
            Assert.That(grain.GetAsync(OriginB, CancellationToken.None).Result, Is.EqualTo(Hlc(3)));
        });
    }

    [Test]
    public async Task TryAdvanceAsync_rolls_back_on_storage_failure_for_first_advance()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>
        {
            ThrowOnWrite = new InvalidOperationException("boom"),
        };
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(state.State.Vector.Entries.ContainsKey(OriginA), Is.False,
            "First-advance failure must remove the speculative origin entry, not leave it at zero.");
    }

    [Test]
    public async Task TryAdvanceAsync_rolls_back_on_storage_failure_for_subsequent_advance()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(3), CancellationToken.None);
        state.ThrowOnWrite = new InvalidOperationException("boom");

        Assert.That(
            async () => await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(3)));
    }

    [Test]
    public async Task TryAdvanceAsync_rollback_does_not_touch_other_origins()
    {
        // Pre-populate OriginB so the failing OriginA advance must not
        // collateral-damage an unrelated origin's diagonal entry.
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginB, Hlc(99), CancellationToken.None);
        state.ThrowOnWrite = new InvalidOperationException("boom");

        Assert.That(
            async () => await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None),
            Throws.InvalidOperationException);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Vector.Entries.ContainsKey(OriginA), Is.False);
            Assert.That(state.State.Vector.GetClock(OriginB), Is.EqualTo(Hlc(99)));
        });
    }

    [Test]
    public void TryAdvanceAsync_throws_when_origin_is_empty()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.TryAdvanceAsync("", Hlc(1), CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public void TryAdvanceAsync_throws_when_origin_is_null()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.TryAdvanceAsync(null!, Hlc(1), CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void TryAdvanceAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.TryAdvanceAsync(OriginA, Hlc(1), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PinSnapshotAsync_replaces_vector_unconditionally()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);

        var frontier = Vector((OriginA, Hlc(50)), (OriginB, Hlc(33)));
        await grain.PinSnapshotAsync(Hlc(50), frontier, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(50)));
            Assert.That(state.State.Vector.GetClock(OriginB), Is.EqualTo(Hlc(33)));
        });
    }

    [Test]
    public async Task PinSnapshotAsync_can_lower_diagonal_for_snapshot_handoff()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(100), CancellationToken.None);

        await grain.PinSnapshotAsync(Hlc(20), Vector((OriginA, Hlc(20))), CancellationToken.None);

        Assert.That(await grain.GetAsync(OriginA, CancellationToken.None), Is.EqualTo(Hlc(20)));
    }

    [Test]
    public async Task PinSnapshotAsync_drops_origins_absent_from_frontier()
    {
        var grain = CreateGrain();
        await grain.TryAdvanceAsync(OriginA, Hlc(10), CancellationToken.None);
        await grain.TryAdvanceAsync(OriginB, Hlc(20), CancellationToken.None);

        await grain.PinSnapshotAsync(Hlc(10), Vector((OriginA, Hlc(10))), CancellationToken.None);

        var vector = await grain.GetVectorAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(vector.Entries, Has.Count.EqualTo(1));
            Assert.That(vector.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(vector.Entries.ContainsKey(OriginB), Is.False);
        });
    }

    [Test]
    public async Task PinSnapshotAsync_is_idempotent_when_already_at_target()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.PinSnapshotAsync(Hlc(7), Vector((OriginA, Hlc(7))), CancellationToken.None);
        var writesBefore = state.WriteCount;

        await grain.PinSnapshotAsync(Hlc(7), Vector((OriginA, Hlc(7))), CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(writesBefore));
    }

    [Test]
    public async Task PinSnapshotAsync_stores_defensive_copy_of_frontier()
    {
        var grain = CreateGrain();
        var frontier = Vector((OriginA, Hlc(10)));

        await grain.PinSnapshotAsync(Hlc(10), frontier, CancellationToken.None);
        // Caller mutation after the pin must not affect grain state.
        frontier.Entries[OriginA] = Hlc(999);
        frontier.Entries[OriginB] = Hlc(42);

        var vector = await grain.GetVectorAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(vector.GetClock(OriginA), Is.EqualTo(Hlc(10)));
            Assert.That(vector.Entries.ContainsKey(OriginB), Is.False);
        });
    }

    [Test]
    public async Task PinSnapshotAsync_rolls_back_on_storage_failure()
    {
        var state = new FakePersistentState<ReplicationHighWaterMarkState>();
        var grain = CreateGrain(state);
        await grain.TryAdvanceAsync(OriginA, Hlc(3), CancellationToken.None);
        state.ThrowOnWrite = new InvalidOperationException("boom");

        Assert.That(
            async () => await grain.PinSnapshotAsync(Hlc(99), Vector((OriginA, Hlc(99))), CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(state.State.Vector.GetClock(OriginA), Is.EqualTo(Hlc(3)));
    }

    [Test]
    public void PinSnapshotAsync_throws_when_frontier_is_null()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.PinSnapshotAsync(Hlc(1), null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void PinSnapshotAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.PinSnapshotAsync(Hlc(1), new VersionVector(), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PinSnapshotAsync_does_not_consult_asOfHlc_when_applying_frontier()
    {
        // Contract: asOfHlc is reserved for future bootstrap-protocol
        // extensions and the grain itself must not gate the write on
        // it. A frontier with diagonal entries strictly greater than
        // asOfHlc still applies, and asOfHlc == Zero with a non-empty
        // frontier still applies.
        var grain = CreateGrain();
        var frontier = Vector((OriginA, Hlc(500)), (OriginB, Hlc(900)));

        await grain.PinSnapshotAsync(HybridLogicalClock.Zero, frontier, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.GetAsync(OriginA, CancellationToken.None).Result, Is.EqualTo(Hlc(500)));
            Assert.That(grain.GetAsync(OriginB, CancellationToken.None).Result, Is.EqualTo(Hlc(900)));
        });
    }
}
