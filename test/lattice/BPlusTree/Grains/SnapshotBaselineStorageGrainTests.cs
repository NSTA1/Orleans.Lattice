using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="SnapshotBaselineStorageGrain"/>, the durable
/// per-cursor, per-shard frozen-baseline store introduced for the snapshot
/// scan WAL-GC fix. Mirrors the conventions of
/// <see cref="LeafSnapshotStorageGrainTests"/>: the grain is instantiated
/// directly against a <see cref="FakePersistentState{T}"/>, so the tests
/// exercise the save / load / clear contract and the <c>Captured</c> sentinel
/// without a cluster.
/// </summary>
[TestFixture]
public sealed class SnapshotBaselineStorageGrainTests
{
    private static (SnapshotBaselineStorageGrain grain, FakePersistentState<SnapshotShardBaseline> state) CreateGrain(
        FakePersistentState<SnapshotShardBaseline>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("snapshot-baseline", Guid.NewGuid().ToString("N")));
        state ??= new FakePersistentState<SnapshotShardBaseline>();
        return (new SnapshotBaselineStorageGrain(context, state), state);
    }

    private static SnapshotShardBaseline NewBaseline(long[] capturedHead, params (string key, byte[] value)[] rows)
    {
        var clock = HybridLogicalClock.Zero;
        var list = new List<LeafSnapshotRow>(rows.Length);
        foreach (var (k, v) in rows)
        {
            list.Add(new LeafSnapshotRow(k, new LwwValue<byte[]> { Value = v, Timestamp = clock }));
        }
        return new SnapshotShardBaseline
        {
            Rows = list,
            CapturedHeadPerPartition = capturedHead,
            CapturedAtTicks = 4242,
            RowBytes = 99,
        };
    }

    [Test]
    public async Task LoadAsync_returns_null_when_no_baseline_has_been_written()
    {
        var (grain, _) = CreateGrain();

        var baseline = await grain.LoadAsync(CancellationToken.None);

        Assert.That(baseline, Is.Null);
    }

    [Test]
    public async Task SaveAsync_then_LoadAsync_round_trips_the_baseline()
    {
        var (grain, state) = CreateGrain();
        var input = NewBaseline([7, 11], ("a", [1, 2]), ("b", [3]));

        await grain.SaveAsync(input, CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Rows, Has.Count.EqualTo(2));
        Assert.That(loaded.Rows[0].Key, Is.EqualTo("a"));
        Assert.That(loaded.Rows[1].Key, Is.EqualTo("b"));
        Assert.That(loaded.CapturedHeadPerPartition, Is.EqualTo(new long[] { 7, 11 }));
        Assert.That(loaded.CapturedAtTicks, Is.EqualTo(4242));
        Assert.That(loaded.RowBytes, Is.EqualTo(99));
    }

    [Test]
    public async Task SaveAsync_stamps_the_captured_sentinel()
    {
        var (grain, state) = CreateGrain();

        await grain.SaveAsync(NewBaseline([3]), CancellationToken.None);

        Assert.That(state.State.Captured, Is.True,
            "SaveAsync must stamp Captured == true so LoadAsync can tell a real baseline apart "
            + "from a default-allocated provider row.");
    }

    [Test]
    public async Task LoadAsync_returns_a_captured_empty_baseline_distinctly_from_never_written()
    {
        // A shard that had zero live keys at capture still persists a real
        // baseline (empty Rows, a real partition head). It must load back as a
        // non-null empty baseline, NOT as the never-written null sentinel.
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBaseline([5]), CancellationToken.None);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Rows, Is.Empty);
        Assert.That(loaded.CapturedHeadPerPartition, Is.EqualTo(new long[] { 5 }));
    }

    [Test]
    public async Task SaveAsync_overwrites_a_previously_persisted_baseline()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(NewBaseline([1], ("a", [1])), CancellationToken.None);

        await grain.SaveAsync(NewBaseline([2], ("b", [2])), CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(2));
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.Rows, Has.Count.EqualTo(1));
        Assert.That(loaded.Rows[0].Key, Is.EqualTo("b"));
        Assert.That(loaded.CapturedHeadPerPartition, Is.EqualTo(new long[] { 2 }));
    }

    [Test]
    public void SaveAsync_throws_on_null_baseline()
    {
        var (grain, _) = CreateGrain();

        Assert.That(
            async () => await grain.SaveAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void SaveAsync_honours_cancellation_before_persist()
    {
        var (grain, state) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.SaveAsync(NewBaseline([1]), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.That(state.WriteCount, Is.EqualTo(0));
    }

    [Test]
    public void LoadAsync_honours_cancellation()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.LoadAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task ClearAsync_is_noop_when_no_baseline_has_been_written()
    {
        var (grain, _) = CreateGrain();

        await grain.ClearAsync(CancellationToken.None);

        // The sentinel short-circuit keeps an idempotent clear on a
        // never-written baseline free of provider traffic; the row still
        // reads back as the never-written null.
        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task ClearAsync_drops_a_previously_persisted_baseline()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBaseline([7], ("k", [9])), CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task ClearAsync_is_idempotent_across_repeated_calls()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBaseline([1], ("k", [1])), CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);
        await grain.ClearAsync(CancellationToken.None);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public void ClearAsync_honours_cancellation_before_persist()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.ClearAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Default_state_row_is_treated_as_no_baseline()
    {
        // A provider that returns a freshly-defaulted SnapshotShardBaseline
        // (Captured == false) must be treated as "no baseline" by LoadAsync,
        // otherwise a snapshot leaf would seed from an empty default row
        // instead of failing loudly on a missing capture.
        var state = new FakePersistentState<SnapshotShardBaseline>
        {
            State = new SnapshotShardBaseline(),
        };
        var (grain, _) = CreateGrain(state);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }
}
