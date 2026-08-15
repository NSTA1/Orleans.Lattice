using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class LeafSnapshotStorageGrainTests
{
    private static (LeafSnapshotStorageGrain grain, FakePersistentState<LeafSnapshotBlob> state) CreateGrain(
        FakePersistentState<LeafSnapshotBlob>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf-snapshot", Guid.NewGuid().ToString("N")));
        state ??= new FakePersistentState<LeafSnapshotBlob>();
        return (new LeafSnapshotStorageGrain(context, state), state);
    }

    private static LeafSnapshotBlob NewBlob(long offset, params (string key, byte[] value)[] rows)
    {
        var clock = HybridLogicalClock.Zero;
        var list = new List<LeafSnapshotRow>(rows.Length);
        foreach (var (k, v) in rows)
        {
            list.Add(new LeafSnapshotRow(k, new LwwValue<byte[]> { Value = v, Timestamp = clock }));
        }
        return new LeafSnapshotBlob
        {
            SnapshotOffset = offset,
            Rows = list,
            CapturedAtTicks = 12345,
        };
    }

    [Test]
    public async Task LoadAsync_returns_null_when_no_snapshot_has_been_written()
    {
        var (grain, _) = CreateGrain();

        var blob = await grain.LoadAsync(CancellationToken.None);

        Assert.That(blob, Is.Null);
    }

    [Test]
    public async Task SaveAsync_then_LoadAsync_round_trips_the_blob()
    {
        var (grain, state) = CreateGrain();
        var input = NewBlob(42, ("a", [1, 2]), ("b", [3]));

        await grain.SaveAsync(input, CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffset, Is.EqualTo(42));
        Assert.That(loaded.Rows, Has.Count.EqualTo(2));
        Assert.That(loaded.Rows[0].Key, Is.EqualTo("a"));
        Assert.That(loaded.Rows[1].Key, Is.EqualTo("b"));
        Assert.That(loaded.CapturedAtTicks, Is.EqualTo(12345));
    }

    [Test]
    public async Task SaveAsync_overwrites_a_previously_persisted_blob()
    {
        var (grain, state) = CreateGrain();
        await grain.SaveAsync(NewBlob(5, ("a", [1])), CancellationToken.None);

        await grain.SaveAsync(NewBlob(11, ("b", [2])), CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(2));
        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.SnapshotOffset, Is.EqualTo(11));
        Assert.That(loaded.Rows, Has.Count.EqualTo(1));
        Assert.That(loaded.Rows[0].Key, Is.EqualTo("b"));
    }

    [Test]
    public void SaveAsync_throws_on_null_blob()
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
            async () => await grain.SaveAsync(NewBlob(1), cts.Token),
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
    public async Task ClearAsync_is_noop_when_no_snapshot_has_been_written()
    {
        var (grain, state) = CreateGrain();

        await grain.ClearAsync(CancellationToken.None);

        // No I/O should fire on a never-persisted snapshot - the
        // ClearAsync sentinel short-circuit keeps idempotent calls
        // free of provider traffic.
        Assert.That(state.WriteCount, Is.EqualTo(0));
        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task ClearAsync_drops_a_previously_persisted_blob()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBlob(7, ("k", [9])), CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);
        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Null);
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
    public async Task LoadAsync_returns_null_after_ClearAsync()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBlob(3), CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task Default_state_with_negative_offset_is_treated_as_no_snapshot()
    {
        // A provider that returns a freshly-defaulted LeafSnapshotBlob
        // (SnapshotOffset = -1) must be treated as "no snapshot" by
        // LoadAsync, otherwise the reactivation path would mistake an
        // empty state row for a captured-but-empty snapshot.
        var state = new FakePersistentState<LeafSnapshotBlob>
        {
            State = new LeafSnapshotBlob(),
        };
        var (grain, _) = CreateGrain(state);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetSnapshotByteSizeAsync_returns_zero_when_no_snapshot_captured()
    {
        var (grain, _) = CreateGrain();

        var bytes = await grain.GetSnapshotByteSizeAsync(CancellationToken.None);

        Assert.That(bytes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetSnapshotByteSizeAsync_sums_key_and_value_bytes_across_rows()
    {
        var (grain, _) = CreateGrain();
        // "a" (1) + value [1,2] (2) = 3; "bb" (2) + value [9] (1) = 3. Total 6.
        await grain.SaveAsync(NewBlob(7, ("a", [1, 2]), ("bb", [9])), CancellationToken.None);

        var bytes = await grain.GetSnapshotByteSizeAsync(CancellationToken.None);

        Assert.That(bytes, Is.EqualTo(6));
    }

    [Test]
    public async Task GetSnapshotByteSizeAsync_serves_precomputed_value_when_blob_carries_one()
    {
        var (grain, _) = CreateGrain();
        var blob = NewBlob(7, ("a", [1, 2]), ("bb", [9]));
        // Simulate a producer that has precomputed the snapshot footprint
        // at capture time (which is what BPlusLeafGrain.CaptureSnapshotAsync
        // now does via Cache.StateBytes).
        blob.SnapshotBytes = 999;
        await grain.SaveAsync(blob, CancellationToken.None);

        var bytes = await grain.GetSnapshotByteSizeAsync(CancellationToken.None);

        Assert.That(bytes, Is.EqualTo(999),
            "the precomputed SnapshotBytes slot must be served directly so a deep read is constant-time");
    }

    // ------------------------------------------------------------------
    // HasCapturedPrefix per-partition-slot recognition (the residual
    // cold-restart fix). The scalar SnapshotOffset only describes partition
    // 0; under the default WalPartitions = 8 a leaf whose live keys hash
    // entirely to a non-zero partition captures a blob whose scalar offset
    // is the -1 "partition 0 idle" sentinel yet whose
    // SnapshotOffsetsByPartition covers the busy partition. Keying the
    // load/clear/size guards on the scalar alone would discard that blob on
    // cold restart, and because the coverage-gated WAL GC has already
    // trimmed the covered prefix, discarding the sole durable copy silently
    // loses it. Legacy blobs (SnapshotOffsetsByPartition == null) must keep
    // the scalar-only semantics exactly.
    // ------------------------------------------------------------------

    private static LeafSnapshotBlob PerPartitionBlob(long scalar, long[] perPartition, params (string key, byte[] value)[] rows)
    {
        var blob = NewBlob(scalar, rows);
        blob.SnapshotOffsetsByPartition = perPartition;
        return blob;
    }

    [Test]
    public async Task LoadAsync_returns_blob_when_scalar_is_idle_sentinel_but_a_per_partition_slot_is_covered()
    {
        // The partition-0-idle capture shape: scalar SnapshotOffset is the -1
        // sentinel, but a non-zero partition slot is covered at offset 3. The
        // load guard MUST recognise this as captured, otherwise cold restart
        // discards the sole durable copy of an already-trimmed prefix.
        var perPartition = new long[] { -1, -1, -1, 3, -1, -1, -1, -1 };
        var state = new FakePersistentState<LeafSnapshotBlob>
        {
            State = PerPartitionBlob(-1L, perPartition, ("busy", [7])),
        };
        var (grain, _) = CreateGrain(state);

        var loaded = await grain.LoadAsync(CancellationToken.None);

        Assert.That(loaded, Is.Not.Null,
            "a blob covering any non-zero partition must load even when partition 0's scalar offset is the -1 sentinel");
        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.EqualTo(perPartition));
    }

    [Test]
    public async Task LoadAsync_returns_null_when_scalar_and_all_per_partition_slots_are_negative()
    {
        // A per-partition array present but wholly uncovered is genuinely
        // "nothing captured" and must remain a null load, exactly like the
        // default-state scalar sentinel.
        var state = new FakePersistentState<LeafSnapshotBlob>
        {
            State = PerPartitionBlob(-1L, new long[] { -1, -1, -1, -1 }),
        };
        var (grain, _) = CreateGrain(state);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task ClearAsync_drops_a_per_partition_only_blob()
    {
        // A per-partition-only blob is captured content, so ClearAsync must
        // reach the provider and drop it rather than short-circuit as a no-op.
        var state = new FakePersistentState<LeafSnapshotBlob>
        {
            State = PerPartitionBlob(-1L, new long[] { -1, 2, -1, -1 }, ("k", [1])),
        };
        var (grain, _) = CreateGrain(state);

        await grain.ClearAsync(CancellationToken.None);

        Assert.That(await grain.LoadAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetSnapshotByteSizeAsync_computes_for_a_per_partition_only_blob()
    {
        // "busy" (4) + value [1,2,3] (3) = 7. A per-partition-only blob is
        // captured, so the size guard must not short-circuit to zero.
        var state = new FakePersistentState<LeafSnapshotBlob>
        {
            State = PerPartitionBlob(-1L, new long[] { -1, -1, 5, -1 }, ("busy", [1, 2, 3])),
        };
        var (grain, _) = CreateGrain(state);

        var bytes = await grain.GetSnapshotByteSizeAsync(CancellationToken.None);

        Assert.That(bytes, Is.EqualTo(7));
    }

    [Test]
    public async Task GetSnapshotByteSizeAsync_returns_zero_after_clear()
    {
        var (grain, _) = CreateGrain();
        await grain.SaveAsync(NewBlob(2, ("k", [1, 2, 3])), CancellationToken.None);

        await grain.ClearAsync(CancellationToken.None);

        Assert.That(await grain.GetSnapshotByteSizeAsync(CancellationToken.None), Is.EqualTo(0));
    }

    [Test]
    public void GetSnapshotByteSizeAsync_honors_cancellation()
    {
        var (grain, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetSnapshotByteSizeAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}

