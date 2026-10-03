using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4383: removing a leaf must delete its snapshot storage. Every removal
/// path - purge, shard retirement, empty-leaf reclaim, orphan repair - funnels
/// through <see cref="BPlusLeafGrain.ClearGrainStateAsync"/>, which used to clear
/// only the leaf's own row. The manifest and segment rows keyed by the leaf's
/// identity were left behind for good: measured on a live deployment, 18,026
/// manifests with no leaf row, holding 1.34 GB.
/// <para>
/// These drive a real leaf against the real <see cref="LeafSnapshotStorageGrain"/>
/// so the assertions are on rows, not on a stub having been called. A stub-only
/// check would pass if the leaf called a clear that did nothing.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Delegates to a real store, recording calls in order. Its capture writes
    /// can be held open, so a test can put a removal in the middle of a capture,
    /// and its clear can be made to fail once.
    /// </summary>
    private sealed class GatedSnapshotStore(ILeafSnapshotStorageGrain inner) : ILeafSnapshotStorageGrain
    {
        internal List<string> Calls { get; } = [];

        internal TaskCompletionSource? WriteGate { get; set; }

        internal TaskCompletionSource WriteEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Exception? FailNextClear { get; set; }

        internal Action? OnClear { get; set; }

        public async Task<LeafSnapshotSaveOutcome> SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken)
        {
            await HoldWriteAsync();
            Calls.Add(nameof(SaveAsync));
            return await inner.SaveAsync(blob, cancellationToken);
        }

        public async Task<bool> CommitStagedSnapshotAsync(LeafSnapshotBlob manifest, CancellationToken cancellationToken)
        {
            await HoldWriteAsync();
            Calls.Add(nameof(CommitStagedSnapshotAsync));
            return await inner.CommitStagedSnapshotAsync(manifest, cancellationToken);
        }

        public Task ClearAsync(CancellationToken cancellationToken)
        {
            Calls.Add(nameof(ClearAsync));
            OnClear?.Invoke();
            if (FailNextClear is { } ex)
            {
                FailNextClear = null;
                throw ex;
            }

            return inner.ClearAsync(cancellationToken);
        }

        private async Task HoldWriteAsync()
        {
            if (WriteGate is { } gate)
            {
                WriteEntered.TrySetResult();
                await gate.Task;
            }
        }

        public Task BeginStagedSnapshotAsync(CancellationToken cancellationToken)
            => inner.BeginStagedSnapshotAsync(cancellationToken);

        public Task<int> StageSnapshotSegmentAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
            => inner.StageSnapshotSegmentAsync(frame, rowCount, cancellationToken);

        public Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken)
            => inner.LoadAsync(cancellationToken);

        public Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken)
            => inner.LoadSegmentFrameAsync(index, cancellationToken);

        public Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken)
            => inner.GetSnapshotByteSizeAsync(cancellationToken);
    }

    /// <summary>
    /// Fills a leaf with enough data that its capture is segmented, captures it,
    /// and returns everything a removal test inspects.
    /// </summary>
    private static async Task<(BPlusLeafGrain Leaf, FakePersistentState<LeafNodeState> LeafState, GatedSnapshotStore Store,
            Dictionary<string, FakeSegmentGrain> Segments, FakePersistentState<LeafSnapshotBlob> Manifest)>
        CreateCapturedLeafAsync()
    {
        var (store, segments, manifest) = CreateSegmentingStore();
        var gated = new GatedSnapshotStore(store);
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(1, gated, leafSnapshotSegmentBytes: SegmentTestWindowBytes);

        await FillAndCaptureAsync(leaf, leafState, rowCount: 256, valueBytes: 2048);

        Assert.That(manifest.State.SegmentCount, Is.GreaterThan(1),
            "precondition: the capture must be segmented, or the segment rows this fixture is about never exist");
        Assert.That(segments.Values.Count(s => s.Frame is not null), Is.EqualTo(manifest.State.SegmentCount),
            "precondition: every segment the manifest references is stored");

        return (leaf, leafState, gated, segments, manifest);
    }

    [Test]
    public async Task ClearGrainStateAsync_deletes_the_snapshot_manifest_and_every_segment_row()
    {
        var (leaf, leafState, _, segments, manifest) = await CreateCapturedLeafAsync();

        await leaf.ClearGrainStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(leafState.RecordExists, Is.False, "the leaf's own row is cleared, as before");
            Assert.That(manifest.RecordExists, Is.False,
                "THE ASSERTION. The snapshot manifest row must be deleted with the leaf; before the fix it was "
                + "left in storage for good");
            Assert.That(segments.Values.Where(s => s.Frame is not null), Is.Empty,
                "every segment row must be deleted with the leaf");
        });
    }

    [Test]
    public async Task ClearGrainStateAsync_deletes_the_snapshot_only_after_the_leafs_own_row()
    {
        // A leaf whose snapshot is gone but whose checkpoint still claims the
        // trimmed prefix cannot activate, which would leave nothing able to retry
        // the clear. A cleared leaf has no tree id and never hydrates, so the
        // snapshot must go second.
        var (leaf, leafState, store, _, _) = await CreateCapturedLeafAsync();
        bool? leafRowExistedAtSnapshotClear = null;
        store.OnClear = () => leafRowExistedAtSnapshotClear = leafState.RecordExists;

        await leaf.ClearGrainStateAsync();

        Assert.That(leafRowExistedAtSnapshotClear, Is.False);
    }

    [Test]
    public async Task A_failed_snapshot_clear_propagates_and_a_retry_finishes_it()
    {
        // Every caller keeps a clear owed when it throws and retries it, so the
        // failure must surface - swallowing it would record a leaf as cleared
        // while its snapshot rows stay behind.
        var (leaf, leafState, store, segments, manifest) = await CreateCapturedLeafAsync();
        store.FailNextClear = new TimeoutException("injected snapshot clear fault");

        Assert.That(async () => await leaf.ClearGrainStateAsync(), Throws.InstanceOf<TimeoutException>());
        Assert.Multiple(() =>
        {
            Assert.That(leafState.RecordExists, Is.False, "the leaf row was cleared before the snapshot clear failed");
            Assert.That(manifest.RecordExists, Is.True, "precondition: the snapshot is still there to retry");
        });

        await leaf.ClearGrainStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(manifest.RecordExists, Is.False, "a retry on an already-cleared leaf finishes the snapshot clear");
            Assert.That(segments.Values.Where(s => s.Frame is not null), Is.Empty);
        });
    }

    [Test]
    public async Task ClearGrainStateAsync_waits_for_an_in_flight_capture_before_deleting_the_snapshot()
    {
        // A capture suspended on its store write when the leaf is removed would
        // otherwise land after the delete and strand a fresh snapshot for a leaf
        // that no longer exists.
        var (leaf, _, store, _, manifest) = await CreateCapturedLeafAsync();
        store.WriteGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Calls.Clear();

        var capture = leaf.CaptureSnapshotAsync();
        await store.WriteEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var clear = leaf.ClearGrainStateAsync();
        await Task.Delay(50);

        Assert.Multiple(() =>
        {
            Assert.That(clear.IsCompleted, Is.False, "the clear must wait for the capture already under way");
            Assert.That(store.Calls, Does.Not.Contain(nameof(GatedSnapshotStore.ClearAsync)));
        });

        store.WriteGate.SetResult();
        await capture.WaitAsync(TimeSpan.FromSeconds(10));
        await clear.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(store.Calls.Last(), Is.EqualTo(nameof(GatedSnapshotStore.ClearAsync)),
                "the clear runs after the capture's write landed");
            Assert.That(manifest.RecordExists, Is.False, "and so deletes what that capture wrote");
        });
    }

    [Test]
    public async Task A_capture_that_will_not_finish_fails_the_clear_and_no_new_capture_starts_after_it()
    {
        var (leaf, leafState, store, _, manifest) = await CreateCapturedLeafAsync();
        leaf.SnapshotCaptureDrainTimeout = TimeSpan.FromMilliseconds(50);
        store.WriteGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.Calls.Clear();

        var capture = leaf.CaptureSnapshotAsync();
        await store.WriteEntered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(async () => await leaf.ClearGrainStateAsync(), Throws.InvalidOperationException,
            "a capture that does not land in time fails the clear, so the caller keeps it owed and retries");
        Assert.Multiple(() =>
        {
            Assert.That(leafState.RecordExists, Is.True, "nothing is cleared when the wait gives up");
            Assert.That(store.Calls, Does.Not.Contain(nameof(GatedSnapshotStore.ClearAsync)));
        });

        store.WriteGate.SetResult();
        await capture.WaitAsync(TimeSpan.FromSeconds(10));
        store.WriteGate = null;
        store.Calls.Clear();

        // The leaf still has its tree id, so only the removal latch can stop a
        // capture here - and one landing now would be stranded by the retry.
        await leaf.CaptureSnapshotAsync();
        Assert.That(store.Calls, Is.Empty, "no capture may start once the leaf's removal has begun");

        await leaf.ClearGrainStateAsync();
        Assert.That(manifest.RecordExists, Is.False, "the retry completes the clear");
    }
}
