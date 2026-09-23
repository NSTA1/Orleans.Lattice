using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the capture half of leaf-snapshot segmentation.
/// <para>
/// Issue #2914 split a leaf snapshot across segment grains, which bounded the
/// largest contiguous array a <b>row write</b> and a <b>hydration read</b>
/// demand. It did not bound capture. Capture encoded every row of the leaf into
/// one contiguous frame and handed that whole frame to the storage grain, which
/// only then split it - so the peak allocation of a capture stayed a function
/// of snapshot size, which is precisely the quantity segmentation exists to
/// stop mattering. A leaf big enough to need segmenting is exactly the leaf
/// whose whole-frame allocation is the problem, so segmenting it only after
/// that allocation has already succeeded bounds nothing.
/// </para>
/// <para>
/// WHY THIS IS MEASURED AT THE SEAM AND NOT INFERRED FROM THE RESULT: the
/// durable shape is identical either way. Before the fix, capture built an
/// 80 MiB frame and the storage grain segmented it; after, capture stages
/// bounded frames directly. Both leave a segmented manifest whose segments each
/// sit under the window, so every assertion about the <em>persisted</em>
/// snapshot passes in both worlds and proves nothing. What changed is the
/// largest single array that ever crossed the capture boundary, so that is what
/// <see cref="RecordingSnapshotStore"/> records and what these tests assert.
/// </para>
/// <para>
/// This is also the invariant that keeps the concern inside the core. Whether a
/// leaf can be captured at all was previously emergent from three
/// independently-validated knobs - <c>MaxLeafKeys</c>, <c>MaxLeafBytes</c> and
/// a null-by-default <c>MaxValueSizeBytes</c> - none of which is checked
/// against the others, so an application-layer package storing large values had
/// to re-derive "key bound x value size" from core constants it cannot even
/// see. Bounding capture makes it a property the core enforces, and these tests
/// read the real <see cref="LatticeConstants.DefaultMaxLeafKeys"/> and the real
/// options rather than restating either, so raising a core default is covered
/// here automatically instead of quietly widening someone else's exposure.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Wraps a real <see cref="ILeafSnapshotStorageGrain"/> and records the
    /// largest single contiguous frame handed to it, across both the inline
    /// save and the staged-segment paths. That maximum is the capture peak: the
    /// biggest array capture had to materialise in one piece.
    /// </summary>
    private sealed class RecordingSnapshotStore(ILeafSnapshotStorageGrain inner) : ILeafSnapshotStorageGrain
    {
        internal long LargestContiguousFrameBytes { get; private set; }

        internal int StagedSegments { get; private set; }

        /// <summary>
        /// Segments staged by the most recently committed capture. Tracked
        /// separately from the running total because a leaf captures more than
        /// once in a single test (the checkpoint flush captures, then the
        /// explicit call does), and the manifest only ever describes the last
        /// one - so comparing it against the total would be comparing two
        /// different captures.
        /// </summary>
        internal int LastCommitSegmentCount { get; private set; }

        private int stagedSinceCommit;

        internal int InlineSaves { get; private set; }

        public Task<LeafSnapshotSaveOutcome> SaveAsync(LeafSnapshotBlob blob, CancellationToken cancellationToken)
        {
            InlineSaves++;
            if (blob.EncodedRows is { Length: > 0 } frame)
            {
                LargestContiguousFrameBytes = Math.Max(LargestContiguousFrameBytes, frame.LongLength);
            }

            return inner.SaveAsync(blob, cancellationToken);
        }

        public Task<int> StageSnapshotSegmentAsync(byte[] frame, int rowCount, CancellationToken cancellationToken)
        {
            StagedSegments++;
            stagedSinceCommit++;
            LargestContiguousFrameBytes = Math.Max(LargestContiguousFrameBytes, frame.LongLength);
            return inner.StageSnapshotSegmentAsync(frame, rowCount, cancellationToken);
        }

        public Task BeginStagedSnapshotAsync(CancellationToken cancellationToken)
            => inner.BeginStagedSnapshotAsync(cancellationToken);

        public Task<bool> CommitStagedSnapshotAsync(LeafSnapshotBlob manifest, CancellationToken cancellationToken)
        {
            LastCommitSegmentCount = stagedSinceCommit;
            stagedSinceCommit = 0;
            return inner.CommitStagedSnapshotAsync(manifest, cancellationToken);
        }

        public Task<LeafSnapshotBlob?> LoadAsync(CancellationToken cancellationToken)
            => inner.LoadAsync(cancellationToken);

        public Task<byte[]?> LoadSegmentFrameAsync(int index, CancellationToken cancellationToken)
            => inner.LoadSegmentFrameAsync(index, cancellationToken);

        public Task<long> GetSnapshotByteSizeAsync(CancellationToken cancellationToken)
            => inner.GetSnapshotByteSizeAsync(cancellationToken);

        public Task ClearAsync(CancellationToken cancellationToken)
            => inner.ClearAsync(cancellationToken);
    }

    /// <summary>
    /// Fills <paramref name="leaf"/> with <paramref name="rowCount"/> rows of
    /// <paramref name="valueBytes"/> each, checkpoints partition 0, and
    /// captures through <paramref name="store"/>.
    /// </summary>
    private static async Task FillAndCaptureAsync(
        BPlusLeafGrain leaf,
        FakePersistentState<LeafNodeState> leafState,
        int rowCount,
        int valueBytes)
    {
        var projection = AsProjection(leaf);
        leafState.State.ProjectionCheckpointOffset = -1L;

        for (var i = 0; i < rowCount; i++)
        {
            var payload = new byte[valueBytes];
            Array.Fill(payload, (byte)(i % 251));
            projection.Apply(BuildSet(
                $"cap-key-{i:D6}",
                payload,
                hlcPhysical: 1_000 + i,
                treeId: ResidualTreeId));
        }

        using (LatticeApplyOffsetContext.BeginScope(0, 5))
        {
            await projection.SetCheckpointOffsetAsync(5, default);
        }

        await projection.FlushCheckpointAsync(default);
        await leaf.CaptureSnapshotAsync();
    }

    [Test]
    public async Task Capture_never_materialises_a_contiguous_frame_larger_than_the_segment_window()
    {
        const int partitions = 1;
        const int rowCount = 256;
        const int valueBytes = 2048;

        var (store, _, manifestState) = CreateSegmentingStore();
        var recorder = new RecordingSnapshotStore(store);
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, recorder, leafSnapshotSegmentBytes: SegmentTestWindowBytes);

        await FillAndCaptureAsync(leaf, leafState, rowCount, valueBytes);

        // Precondition. A fixture whose input quietly fell under the window
        // would take the inline branch and report a clean pass for having
        // exercised nothing, which is the failure mode this class of test is
        // most prone to.
        var snapshotBytes = (long)rowCount * valueBytes;
        Assert.That(snapshotBytes, Is.GreaterThan(SegmentTestWindowBytes * 4),
            "precondition: the snapshot must be several windows wide, otherwise capture legitimately "
            + "produces one frame and the bound under test is never exercised");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.LargestContiguousFrameBytes, Is.LessThanOrEqualTo(SegmentTestWindowBytes),
                "THE ASSERTION. The largest array capture materialised in one piece must fit the segment "
                + "window. Before this fix capture encoded the whole leaf into one frame and let the "
                + "storage grain split it afterwards, so this value tracked snapshot size - which is the "
                + "allocation that fails, and the one segmentation was supposed to have removed");

            Assert.That(recorder.StagedSegments, Is.GreaterThan(1),
                "the capture must have gone down the staged path and produced several segments");

            Assert.That(recorder.InlineSaves, Is.Zero,
                "a staged capture must not also call SaveAsync: doing so would hand the whole frame "
                + "across the seam anyway and silently restore the unbounded allocation");

            Assert.That(manifestState.State.SegmentCount, Is.EqualTo(recorder.LastCommitSegmentCount),
                "the committed manifest must reference exactly the segments that capture staged for it - a "
                + "manifest claiming coverage backed by a segment that never landed is what lets the "
                + "coverage-gated WAL GC trim a prefix nothing can reproduce");

            Assert.That(manifestState.State.SegmentGeneration, Is.GreaterThan(0),
                "a staged capture commits into a fresh generation, so the previous snapshot's segments "
                + "are never overwritten in place before the new manifest lands");
        });
    }

    [Test]
    public async Task The_capture_bound_holds_at_the_core_leaf_key_limit_whatever_the_value_size()
    {
        // The bound must hold for a leaf filled to the core's OWN key limit
        // with values of a size the core never constrains: MaxValueSizeBytes is
        // null by default, so nothing refuses these writes, and MaxLeafKeys is
        // what decides how many of them accumulate in one leaf. Multiplying the
        // two is how a leaf reaches a size no single allocation can serve.
        //
        // Both operands are READ, never restated. A test that hard-codes 128
        // and then computes with 128 stays green when the core default rises,
        // while real exposure scales with it - a copied constant cannot detect
        // a change to its original, which is exactly why this invariant belongs
        // in the core and not in the packages that used to re-derive it.
        const int partitions = 1;
        var rowCount = LatticeConstants.DefaultMaxLeafKeys;
        const int valueBytes = 4096;

        var (store, _, _) = CreateSegmentingStore();
        var recorder = new RecordingSnapshotStore(store);
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, recorder, leafSnapshotSegmentBytes: SegmentTestWindowBytes);

        await FillAndCaptureAsync(leaf, leafState, rowCount, valueBytes);

        var snapshotBytes = (long)rowCount * valueBytes;
        Assert.That(snapshotBytes, Is.GreaterThan(SegmentTestWindowBytes),
            $"precondition: a full leaf of {rowCount} x {valueBytes} B values must exceed the "
            + "segment window, otherwise this proves nothing about the key-bound-times-value-size "
            + "product that made an unbounded capture possible");

        Assert.That(recorder.LargestContiguousFrameBytes, Is.LessThanOrEqualTo(SegmentTestWindowBytes),
            $"a leaf filled to the core's own key bound ({rowCount}) with values the core does not "
            + "bound must still capture inside one segment window. If this fails, leaf capturability "
            + "is once again an emergent property of knobs validated independently of each other, and "
            + "application-layer packages are once again exposed to a core concern");
    }
}
