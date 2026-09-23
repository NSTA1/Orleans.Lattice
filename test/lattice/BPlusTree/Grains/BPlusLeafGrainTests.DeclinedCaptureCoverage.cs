using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3421: a capture the snapshot store DECLINED
/// must not advance the leaf's in-memory durable coverage.
/// <para>
/// That coverage is what <c>ResolveDurablePinForPartition</c> reports as the
/// durable pin (<c>min(checkpoint, covered)</c>), and the pin licenses the
/// coverage-gated WAL GC to trim. The store keeps coverage monotone, so it
/// declines an offer it cannot merge safely and keeps its existing snapshot
/// verbatim. Before the fix the leaf could not tell - the inline save returned a
/// bare <c>Task</c> and the staged commit's answer was discarded - and recorded
/// the OFFER's coverage anyway, so the GC was licensed to trim a prefix whose
/// only copy was the WAL itself. Each fixture below pins the invariant
/// "in-memory coverage never exceeds what the durable store covers", on the
/// inline and the staged paths, and a positive control proves the gate is narrow
/// enough that a kept (merged) capture still advances coverage.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Stored blob covering partition 0 to 50 and <paramref name="dataPartition"/>
    /// to 3, optionally carrying a live row the leaf under test does not hold.
    /// </summary>
    private static LeafSnapshotBlob DeclineTestStoredBlob(int partitions, int dataPartition, string? ghostKey)
    {
        var coverage = new long[partitions];
        Array.Fill(coverage, -1L);
        coverage[0] = 50L;
        coverage[dataPartition] = 3L;

        var rows = new List<LeafSnapshotRow>();
        if (ghostKey is not null)
        {
            rows.Add(new LeafSnapshotRow(
                ghostKey,
                LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("g"), new HybridLogicalClock { WallClockTicks = 100L })));
        }

        return new LeafSnapshotBlob
        {
            SnapshotOffset = 50L,
            Rows = rows,
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotOffsetsByPartition = coverage,
        };
    }

    /// <summary>
    /// Leaves partition 0 idle (so the capture regresses the stored 50) and
    /// checkpoints <paramref name="dataPartition"/> to 10 (so the capture
    /// advances the stored 3), then captures.
    /// </summary>
    private static async Task CheckpointDataPartitionAndCaptureAsync(
        BPlusLeafGrain leaf,
        FakePersistentState<LeafNodeState> leafState,
        int dataPartition,
        IEnumerable<(string Key, byte[] Value)> writes)
    {
        var projection = AsProjection(leaf);
        leafState.State.ProjectionCheckpointOffset = -1L;

        var hlc = 500L;
        foreach (var (key, value) in writes)
        {
            projection.Apply(BuildSet(key, value, hlcPhysical: hlc++, treeId: ResidualTreeId));
        }

        using (LatticeApplyOffsetContext.BeginScope(dataPartition, 10))
        {
            await projection.SetCheckpointOffsetAsync(10, default);
        }

        await projection.FlushCheckpointAsync(default);
        await leaf.CaptureSnapshotAsync();
    }

    private static LeafSnapshotStorageGrain CreateInlineSnapshotStore()
        => new(Substitute.For<IGrainContext>(), new FakePersistentState<LeafSnapshotBlob>());

    [Test]
    public async Task Inline_capture_declined_by_the_store_does_not_advance_durable_coverage()
    {
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var store = CreateInlineSnapshotStore();
        await store.SaveAsync(DeclineTestStoredBlob(partitions, dataPartition, ghostKey: "ghost"), default);

        var (warm, warmState) = CreateResidualLeafWithSnapshotStore(partitions, store, coordinatorTail: 1);
        var hintBefore = warmState.State.SnapshotLoadHintBytes;

        // The capture regresses partition 0 and omits "ghost", so the store
        // declines it and keeps the stored blob verbatim.
        await CheckpointDataPartitionAndCaptureAsync(
            warm, warmState, dataPartition, [(dataKey, Encoding.UTF8.GetBytes("v"))]);

        var durable = await store.LoadAsync(default);
        Assert.That(durable!.SnapshotOffsetsByPartition![dataPartition], Is.EqualTo(3L),
            "precondition: the capture regressed partition 0 and omitted 'ghost', so the store declined it");

        Assert.Multiple(() =>
        {
            Assert.That(warm.DurableSnapshotCoverageForPartition(dataPartition),
                Is.LessThanOrEqualTo(durable.SnapshotOffsetsByPartition[dataPartition]),
                "THE ASSERTION. In-memory durable coverage gates the WAL GC trim floor and must never exceed "
                + "what the durable store covers. Before the fix the declined offer's 10 was recorded, so the "
                + "GC could trim [0, 10] of a partition whose write to the data key has no durable copy");
            Assert.That(warmState.State.SnapshotLoadHintBytes, Is.EqualTo(hintBefore),
                "the load hint describes the stored snapshot, which a declined capture did not replace");
        });
    }

    [Test]
    public async Task Staged_capture_whose_commit_is_declined_does_not_advance_durable_coverage()
    {
        // The pre-existing decline site: CommitStagedSnapshotAsync returns false
        // for a regressing manifest, and the caller used to discard that answer.
        const int partitions = 8;
        const int rowCount = 64;
        const int valueBytes = 4096;
        var (_, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var (inner, _, storedState) = CreateSegmentingStore();
        await inner.SaveAsync(DeclineTestStoredBlob(partitions, dataPartition, ghostKey: null), default);
        var recorder = new RecordingSnapshotStore(inner);

        var (warm, warmState) = CreateResidualLeafWithSnapshotStore(
            partitions, recorder, coordinatorTail: 1, leafSnapshotSegmentBytes: SegmentTestWindowBytes);

        var writes = Enumerable.Range(0, rowCount).Select(i =>
        {
            var payload = new byte[valueBytes];
            Array.Fill(payload, (byte)(i % 251));
            return ($"decline-key-{i:D4}", payload);
        });
        await CheckpointDataPartitionAndCaptureAsync(warm, warmState, dataPartition, writes);

        Assert.Multiple(() =>
        {
            Assert.That((long)rowCount * valueBytes, Is.GreaterThan(SegmentTestWindowBytes * 2),
                "precondition: the snapshot is several windows wide, so capture must stage");
            Assert.That(recorder.StagedSegments, Is.GreaterThan(1),
                "precondition: the capture went down the staged path");
            Assert.That(recorder.InlineSaves, Is.Zero,
                "precondition: the staged capture did not also save inline");
            Assert.That(storedState.State.SnapshotOffsetsByPartition![dataPartition], Is.EqualTo(3L),
                "precondition: the regressing manifest was declined, so the stored coverage is unchanged");
        });

        Assert.That(warm.DurableSnapshotCoverageForPartition(dataPartition),
            Is.LessThanOrEqualTo(storedState.State.SnapshotOffsetsByPartition![dataPartition]),
            "THE ASSERTION. The staged commit was declined, so the offered coverage of 10 is not durable and "
            + "must not reach the in-memory coverage that sets the WAL GC trim floor");
    }

    [Test]
    public async Task Inline_capture_the_store_merges_still_advances_durable_coverage()
    {
        // The gate must be narrow. The same regressing capture, against a stored
        // blob holding no key the leaf lacks, is merged element-wise and kept -
        // so its coverage IS durable and must still be recorded, or the leaf's
        // pin would freeze and its WAL would never trim.
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var store = CreateInlineSnapshotStore();
        await store.SaveAsync(DeclineTestStoredBlob(partitions, dataPartition, ghostKey: null), default);

        var (warm, warmState) = CreateResidualLeafWithSnapshotStore(partitions, store, coordinatorTail: 1);
        await CheckpointDataPartitionAndCaptureAsync(
            warm, warmState, dataPartition, [(dataKey, Encoding.UTF8.GetBytes("v"))]);

        var durable = await store.LoadAsync(default);
        Assert.Multiple(() =>
        {
            Assert.That(durable!.SnapshotOffsetsByPartition![dataPartition], Is.EqualTo(10L),
                "precondition: the store merged the capture and now covers the data partition to 10");
            Assert.That(durable.SnapshotOffsetsByPartition[0], Is.EqualTo(50L),
                "precondition: and kept the stored coverage of the regressed partition");
            Assert.That(warm.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(10L),
                "a kept capture's coverage is durable and must be recorded");
        });
    }
}
