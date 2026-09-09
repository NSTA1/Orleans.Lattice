using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the <c>HasUsableSnapshot</c> PARTIAL-COVERAGE hazard
/// (issue #2404, candidate 1 carried over from #2364): a snapshot that loads
/// successfully while claiming WAL coverage the reloaded projection cannot
/// actually reproduce.
/// <para>
/// <c>LeafSnapshotStorageGrain.HasUsableSnapshot</c> is sound as a GATE and is
/// deliberately not tightened here. It answers "can this blob be rehydrated at
/// all" - a captured prefix plus a row payload that reads back - and it must NOT
/// also demand that the blob cover every partition, because rejecting an
/// under-covering blob would discard the sole durable copy of a prefix the
/// coverage-gated WAL GC has already trimmed. That is the loss the whole
/// mechanism exists to prevent.
/// </para>
/// <para>
/// The unsoundness is that the gate's contract - <em>usable does not mean
/// complete</em> - was not honoured by its consumer. On the rehydrate ACCEPT
/// path <c>BPlusLeafGrain.TryRehydrateFromSnapshotAsync</c> clears the cache and
/// reloads ONLY the snapshot's rows, then reset per-partition checkpoints in a
/// loop bounded by <c>blob.SnapshotOffsetsByPartition.Length</c>. A partition the
/// blob carries NO SLOT FOR therefore kept its old, higher persisted checkpoint
/// over a cache that no longer holds those rows, so the tail replay resumed at
/// <c>(checkpoint_p, head]</c> and silently skipped <c>[0, checkpoint_p]</c>.
/// </para>
/// <para>
/// This is the same loss already guarded for the in-array <c>-1</c> sentinel by
/// <see cref="Rehydrate_resets_uncovered_partition_checkpoint_so_retained_prefix_is_not_skipped"/>.
/// That fix bounded the reset by the blob's array, which closes the sentinel
/// shape and leaves the ABSENT-SLOT shape open. Two shapes reach it:
/// </para>
/// <list type="number">
/// <item><description>A legacy blob whose <c>SnapshotOffsetsByPartition</c> is
/// <see langword="null"/> - the old fallback assigned the scalar
/// <c>ProjectionCheckpointOffset</c> and nothing else, so every non-zero
/// partition kept a checkpoint ahead of the cleared cache. Multi-partition WALs
/// predate the per-partition field, so this is real persisted data.</description></item>
/// <item><description>A blob captured while <c>WalPartitions</c> was smaller than
/// it is now. The leaf's own <c>ProjectionCheckpointOffsetsByPartition</c> never
/// shrinks (see <c>BPlusLeafGrain.Projection.cs</c>), so it is strictly longer
/// than the blob's array after the setting is raised and a cold restart lands
/// before the next capture.</description></item>
/// </list>
/// <para>
/// Resetting the uncovered partitions to <c>-1</c> is loss-free by exactly the
/// coupling the sentinel reset already relies on:
/// <c>DurableSnapshotCoverageForPartition</c> reports <c>-1</c> for a partition
/// outside the recorded coverage array, so <c>ResolveDurablePinForPartition</c>
/// held a Zero block pin for it and the WAL GC never authorised trimming its
/// prefix. Its full WAL <c>[0, checkpoint_p]</c> survives and the from-zero
/// replay rebuilds it intact.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Rehydrate_resets_a_partition_the_blob_carries_no_slot_for_so_its_retained_prefix_is_not_skipped()
    {
        // SHAPE 2: the blob's per-partition array is SHORTER than the leaf's
        // partition space (WalPartitions was raised after the capture, and the
        // leaf's own checkpoint array never shrinks). RED (pre-fix): the reset
        // loop ran only over the blob's single slot, leaving the data
        // partition's checkpoint at 7 over a cache holding none of its rows -
        // the tail replay resumes at (7, head] and silently drops [0, 7].
        // GREEN: the reset spans the leaf's whole partition space, so the
        // uncovered partition is reset to -1 and its retained WAL is replayed.
        const int partitions = 8;
        // coordinatorTail = 1 makes AnyPartitionWalPrefixTrimmedAsync report a
        // trimmed prefix, so the rehydrate takes the ACCEPT path it is built for.
        var (grain, state, _, snapshotStub) = CreateResidualLeaf(partitions, coordinatorTail: 1);
        var (_, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        // Persisted state: partition 0 checkpointed at 0; the non-zero partition
        // holds data checkpointed at offset 7, whose uncovered, block-pinned WAL
        // [0, 7] is retained.
        state.State.ProjectionCheckpointOffset = 0L;
        var persisted = new long[partitions];
        Array.Fill(persisted, -1L);
        persisted[0] = 0L;
        persisted[dataPartition] = 7L;
        state.State.ProjectionCheckpointOffsetsByPartition = persisted;

        // The loaded (latest durable) blob was captured when WalPartitions was 1,
        // so it carries ONE slot. It says nothing at all about the data
        // partition - not even the -1 sentinel the existing guard keys on.
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 0L,
            Rows = new List<LeafSnapshotRow>(),
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = new[] { 0L },
        };
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(blob));

        var accepted = await grain.TryRehydrateFromSnapshotAsync(default);
        Assert.That(accepted, Is.True,
            "the snapshot is the sole durable coverage of a trimmed prefix, so rehydrate must accept");

        Assert.That(grain.GetCurrentCheckpointForPartition(dataPartition), Is.EqualTo(-1L),
            "a partition the blob carries NO SLOT for is exactly as uncovered as one carrying the -1 "
            + "sentinel, so its checkpoint MUST be reset on rehydrate; leaving it at 7 over a cleared "
            + "cache makes the tail replay resume at (7, head] and silently drop [0, 7]");
        // The safety coupling that makes the reset loss-free: an absent slot
        // records no coverage, so the gate never authorised trimming the prefix.
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
            "the reset is loss-free precisely because the partition is uncovered and its full WAL is retained");
        // Partition 0 still takes the coverage the blob genuinely claims - the
        // widened reset must not flatten a slot the blob does carry.
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0L),
            "the widened reset must still honour every slot the blob DOES carry");
    }

    [Test]
    public async Task Rehydrate_resets_every_non_zero_partition_for_a_legacy_blob_with_no_per_partition_array()
    {
        // SHAPE 1: a legacy blob captured before SnapshotOffsetsByPartition
        // existed, loaded by a leaf that has since checkpointed a non-zero
        // partition. RED (pre-fix): the null-array fallback assigned the scalar
        // ProjectionCheckpointOffset and nothing else, so the data partition
        // kept its checkpoint of 7 over a cache reloaded from a blob that covers
        // partition 0 only. GREEN: the legacy shape is folded into the same
        // reset, so every non-zero partition is reset to -1.
        const int partitions = 8;
        var (grain, state, _, snapshotStub) = CreateResidualLeaf(partitions, coordinatorTail: 1);
        var (_, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        state.State.ProjectionCheckpointOffset = 0L;
        var persisted = new long[partitions];
        Array.Fill(persisted, -1L);
        persisted[0] = 0L;
        persisted[dataPartition] = 7L;
        state.State.ProjectionCheckpointOffsetsByPartition = persisted;

        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 0L,
            Rows = new List<LeafSnapshotRow>(),
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = null,
        };
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(blob));

        var accepted = await grain.TryRehydrateFromSnapshotAsync(default);
        Assert.That(accepted, Is.True,
            "a legacy blob is the sole durable coverage of a trimmed prefix, so rehydrate must accept it");

        Assert.That(grain.GetCurrentCheckpointForPartition(dataPartition), Is.EqualTo(-1L),
            "a legacy blob covers partition 0 only, so every non-zero partition is uncovered and MUST be "
            + "reset on rehydrate; the scalar-only fallback left this checkpoint at 7 over a cleared cache");
        Assert.That(grain.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
            "the reset is loss-free precisely because the partition is uncovered and its full WAL is retained");
        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(0L),
            "the legacy blob's scalar coverage of partition 0 must still be applied");
    }
}
