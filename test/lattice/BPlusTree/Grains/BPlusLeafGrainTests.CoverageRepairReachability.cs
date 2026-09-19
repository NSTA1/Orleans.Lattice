using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3157: the WAL GC's blocking-pin classifier and the leaf's
/// zero-coverage repairer must agree about WHICH partitions exist.
/// <para>
/// These two components decide the same proposition - "this partition holds a
/// durable checkpoint that no durable snapshot covers" - from two hand-written
/// predicates in two files, joined by nothing but prose. The estate symptom is
/// the pair disagreeing: <c>wal_gc_blocking_pin_state{checkpointed_uncovered}</c>
/// reports the condition while
/// <c>leaf_snapshot_coverage_repairs{no_checkpointed_uncovered_partition}</c>
/// reports its absence, the tree-wide offset floor stops every trim scan, and
/// the WAL grows without bound (#3094).
/// </para>
/// <para>
/// <b>The defect these tests pin is a reachability gap, not a labelling one.</b>
/// The classifier does not bound itself by
/// <see cref="LatticeOptions.WalPartitions"/>: it parses the partition ordinal
/// out of the durable pin's consumer id and reads the persisted per-partition
/// checkpoint array directly, so every slot that array carries is classifiable.
/// The repairer bounded itself by the CONFIGURED count. Since
/// <c>SetPersistedCheckpointForPartition</c> only ever grows that array, its
/// length is the widest partition count the leaf has ever seen, so narrowing the
/// configured width strands the tail slots: their checkpoints survive, the
/// classifier keeps reporting them repairable, and the repairer walked a range
/// that structurally excluded them.
/// </para>
/// <para>
/// A fixture asserting the classifier LABELS such a partition correctly would
/// re-prove what already works - the classification was never the defect. So
/// every test here drives the repair end to end and asserts the outcome the
/// estate needs: coverage is actually stamped for the stranded partition, which
/// is exactly what takes its pin from <c>min(checkpoint, covered) &lt; 0</c>
/// (the Zero block that makes <c>ApplyDurableMaterialiserFloorAsync</c> abandon
/// the whole tree) to a usable offset, so the floor can advance.
/// </para>
/// <para>
/// The negative direction is load-bearing in equal measure. Widening the walked
/// range must not widen what is CLAIMED: a tail partition that never
/// checkpointed makes no honest offset claim, and stamping one would drop both
/// the HLC block and the offset abstention at once and authorise trimming a
/// prefix no consumer has read.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Builds the persisted per-partition checkpoint array of a leaf that was
    /// once configured for <paramref name="persistedWidth"/> partitions and has
    /// a surviving durable checkpoint in the tail slot
    /// <paramref name="partition"/>.
    /// </summary>
    private static long[] PersistedCheckpointsWithTailSlot(
        int persistedWidth, int partition, long offset)
    {
        var arr = new long[persistedWidth];
        for (var p = 0; p < arr.Length; p++)
            arr[p] = -1L;
        arr[partition] = offset;
        return arr;
    }

    /// <summary>
    /// THE load-bearing test. A partition the classifier calls
    /// <see cref="WalGcBlockingPinState.CheckpointedUncovered"/> must actually be
    /// repaired - coverage stamped - even when its ordinal lies beyond the
    /// currently configured partition count.
    /// <para>
    /// The leaf here is configured for 2 partitions but carries a persisted
    /// array of width 4 with a durable checkpoint at slot 3, the shape a
    /// narrowing of <c>WalPartitions</c> leaves behind. The GC can see slot 3 -
    /// asserted here against the same <see cref="LeafNodeState"/> instance the
    /// leaf is running on, so the two components cannot be reading different
    /// state - and after the repair runs, slot 3 must hold coverage.
    /// </para>
    /// <para>
    /// RED pre-fix: the repair DOES fire (partition 0 becomes uncovered on the
    /// persist) and DOES capture, so a test asserting merely that a capture
    /// happened would pass. The capture's coverage array is sized by the
    /// configured count, so it is 2 wide, slot 3 is never stamped, the pin for
    /// slot 3 stays at the Zero block, and the floor never lifts. That is the
    /// failure mode the estate measured: a repair that reports success while the
    /// blocking partition is untouched.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_partition_beyond_the_configured_width_is_repaired_not_merely_classified()
    {
        var (grain, state, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: 2,
            treeId: "tree-3157-tail-partition-repaired");

        state.State.ProjectionCheckpointOffsetsByPartition =
            PersistedCheckpointsWithTailSlot(persistedWidth: 4, partition: 3, offset: 42L);

        Assert.That(
            LatticeWalGcScheduler.ClassifyCheckpoint(state.State, partition: 3),
            Is.EqualTo(WalGcBlockingPinState.CheckpointedUncovered),
            "precondition: the GC classifies slot 3 as repairable off this exact state, so any "
                + "disagreement below is the repairer's reachability and not a difference of input.");

        Assert.That(grain.DurableSnapshotCoverageForPartition(3), Is.EqualTo(-1L),
            "precondition: nothing covers the stranded partition yet.");

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        // One checkpoint persist, which is what drives the repair.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(saved, Is.Not.Empty,
                "the repair must capture at all.");
            Assert.That(grain.DurableSnapshotCoverageForPartition(3), Is.EqualTo(42L),
                "the stranded partition must end up covered at its own checkpoint - this is what takes "
                    + "its pin from min(checkpoint, covered) < 0 (the tree-wide Zero block) to a usable "
                    + "offset, so the WAL GC's offset floor can advance past it.");
            Assert.That(grain.HasCheckpointedPartitionWithoutCoverage(2), Is.False,
                "and the repair must EXTINGUISH the condition, not merely act on it: a detector that "
                    + "stayed true would re-fire on every subsequent persist for ever.");
        });
    }

    /// <summary>
    /// The agreement property itself, stated directly: any partition the GC
    /// classifies repairable is one the leaf's detector can see.
    /// <para>
    /// This is the guard against silent re-divergence. #3157 was caused by a
    /// second reader of the checkpoint appearing beside the leaf's own accessor
    /// and drifting from it; nothing prevented the range the two walk from
    /// drifting the same way. Asserting the implication over a persisted array
    /// wider than the configured count fails the moment either side narrows
    /// alone.
    /// </para>
    /// </summary>
    [Test]
    public void Every_partition_the_classifier_calls_repairable_is_visible_to_the_repairer()
    {
        var (grain, state, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: 2,
            treeId: "tree-3157-classifier-repairer-agreement");

        state.State.ProjectionCheckpointOffsetsByPartition =
            PersistedCheckpointsWithTailSlot(persistedWidth: 6, partition: 5, offset: 11L);

        // Deliberately NOT activated. The agreement being pinned is between two
        // predicates over one durable state, and activation now RESOLVES the
        // condition (the activation-time repair hook stamps the stranded slot),
        // which would leave the property asserted against an already-repaired
        // leaf and therefore assert nothing.
        var classifiedRepairable = false;
        for (var p = 0; p < 6; p++)
        {
            if (LatticeWalGcScheduler.ClassifyCheckpoint(state.State, p)
                == WalGcBlockingPinState.CheckpointedUncovered)
            {
                classifiedRepairable = true;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(classifiedRepairable, Is.True,
                "precondition: the GC finds a repairable partition on this state.");
            Assert.That(grain.HasCheckpointedPartitionWithoutCoverage(2), Is.True,
                "so the leaf's repairer must see one too. When it does not, the GC reports "
                    + "checkpointed_uncovered while the leaf reports no_checkpointed_uncovered_partition "
                    + "and the tree's WAL grows without bound with both halves looking correct.");
        });
    }

    /// <summary>
    /// The negative direction. Widening the walked range widens what is LOOKED
    /// AT, never what is CLAIMED: a tail partition carrying the sentinel has no
    /// WAL offset it could honestly claim, so the repair must leave it at
    /// <c>-1</c> and leave its Zero block pin standing.
    /// <para>
    /// Stamping one would be worse than the stall it fixes. The offset plane
    /// does not merely ignore a <c>-1</c>; it ABSTAINS on the stated assumption
    /// that the HLC block is enforcing retention, so a fabricated claim drops
    /// both protections at once and authorises trimming a prefix no consumer has
    /// read.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_never_checkpointed_partition_beyond_the_configured_width_is_not_stamped()
    {
        var (grain, state, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: 2,
            treeId: "tree-3157-tail-partition-not-stamped");

        // Slot 3 is checkpointed and repairable; slot 2 is a tail slot that
        // never checkpointed and must stay unclaimed even though the widened
        // range now walks it.
        state.State.ProjectionCheckpointOffsetsByPartition =
            PersistedCheckpointsWithTailSlot(persistedWidth: 4, partition: 3, offset: 42L);

        Assert.That(
            LatticeWalGcScheduler.ClassifyCheckpoint(state.State, partition: 2),
            Is.EqualTo(WalGcBlockingPinState.NeverCheckpointed),
            "precondition: the GC agrees slot 2 makes no claim, so its pin is correct by design.");

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(saved, Is.Not.Empty,
                "precondition: a capture did happen, so the assertion below is about what it claimed.");
            Assert.That(grain.DurableSnapshotCoverageForPartition(2), Is.EqualTo(-1L),
                "a partition that never checkpointed must keep the sentinel. Stamping it would drop the "
                    + "HLC block and the offset abstention together and license trimming committed data "
                    + "no consumer has read.");
            Assert.That(
                saved[^1].SnapshotOffsetsByPartition![2], Is.EqualTo(-1L),
                "and the claim must be absent in the blob itself, not merely absent from the in-memory "
                    + "view - the blob is what the next activation reads back.");
        });
    }

    /// <summary>
    /// The widening must not perturb the ordinary case. A leaf whose persisted
    /// array matches its configured width behaves exactly as before: the claim
    /// is as wide as the configuration and no wider.
    /// </summary>
    [Test]
    public async Task A_leaf_whose_persisted_width_matches_its_configuration_claims_exactly_that_width()
    {
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L, reClassifyEveryN: 1000, walPartitions: 4,
            treeId: "tree-3157-ordinary-width-unchanged");

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.That(saved, Is.Not.Empty);
        Assert.That(saved[^1].SnapshotOffsetsByPartition, Has.Length.EqualTo(4),
            "the widening is bounded by the leaf's own durable evidence, so a leaf that has never "
                + "observed a wider partition count claims exactly its configured width.");
    }
}
