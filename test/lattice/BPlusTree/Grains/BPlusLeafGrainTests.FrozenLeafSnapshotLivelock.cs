using System.Linq;
using System.Text;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the FROZEN-LEAF snapshot livelock (#2220), the field
/// fault behind the repocontext-reliability "persistedCheckpoint unchanged /
/// writes being lost" warnings. On the deployed container a single leaf kept two
/// durable rows that agreed on seven of eight WAL partitions and disagreed on one:
/// the grain state held that partition's checkpoint at 160972 while a two-day-old
/// snapshot (written three times in its life) held it at 155852. Every
/// reactivation reloaded the stale snapshot, the per-partition rehydrate loop in
/// <see cref="BPlusLeafGrain.TryRehydrateFromSnapshotAsync"/> rolled that one
/// partition back 5,120 entries, the tail replay re-advanced it, the activation
/// was torn down before the periodic snapshot cadence was ever reached, and the
/// snapshot never advanced - so the loop repeated forever while that partition's
/// WAL pin stayed frozen at 155852 and its WAL grew unbounded.
/// <para>
/// The rollback itself is NOT the bug: after the whole-cache <c>Cache.Clear()</c>
/// the rehydrate performs, lowering the checkpoint to the snapshot offset is
/// REQUIRED so the tail replay rebuilds the dropped (155852, 160972] rows rather
/// than skipping them (a naive clamp to <c>max(snapshot, persisted)</c> would
/// assert coverage the cleared cache does not hold - silent loss). The binding
/// defect is that the leaf never banks a FRESH durable snapshot covering the
/// re-advanced checkpoint, because the only in-activation capture driver
/// (<c>MaybeRunPeriodicSnapshotRecheckAsync</c>) sits behind a per-activation
/// persist counter that resets every activation and that a short over-budget
/// activation never drives to
/// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/> (default
/// 64). The fix latches the deficit when rehydrate lowers a partition and drives
/// a single off-cadence capture the moment the tail replay re-advances it, off
/// the deactivation deadline, so durable coverage converges within one activation
/// and the reload/rollback loop is broken.
/// </para>
/// <para>
/// These tests reproduce the exact eight-slot arrays measured on the box so the
/// fixture documents the field fault. Partition 2 is the divergent slot; the WAL
/// partition a data key hashes to under an 8-partition layout is not
/// controllable, so a key that hashes to partition 2 is discovered rather than
/// assumed.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    // The two durable rows the PM sampled on the frozen leaf, in the same second
    // as the newest fault line. Identical on every partition except index 2.
    private static readonly long[] FrozenLeafDurableCheckpoints =
        { 146262, 176247, 160972, 137361, 66271, 59156, 71018, 60521 };

    private static readonly long[] FrozenLeafStaleSnapshotOffsets =
        { 146262, 176247, 155852, 137361, 66271, 59156, 71018, 60521 };

    private const int FrozenLeafDivergentPartition = 2;
    private const long FrozenLeafStaleOffset = 155852;   // snapshot (behind)
    private const long FrozenLeafDurableOffset = 160972;  // grain-state checkpoint (ahead)

    /// <summary>
    /// Lowest test key that hashes to <paramref name="partition"/> under
    /// <paramref name="walPartitions"/>, so a data row can be planted in the
    /// specific divergent partition the field fixture uses.
    /// </summary>
    private static string FrozenLeafKeyForPartition(int partition, int walPartitions)
        => Enumerable.Range(0, 1 << 20)
            .Select(i => $"k{i}")
            .First(k => WalPartitionHash.Compute(k, walPartitions) == partition);

    private static LeafSnapshotBlob FrozenLeafStaleSnapshotBlob(string backingKey)
        => new()
        {
            // Partition 0 is covered at 146262, matching the grain state, so the
            // decline gate (which keys on the partition-0 scalar) enters the
            // rehydrate branch exactly as it did on the box.
            SnapshotOffset = FrozenLeafStaleSnapshotOffsets[0],
            Rows = new List<LeafSnapshotRow>
            {
                new(backingKey, LwwValue<byte[]>.Create(
                    Encoding.UTF8.GetBytes("stale"),
                    new HybridLogicalClock { WallClockTicks = 900L })),
            },
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = (long[])FrozenLeafStaleSnapshotOffsets.Clone(),
        };

    [Test]
    public async Task Frozen_leaf_banks_fresh_coverage_off_cadence_when_rehydrate_lowered_a_partition()
    {
        // GUARD ARM. Discriminator (pre-declared): after a rehydrate rolls the
        // divergent partition back to the stale snapshot offset and a SINGLE
        // post-rehydrate checkpoint flush re-advances it - far below the
        // periodic cadence of 64 - does the durable snapshot store's coverage
        // for that partition advance to the re-advanced checkpoint (160972)?
        // GREEN (fixed): yes - the off-cadence deficit capture banks it.
        // RED (fix removed): no - coverage stays frozen at 155852, the reload/
        // rollback loop is intact and the WAL pin never lifts.
        const int partitions = 8;
        var dataKey = FrozenLeafKeyForPartition(FrozenLeafDivergentPartition, partitions);

        // One real durable snapshot store, shared across activations, exercised
        // through the production load/save/merge seam.
        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);
        await store.SaveAsync(FrozenLeafStaleSnapshotBlob(dataKey), default);

        // Cold leaf: production default cadence (64) so the ONLY way coverage can
        // advance within this activation is the off-cadence deficit path. The
        // durable state carries the grain-state array (partition 2 at 160972);
        // coordinatorTail = 1 makes the rehydrate take the ACCEPT path (a prefix
        // was trimmed), exactly as on the box.
        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(
            partitions, store, coordinatorTail: 1,
            reclassifyEveryN: LatticeOptions.DefaultLeafSnapshotReClassifyEveryNCheckpoints);
        leafState.State.ProjectionCheckpointOffset = FrozenLeafDurableCheckpoints[0];
        leafState.State.ProjectionCheckpointOffsetsByPartition =
            (long[])FrozenLeafDurableCheckpoints.Clone();

        // 1) Rehydrate reproduces the field rollback: partition 2 is driven from
        //    160972 down to the stale 155852; every other partition is a no-op.
        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);
        Assert.That(rehydrated, Is.True, "the stale snapshot must be accepted (a prefix was trimmed)");
        Assert.That(
            leaf.GetCurrentCheckpointForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafStaleOffset),
            "rehydrate must roll the divergent partition back to the snapshot offset - the field rollback, "
            + "required for cache coherence after Cache.Clear()");
        Assert.That(
            leaf.DurableSnapshotCoverageForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafStaleOffset),
            "durable coverage on entry is the stale snapshot's - this is the deficit the fix must close");

        // 2) The tail replay re-absorbs (155852, 160972] and re-advances the
        //    partition. Model that with an explicit checkpoint advance + a single
        //    flush; this is the seam that on the box only ran a handful of times
        //    before teardown, never reaching the cadence.
        AsProjection(leaf).Apply(BuildSet(
            dataKey, Encoding.UTF8.GetBytes("readvanced"), hlcPhysical: 1000, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(FrozenLeafDivergentPartition, FrozenLeafDurableOffset))
        {
            await AsProjection(leaf).SetCheckpointOffsetAsync(FrozenLeafDurableOffset, default);
        }
        await AsProjection(leaf).FlushCheckpointAsync(default);

        // 3) The durable snapshot store now covers partition 2 at the re-advanced
        //    checkpoint - banked by the off-cadence deficit capture during this
        //    activation, after exactly one flush (cadence 64 never reached).
        Assert.That(
            leaf.DurableSnapshotCoverageForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafDurableOffset),
            "the off-cadence deficit capture must bank fresh coverage to the re-advanced checkpoint DURING the "
            + "activation; without it a short over-budget activation never reaches the periodic cadence and "
            + "coverage stays frozen at 155852 forever");

        var reloaded = await store.LoadAsync(default);
        Assert.That(reloaded, Is.Not.Null);
        Assert.That(reloaded!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(
            reloaded.SnapshotOffsetsByPartition![FrozenLeafDivergentPartition],
            Is.EqualTo(FrozenLeafDurableOffset),
            "the freshly captured blob is durable and covers the divergent partition at 160972 - the durable "
            + "side effect that lifts the WAL pin (min(checkpoint, covered) advances to the real frontier)");

        // 4) LIVELOCK BROKEN. A subsequent reactivation over the SAME store now
        //    finds snapshot == durable on partition 2, so the rehydrate no longer
        //    rolls it back: the reload/rollback loop cannot recur.
        var (next, nextState) = CreateResidualLeafWithSnapshotStore(
            partitions, store, coordinatorTail: 1,
            reclassifyEveryN: LatticeOptions.DefaultLeafSnapshotReClassifyEveryNCheckpoints);
        nextState.State.ProjectionCheckpointOffset = FrozenLeafDurableCheckpoints[0];
        nextState.State.ProjectionCheckpointOffsetsByPartition =
            (long[])FrozenLeafDurableCheckpoints.Clone();

        await next.TryRehydrateFromSnapshotAsync(default);
        Assert.That(
            next.GetCurrentCheckpointForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafDurableOffset),
            "after coverage caught up, the next rehydrate MUST NOT lower the divergent partition - the frozen-leaf "
            + "livelock is broken");
    }

    [Test]
    public async Task Healthy_leaf_whose_snapshot_matches_its_checkpoint_does_not_capture_off_cadence()
    {
        // TARGETING CONTROL. The deficit fast path must fire ONLY for a leaf whose
        // rehydrate actually lowered a partition. A healthy leaf whose durable
        // snapshot already matches its checkpoint takes no rollback, so the latch
        // stays clear and a single post-rehydrate flush (well under the cadence)
        // must NOT trigger an off-cadence capture - otherwise the fix would tax
        // every leaf on every activation with a redundant blob write. This test
        // passes with OR without the fix; it guards the fix's precision.
        const int partitions = 8;
        var dataKey = FrozenLeafKeyForPartition(FrozenLeafDivergentPartition, partitions);

        // Snapshot already covers partition 2 at the durable checkpoint (160972):
        // no divergence anywhere.
        var healthySnapshot = FrozenLeafStaleSnapshotBlob(dataKey);
        healthySnapshot.SnapshotOffsetsByPartition![FrozenLeafDivergentPartition] = FrozenLeafDurableOffset;

        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var store = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);
        await store.SaveAsync(healthySnapshot, default);

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(
            partitions, store, coordinatorTail: 1,
            reclassifyEveryN: LatticeOptions.DefaultLeafSnapshotReClassifyEveryNCheckpoints);
        leafState.State.ProjectionCheckpointOffset = FrozenLeafDurableCheckpoints[0];
        leafState.State.ProjectionCheckpointOffsetsByPartition =
            (long[])FrozenLeafDurableCheckpoints.Clone();

        var rehydrated = await leaf.TryRehydrateFromSnapshotAsync(default);
        Assert.That(rehydrated, Is.True);
        Assert.That(
            leaf.GetCurrentCheckpointForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafDurableOffset),
            "precondition: no rollback - snapshot already matches the durable checkpoint");

        // Advance the divergent partition one step past its coverage and flush
        // once. With no inherited deficit the fast path must stay dormant.
        AsProjection(leaf).Apply(BuildSet(
            dataKey, Encoding.UTF8.GetBytes("advance"), hlcPhysical: 1000, treeId: ResidualTreeId));
        using (LatticeApplyOffsetContext.BeginScope(FrozenLeafDivergentPartition, FrozenLeafDurableOffset + 1))
        {
            await AsProjection(leaf).SetCheckpointOffsetAsync(FrozenLeafDurableOffset + 1, default);
        }
        await AsProjection(leaf).FlushCheckpointAsync(default);

        Assert.That(
            leaf.DurableSnapshotCoverageForPartition(FrozenLeafDivergentPartition),
            Is.EqualTo(FrozenLeafDurableOffset),
            "a healthy leaf must NOT capture off-cadence on a single flush: coverage stays at the rehydrated "
            + "offset until the ordinary periodic cadence fires, so the fix does not tax leaves that never "
            + "rolled back");
    }
}
