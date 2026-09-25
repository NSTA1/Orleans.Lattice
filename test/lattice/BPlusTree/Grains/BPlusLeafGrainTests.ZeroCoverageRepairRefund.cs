using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3576: the zero-coverage repair budget must be consumed by captures
/// that make no progress, not by captures that succeed.
/// <para>
/// A bulk-loaded leaf spans up to <c>WalPartitions</c> partitions, and each one
/// checkpoints on its own schedule. Every newly checkpointed partition re-arms
/// the repair predicate, so the leaf re-enters the repair once per partition -
/// each time successfully. Charging those successes against the eight-attempt
/// budget exhausted it on every such leaf (observed on the pl3c rig as roughly
/// one exhaustion warning per leaf on every run), abandoning the repair for the
/// re-arm backoff with partitions still uncovered and their block pins holding
/// the tree's WAL trim floor.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Repair_budget_is_not_spent_by_successful_captures_as_partitions_checkpoint_one_by_one()
    {
        const int partitions = 16;
        var treeId = UniqueCoverageRepairTreeId("refund-progress");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            walPartitions: partitions,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        for (var p = 0; p < partitions; p++)
        {
            var hints = Enumerable.Repeat(0L, partitions).ToArray();
            hints[p] = 10L + p;
            await grain.SetCheckpointOffsetHintsAsync(hints);
        }

        Assert.That(saved, Has.Count.EqualTo(partitions),
            "scanned count: one successful capture per newly checkpointed partition - twice the "
            + "eight-attempt budget, so the pre-fix accounting would have abandoned half of them");

        Assert.Multiple(() =>
        {
            for (var p = 0; p < partitions; p++)
            {
                Assert.That(grain.DurableSnapshotCoverageForPartition(p), Is.GreaterThanOrEqualTo(0L),
                    $"partition {p} checkpointed and a capture succeeded afterwards, so it must be covered");
            }

            Assert.That(grain.HasCheckpointedPartitionWithoutCoverage(partitions), Is.False,
                "every checkpointed partition is covered");
            Assert.That(recorder.Sum("exhausted"), Is.Zero,
                "successful captures must not exhaust the budget");
            Assert.That(recorder.Sum("backing_off"), Is.Zero,
                "and so no later evaluation is suppressed by a re-arm backoff");
            Assert.That(recorder.Sum("repaired"), Is.EqualTo((long)partitions),
                "positive control: the same listener observes every successful repair");
        });
    }

    [Test]
    public async Task Repair_budget_still_bounds_failing_captures_on_a_multi_partition_leaf()
    {
        const int partitions = 16;
        var treeId = UniqueCoverageRepairTreeId("refund-failing");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            walPartitions: partitions,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        for (var round = 0; round < 3; round++)
        {
            for (var p = 0; p < partitions; p++)
            {
                var hints = Enumerable.Repeat(0L, partitions).ToArray();
                hints[p] = 10L + (round * partitions) + p;
                await grain.SetCheckpointOffsetHintsAsync(hints);
            }
        }

        Assert.That(saved, Has.Count.EqualTo(8),
            "a failing capture covers nothing and is never refunded, so the budget still caps the "
            + "attempts against a failing store at eight, however many partitions keep checkpointing");
        Assert.That(recorder.Sum("exhausted"), Is.EqualTo(1L),
            "positive control: the budget was genuinely spent and exhaustion reported once");
    }
}
