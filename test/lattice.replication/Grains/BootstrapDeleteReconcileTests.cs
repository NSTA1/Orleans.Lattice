using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class BootstrapDeleteReconcileTests
{
    private static readonly Guid Lineage = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Test]
    public void Decide_all_gates_pass_reconciles()
    {
        var decision = Decide();

        Assert.Multiple(() =>
        {
            Assert.That(decision.ShouldReconcile, Is.True);
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.Reconciled));
            Assert.That(decision.OweRetry, Is.False);
        });
    }

    [Test]
    public void Decide_scoped_export_skips_reconcile()
    {
        var decision = Decide(isScopedExport: true);

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedScoped));
        Assert.That(decision.ShouldReconcile, Is.False);
    }

    [TestCase("physical")]
    [TestCase("shard-map")]
    [TestCase("lineage")]
    [TestCase("epoch")]
    public void Decide_open_close_mismatch_skips_and_owes_retry(string field)
    {
        var close = Generation();
        close = field switch
        {
            "physical" => close with { PhysicalTreeId = "tree-b" },
            "shard-map" => close with { ShardMapVersion = 43 },
            "lineage" => close with { Lineage = Guid.Parse("22222222-2222-2222-2222-222222222222") },
            "epoch" => close with { DeleteEpoch = 8 },
            _ => close,
        };

        var decision = Decide(closeGeneration: close);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedUnstable));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.OweRetry, Is.True);
        });
    }

    [Test]
    public void Decide_deleted_at_open_skips_and_owes_retry()
    {
        var decision = Decide(openGeneration: Generation(isDeleted: true));

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedDeleted));
        Assert.That(decision.OweRetry, Is.True);
    }

    [Test]
    public void Decide_deleted_at_close_skips_and_owes_retry()
    {
        var decision = Decide(closeGeneration: Generation(isDeleted: true));

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedDeleted));
        Assert.That(decision.OweRetry, Is.True);
    }

    [Test]
    public void Decide_unknown_generation_skips_reconcile()
    {
        var decision = Decide(openGeneration: Generation(lineage: null, useLineage: false));

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedUnknown));
        Assert.That(decision.ShouldReconcile, Is.False);
    }

    [Test]
    public void Decide_never_aligned_on_populated_receiver_skips_reconcile()
    {
        var decision = Decide(alignedLineage: null, receiverWasEmpty: false, useAlignedLineage: false);

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedNeverAligned));
        Assert.That(decision.ShouldReconcile, Is.False);
    }

    [Test]
    public void Decide_lineage_mismatch_skips_without_owed_retry()
    {
        var decision = Decide(alignedLineage: Guid.Parse("33333333-3333-3333-3333-333333333333"));

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedLineageMismatch));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.OweRetry, Is.False);
        });
    }

    [Test]
    public void Decide_non_lww_tree_skips_reconcile()
    {
        var decision = Decide(mergeMode: LatticeMergeMode.OrSet);

        Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedNotLww));
        Assert.That(decision.ShouldReconcile, Is.False);
    }

    [Test]
    public void Decide_empty_receiver_records_aligned_lineage()
    {
        var decision = Decide(alignedLineage: null, receiverWasEmpty: true, useAlignedLineage: false);

        Assert.Multiple(() =>
        {
            Assert.That(decision.ShouldReconcile, Is.True);
            Assert.That(decision.RecordAlignedLineage, Is.True);
        });
    }

    private static BootstrapReconcileDecision Decide(
        bool isScopedExport = false,
        SnapshotSourceGeneration? openGeneration = null,
        SnapshotSourceGeneration? closeGeneration = null,
        Guid? alignedLineage = null,
        bool useAlignedLineage = true,
        bool receiverWasEmpty = false,
        LatticeMergeMode mergeMode = LatticeMergeMode.LwwRegister) =>
        BootstrapDeleteReconcile.Decide(
            isScopedExport,
            openGeneration ?? Generation(),
            closeGeneration ?? Generation(),
            useAlignedLineage ? alignedLineage ?? Lineage : null,
            receiverWasEmpty,
            mergeMode);

    private static SnapshotSourceGeneration Generation(
        string? physicalTreeId = "tree-a",
        long? shardMapVersion = 42,
        Guid? lineage = null,
        long? deleteEpoch = 7,
        bool? isDeleted = false,
        bool useLineage = true) =>
        new()
        {
            PhysicalTreeId = physicalTreeId,
            ShardMapVersion = shardMapVersion,
            Lineage = useLineage ? lineage ?? Lineage : null,
            DeleteEpoch = deleteEpoch,
            IsDeleted = isDeleted,
        };
}
