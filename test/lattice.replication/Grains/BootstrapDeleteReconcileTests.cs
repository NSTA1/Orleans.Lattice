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
    public void Decide_unknown_generation_skips_and_owes_retry()
    {
        var unknownOpen = Decide(openGeneration: Generation(lineage: null, useLineage: false));
        var missingClose = BootstrapDeleteReconcile.Decide(
            false, Generation(), null, Lineage, false, false, false, LatticeMergeMode.LwwRegister);

        Assert.Multiple(() =>
        {
            Assert.That(unknownOpen.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedUnknown));
            Assert.That(unknownOpen.ShouldReconcile, Is.False);
            Assert.That(unknownOpen.OweRetry, Is.True, "an older sender must reconcile once it upgrades");
            Assert.That(missingClose.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedUnknown));
            Assert.That(missingClose.OweRetry, Is.True);
        });
    }

    [Test]
    public void Decide_never_aligned_with_an_orphaned_source_key_skips_reconcile()
    {
        var decision = Decide(useAlignedLineage: false, heldNoSourceRows: false, orphaned: true);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedNeverAligned));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.RecordAlignedLineage, Is.False);
        });
    }

    [Test]
    public void Decide_never_aligned_with_no_orphaned_source_key_aligns_without_deleting()
    {
        var decision = Decide(useAlignedLineage: false, heldNoSourceRows: false, orphaned: false);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.Aligned));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.RecordAlignedLineage, Is.True);
            Assert.That(decision.OweRetry, Is.False);
        });
    }

    [Test]
    public void Decide_lineage_mismatch_with_an_orphaned_source_key_skips_without_owed_retry()
    {
        var decision = Decide(alignedLineage: Guid.Parse("33333333-3333-3333-3333-333333333333"), orphaned: true);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.SkippedLineageMismatch));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.OweRetry, Is.False);
            Assert.That(decision.RecordAlignedLineage, Is.False);
        });
    }

    [Test]
    public void Decide_lineage_mismatch_with_no_orphaned_source_key_realigns()
    {
        var decision = Decide(alignedLineage: Guid.Parse("33333333-3333-3333-3333-333333333333"), orphaned: false);

        Assert.Multiple(() =>
        {
            Assert.That(decision.Outcome, Is.EqualTo(BootstrapReconcileOutcome.Aligned));
            Assert.That(decision.ShouldReconcile, Is.False);
            Assert.That(decision.RecordAlignedLineage, Is.True);
        });
    }

    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    public void Decide_never_aligns_over_a_source_row_taken_during_the_drain_that_the_export_lacks(bool useAlignedLineage, bool heldNoSourceRows)
    {
        var decision = Decide(
            alignedLineage: Guid.Parse("33333333-3333-3333-3333-333333333333"),
            useAlignedLineage: useAlignedLineage,
            heldNoSourceRows: heldNoSourceRows,
            orphaned: false,
            orphanedAtEnd: true);

        Assert.Multiple(() =>
        {
            Assert.That(decision.RecordAlignedLineage, Is.False,
                "an old-lineage row taken mid-drain would otherwise be vouched for, and later deleted");
            Assert.That(decision.ShouldReconcile, Is.False);
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
    public void Decide_receiver_with_no_source_rows_records_aligned_lineage_and_reconciles()
    {
        var decision = Decide(useAlignedLineage: false, heldNoSourceRows: true, orphaned: true);

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
        bool heldNoSourceRows = false,
        bool orphaned = true,
        LatticeMergeMode mergeMode = LatticeMergeMode.LwwRegister,
        bool orphanedAtEnd = false) =>
        BootstrapDeleteReconcile.Decide(
            isScopedExport,
            openGeneration ?? Generation(),
            closeGeneration ?? Generation(),
            useAlignedLineage ? alignedLineage ?? Lineage : null,
            heldNoSourceRows,
            orphaned,
            orphanedAtEnd,
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
