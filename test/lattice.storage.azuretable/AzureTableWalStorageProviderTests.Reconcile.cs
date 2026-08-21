namespace Orleans.Lattice.Storage.AzureTable.Tests;

/// <summary>
/// Pure-logic tests for the activation-time reconciliation planner
/// (<c>AzureTableWalStorageProvider.PlanReconciliation</c>) that the
/// per-batch / manifest schema uses to decide which
/// orphan batch partitions to roll forward and which to roll back at
/// silo restart. The planner is a deterministic function of
/// <c>(currentTail, orphansAscending)</c>; behavioural coverage of
/// the surrounding I/O (table scans, transactional writes) lives in
/// the Azurite-backed integration tests under the
/// <c>AzureStorageEmulator</c> category.
/// </summary>
public partial class AzureTableWalStorageProviderTests
{
    private static AzureTableWalStorageProvider.OrphanBatch Orphan(long startOffset, long endOffsetInclusive) =>
        new(startOffset, endOffsetInclusive, $"_b_|t|0|S{startOffset:D19}", HasCandidateRow: true);

    [Test]
    public void PlanReconciliation_no_orphans_returns_unchanged_tail()
    {
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 42L,
            orphansAscending: Array.Empty<AzureTableWalStorageProvider.OrphanBatch>());

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(42L));
            Assert.That(plan.RollForward, Is.Empty);
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_single_orphan_contiguous_with_tail_rolls_forward()
    {
        // TAIL = 9, orphan covers offsets [10, 14] - perfect
        // continuation, rollforward should advance TAIL to 14.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Orphan(10L, 14L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(14L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 10L }));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_single_orphan_with_no_committed_tail_rolls_forward_when_starts_at_zero()
    {
        // TAIL = -1 (fresh shard); orphan starts at 0 - rollforward
        // because there is no gap below it.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: -1L,
            orphansAscending: new[] { Orphan(0L, 4L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(4L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 0L }));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_single_orphan_with_no_committed_tail_rolls_back_when_starts_above_zero()
    {
        // TAIL = -1, orphan starts at 5 - there is a gap below
        // (offsets 0..4 are missing) so the orphan rolls back.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: -1L,
            orphansAscending: new[] { Orphan(5L, 9L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(-1L), "TAIL must not advance when no orphan rolls forward");
            Assert.That(plan.RollForward, Is.Empty);
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 5L }));
        });
    }

    [Test]
    public void PlanReconciliation_orphan_with_gap_below_rolls_back()
    {
        // TAIL = 9, orphan starts at 15 - there's a gap (offsets
        // 10..14 are missing) so the orphan rolls back.
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Orphan(15L, 19L) });

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(9L));
            Assert.That(plan.RollForward, Is.Empty);
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 15L }));
        });
    }

    [Test]
    public void PlanReconciliation_multiple_contiguous_orphans_all_roll_forward()
    {
        // TAIL = 9; orphans cover [10,14], [15,19], [20,24] - every
        // one extends the prior end exactly, so all three roll
        // forward and TAIL advances to 24.
        var orphans = new[]
        {
            Orphan(10L, 14L),
            Orphan(15L, 19L),
            Orphan(20L, 24L),
        };

        var plan = AzureTableWalStorageProvider.PlanReconciliation(currentTail: 9L, orphansAscending: orphans);

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(24L));
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 10L, 15L, 20L }));
            Assert.That(plan.RollBack, Is.Empty);
        });
    }

    [Test]
    public void PlanReconciliation_orphans_after_first_gap_all_roll_back()
    {
        // TAIL = 9; orphans cover [10,14] (contiguous - rollforward),
        // [25,29] (gap - rollback), [30,34] (above a rollback -
        // rollback). Everything after the first contiguity break is
        // rolled back regardless of internal contiguity.
        var orphans = new[]
        {
            Orphan(10L, 14L),
            Orphan(25L, 29L),
            Orphan(30L, 34L),
        };

        var plan = AzureTableWalStorageProvider.PlanReconciliation(currentTail: 9L, orphansAscending: orphans);

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(14L), "TAIL advances only through the rollforward prefix");
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 10L }));
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 25L, 30L }));
        });
    }

    [Test]
    public void PlanReconciliation_first_orphan_above_tail_breaks_contiguity_for_every_orphan()
    {
        // TAIL = 9; the first orphan starts at 15 (gap from 10..14)
        // - everything rolls back including a later orphan that
        // would have been contiguous with the (rolled-back) first.
        var orphans = new[]
        {
            Orphan(15L, 19L),
            Orphan(20L, 24L),
        };

        var plan = AzureTableWalStorageProvider.PlanReconciliation(currentTail: 9L, orphansAscending: orphans);

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(9L));
            Assert.That(plan.RollForward, Is.Empty);
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 15L, 20L }));
        });
    }

    [Test]
    public void PlanReconciliation_malformed_orphan_with_end_below_start_rolls_back_without_advancing_tail()
    {
        // Defensive: an orphan whose recovered end < start (e.g.
        // empty batch partition with a recovered offset of -1) must
        // never advance TAIL. Treat it as a rollback and break
        // contiguity for every subsequent orphan.
        var orphans = new[]
        {
            new AzureTableWalStorageProvider.OrphanBatch(10L, 5L, "_b_|t|0|S0000000000000000010", HasCandidateRow: true),
            Orphan(15L, 19L),
        };

        var plan = AzureTableWalStorageProvider.PlanReconciliation(currentTail: 9L, orphansAscending: orphans);

        Assert.Multiple(() =>
        {
            Assert.That(plan.ResultingTail, Is.EqualTo(9L));
            Assert.That(plan.RollForward, Is.Empty);
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 10L, 15L }));
        });
    }

    [Test]
    public void PlanReconciliation_throws_on_null_orphans_list()
    {
        Assert.That(
            () => AzureTableWalStorageProvider.PlanReconciliation(currentTail: 0L, orphansAscending: null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void PlanReconciliation_preserves_orphan_end_offset_through_to_resulting_tail()
    {
        // A 100-entry orphan covers [10, 109]; the planner must
        // advance TAIL to the orphan's actual EndOffsetInclusive, not
        // re-derive it from StartOffset + count or any other proxy
        // (the recovered end is the authoritative value).
        var plan = AzureTableWalStorageProvider.PlanReconciliation(
            currentTail: 9L,
            orphansAscending: new[] { Orphan(10L, 109L) });

        Assert.That(plan.ResultingTail, Is.EqualTo(109L));
    }

    [Test]
    public void OrphanBatch_carries_HasCandidateRow_flag()
    {
        // The flag is the discriminator the rollforward / rollback
        // paths read to decide whether to emit a C-row delete. Pin
        // both shapes so a future refactor that drops or repurposes
        // the field surfaces here.
        var legacy = new AzureTableWalStorageProvider.OrphanBatch(0L, 4L, "_b_|t|0|S0", HasCandidateRow: true);
        var dmode = new AzureTableWalStorageProvider.OrphanBatch(5L, 9L, "_b_|t|0|S5", HasCandidateRow: false);

        Assert.Multiple(() =>
        {
            Assert.That(legacy.HasCandidateRow, Is.True);
            Assert.That(dmode.HasCandidateRow, Is.False);
            // Other fields untouched by the new flag.
            Assert.That(legacy.StartOffset, Is.EqualTo(0L));
            Assert.That(legacy.EndOffsetInclusive, Is.EqualTo(4L));
            Assert.That(dmode.StartOffset, Is.EqualTo(5L));
            Assert.That(dmode.EndOffsetInclusive, Is.EqualTo(9L));
        });
    }

    [Test]
    public void PlanReconciliation_decision_is_independent_of_HasCandidateRow_flag()
    {
        // The contiguity test must not branch on HasCandidateRow:
        // discovery mechanism (C-row scan vs partition scan) is
        // orthogonal to the rollforward-vs-rollback decision. A
        // D-mode orphan that is contiguous with TAIL must still
        // roll forward; a legacy orphan that has a gap must still
        // roll back.
        var orphans = new[]
        {
            new AzureTableWalStorageProvider.OrphanBatch(0L, 2L, "_b_|t|0|S0", HasCandidateRow: false),
            new AzureTableWalStorageProvider.OrphanBatch(3L, 5L, "_b_|t|0|S3", HasCandidateRow: true),
            new AzureTableWalStorageProvider.OrphanBatch(20L, 22L, "_b_|t|0|S20", HasCandidateRow: false),
        };

        var plan = AzureTableWalStorageProvider.PlanReconciliation(currentTail: -1L, orphansAscending: orphans);

        Assert.Multiple(() =>
        {
            Assert.That(plan.RollForward.Select(o => o.StartOffset), Is.EqualTo(new[] { 0L, 3L }));
            Assert.That(plan.RollBack.Select(o => o.StartOffset), Is.EqualTo(new[] { 20L }));
            Assert.That(plan.ResultingTail, Is.EqualTo(5L));

            // The flag round-trips through the plan untouched - the
            // planner copies the orphan into the rollforward /
            // rollback list verbatim so the executor can decide
            // whether to emit a C-row delete per orphan.
            Assert.That(plan.RollForward[0].HasCandidateRow, Is.False);
            Assert.That(plan.RollForward[1].HasCandidateRow, Is.True);
            Assert.That(plan.RollBack[0].HasCandidateRow, Is.False);
        });
    }
}
