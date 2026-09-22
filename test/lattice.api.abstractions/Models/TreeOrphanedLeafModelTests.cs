using System.Collections.Immutable;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit tests for the transport-agnostic orphaned-leaf report mirrors
/// (<see cref="TreeOrphanedLeafReport"/>, <see cref="TreeOrphanedLeafFinding"/>,
/// <see cref="TreeOrphanedLeafDisposition"/>). Pins the derived tallies an operator
/// reads first - how many leaves were unspliced and how many were refused - and the
/// per-finding refusal predicate they are computed from, so a new disposition cannot
/// be added without deciding which side of that line it falls on. Pure value types,
/// no cluster.
/// </summary>
[TestFixture]
public sealed class TreeOrphanedLeafModelTests
{
    private static TreeOrphanedLeafFinding Finding(TreeOrphanedLeafDisposition disposition)
        => new() { LeafId = "leaf-1", Disposition = disposition };

    [Test]
    public void Survey_counts_distinguish_zero_unknown_and_partial_batch_and_break_down_repair_scope()
    {
        var healthy = new TreeOrphanedLeafReport { TreeId = "tree", Survey = true };
        var refused = Finding(TreeOrphanedLeafDisposition.RefusedUnverifiedKeys) with
        {
            SurveyVerifiedKeyCount = 2, SurveyMissingKeyCount = 3,
            SurveyRoutingContradictionKeyCount = 1,
        };
        var repairable = Finding(TreeOrphanedLeafDisposition.Repairable) with
        {
            SurveyVerifiedKeyCount = 4, SurveyMissingKeyCount = 0,
            SurveyRoutingContradictionKeyCount = 0,
        };
        var report = healthy with { Findings = [refused, repairable] };
        Assert.Multiple(() =>
        {
            Assert.That(healthy.SurveyMissingKeyCount, Is.Zero);
            Assert.That(healthy.OrphanedLeafCount, Is.Zero);
            Assert.That(healthy.RepairableCount, Is.Zero);
            Assert.That((healthy with { Survey = false }).SurveyMissingKeyCount, Is.Null);
            Assert.That((healthy with { Findings = [Finding(TreeOrphanedLeafDisposition.RefusedBlockingState)] }).SurveyMissingKeyCount, Is.Null);
            Assert.That((healthy with { Gaps = [new TreeOrphanedLeafGap { ShardIndex = 1 }] }).SurveyMissingKeyCount, Is.Null);
            Assert.That(report.SurveyMissingKeyCount, Is.EqualTo(3));
            Assert.That(report.OrphanedLeafCount, Is.EqualTo(2));
            Assert.That(report.RepairableCount, Is.EqualTo(1));
            Assert.That(report.RefusedCount, Is.EqualTo(1));
            Assert.That((report with { ResumeFrom = "next" }).IsComplete, Is.False);
            Assert.That((report with { ResumeFrom = "next" }).SurveyMissingKeyCount, Is.EqualTo(3));
        });
    }

    [Test]
    public void Repaired_is_not_a_refusal()
    {
        Assert.That(Finding(TreeOrphanedLeafDisposition.Repaired).IsRefusal, Is.False);
    }

    [Test]
    public void Repairable_is_not_a_refusal()
    {
        Assert.That(Finding(TreeOrphanedLeafDisposition.Repairable).IsRefusal, Is.False,
            "Repairable is the audit's verdict that the leaf would be repaired, not a refusal.");
    }

    [TestCase(TreeOrphanedLeafDisposition.RefusedUnverifiedKeys)]
    [TestCase(TreeOrphanedLeafDisposition.RefusedKeyCountExceeded)]
    [TestCase(TreeOrphanedLeafDisposition.RefusedBlockingState)]
    [TestCase(TreeOrphanedLeafDisposition.RefusedChainRace)]
    [TestCase(TreeOrphanedLeafDisposition.RefusedRoutingContradiction)]
    public void Every_refused_disposition_reports_a_refusal(TreeOrphanedLeafDisposition disposition)
    {
        Assert.That(Finding(disposition).IsRefusal, Is.True);
    }

    [Test]
    public void The_report_tallies_repairs_and_refusals()
    {
        var report = new TreeOrphanedLeafReport
        {
            TreeId = "orders",
            DryRun = false,
            LeavesWalked = 12,
            Findings = ImmutableArray.Create(
                Finding(TreeOrphanedLeafDisposition.Repaired),
                Finding(TreeOrphanedLeafDisposition.Repaired),
                Finding(TreeOrphanedLeafDisposition.RefusedChainRace)),
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.RepairedCount, Is.EqualTo(2));
            Assert.That(report.RefusedCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_clean_report_tallies_zero_on_both_counts()
    {
        var report = new TreeOrphanedLeafReport { TreeId = "orders", DryRun = true, LeavesWalked = 40 };

        Assert.Multiple(() =>
        {
            Assert.That(report.Findings, Is.Empty);
            Assert.That(report.RepairedCount, Is.Zero);
            Assert.That(report.RefusedCount, Is.Zero,
                "A tree with no orphans is a verdict in its own right, not a failure.");
            Assert.That(report.VerdictComplete, Is.True,
                "and it is only a verdict at all because nothing went unexamined.");
        });
    }

    [Test]
    public void A_report_carrying_a_gap_is_not_a_complete_verdict()
    {
        // The false-green this whole model exists to make impossible: a
        // report assembled from shards that declined, or from chains severed
        // part-way across the keyspace, carries zero findings by
        // construction. An operator reading RefusedCount and RepairedCount
        // alone sees a clean tree (issue 3301).
        var report = new TreeOrphanedLeafReport
        {
            TreeId = "orders",
            DryRun = true,
            LeavesWalked = 40,
            Gaps = ImmutableArray.Create(new TreeOrphanedLeafGap
            {
                ShardIndex = 17,
                Reason = TreeOrphanedLeafGapReason.ChainTruncatedUnrecoverable,
                LeafId = "leaf-9",
            }),
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.Findings, Is.Empty);
            Assert.That(report.RepairedCount, Is.Zero);
            Assert.That(report.RefusedCount, Is.Zero);
            Assert.That(report.VerdictComplete, Is.False,
                "an empty finding list says nothing about the part of the tree the pass never reached");
        });
    }

    [Test]
    public void Every_core_gap_reason_has_a_mirror_with_the_same_ordinal()
    {
        // The control-API mirror is mapped from the core enum by a plain
        // cast, so a reason added on one side and not the other would be
        // reported to an operator as whichever reason happens to share its
        // ordinal.
        var mirrored = Enum.GetValues<TreeOrphanedLeafGapReason>()
            .Select(r => ((int)r, r.ToString()))
            .ToArray();

        var core = Enum.GetValues<Orleans.Lattice.OrphanedLeafAuditGapReason>()
            .Select(r => ((int)r, r.ToString()))
            .ToArray();

        Assert.That(mirrored, Is.EqualTo(core));
    }

    [Test]
    public void An_audit_report_tallies_no_repairs_even_when_leaves_are_repairable()
    {
        var report = new TreeOrphanedLeafReport
        {
            TreeId = "orders",
            DryRun = true,
            LeavesWalked = 9,
            Findings = ImmutableArray.Create(Finding(TreeOrphanedLeafDisposition.Repairable)),
        };

        Assert.Multiple(() =>
        {
            Assert.That(report.DryRun, Is.True);
            Assert.That(report.RepairedCount, Is.Zero, "The audit changes nothing, so nothing is repaired.");
            Assert.That(report.RefusedCount, Is.Zero);
        });
    }
}
