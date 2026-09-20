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
        });
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
