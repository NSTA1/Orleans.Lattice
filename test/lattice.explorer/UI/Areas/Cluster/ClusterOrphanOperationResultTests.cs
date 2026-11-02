using System.Globalization;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

[TestFixture]
public sealed class ClusterOrphanOperationResultTests
{
    [Test]
    public void From_null_status_is_rejected()
    {
        Assert.That(() => ClusterOrphanOperationResult.From(null!), Throws.ArgumentNullException);
    }

    [TestCase(TreeAdminOperationKinds.OrphanedLeavesAudit, false)]
    [TestCase(TreeAdminOperationKinds.OrphanedLeavesRepair, true)]
    [TestCase("unknown", false)]
    public void From_maps_every_total_and_identifies_only_the_exact_repair_kind(string kind, bool repair)
    {
        var result = ClusterOrphanOperationResult.From(Status(kind, LatticeOperationState.Succeeded,
            result: new Dictionary<string, string>
            {
                [TreeAdminOperationResultKeys.LeavesWalked] = "9223372036854775807",
                [TreeAdminOperationResultKeys.OrphanedLeaves] = "2",
                [TreeAdminOperationResultKeys.Repaired] = "3",
                [TreeAdminOperationResultKeys.Repairable] = "4",
                [TreeAdminOperationResultKeys.Refused] = "5",
                [TreeAdminOperationResultKeys.Gaps] = "6",
            }));

        Assert.That(result, Is.EqualTo(new ClusterOrphanOperationResult(repair, long.MaxValue, 2, 3, 4, 5, 6)));
    }

    [TestCase("")]
    [TestCase("-1")]
    [TestCase("+1")]
    [TestCase(" 1")]
    [TestCase("1 ")]
    [TestCase("1,000")]
    [TestCase("1.0")]
    [TestCase("9223372036854775808")]
    [TestCase("not-a-number")]
    public void From_invalid_totals_default_independently_to_zero(string text)
    {
        var result = ClusterOrphanOperationResult.From(Status(TreeAdminOperationKinds.OrphanedLeavesAudit,
            LatticeOperationState.Succeeded, result: new Dictionary<string, string>
            {
                [TreeAdminOperationResultKeys.OrphanedLeaves] = text,
                [TreeAdminOperationResultKeys.LeavesWalked] = "17",
            }));

        Assert.Multiple(() =>
        {
            Assert.That(result.LeavesWalked, Is.EqualTo(17));
            Assert.That(result.Orphaned, Is.Zero);
            Assert.That(result.Repaired, Is.Zero);
            Assert.That(result.Repairable, Is.Zero);
            Assert.That(result.Refused, Is.Zero);
            Assert.That(result.Gaps, Is.Zero);
        });
    }

    [Test]
    public void From_totals_are_parsed_using_invariant_culture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("fr-FR");
            var result = ClusterOrphanOperationResult.From(Status(TreeAdminOperationKinds.OrphanedLeavesAudit,
                LatticeOperationState.Succeeded, result: new Dictionary<string, string>
                {
                    [TreeAdminOperationResultKeys.LeavesWalked] = "1234",
                }));

            Assert.That(result.LeavesWalked, Is.EqualTo(1234));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [TestCase(0, 0, true)]
    [TestCase(1, 0, false)]
    [TestCase(0, 1, false)]
    [TestCase(1, 1, false)]
    public void IsClean_requires_both_no_orphans_and_no_unjudged_regions(long orphaned, long gaps, bool clean)
    {
        var result = new ClusterOrphanOperationResult(false, 10, orphaned, 0, 0, 0, gaps);

        Assert.That(result.IsClean, Is.EqualTo(clean));
    }

    [TestCase(1, 0, 0, "Repair finished: 1 leaf unspliced. Auditing again.")]
    [TestCase(0, 1, 0, "Repair finished: 0 leaves unspliced, 1 refused. Auditing again.")]
    [TestCase(2, 3, 1, "Repair finished: 2 leaves unspliced, 3 refused, 1 region not judged. Auditing again.")]
    [TestCase(0, 0, 2, "Repair finished: 0 leaves unspliced, 2 regions not judged. Auditing again.")]
    public void The_repair_toast_reports_refused_leaves_and_unjudged_regions(long repaired, long refused, long gaps, string expected)
    {
        var totals = new ClusterOrphanOperationResult(true, 10, repaired + refused, repaired, 0, refused, gaps);

        Assert.That(ClusterOrphansPage.RepairFinishedMessage(totals), Is.EqualTo(expected));
    }
}
