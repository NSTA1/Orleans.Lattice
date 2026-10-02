using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Operations;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster/orphans</c>: an audit or repair runs on the cluster as a tracked
/// operation (#4124) whose progress the page follows, picks up again and can stop;
/// its verdict distinguishes clean, found and not-judged, and the per-leaf findings
/// are read on request batch by batch. The survey is a separate batch-driven read,
/// and repair needs the TreeLifecycle grant, a typed confirmation, and is always
/// followed by a fresh audit.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOrphansPageTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string Address = "/cluster/orphans?tree=orders";

    [Test]
    public void An_audit_runs_on_the_cluster_and_a_clean_result_is_a_clean_bill_of_health()
    {
        Tracked.Script(
            TreeAdminOperationKinds.OrphanedLeavesAudit,
            Status(TreeAdminOperationKinds.OrphanedLeavesAudit, LatticeOperationState.Running, TreeAdminOperationPhases.Walking, 2, 4, TreeAdminOperationUnits.Shards),
            Audited(leaves: 60));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(TreeAdminOperationIds.Matches(Tracked.Started.Single(), TreeAdminOperationKinds.OrphanedLeavesAudit, TreeId), Is.True);
            Assert.That(cut.Find("[data-lt-cluster='orphan-progress']").TextContent, Does.Contain("Walking").And.Contain("2 of 4 shards"));
            Assert.That(HasButton(cut, "Stop"), Is.True);
        });

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Clean").And.Contain("rules them out as the cause of an unbounded WAL"));
            Assert.That(cut.Markup, Does.Contain("Audit: 60 leaves walked."));
            Assert.That(HasButton(cut, "Repair..."), Is.False);
            Assert.That(HasButton(cut, "Show each leaf"), Is.False);
            Assert.That(cut.FindAll("[data-lt-cluster='orphan-progress']"), Is.Empty);
        });
        Admin.DidNotReceive().AuditOrphanedLeavesAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_region_the_pass_could_not_judge_is_not_a_clean_bill_of_health_and_each_leaf_is_read_on_request()
    {
        Tracked.Script(TreeAdminOperationKinds.OrphanedLeavesAudit, Audited(gaps: 1));
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).Returns(Report(gaps: [new TreeOrphanedLeafGap { ShardIndex = 2, Reason = TreeOrphanedLeafGapReason.ShardSplitInProgress, KeyHint = "k100" }]));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Not judged").And.Contain("1 region could not be judged: that is not a clean bill of health")));

        Button(cut, "Show each leaf").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Not judged"));
            Assert.That(cut.Find(".lt-cluster-list").TextContent, Does.Contain("Shard 2: a shard split was in progress").And.Contain("near k100"));
        });
    }

    [Test]
    public void Repairable_orphans_are_repaired_behind_a_confirmation_and_the_tree_is_audited_again()
    {
        Tracked.Script(TreeAdminOperationKinds.OrphanedLeavesAudit, Audited(orphaned: 2, repairable: 1, refused: 1));
        Tracked.Script(
            TreeAdminOperationKinds.OrphanedLeavesRepair,
            Status(TreeAdminOperationKinds.OrphanedLeavesRepair, LatticeOperationState.Running, TreeAdminOperationPhases.Walking, 1, 4, TreeAdminOperationUnits.Shards),
            Status(
                TreeAdminOperationKinds.OrphanedLeavesRepair,
                LatticeOperationState.Succeeded,
                "Completed",
                result: Totals(orphaned: 2, refused: 1, repaired: 1)));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("2 orphaned leaves, 1 repairable.")));

        Button(cut, "Repair...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("cannot be undone").And.Contain("audits again"));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cluster='orphan-progress']").TextContent, Does.Contain("1 of 4 shards")));

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Repair finished: 1 leaf unspliced. Auditing again."));
            Assert.That(Tracked.Started.Select(Kind), Is.EqualTo(new[] { "audit", "repair", "audit" }));
            Assert.That(cut.Markup, Does.Contain("Audit: 10 leaves walked."));
        });
    }

    [Test]
    public void The_survey_is_its_own_batch_driven_read_and_repair_is_hidden_without_the_lifecycle_grant()
    {
        Granted = Grants.Read;
        Admin.SurveyOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>())
            .Returns(Report(findings: [Finding("leaf-1", TreeOrphanedLeafDisposition.Repairable)]));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        cut.Find("input[type=checkbox]").Change(true);
        Button(cut, "Audit").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Survey: 1 batch"));
            Assert.That(HasButton(cut, "Repair..."), Is.False);
        });
        Assert.That(Tracked.Started, Is.Empty);
    }

    [Test]
    public void A_repair_whose_reply_is_lost_tells_the_operator_to_audit_rather_than_trust_it()
    {
        Tracked.Script(TreeAdminOperationKinds.OrphanedLeavesAudit, Audited(orphaned: 1, repairable: 1));
        Tracked.Operations.StartOrphanedLeavesRepairAsync(TreeId, Arg.Any<string?>(), Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Repair..."), Is.True));

        Button(cut, "Repair...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("audit again to learn the tree's true state")));
    }

    [Test]
    public void A_running_audit_can_be_stopped_and_says_what_its_totals_mean()
    {
        Tracked.Script(
            TreeAdminOperationKinds.OrphanedLeavesAudit,
            Status(TreeAdminOperationKinds.OrphanedLeavesAudit, LatticeOperationState.Running, TreeAdminOperationPhases.Walking, 1, 4, TreeAdminOperationUnits.Shards),
            Status(TreeAdminOperationKinds.OrphanedLeavesAudit, LatticeOperationState.Running, TreeAdminOperationPhases.Walking, 2, 4, TreeAdminOperationUnits.Shards),
            Status(TreeAdminOperationKinds.OrphanedLeavesAudit, LatticeOperationState.Cancelled, TreeAdminOperationPhases.Walking, 2, 4, TreeAdminOperationUnits.Shards));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Stop"), Is.True));

        Button(cut, "Stop").Click();
        cut.WaitUntil(() => Assert.That(Tracked.Cancelled, Is.EqualTo(Tracked.Started)));

        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("described only the part of the tree it reached"));
            Assert.That(cut.Find("[data-lt-cluster='orphan-progress']").TextContent, Does.Contain("Stopped during Walking, after 2 of 4 shards."));
            Assert.That(HasButton(cut, "Audit"), Is.True);
        });
    }

    [Test]
    public void A_running_survey_can_be_stopped_and_says_what_its_findings_mean()
    {
        var pending = new TaskCompletionSource<TreeOrphanedLeafReport>();
        Admin.SurveyOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                call.ArgAt<CancellationToken>(2).Register(() => pending.TrySetCanceled(call.ArgAt<CancellationToken>(2)));
                return pending.Task;
            });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        cut.Find("input[type=checkbox]").Change(true);
        Button(cut, "Audit").Click();
        Button(cut, "Stop").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("describe only the part of the tree the pass reached")));
    }

    [Test]
    public void A_repair_started_in_another_tab_is_followed_when_the_page_opens()
    {
        Tracked.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.OrphanedLeavesRepair, TreeId),
            Status(TreeAdminOperationKinds.OrphanedLeavesRepair, LatticeOperationState.Running, TreeAdminOperationPhases.Walking, 3, 4, TreeAdminOperationUnits.Shards));

        var cut = RenderAt(Address);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[data-lt-cluster='orphan-progress'] [role=progressbar]").GetAttribute("aria-label"), Is.EqualTo("Orphaned-leaf repair progress"));
            Assert.That(cut.Find("[data-lt-cluster='orphan-progress']").TextContent, Does.Contain("3 of 4 shards"));
            Assert.That(HasButton(cut, "Audit"), Is.False);
        });
    }
    [Test]
    public void Without_a_tree_or_read_authority_it_says_so()
    {
        UseTrees(Tree("orders"));
        var none = RenderAt("/cluster/orphans");
        Assert.That(none.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a tree"));
        none.Find("form").Submit();
        Assert.That(none.Find(".lt-field__error").TextContent, Does.Contain("Name the tree to audit."));
        none.Find("form input").Input("orders");
        none.Find("form").Submit();
        none.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/orphans?tree=orders")));

        Granted = Grants.Admin;
        var denied = RenderAt(Address);
        denied.WaitUntil(() => Assert.That(denied.Markup, Does.Contain("needs whole-tree read authority")));
    }

    private static string Kind(string operationId) => operationId.StartsWith("orphaned-leaves-repair-", StringComparison.Ordinal) ? "repair" : "audit";

    private static LatticeOperationStatus Audited(long leaves = 10, long orphaned = 0, long repairable = 0, long refused = 0, long gaps = 0) =>
        Status(TreeAdminOperationKinds.OrphanedLeavesAudit, LatticeOperationState.Succeeded, "Completed", result: Totals(leaves, orphaned, refused, gaps, repairable: repairable));

    private static Dictionary<string, string> Totals(long leaves = 10, long orphaned = 0, long refused = 0, long gaps = 0, long? repairable = null, long? repaired = null)
    {
        var totals = new Dictionary<string, string>
        {
            [TreeAdminOperationResultKeys.TreeId] = TreeId,
            [TreeAdminOperationResultKeys.LeavesWalked] = leaves.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [TreeAdminOperationResultKeys.OrphanedLeaves] = orphaned.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [TreeAdminOperationResultKeys.Refused] = refused.ToString(System.Globalization.CultureInfo.InvariantCulture),
            [TreeAdminOperationResultKeys.Gaps] = gaps.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (repairable is { } count)
        {
            totals[TreeAdminOperationResultKeys.Repairable] = count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (repaired is { } done)
        {
            totals[TreeAdminOperationResultKeys.Repaired] = done.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return totals;
    }
    private static TreeOrphanedLeafReport Report(int leaves = 10, string? resume = null, TreeOrphanedLeafFinding[]? findings = null, TreeOrphanedLeafGap[]? gaps = null) => new()
    {
        TreeId = TreeId,
        LeavesWalked = leaves,
        ResumeFrom = resume,
        Findings = [.. findings ?? []],
        Gaps = [.. gaps ?? []],
    };

    private static TreeOrphanedLeafFinding Finding(string leaf, TreeOrphanedLeafDisposition disposition) => new()
    {
        LeafId = leaf,
        ShardIndex = 1,
        KeyCount = 3,
        VerifiedKeyCount = 3,
        LowKeyInclusive = "a",
        Disposition = disposition,
    };
}
