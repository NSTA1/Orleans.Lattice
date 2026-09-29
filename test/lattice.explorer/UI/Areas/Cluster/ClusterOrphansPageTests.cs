using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster/orphans</c>: a pass is driven batch by batch to completion, its
/// verdict distinguishes clean, found and not-judged, the survey is a separate
/// read, and repair needs the TreeLifecycle grant, a typed confirmation, and is
/// always followed by a fresh audit.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOrphansPageTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string Address = "/cluster/orphans?tree=orders";

    [Test]
    public void An_audit_drives_every_batch_and_a_complete_empty_pass_is_a_clean_bill_of_health()
    {
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).Returns(Report(leaves: 40, resume: "r1"));
        Admin.AuditOrphanedLeavesAsync(TreeId, "r1", Arg.Any<CancellationToken>()).Returns(Report(leaves: 20));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Clean").And.Contain("rules them out as the cause of an unbounded WAL"));
            Assert.That(cut.Markup, Does.Contain("Audit: 2 batches, 60 leaves walked."));
            Assert.That(HasButton(cut, "Repair..."), Is.False);
        });
    }

    [Test]
    public void A_region_the_pass_could_not_judge_is_not_a_clean_bill_of_health()
    {
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).Returns(Report(gaps: [new TreeOrphanedLeafGap { ShardIndex = 2, Reason = TreeOrphanedLeafGapReason.ShardSplitInProgress, KeyHint = "k100" }]));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Not judged").And.Contain("that is not a clean bill of health"));
            Assert.That(cut.Find(".lt-cluster-list").TextContent, Does.Contain("Shard 2: a shard split was in progress").And.Contain("near k100"));
        });
    }

    [Test]
    public void Repairable_orphans_are_repaired_behind_a_confirmation_and_the_tree_is_audited_again()
    {
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).Returns(
            Report(findings: [Finding("leaf-1", TreeOrphanedLeafDisposition.Repairable), Finding("leaf-2", TreeOrphanedLeafDisposition.RefusedUnverifiedKeys)]),
            Report(findings: [Finding("leaf-2", TreeOrphanedLeafDisposition.RefusedUnverifiedKeys)]));
        Admin.RepairOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>())
            .Returns(Report(findings: [Finding("leaf-1", TreeOrphanedLeafDisposition.Repaired), Finding("leaf-2", TreeOrphanedLeafDisposition.RefusedUnverifiedKeys)]));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("2 orphaned leaves, 1 repairable.")));
        Assert.That(cut.FindAll("tbody tr")[1].TextContent, Does.Contain("Refused: a key is not readable elsewhere"));

        Button(cut, "Repair...").Click();
        Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("cannot be undone").And.Contain("audits again"));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Repair finished: 1 leaf unspliced. Auditing again."));
            Assert.That(cut.Markup, Does.Contain("Audit: 1 batch,"));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void The_survey_is_its_own_read_and_repair_is_hidden_without_the_lifecycle_grant()
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
        Admin.DidNotReceive().AuditOrphanedLeavesAsync(Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public void A_repair_whose_reply_is_lost_tells_the_operator_to_audit_rather_than_trust_it()
    {
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).Returns(Report(findings: [Finding("leaf-1", TreeOrphanedLeafDisposition.Repairable)]));
        Admin.RepairOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        Button(cut, "Audit").Click();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Repair..."), Is.True));

        Button(cut, "Repair...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("audit again to learn the tree's true state")));
    }

    [Test]
    public void A_running_pass_can_be_stopped_and_says_what_its_findings_mean()
    {
        var pending = new TaskCompletionSource<TreeOrphanedLeafReport>();
        Admin.AuditOrphanedLeavesAsync(TreeId, null, Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                call.ArgAt<CancellationToken>(2).Register(() => pending.TrySetCanceled(call.ArgAt<CancellationToken>(2)));
                return pending.Task;
            });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));

        Button(cut, "Audit").Click();
        Button(cut, "Stop").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("describe only the part of the tree the pass reached")));
    }

    [Test]
    public void Without_a_tree_or_read_authority_it_says_so()
    {
        var none = RenderAt("/cluster/orphans");
        Assert.That(none.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a tree"));
        none.Find("form").Submit();
        Assert.That(none.Find(".lt-field__error").TextContent, Does.Contain("Name the tree to audit."));
        none.Find("form input").Input("orders");
        none.Find("form").Submit();
        Assert.That(Navigation.Uri, Does.EndWith("/cluster/orphans?tree=orders"));

        Granted = Grants.Admin;
        var denied = RenderAt(Address);
        denied.WaitUntil(() => Assert.That(denied.Markup, Does.Contain("needs whole-tree read authority")));
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
