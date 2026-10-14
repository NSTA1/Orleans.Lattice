using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>A stopped survey cannot consume late batches or change the next survey.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOrphanCancellationTests : ClusterTestContext
{
    [Test]
    public async Task A_stopped_survey_discards_a_late_batch_and_requests_no_continuation()
    {
        var pending = new TaskCompletionSource<TreeOrphanedLeafReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.SurveyOrphanedLeavesAsync("orders", null, Arg.Any<CancellationToken>()).Returns(pending.Task);
        Admin.SurveyOrphanedLeavesAsync("orders", "next", Arg.Any<CancellationToken>())
            .Returns(new TreeOrphanedLeafReport { TreeId = "orders", LeavesWalked = 999 });
        var cut = RenderAt("/cluster/orphans?tree=orders");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        cut.Find("input[type=checkbox]").Change(true);
        var run = Button(cut, "Audit").ClickAsync(new());
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Stop"), Is.True));

        await Button(cut, "Stop").ClickAsync(new());
        pending.SetResult(new TreeOrphanedLeafReport { TreeId = "orders", LeavesWalked = 123, ResumeFrom = "next" });
        await run;

        Assert.That(cut.Markup, Does.Not.Contain("123").And.Not.Contain("999"));
        Assert.That(cut.Find(".lt-cluster-error").TextContent, Does.Contain("Stopped"));
        await Admin.DidNotReceive().SurveyOrphanedLeavesAsync("orders", "next", Arg.Any<CancellationToken>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_stopped_survey_cannot_replace_the_next_surveys_error(bool cancellation)
    {
        var pending = new TaskCompletionSource<TreeOrphanedLeafReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reads = 0;
        Admin.SurveyOrphanedLeavesAsync("orders", null, Arg.Any<CancellationToken>())
            .Returns(_ => ++reads == 1 ? pending.Task : Task.FromException<TreeOrphanedLeafReport>(new InvalidOperationException("current survey failed")));
        var cut = RenderAt("/cluster/orphans?tree=orders");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Audit"), Is.True));
        cut.Find("input[type=checkbox]").Change(true);
        var previous = Button(cut, "Audit").ClickAsync(new());
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Stop"), Is.True));
        await Button(cut, "Stop").ClickAsync(new());
        await Button(cut, "Audit again").ClickAsync(new());
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Is.EqualTo("current survey failed")));

        if (cancellation)
        {
            pending.SetCanceled();
        }
        else
        {
            pending.SetException(new InvalidOperationException("previous survey failed"));
        }

        await previous;
        Assert.That(cut.Find(".lt-cluster-error").TextContent, Is.EqualTo("current survey failed"));
    }
}
