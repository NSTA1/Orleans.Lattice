using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

public sealed partial class ClusterWalPageTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Reclaim_answering_after_navigation_does_not_change_the_new_plan(bool fail)
    {
        Admin.PlanWalMoveAsync(TreeId, 0, "blob-a", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 0, FromProviderKey = "blob-a", ToProviderKey = "blob-a", AlreadyAtTarget = true,
        });
        var receipt = new TaskCompletionSource<TreeWalMoveReceipt>(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.ReclaimMovedWalSourceAsync(TreeId, 0, "blob-b", Arg.Any<CancellationToken>()).Returns(receipt.Task);
        var cut = Render<ClusterWalPage>(parameters => parameters
            .Add(page => page.TreeId, TreeId)
            .Add(page => page.Partition, 0)
            .Add(page => page.Target, "blob-a"));
        cut.WaitForElement("select").Change("blob-b");
        cut.WaitUntil(() => HasButton(cut, "Reclaim the source..."));
        Button(cut, "Reclaim the source...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == "ReclaimMovedWalSourceAsync"), Is.EqualTo(1)));

        cut.Render(parameters => parameters
            .Add(page => page.Partition, 1)
            .Add(page => page.Target, "blob-b"));
        cut.WaitUntil(() => Assert.That(Plan(cut), Does.Contain("1,200")));
        if (fail)
        {
            receipt.SetException(new TimeoutException("old reclaim failed"));
        }
        else
        {
            receipt.SetResult(new TreeWalMoveReceipt
            {
                TreeId = TreeId, Partition = 0, FromProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed,
            });
        }

        Assert.That(await EverShows(cut, "was reclaimed"), Is.False);
        Assert.Multiple(() =>
        {
            Assert.That(Toasts, Is.Empty, "The old plan cannot announce success or failure on the new one.");
            Assert.That(Button(cut, "Move partition...").HasAttribute("disabled"), Is.False);
        });
    }
}
