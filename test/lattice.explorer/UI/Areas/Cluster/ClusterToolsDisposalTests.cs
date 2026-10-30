using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>An abandoned compaction must not publish its reply into the next page's toast service.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterToolsDisposalTests : ClusterTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Compaction_reply_after_leaving_posts_no_toast(bool fails)
    {
        var reply = new TaskCompletionSource<TreeCompactionTriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken issued = default;
        Admin.TriggerShardCompactionAsync("orders", 0, Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                issued = call.Arg<CancellationToken>();
                started.SetResult();
                return reply.Task;
            });
        var cut = RenderAt("/cluster/trees/orders/tools");
        cut.WaitForElement("form[aria-label='Trigger compaction'] input").Input("0");
        cut.Find("form[aria-label='Trigger compaction']").Submit();
        cut.WaitForElement(".lt-confirm input").Input("orders");
        var submit = cut.Find("form.lt-confirm").SubmitAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await DisposeComponentsAsync();
        Assert.That(issued.IsCancellationRequested, Is.True);
        await cut.InvokeAsync(() =>
        {
            if (fails)
            {
                reply.SetException(new InvalidOperationException("The old compaction failed."));
            }
            else
            {
                reply.SetResult(new TreeCompactionTriggerResult { TreeId = "orders", Accepted = true });
            }
        });
        await submit;

        Assert.That(Toasts, Is.Empty, "the next page must not receive the abandoned tool's failure");
        Assert.That(await LeftPage.FaultAsync(Renderer), Is.Null);
    }
}
