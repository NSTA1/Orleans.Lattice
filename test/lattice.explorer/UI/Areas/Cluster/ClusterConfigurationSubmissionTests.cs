using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Configuration and retention share one in-flight write, including queued form submissions.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterConfigurationSubmissionTests : ClusterTestContext
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Save_while_either_form_is_pending_does_not_issue_another_write(bool retention, bool secondRetention)
    {
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.GetHistoryRetentionAsync("orders", Arg.Any<CancellationToken>())
            .Returns(new TreeHistoryRetention { TreeId = "orders" });
        Admin.SetTreeConfigAsync("orders", Arg.Any<TreeConfigurationUpdate>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                started.TrySetResult();
                await reply.Task;
                return new TreeConfigurationReport { TreeId = "orders", Exists = true, PublishEvents = false };
            });
        Admin.SetHistoryRetentionAsync("orders", Arg.Any<TreeHistoryRetentionMode?>(), Arg.Any<TimeSpan?>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                started.TrySetResult();
                await reply.Task;
                return new TreeHistoryRetention { TreeId = "orders" };
            });
        var cut = RenderAt("/cluster/trees/orders?tab=configuration");
        cut.WaitForElement("form[aria-label='Change configuration'] select").Change("off");
        var first = Form(retention).SubmitAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var second = Form(secondRetention).SubmitAsync();
        await cut.InvokeAsync(reply.SetResult);
        await Task.WhenAll(first, second);

        Assert.That(Writes(), Is.EqualTo(1), "already queued submit events must share the same in-flight guard");
        Assert.That(Toasts, Has.Count.EqualTo(1));

        await Form(true).SubmitAsync();
        Assert.That(Writes(), Is.EqualTo(2), "the guard must be released after the first save");

        AngleSharp.Dom.IElement Form(bool history) => cut.Find(history
            ? "form[aria-label='Change history retention']"
            : "form[aria-label='Change configuration']");
    }

    private int Writes() => Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name is
        nameof(ILatticeTreeAdmin.SetTreeConfigAsync) or nameof(ILatticeTreeAdmin.SetHistoryRetentionAsync));
}
