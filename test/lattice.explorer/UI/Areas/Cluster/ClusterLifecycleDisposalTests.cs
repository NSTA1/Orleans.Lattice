using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Lifecycle reads and mutations cannot revive or notify a departed page.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterLifecycleDisposalTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string Address = "/cluster/trees/orders?tab=lifecycle";

    [Test]
    public async Task Initial_status_answered_after_leaving_does_not_start_purge_polling()
    {
        var reply = new TaskCompletionSource<TreeDeletionStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(reply.Task);
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Admin.Received(1).GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(() => reply.SetResult(Deletion(purging: true)));
        await AssertQuietAsync();

        Assert.That(Time.ArmedTimers, Is.Zero, "a late initial answer must not create a new poller");
    }

    [Test]
    public async Task Purge_poll_answered_after_leaving_does_not_announce_completion()
    {
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Deletion(purging: true));
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));
        var reply = new TaskCompletionSource<TreeDeletionStatus>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(_ =>
        {
            started.SetResult();
            return reply.Task;
        });
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(() => reply.SetResult(Deletion() with { PurgeComplete = true }));
        await AssertQuietAsync();

        Assert.That(Toasts, Is.Empty);
        Assert.That(Time.ArmedTimers, Is.Zero);
    }

    [TestCase("delete", false)]
    [TestCase("delete", true)]
    [TestCase("recover", false)]
    [TestCase("recover", true)]
    [TestCase("purge", false)]
    [TestCase("purge", true)]
    [TestCase("alias", false)]
    [TestCase("alias", true)]
    public async Task Mutation_answered_after_leaving_invalidates_lists_but_does_not_notify_the_page(string verb, bool fault)
    {
        UseTrees(Tree(TreeId), Tree("target"));
        var initial = Deletion() with { IsDeleted = verb is "recover" or "purge", CanRecover = true };
        Admin.GetTreeDeletionStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(initial);
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task WaitAsync()
        {
            started.SetResult();
            await reply.Task;
            if (fault)
            {
                throw new TimeoutException("Late lifecycle failure.");
            }
        }

        Admin.DeleteTreeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Deletion();
        });
        Admin.RecoverTreeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Deletion();
        });
        Admin.PurgeTreeAsync(TreeId, true, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Deletion(purging: true);
        });
        Admin.SetTreeAliasAsync(TreeId, "target", Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return new TreeAliasResolution { TreeId = TreeId, PhysicalTreeId = "physical-target", IsAliased = true };
        });
        var cut = RenderAt(Address);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Is.Empty));
        if (verb == "alias")
        {
            cut.Find("form[aria-label='Set alias'] input").Input("target");
            cut.Find("form[aria-label='Set alias']").Submit();
        }
        else
        {
            Button(cut, verb switch
            {
                "delete" => "Delete tree...",
                "recover" => "Recover tree...",
                _ => "Purge now...",
            }).Click();
        }

        var probe = await TreeListProbe.PrimeAsync(Services);
        var configReads = ConfigReads();
        cut.WaitForElement(".lt-confirm input").Input(TreeId);
        var submit = cut.Find("form.lt-confirm").SubmitAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await DisposeComponentsAsync();
        await cut.InvokeAsync(reply.SetResult);
        await submit;
        await AssertQuietAsync();

        Assert.Multiple(() =>
        {
            Assert.That(Toasts, Is.Empty, "the departed page cannot post a success or failure on its replacement");
            Assert.That(Time.ArmedTimers, Is.Zero);
            Assert.That(ConfigReads(), Is.EqualTo(configReads), "OperationSettled must not reload a departed parent");
        });
        if (fault)
        {
            await probe.AssertNeitherForgottenAsync();
        }
        else
        {
            await probe.AssertBothForgottenAsync();
        }
    }

    private int ConfigReads() => Admin.ReceivedCalls().Count(call =>
        call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetTreeConfigAsync));

    private async Task AssertQuietAsync() =>
        Assert.That(await TestPoll.TryUntilAsync(
            () => Time.ArmedTimers != 0 || Toasts.Count != 0,
            TimeSpan.FromMilliseconds(250)), Is.False, "late replies must not publish after disposal");

    private static TreeDeletionStatus Deletion(bool purging = false) => new()
    {
        TreeId = TreeId, PurgeInProgress = purging,
    };
}
