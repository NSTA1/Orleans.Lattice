using Bunit;
using NSubstitute;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Late operation replies must not revive a page or notify its replacement.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOperationDisposalTests : ClusterTestContext
{
    private const string TreeId = "orders";

    [TestCase("reshard")]
    [TestCase("resize")]
    [TestCase("snapshot")]
    public async Task Initial_status_answered_after_leaving_does_not_create_a_poller(string operation)
    {
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        SetStatusRead(operation, async () =>
        {
            await reply.Task;
            returned.SetResult();
        }, running: true);
        var cut = RenderAt($"/cluster/trees/{TreeId}/{operation}");
        cut.WaitUntil(() => Assert.That(StatusReads(operation), Is.EqualTo(1)));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(reply.SetResult);
        await returned.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await AssertNoLatePublicationAsync(cut);

        Assert.That(Time.ArmedTimers, Is.Zero, "the lazy poller must not be constructed after Dispose");
        Assert.That(Toasts, Is.Empty);
    }

    [TestCase("reshard", false)]
    [TestCase("reshard", true)]
    [TestCase("resize", false)]
    [TestCase("resize", true)]
    [TestCase("snapshot", false)]
    [TestCase("snapshot", true)]
    [TestCase("undo", false)]
    [TestCase("undo", true)]
    public async Task Mutation_answered_after_leaving_does_not_toast_or_poll(string operation, bool fault)
    {
        UseTrees(Tree(TreeId));
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task WaitAsync()
        {
            started.TrySetResult();
            await reply.Task;
            if (fault)
            {
                throw new TimeoutException("Late operation failure.");
            }
        }

        SetStatusRead(operation, () => Task.CompletedTask, running: false);
        Admin.ReshardTreeAsync(TreeId, 8, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Reshard(true);
        });
        Admin.ResizeTreeAsync(TreeId, 256, 32, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Resize(true);
        });
        Admin.SnapshotTreeAsync(TreeId, "copy", TreeSnapshotMode.Online, null, null, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Snapshot(true);
        });
        Admin.UndoTreeResizeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
        {
            await WaitAsync();
            return Resize(false) with { UndoRequested = true };
        });
        var cut = RenderAt($"/cluster/trees/{TreeId}/{(operation == "undo" ? "resize" : operation)}");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Is.Empty));
        if (operation == "undo")
        {
            Button(cut, "Undo the last resize...").Click();
        }
        else
        {
            var inputs = cut.FindAll("form input");
            inputs[0].Input(operation == "snapshot" ? "copy" : operation == "resize" ? "256" : "8");
            if (operation == "resize")
            {
                inputs[1].Input("32");
            }

            cut.Find("form").Submit();
            cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-cluster-review"), Has.Count.EqualTo(1)));
            Button(cut, operation == "snapshot" ? "Snapshot..." : operation == "resize" ? "Resize..." : "Reshard...").Click();
        }

        var probe = await TreeListProbe.PrimeAsync(Services);
        cut.WaitForElement(".lt-confirm input").Input(TreeId);
        var submit = cut.Find("form.lt-confirm").SubmitAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await DisposeComponentsAsync();
        await cut.InvokeAsync(reply.SetResult);
        await submit;
        await AssertNoLatePublicationAsync(cut);
        if (fault)
        {
            await probe.AssertNeitherForgottenAsync();
        }
        else
        {
            await probe.AssertBothForgottenAsync();
        }

        Assert.Multiple(() =>
        {
            Assert.That(Toasts, Is.Empty, "a reply belongs to the departed page, not the replacement");
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    [TestCase("reshard")]
    [TestCase("resize")]
    [TestCase("snapshot")]
    public async Task Poll_answered_after_leaving_does_not_announce_completion(string operation)
    {
        SetStatusRead(operation, () => Task.CompletedTask, running: true);
        var cut = RenderAt($"/cluster/trees/{TreeId}/{operation}");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));
        var reply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        SetStatusRead(operation, async () =>
        {
            started.SetResult();
            await reply.Task;
            returned.SetResult();
        }, running: false);
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await DisposeComponentsAsync();
        await cut.InvokeAsync(reply.SetResult);
        await returned.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await AssertNoLatePublicationAsync(cut);

        Assert.That(Toasts, Is.Empty, "a cancelled poll cannot announce completion on another page");
        Assert.That(Time.ArmedTimers, Is.Zero);
    }

    private void SetStatusRead(string operation, Func<Task> wait, bool running)
    {
        switch (operation)
        {
            case "reshard":
                Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
                {
                    await wait();
                    return Reshard(running);
                });
                break;
            case "resize":
            case "undo":
                Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
                {
                    await wait();
                    return Resize(running);
                });
                break;
            case "snapshot":
                Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(async _ =>
                {
                    await wait();
                    return Snapshot(running);
                });
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }
    }

    private int StatusReads(string operation) => Admin.ReceivedCalls().Count(call =>
        call.GetMethodInfo().Name == (operation switch
        {
            "reshard" => nameof(ILatticeTreeAdmin.GetReshardStatusAsync),
            "resize" => nameof(ILatticeTreeAdmin.GetResizeStatusAsync),
            _ => nameof(ILatticeTreeAdmin.GetSnapshotStatusAsync),
        }));

    private async Task AssertNoLatePublicationAsync(IRenderedComponent<ClusterPage> cut)
    {
        await cut.InvokeAsync(() => Task.CompletedTask);
        Assert.That(await TestPoll.TryUntilAsync(
            () => Time.ArmedTimers != 0 || Toasts.Count != 0,
            TimeSpan.FromMilliseconds(250)), Is.False, "a departed page must stay quiet");
    }

    private static TreeReshardStatus Reshard(bool running) => new()
    {
        TreeId = TreeId, InProgress = running, CurrentPhysicalShardCount = 4, VirtualShardCount = 4096,
    };

    private static TreeResizeStatus Resize(bool running) => new()
    {
        TreeId = TreeId, InProgress = running, CurrentMaxLeafKeys = 128, CurrentMaxInternalChildren = 64,
    };

    private static TreeSnapshotStatus Snapshot(bool running) => new() { TreeId = TreeId, InProgress = running };
}
