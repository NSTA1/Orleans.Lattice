using Bunit;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// The staged, resumable status pages of the long-running operations (E15):
/// reshard, resize and its undo, and snapshot - each staged through a review and a
/// typed confirmation, each hidden without its grant, and each followed on the
/// circuit's clock while it runs and picked up again on arrival.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOperationPagesTests : ClusterTestContext
{
    private const string TreeId = "a/crm/orders";

    [Test]
    public async Task A_reshard_is_staged_reviewed_confirmed_and_followed_to_completion()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(Reshard(false, 4), Reshard(true, 4, 8), Reshard(false, 8));
        Admin.ReshardTreeAsync(TreeId, 8, Arg.Any<CancellationToken>()).Returns(Reshard(true, 4, 8));
        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No reshard is running.")));
        Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Reshard a/crm/orders"));

        Stage(cut, "4");
        // #4254: the field error renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("The tree already has 4 physical shards")));
        Stage(cut, "5000");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("at most 4096")));
        Stage(cut, "eight");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("whole number")));
        Stage(cut, "8");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("from 4 physical shards to 8 physical shards")));

        Button(cut, "Reshard...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Resharding to 8 physical shards.").And.Contain("You can leave and come back")));
        Assert.That(Time.ArmedTimers, Is.EqualTo(1), "the page follows the running reshard");

        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == nameof(ILatticeTreeAdmin.GetReshardStatusAsync)), Is.EqualTo(2)));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers == 1, TimeSpan.FromSeconds(10)), Is.True, "a still-running reshard is asked again");
        await cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("No reshard is running."));
            Assert.That(Toasts, Does.Contain("Reshard complete: 8 physical shards."));
            Assert.That(Time.ArmedTimers, Is.Zero, "a finished reshard is no longer followed");
        });
    }

    [Test]
    public async Task Arriving_at_a_running_reshard_resumes_following_it_and_leaving_stops()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Reshard(true, 4, 16));

        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("Resharding to 16 physical shards."));
            Assert.That(cut.FindAll("form"), Is.Empty, "no second reshard is staged over a running one");
            Assert.That(Time.ArmedTimers, Is.EqualTo(1));
        });

        await DisposeComponentsAsync();
        Assert.That(Time.ArmedTimers, Is.Zero);
    }

    [Test]
    public void Resharding_is_hidden_without_the_lifecycle_grant_and_a_refusal_is_a_toast()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Reshard(false, 4));
        Granted = Grants.Read | Grants.Admin;
        var hidden = RenderAt("/cluster/trees/a/crm/orders/reshard");
        hidden.WaitUntil(() => Assert.That(hidden.Markup, Does.Contain("Resharding needs the TreeLifecycle grant")));
        Assert.That(hidden.FindAll("form"), Is.Empty);

        Granted = Grants.All;
        Admin.ReshardTreeAsync(TreeId, 8, Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException("A resize is in flight."));
        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(1)));
        Stage(cut, "8");
        // #4254: the review renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("from 4 physical shards to 8 physical shards")));
        Button(cut, "Reshard...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("A resize is in flight.")));
    }

    [Test]
    public void A_resize_is_staged_and_confirmed_and_can_be_undone_behind_its_own_confirmation()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        Admin.ResizeTreeAsync(TreeId, 256, 32, Arg.Any<CancellationToken>()).Returns(Resize(true, 256, 32));
        Admin.UndoTreeResizeAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(false));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No resize is running.")));

        cut.FindAll("form input")[0].Input("1");
        cut.FindAll("form input")[1].Input("2");
        cut.Find("form").Submit();
        // #4254: the field errors render after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-field__error"), Has.Count.EqualTo(2)));

        cut.FindAll("form input")[0].Input("256");
        cut.FindAll("form input")[1].Input("32");
        cut.Find("form").Submit();
        // #4254: the review renders after an async continuation.
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Resize..."), Is.True));
        Button(cut, "Resize...").Click();
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Resizing to 256 keys per leaf and 32 children per node.")));

        Button(cut, "Undo the last resize...").Click();
        // #4254: the confirmation dialog renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("deletes the resized copy")));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("Resize undone."));
            Assert.That(Time.ArmedTimers, Is.Zero);
        });
    }

    [Test]
    public void A_running_resize_is_followed_and_resizing_is_hidden_without_the_grant()
    {
        Admin.GetResizeStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(Resize(true, 256, 32), Resize(false));
        var cut = RenderAt("/cluster/trees/a/crm/orders/resize");
        cut.WaitUntil(() => Assert.That(Time.ArmedTimers, Is.EqualTo(1)));

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));
        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Resize complete.")));

        Granted = Grants.Read;
        var hidden = RenderAt("/cluster/trees/a/crm/orders/resize");
        hidden.WaitUntil(() => Assert.That(hidden.Markup, Does.Contain("Resizing needs the TreeLifecycle grant")));
        Assert.That(HasButton(hidden, "Undo the last resize..."), Is.False);
    }

    [Test]
    public void A_snapshot_is_staged_with_its_mode_and_stating_offline_stops_the_source()
    {
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeSnapshotStatus { TreeId = TreeId });
        Admin.SnapshotTreeAsync(TreeId, "a/crm/orders-copy", TreeSnapshotMode.Offline, 64, null, Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId, InProgress = true, RequestedDestinationTreeId = "a/crm/orders-copy", RequestedMode = TreeSnapshotMode.Offline });
        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("No snapshot is running.")));

        cut.Find("form").Submit();
        // #4254: the field error renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the new tree")));

        cut.FindAll("form input")[0].Input(TreeId);
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("cannot copy a tree into itself")));

        cut.FindAll("form input")[0].Input("a/crm/orders-copy");
        cut.Find("form select").Change("Offline");
        cut.FindAll("form input")[1].Input("1");
        cut.Find("form").Submit();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-field__error").Select(error => error.TextContent), Has.Some.Contains("Sizing must be whole numbers")));

        cut.FindAll("form input")[1].Input("64");
        cut.Find("form").Submit();
        // #4254: the review renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-review").TextContent, Does.Contain("offline: the source stops serving")));
        Button(cut, "Snapshot...").Click();
        // #4254: the confirmation dialog renders after an async continuation.
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("stops serving reads and writes")));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Copying into a/crm/orders-copy, offline.")));
    }

    [Test]
    public void A_snapshot_needs_admin_and_a_running_one_is_followed_to_completion()
    {
        Admin.GetSnapshotStatusAsync(TreeId, Arg.Any<CancellationToken>())
            .Returns(new TreeSnapshotStatus { TreeId = TreeId, InProgress = true, RequestedDestinationTreeId = "copy" }, new TreeSnapshotStatus { TreeId = TreeId });
        Granted = Grants.Read;
        var cut = RenderAt("/cluster/trees/a/crm/orders/snapshot");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("A snapshot needs whole-tree admin authority")));

        cut.InvokeAsync(() => Time.Advance(ClusterStatusPoller.Interval));

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("Snapshot complete.")));
    }

    [Test]
    public void A_status_the_caller_may_not_read_says_so()
    {
        Admin.GetReshardStatusAsync(TreeId, Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("no"));

        var cut = RenderAt("/cluster/trees/a/crm/orders/reshard");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-error").TextContent, Is.EqualTo("You do not have permission to do this.")));
    }

    private static void Stage(IRenderedComponent<Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages.ClusterPage> cut, string target)
    {
        cut.Find("form input").Input(target);
        cut.Find("form").Submit();
    }

    private static TreeReshardStatus Reshard(bool running, int current, int? requested = null) =>
        new() { TreeId = TreeId, InProgress = running, CurrentPhysicalShardCount = current, RequestedShardCount = requested, VirtualShardCount = 4096 };

    private static TreeResizeStatus Resize(bool running, int? leaf = null, int? children = null) =>
        new() { TreeId = TreeId, InProgress = running, CurrentMaxLeafKeys = 128, CurrentMaxInternalChildren = 64, RequestedMaxLeafKeys = leaf, RequestedMaxInternalChildren = children };
}
