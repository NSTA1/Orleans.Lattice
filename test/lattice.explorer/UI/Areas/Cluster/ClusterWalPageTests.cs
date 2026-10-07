using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Testing;
using static Orleans.Lattice.Explorer.Tests.UI.Operations.TreeAdminOperationScript;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster/wal</c>: the placement audit, the "Plan WAL move..." command's
/// visible control and its dialog, a plan at its own resumable address, and the
/// execute and reclaim verbs behind typed confirmations and the TreeLifecycle
/// grant. A move runs on the cluster as a tracked operation (#4124) whose copy,
/// verify and flip the page follows, picks up again, and can stop.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterWalPageTests : ClusterTestContext
{
    private const string TreeId = "orders";
    private const string PreviousTree = "ledger";

    [SetUp]
    public void Placement()
    {
        Admin.AuditWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(new TreeWalPlacementAudit
        {
            TreeId = TreeId, Version = 5, PartitionCount = 2, AllResolvableOnThisSilo = false,
            Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-a", ResolvableOnThisSilo = true }, new TreeWalPartitionPlacement { Partition = 1, ProviderKey = "blob-x" }],
            KnownProviderKeys = ["blob-a", "blob-b"],
        });
        Admin.PlanWalMoveAsync(TreeId, 1, "blob-b", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", EntriesToCopy = 1200,
            SourceLowestOffset = 10, SourceHighestOffset = 1210, TargetResolvableOnThisSilo = true, PlacementVersion = 5,
        });
    }

    [Test]
    public void Without_a_tree_it_asks_for_one_and_the_audit_form_navigates()
    {
        UseTrees(Tree("orders"));
        var cut = RenderAt("/cluster/wal");

        Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Choose a tree"));
        cut.Find("form[aria-label='Choose a tree']").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Name the tree to audit.")));

        cut.Find("form[aria-label='Choose a tree'] input").Input("orders");
        cut.Find("form[aria-label='Choose a tree']").Submit();
        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/wal?tree=orders")));
    }

    [Test]
    public void The_audit_names_drift_in_words_and_lists_each_partition()
    {
        var cut = RenderAt("/cluster/wal?tree=orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("Drift").And.Contain("2 partitions, placement version 5").And.Contain("blob-a, blob-b"));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
        });
    }

    // The page's WAL reclamation section finishes its own read on another thread, and
    // its render can hold the renderer while the test fills the dialog in. Each step
    // therefore finds its element and fires its event in one turn of the renderer and
    // waits for the handler, so no step acts on a DOM a pending render has replaced
    // (#4630).
    [Test]
    public async Task The_plan_command_has_a_visible_control_whose_dialog_plans_at_a_resumable_address()
    {
        UseTrees(Tree("orders"));
        var cut = RenderAt("/cluster/wal?tree=orders");
        var command = Services.GetServices<IExplorerArea>().OfType<ClusterArea>().Single().Commands.Single(candidate => candidate.Id == ClusterArea.PlanWalMoveCommandId);

        ExplorerCommandControls.AssertVisibleControl(cut, command);
        await cut.FireAsync(page => page.Find($"[data-lt-command=\"{command.Id}\"]").ClickAsync(new MouseEventArgs()));
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog input").GetAttribute("value"), Is.EqualTo("orders")));
        Assert.That(cut.Markup, Does.Contain("Known keys: blob-a, blob-b."));

        await cut.FireAsync(page => page.Find(".lt-dialog form").SubmitAsync());
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-dialog .lt-field__error"), Has.Count.EqualTo(2)));

        await cut.FireAsync(page => page.FindAll(".lt-dialog input")[1].InputAsync(new ChangeEventArgs { Value = "1" }));
        await cut.FireAsync(page => page.FindAll(".lt-dialog input")[2].InputAsync(new ChangeEventArgs { Value = "blob-b" }));
        await cut.FireAsync(page => page.Find(".lt-dialog form").SubmitAsync());

        cut.WaitUntil(() => Assert.That(Navigation.Uri, Does.EndWith("/cluster/wal?tree=orders&partition=1&target=blob-b")));
    }

    [Test]
    public void The_palette_opens_the_plan_dialog_on_the_page()
    {
        var cut = RenderAt("/cluster/wal");
        var signals = Services.GetRequiredService<ClusterCommandSignals>();

        cut.InvokeAsync(() => signals.RequestAsync(ClusterArea.PlanWalMoveCommandId).AsTask());

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog h2").TextContent, Is.EqualTo("Plan a WAL move")));
    }

    [Test]
    public void A_plan_is_moved_behind_a_confirmation_and_its_retained_source_reclaimed_behind_another()
    {
        Tracked.Script(
            TreeAdminOperationKinds.WalMove,
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 400, 1200, TreeAdminOperationUnits.Entries),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Verifying),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Succeeded, "Completed", result: MovedResult()));
        Admin.ReclaimMovedWalSourceAsync(TreeId, 1, "blob-x", Arg.Any<CancellationToken>()).Returns(new TreeWalMoveReceipt
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed,
        });
        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("1,200").And.Contain("10 to 1,210")));
        Assert.That(HasButton(cut, "Reclaim the source..."), Is.False, "nothing to reclaim before a move");

        Button(cut, "Move partition...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("Quiesces partition 1 briefly")));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Started, Has.Count.EqualTo(1));
            Assert.That(TreeAdminOperationIds.Matches(Tracked.Started[0], TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target(TreeId, 1)), Is.True);
            Assert.That(cut.Find("[data-lt-cluster='move-progress']").TextContent, Does.Contain("Copying").And.Contain("400 of 1,200 entries"));
            Assert.That(HasButton(cut, "Move partition..."), Is.True);
            Assert.That(Button(cut, "Move partition...").HasAttribute("disabled"), Is.True);
        });

        Time.Advance(ClusterStatusPoller.Interval);
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cluster='move-progress']").TextContent, Does.Contain("Verifying")));

        Time.Advance(ClusterStatusPoller.Interval);
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("offsets 10 to 1,210 copied to blob-b").And.Contain("The source is retained until you reclaim it."));
            Assert.That(Toasts, Does.Contain("WAL partition moved."));
            Assert.That(cut.FindAll("[data-lt-cluster='move-progress']"), Is.Empty);
        });
        Assert.That(Admin.ReceivedCalls().Count(call => call.GetMethodInfo().Name == "ExecuteWalMoveAsync"), Is.Zero, "The blocking verb is never called.");

        Button(cut, "Reclaim the source...").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-confirm__consequence").TextContent, Does.Contain("blob-x").And.Contain("can no longer be reverted")));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-cluster-result").TextContent, Does.Contain("was reclaimed"));
            Assert.That(Toasts, Does.Contain("WAL source reclaimed."));
        });
    }

    [Test]
    public void A_finished_move_and_a_reclaim_each_re_read_the_placement_so_the_audit_is_never_stale()
    {
        Admin.AuditWalPlacementAsync(TreeId, Arg.Any<CancellationToken>()).Returns(
            Audit(5, "blob-x"),
            Audit(6, "blob-b"),
            Audit(7, "blob-b"));
        Tracked.Script(
            TreeAdminOperationKinds.WalMove,
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 400, 1200, TreeAdminOperationUnits.Entries),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Succeeded, "Completed", result: MovedResult()));
        Admin.ReclaimMovedWalSourceAsync(TreeId, 1, "blob-x", Arg.Any<CancellationToken>()).Returns(new TreeWalMoveReceipt
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed,
        });
        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("placement version 5")));

        Button(cut, "Move partition...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Not.Empty));
        ConfirmTyping(cut, TreeId);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[data-lt-cluster='move-progress']"), Is.Not.Empty));
        Time.Advance(ClusterStatusPoller.Interval);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("WAL partition moved."));
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("placement version 6").And.Not.Contain("placement version 5"));
            Assert.That(cut.Find("tbody").TextContent, Does.Contain("blob-b").And.Not.Contain("blob-x"));
        });

        Button(cut, "Reclaim the source...").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-confirm"), Is.Not.Empty));
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("WAL source reclaimed."));
            Assert.That(cut.Find(".lt-cluster-stage").TextContent, Does.Contain("placement version 7"));
        });
    }

    private static TreeWalPlacementAudit Audit(long version, string partitionOne) => new()
    {
        TreeId = TreeId, Version = version, PartitionCount = 2, AllResolvableOnThisSilo = false,
        Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-a", ResolvableOnThisSilo = true }, new TreeWalPartitionPlacement { Partition = 1, ProviderKey = partitionOne }],
        KnownProviderKeys = ["blob-a", "blob-b"],
    };

    [Test]
    public void A_move_running_from_another_tab_is_followed_and_can_be_stopped_before_its_flip()
    {
        var operationId = TreeAdminOperationIds.New(TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target(TreeId, 1));
        Tracked.Running(
            operationId,
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 4, 20, TreeAdminOperationUnits.Entries),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 8, 20, TreeAdminOperationUnits.Entries),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Cancelled, TreeAdminOperationPhases.Copying, 8, 20, TreeAdminOperationUnits.Entries));

        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");
        cut.WaitUntil(() => Assert.That(cut.Find("[data-lt-cluster='move-progress']").TextContent, Does.Contain("4 of 20 entries")));

        cut.Find("[data-lt-cluster='stop-move']").Click();
        cut.WaitUntil(() =>
        {
            Assert.That(Tracked.Cancelled, Is.EqualTo(new[] { operationId }));
            Assert.That(cut.Find("[data-lt-cluster='move-progress']").TextContent, Does.Contain("Cancelling"));
        });

        Time.Advance(ClusterStatusPoller.Interval);
        cut.WaitUntil(() =>
        {
            Assert.That(Toasts, Does.Contain("The move was stopped before its flip: the partition stays on its source."));
            Assert.That(cut.Find("[data-lt-cluster='move-progress']").TextContent, Does.Contain("Stopped during Copying, after 8 of 20 entries."));
            Assert.That(Button(cut, "Move partition...").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void A_move_on_another_partition_is_not_followed_here()
    {
        Tracked.Running(
            TreeAdminOperationIds.New(TreeAdminOperationKinds.WalMove, TreeAdminOperationIds.Target(TreeId, 0)),
            Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Running, TreeAdminOperationPhases.Copying, 1, 2, TreeAdminOperationUnits.Entries));

        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");

        cut.WaitUntil(() => Assert.That(Button(cut, "Move partition...").HasAttribute("disabled"), Is.False));
        Assert.That(cut.FindAll("[data-lt-cluster='move-progress']"), Is.Empty);
    }

    [Test]
    public void A_finished_moves_result_reads_back_as_its_receipt()
    {
        var receipt = ClusterWalPage.ReceiptFrom(Status(TreeAdminOperationKinds.WalMove, LatticeOperationState.Succeeded, "Completed", result: MovedResult()));

        Assert.That(receipt, Is.EqualTo(new TreeWalMoveReceipt
        {
            TreeId = TreeId, Partition = 1, FromProviderKey = "blob-x", ToProviderKey = "blob-b", PreviousPlacementVersion = 5, NewPlacementVersion = 6,
            CopiedFromOffset = 10, CopiedThroughOffset = 1210, SourceHighestOffset = 1210, TargetHighestOffset = 1210, SourceRetained = true, Outcome = TreeWalMoveOutcome.Moved,
        }));
    }

    [Test]
    public void A_partition_already_at_its_target_offers_to_reclaim_a_chosen_orphaned_source()
    {
        Admin.PlanWalMoveAsync(TreeId, 0, "blob-a", Arg.Any<CancellationToken>()).Returns(new TreeWalMovePlan
        {
            TreeId = TreeId, Partition = 0, FromProviderKey = "blob-a", ToProviderKey = "blob-a", AlreadyAtTarget = true,
        });
        Admin.ReclaimMovedWalSourceAsync(TreeId, 0, "blob-b", Arg.Any<CancellationToken>())
            .Returns(new TreeWalMoveReceipt { TreeId = TreeId, FromProviderKey = "blob-b", Outcome = TreeWalMoveOutcome.SourceReclaimed });
        var cut = RenderAt("/cluster/wal?tree=orders&partition=0&target=blob-a");
        cut.WaitUntil(() => Assert.That(cut.FindAll("select"), Has.Count.EqualTo(1)));

        Assert.That(HasButton(cut, "Move partition..."), Is.False);
        cut.Find("select").Change("blob-b");
        // The change handler can re-render asynchronously under load before the
        // reclaim button settles, so wait rather than assuming it is ready (#4254).
        cut.WaitUntil(() => HasButton(cut, "Reclaim the source..."));
        Button(cut, "Reclaim the source...").Click();
        ConfirmTyping(cut, TreeId);

        cut.WaitUntil(() => Assert.That(Toasts, Does.Contain("WAL source reclaimed.")));
    }

    [Test]
    public void Moving_needs_the_lifecycle_grant()
    {
        Granted = Grants.Read;

        var cut = RenderAt("/cluster/wal?tree=orders&partition=1&target=blob-b");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("needs the TreeLifecycle grant"));
            Assert.That(HasButton(cut, "Move partition..."), Is.False);
        });
    }

    [Test]
    public void At_compact_the_plan_dialog_is_a_sheet_and_the_partitions_are_rows()
    {
        var cut = RenderAt("/cluster/wal?tree=orders", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(2)));

        Button(cut, "Plan a move...").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog").ClassList, Does.Contain("lt-dialog--end")));
    }

    // The page stays mounted while its address moves to another tree (#4512), so a read of
    // the previous tree can answer after the current tree's. Neither its audit, its plan nor
    // its fault may then stand in for the current tree's.
    [Test]
    public async Task A_late_audit_for_the_previous_tree_does_not_replace_the_current_trees()
    {
        var previous = new TaskCompletionSource<TreeWalPlacementAudit>();
        Admin.AuditWalPlacementAsync(PreviousTree, Arg.Any<CancellationToken>()).Returns(previous.Task);
        var cut = RenderSupersededBy(TreeId);

        previous.SetResult(new TreeWalPlacementAudit
        {
            TreeId = PreviousTree, Version = 9, PartitionCount = 1, AllResolvableOnThisSilo = true,
            Partitions = [new TreeWalPartitionPlacement { Partition = 0, ProviderKey = "blob-z", ResolvableOnThisSilo = true }],
            KnownProviderKeys = ["blob-z"],
        });

        Assert.That(await EverShows(cut, "blob-z"), Is.False, "the previous tree's audit is never shown");
        Assert.Multiple(() =>
        {
            Assert.That(Stage(cut), Does.Contain("Drift").And.Contain("2 partitions, placement version 5"));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task A_late_fault_for_the_previous_tree_does_not_replace_the_current_trees_audit()
    {
        var previous = new TaskCompletionSource<TreeWalPlacementAudit>();
        Admin.AuditWalPlacementAsync(PreviousTree, Arg.Any<CancellationToken>()).Returns(previous.Task);
        var cut = RenderSupersededBy(TreeId);

        previous.SetException(new TimeoutException("The previous tree's audit timed out."));

        Assert.That(await EverShows(cut, "audit timed out"), Is.False, "the previous tree's fault is never shown");
        Assert.That(Stage(cut), Does.Contain("placement version 5"));
    }

    [Test]
    public async Task A_late_plan_for_the_previous_tree_does_not_replace_the_current_trees()
    {
        var previous = new TaskCompletionSource<TreeWalMovePlan>();
        Admin.PlanWalMoveAsync(PreviousTree, 1, "blob-b", Arg.Any<CancellationToken>()).Returns(previous.Task);
        var cut = RenderSupersededBy(TreeId, partition: 1, target: "blob-b");
        cut.WaitUntil(() => Assert.That(Plan(cut), Does.Contain("1,200")));

        previous.SetResult(new TreeWalMovePlan
        {
            TreeId = PreviousTree, Partition = 1, FromProviderKey = "blob-y", ToProviderKey = "blob-b", EntriesToCopy = 987654,
            SourceLowestOffset = 3, SourceHighestOffset = 987657, TargetResolvableOnThisSilo = true, PlacementVersion = 9,
        });

        Assert.That(await EverShows(cut, "987,654"), Is.False, "the previous tree's plan is never shown");
        Assert.That(Plan(cut), Does.Contain("1,200").And.Contain("blob-x"));
    }

    /// <summary>
    /// Renders the page for <see cref="PreviousTree"/>, whose reads are left as the test
    /// arranged them, then moves it to <paramref name="current"/> and waits for that tree's audit.
    /// </summary>
    private IRenderedComponent<ClusterWalPage> RenderSupersededBy(string current, int? partition = null, string? target = null)
    {
        var cut = Render<ClusterWalPage>(parameters => parameters
            .Add(page => page.TreeId, PreviousTree)
            .Add(page => page.Partition, partition)
            .Add(page => page.Target, target));
        cut.Render(parameters => parameters.Add(page => page.TreeId, current));
        cut.WaitUntil(() => Assert.That(Stage(cut), Does.Contain("placement version 5")));
        return cut;
    }

    /// <summary>
    /// Whether <paramref name="text"/> shows within a second of the previous tree's late answer.
    /// The page's waiting load resumes on a later turn of the renderer, which no single
    /// barrier orders against, so a stale answer is looked for over a window: one that is
    /// applied shows within tens of milliseconds.
    /// </summary>
    private static Task<bool> EverShows(IRenderedComponent<ClusterWalPage> cut, string text) =>
        TestPoll.TryUntilAsync(() => cut.Markup.Contains(text, StringComparison.Ordinal), TimeSpan.FromSeconds(1));

    private static string Stage(IRenderedComponent<ClusterWalPage> cut) =>
        cut.Find("[aria-labelledby='lt-cluster-wal-audit'] .lt-cluster-stage").TextContent;

    private static string Plan(IRenderedComponent<ClusterWalPage> cut) =>
        cut.Find("[aria-labelledby='lt-cluster-wal-plan']").TextContent;
    private static Dictionary<string, string> MovedResult() => new()
    {
        [TreeAdminOperationResultKeys.TreeId] = TreeId,
        [TreeAdminOperationResultKeys.Partition] = "1",
        [TreeAdminOperationResultKeys.FromProviderKey] = "blob-x",
        [TreeAdminOperationResultKeys.ToProviderKey] = "blob-b",
        [TreeAdminOperationResultKeys.Outcome] = nameof(TreeWalMoveOutcome.Moved),
        [TreeAdminOperationResultKeys.PreviousPlacementVersion] = "5",
        [TreeAdminOperationResultKeys.NewPlacementVersion] = "6",
        [TreeAdminOperationResultKeys.CopiedFromOffset] = "10",
        [TreeAdminOperationResultKeys.CopiedThroughOffset] = "1210",
        [TreeAdminOperationResultKeys.SourceHighestOffset] = "1210",
        [TreeAdminOperationResultKeys.TargetHighestOffset] = "1210",
        [TreeAdminOperationResultKeys.SourceRetained] = "true",
    };
}
