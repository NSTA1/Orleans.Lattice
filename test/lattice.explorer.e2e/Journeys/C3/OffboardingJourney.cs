using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>
/// Journey C3 (#5391), offboarding and lifecycle: priya moves to another team, and olivia
/// removes her from the stewards while her session is still open and watches her rights
/// withdraw; then month-end housekeeping undoes, in the order a real estate would, what
/// Acts I and II built - the task board is disabled and uninstalled, the stewards' rule and
/// group are deleted, replication and schedules are switched off, the old baseline is
/// deleted, a storage-side deletion is reconciled, the snapshot trees are purged, and the
/// status view is rebuilt, stopped and dropped.
/// </summary>
/// <remarks>
/// Every step is driven through the Explorer. The one off-UI action, C3.7's deletion of
/// <c>lines-full</c>'s manifest from the backup store, is the harness acting as the storage
/// incident it models (<see cref="Estate.JourneyEstate.DeleteBackupArtifactAsync"/>).
/// </remarks>
[Journey(C3Step.JourneyId)]
[JourneyChapter("C3", After = new[] { "U3", "A5", "A6", "A10", "A12", "C1" })]
public sealed partial class OffboardingJourney : JourneyChapter
{
    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(5);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var task = U3Task(context);

        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        await using var priya = await estate.SignInAsAsync(Personas.Priya, options: new PersonaSessionOptions { Path = EntryPath });
        await using var priyasBoard = await estate.SignInAsAsync(Personas.Priya);
        await priyasBoard.Shell.GotoAsync(BoardPath(task.Id));

        await RemoveMemberWhileSessionIsOpenAsync(olivia, priya, priyasBoard);
        await ConfirmRoleWithdrawnAsync(olivia, priya, priyasBoard, task.Title);
        await DisableAndUninstallTaskBoardAsync(context, olivia);
        await DeleteRuleAndGroupAsync(olivia);
        await DisableReplicationAsync(context, olivia);
        await CancelSchedulesAndDeleteBaselineAsync(olivia);
        await ReconcileStorageDeletionAsync(context, olivia);
        await PurgeSnapshotTreesAsync(olivia);
        await RebuildStopAndDropViewAsync(olivia);
    }

    /// <summary>The order priya has open when the journey starts: one the line feed never rewrites.</summary>
    private static string EntryPath => $"/data/{Names.Trees.FactoryOrders}?key={Uri.EscapeDataString(Names.Orders.Order42)}";

    private static string BoardPath(string taskId) => $"/apps/{Names.Apps.TaskBoard}/window/tasks/{taskId}";

    /// <summary>The U3 task priya has open on her board: the first of her team's cards still to do.</summary>
    private static (string Id, string Title) U3Task(ChapterContext context)
    {
        var ids = context.Recall(StoryKeys.TaskBoardTasks);
        var task = Contracts.Contoso.U3Contract.Tasks.First(candidate => candidate.Column == "todo" && ids.Contains(candidate.Id));
        return (task.Id, task.Title);
    }

    /// <summary>
    /// C3.1: olivia removes priya from <c>data-stewards</c>. priya's editor, opened before the
    /// change, is refused on save, and on reload the tree offers her no write affordance.
    /// </summary>
    private static async Task RemoveMemberWhileSessionIsOpenAsync(PersonaSession olivia, PersonaSession priya, PersonaSession priyasBoard)
    {
        using var step = C3Step.Begin(1, "olivia removes priya from data-stewards while priya's session is open");
        var entry = priya.Shell.Content.Locator(".lt-data-entry");
        var edit = entry.GetByRole(AriaRole.Button, new() { Name = "Edit", Exact = true });
        await Expect(edit).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(priya.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "New entry...", Exact = true })).ToBeVisibleAsync();
        await step.ShotAsync(priya, "priya-can-edit", "priya, a data steward, has order/000042 open with Edit, Delete... and New entry... offered");

        var board = new TaskBoardWindow(priyasBoard.Page);
        await board.WaitForBoardAsync();
        await board.ExpectRoleAsync(editor: true);
        await step.ShotAsync(priyasBoard, "priya-board-editor", "In another window priya has the task board open as an editor, a task's detail shown");

        await edit.ClickAsync();
        var editor = priya.Page.GetByRole(AriaRole.Dialog, new() { Name = "Edit entry" });
        await Expect(editor.GetByRole(AriaRole.Textbox, new() { Name = "Value" })).ToBeVisibleAsync();
        await step.ShotAsync(priya, "priya-editor-open", "priya opens the editor on the order before anything changes");

        await olivia.Shell.GotoAsync($"/access/groups/{Names.Groups.DataStewards}");
        await olivia.Shell.ExpectHeadingAsync(Names.Groups.DataStewards);
        var members = olivia.Shell.Content.GetByRole(AriaRole.Table, new() { Name = "Direct members" });
        await Expect(members).ToContainTextAsync(Personas.Priya.Id);
        await step.ShotAsync(olivia, "group-with-priya", "olivia opens data-stewards: priya is a direct member");

        await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Remove {Personas.Priya.Id}", Exact = true }).ClickAsync();
        var confirm = new ConfirmDialog(olivia.Page, "Remove this member");
        await confirm.TypeAsync(Personas.Priya.Id);
        await step.ShotAsync(olivia, "confirm-remove", "Remove this member: olivia types priya to confirm");
        await confirm.ConfirmAsync("Remove member");
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"{Personas.Priya.Id} removed from {Names.Groups.DataStewards}." })).ToHaveCountAsync(1);
        await Expect(members.GetByText(Personas.Priya.Id, new() { Exact = true })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "priya-removed", "priya is removed from data-stewards");

        var save = editor.GetByRole(AriaRole.Button, new() { Name = "Save", Exact = true });
        var refused = editor.Locator(".lt-data-editor__error");
        await MembershipPropagation.EnforcedAsync("priya's save", async () =>
        {
            if (!await editor.IsVisibleAsync())
            {
                await edit.ClickAsync();
                await Expect(editor.GetByRole(AriaRole.Textbox, new() { Name = "Value" })).ToBeVisibleAsync();
            }

            await save.ClickAsync();
            await Expect(refused.Or(priya.Shell.Toasts.Filter(new() { HasText = "saved." }))).ToBeVisibleAsync(new() { Timeout = 30_000 });
            return await refused.IsVisibleAsync();
        });
        await step.ShotAsync(priya, "priya-save-refused", "priya's next save, from the editor she already had open, is refused");

        await priya.Page.Keyboard.PressAsync("Escape");
        var writeAffordances = priya.Shell.Content.Locator("[data-lt-command^=\"data.\"][data-lt-command$=\"-entry\"]");
        await MembershipPropagation.EnforcedAsync("priya's write affordances after a reload", async () =>
        {
            await priya.Shell.GotoAsync(EntryPath);
            await Expect(priya.Shell.Heading).ToBeVisibleAsync();
            await Expect(edit.Or(priya.Shell.Content.Locator(".lt-empty").First)).ToBeVisibleAsync(new() { Timeout = 60_000 });
            return await writeAffordances.CountAsync() == 0 && await edit.CountAsync() == 0;
        });
        await step.ShotAsync(priya, "priya-affordances-gone", "On refresh the tree offers priya no write affordance");
        Coverage.Hit("use-case:access/remove-member-live");
        step.Complete();
    }

    /// <summary>
    /// C3.2: priya's open task board loses its authority on her next change, and the board is
    /// no longer among her apps; the task board's own page shows olivia the role-holding notice,
    /// which Check again reads afresh; Explain answers priya writing <c>factory-orders</c>: denied.
    /// </summary>
    private static async Task ConfirmRoleWithdrawnAsync(PersonaSession olivia, PersonaSession priya, PersonaSession priyasBoard, string taskTitle)
    {
        using var step = C3Step.Begin(2, "priya no longer holds the task board's editor role; Explain says she may not write the orders");
        var board = new TaskBoardWindow(priyasBoard.Page);
        await Expect(board.Board.Locator("#tb-detail-title")).ToHaveTextAsync(taskTitle);
        await MembershipPropagation.EnforcedAsync("priya's move on her open board", board.MoveIsWithdrawnAsync);
        await Expect(board.Closed).ToContainTextAsync("This app is no longer open to you");
        await Expect(board.Closed).Not.ToContainTextAsync("upgraded");
        await step.ShotAsync(priyasBoard, "priya-board-withdrawn", "priya moves a task on the board she had open: the frame closes, saying the app is no longer open to her");

        var yourApps = priya.Shell.Content;
        await MembershipPropagation.EnforcedAsync("priya's Your apps", async () =>
        {
            await priya.Shell.GotoAsync("/apps");
            await priya.Shell.ExpectHeadingAsync("Apps");
            await Expect(yourApps.Locator(".lt-empty").Or(yourApps.GetByRole(AriaRole.Table)).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
            return await yourApps.GetByText(TaskBoardName, new() { Exact = true }).CountAsync() == 0;
        });
        await step.ShotAsync(priya, "priya-apps", "Your apps no longer offers priya the task board");

        await olivia.Shell.GotoAsync($"/apps/{Names.Apps.TaskBoard}");
        var notice = olivia.Shell.Content.Locator("[data-lt-holding]");
        await Expect(notice).ToContainTextAsync(Names.Groups.DataStewards, new() { Timeout = 60_000 });
        await notice.GetByRole(AriaRole.Button, new() { Name = "Check again", Exact = true }).ClickAsync();
        await Expect(notice).ToContainTextAsync($"editor to {Names.Groups.DataStewards}");
        await step.ShotAsync(olivia, "role-holding-notice", "The task board's page: its editor role is held only through data-stewards; Check again reads the bindings afresh");
        Coverage.Hit("use-case:apps/role-holding-notice");

        await olivia.Shell.GotoAsync($"/access/explain?subject={Personas.Priya.Id}&kind=user&operation=write&tree={Names.Trees.FactoryOrders}");
        var verdict = olivia.Shell.Content.Locator("#lt-access-verdict");
        await Expect(verdict).ToHaveTextAsync("Denied", new() { Timeout = 60_000 });
        await Expect(olivia.Shell.Content.GetByText("No groups.")).ToBeVisibleAsync();
        await step.ShotAsync(olivia, "explain-denied", "Explain priya writing factory-orders: denied, and she is in no group");
        step.Complete();
    }
}
