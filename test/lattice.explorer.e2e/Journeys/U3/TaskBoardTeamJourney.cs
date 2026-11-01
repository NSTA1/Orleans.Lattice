using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U3;

/// <summary>
/// U3 (#5386): a team on the task board. priya, an editor, opens the task board A5 installed,
/// creates three tasks and moves one to Done, while dana watches them arrive live in Data and
/// the directory's App column names the board. dana and sam open the board as viewers, and a
/// write dana forces is refused by the app's roles. priya opens the board's standalone window,
/// leaves it, follows the legacy <c>/open</c> address and deep-links into one task. nina, who
/// holds no role, finds no app and no address under it; and each persona's Apps page shows the
/// roles they hold.
/// </summary>
/// <remarks>
/// Steps 1 and 2 run concurrently, as the story tells them: dana opens the tree's Keys tab
/// after priya's board has loaded and before priya's first write, so their checkpoints
/// interleave in the manifest. The only action outside the Explorer's own controls is the write
/// dana forces from the browser's developer console on the frame (step 3), through the same
/// bridge the board uses, under her own identity.
/// </remarks>
[JourneyChapter("U3", After = new[] { "A5" })]
public sealed class TaskBoardTeamJourney : JourneyChapter
{
    private const string Journey = "u3";

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var recorder = JourneyRecorder.For(Journey);
        var tree = context.Recall(StoryKeys.TaskBoardTree);
        var tasks = U3Contract.Tasks;
        var ids = new Dictionary<string, string>(StringComparer.Ordinal);

        await using var priya = await estate.SignInAsAsync(Personas.Priya);
        await using var dana = await estate.SignInAsAsync(Personas.Dana);
        var priyaApps = new AppsAreaPage(priya);
        var danaData = new DataTaskTreePage(dana, tree);
        TaskBoardWindow board = null!;
        var boardOpens = false;
        string[] keys = [];

        // U3.1, first half: priya opens the task board, and its frame loads.
        await StepAsync("U3.1", async () =>
        {
            await priyaApps.OpenAsync(expectTaskBoard: true);
            await priyaApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.EditorRole);
            await Capture(priya, "u3-1", "your-apps", "priya's Apps: the task board is among her apps, and she holds its editor role");

            board = await TaskBoardWindow.OpenNewWindowAsync(priya, estate, () => priyaApps.OpenTaskBoard.ClickAsync());
            boardOpens = await board.TryWaitForBoardAsync();
            if (!boardOpens)
            {
                // Every step below that needs the board loaded is not driven while this reproduces.
                // The wrapper asserts the refusal is the server's: if the workspace grants the board
                // while the window refuses it, the Explorer's frame gate is wrong, and this fails.
                await new KnownDefect("#5595", "The apps workspace answers the task board's editor with no enabled grant, so its window shows her a not-found page or 'This app is not available' though Your apps lists it").ReproducesAsync(() =>
                {
                    Assert.That(board.LastServerView, Is.Not.Null, "The cluster's answers were not read at the refusal.");
                    Assert.That(board.LastServerView!.WorkspaceRefuses, Is.True, $"The window refused the board ({board.LastRefusal}) while the workspace grants it: the Explorer's frame gate is wrong. {board.LastServerView}");
                    return Task.CompletedTask;
                });
                await board.CaptureAsync(recorder, "u3-1", "frame-refused", "The board's window refuses priya although Your apps lists the board: the apps workspace answers her with no enabled grant (#5595)");
                return;
            }

            await board.ExpectStandaloneAsync();
            await board.ExpectEditorAsync();
            await board.ExpectCountsAsync(0, 0, 0);
            await board.CaptureAsync(recorder, "u3-1", "frame-loaded", "Open puts the board in a window of its own: the frame loads, empty, with the editor's New task field");
            Coverage.Hit("route:/apps/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
            Coverage.Hit("subroute:apps/window");
            Coverage.Hit("subroute:apps/{app}/window");
        });

        // U3.2, first half: before priya writes, dana opens the board's tree in Data and keeps its Keys tab open.
        await StepAsync("U3.2", async () =>
        {
            await danaData.OpenDirectoryAsync();
            await danaData.ExpectAppColumnAsync();
            await Capture(dana, "u3-2", "directory-app-column", "dana's Data directory: the App column names task-board as the owner of a/task-board/tasks");
            Coverage.Hit("use-case:data/app-column");

            await danaData.OpenKeysAsync();
            await Expect(danaData.Keys).ToHaveCountAsync(0);
            await Capture(dana, "u3-2", "keys-watching", "dana keeps the tree's Keys tab open, live updates on: no task yet");
        });

        // While the board refuses (KnownDefect #5595 at U3.1), the steps that need it loaded are not driven.
        if (boardOpens)
        {
            // U3.1, second half: priya creates three tasks and moves one to Done.
            await StepAsync("U3.1", async () =>
            {
                ids[tasks[0].Title] = await board.AddTaskAsync(tasks[0].Title);
                await board.CaptureAsync(recorder, "u3-1", "task-added", $"priya adds \"{tasks[0].Title}\": it lands in To do, and the board says so");

                ids[tasks[1].Title] = await board.AddTaskAsync(tasks[1].Title);
                ids[tasks[2].Title] = await board.AddTaskAsync(tasks[2].Title);
                await board.ExpectCountsAsync(3, 0, 0);
                await board.CaptureAsync(recorder, "u3-1", "three-tasks", "Three tasks in To do");

                await board.SelectAsync("todo", tasks[2].Title);
                await Expect(board.Page).ToHaveURLAsync(new Regex(Regex.Escape(TaskBoardWindow.Address) + "/tasks/" + Regex.Escape(ids[tasks[2].Title]) + "$"));
                await board.CaptureAsync(recorder, "u3-1", "task-selected", "Selecting a card opens its detail with the Move controls, and writes its path into the window's address");

                await board.MoveSelectedAsync(tasks[2].Title, "done", "Done");
                await board.ExpectCountsAsync(2, 0, 1);
                await board.CaptureAsync(recorder, "u3-1", "moved-to-done", $"\"{tasks[2].Title}\" is moved to Done");
            });

            keys = tasks.Select(task => U3Contract.Key(ids[task.Title])).ToArray();

            // U3.2, second half: priya's tasks have arrived in dana's open Keys tab, without a reload.
            await StepAsync("U3.2", async () =>
            {
                await danaData.ExpectKeysAsync(keys);
                await danaData.ExpectSameDocumentAsync();
                await Capture(dana, "u3-2", "tasks-arrived-live", "In dana's Keys tab, still the same page, priya's three tasks have arrived live");
                Coverage.Hit("use-case:data/app-data-live");
            });
        }

        // U3.3: dana opens the task board; it is read-only for her, and a write she forces is refused.
        await StepAsync("U3.3", async () =>
        {
            var danaApps = new AppsAreaPage(dana);
            await danaApps.OpenAsync(expectTaskBoard: true);
            await danaApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.ViewerRole);
            await Capture(dana, "u3-3", "your-apps", "dana's Apps: the task board, where she holds the viewer role");
            if (!boardOpens)
            {
                return;
            }

            var danaBoard = await TaskBoardWindow.OpenNewWindowAsync(dana, estate, () => danaApps.OpenTaskBoard.ClickAsync());
            await danaBoard.WaitForBoardAsync();
            await danaBoard.ExpectReadOnlyAsync();
            await danaBoard.ExpectCountsAsync(2, 0, 1);
            await Expect(danaBoard.Card("done", tasks[2].Title)).ToHaveCountAsync(1);
            await danaBoard.CaptureAsync(recorder, "u3-3", "board-read-only", "dana sees priya's tasks; the board offers her no edit control and says her role does not let her change it");

            var outcome = await danaBoard.ForceWriteAsync("tasks/u3-dana-forced");
            Assert.That(outcome, Is.EqualTo("denied"), "A viewer's write through the frame's bridge must be refused by the app's roles.");
            await danaBoard.ExpectReadOnlyAsync();
            await danaBoard.CaptureAsync(recorder, "u3-3", "write-refused", "A write dana forces from the browser's console is refused (denied); the board stays read-only");

            await danaData.OpenDirectoryAsync();
            await danaData.OpenKeysAsync();
            await danaData.ExpectKeysAsync(keys);
            await Capture(dana, "u3-3", "tree-unchanged", "Back in Data, the tree still holds only priya's three tasks: the refused write left nothing");
            Coverage.Hit("use-case:apps/frame-write-refused");
        });

        // U3.4: sam opens it as a viewer: the same read-only view.
        await using var sam = await estate.SignInAsAsync(Personas.Sam);
        var samApps = new AppsAreaPage(sam);
        await StepAsync("U3.4", async () =>
        {
            await samApps.OpenAsync(expectTaskBoard: true);
            await samApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.ViewerRole);
            await Capture(sam, "u3-4", "your-apps", "sam's Apps: the task board, where he holds the viewer role");
            if (!boardOpens)
            {
                return;
            }

            var samBoard = await TaskBoardWindow.OpenNewWindowAsync(sam, estate, () => samApps.OpenTaskBoard.ClickAsync());
            await samBoard.WaitForBoardAsync();
            await samBoard.ExpectReadOnlyAsync();
            await samBoard.ExpectCountsAsync(2, 0, 1);
            await samBoard.CaptureAsync(recorder, "u3-4", "board-read-only", "sam sees the same read-only board");
        });

        if (boardOpens)
        {
            // U3.5: priya's standalone window, Leave app, the legacy /open address, and deep links.
            await StepAsync("U3.5", async () =>
            {
                var window = TaskBoardWindow.InSession(priya);
                await priya.Page.GotoAsync(TaskBoardWindow.Address.TrimStart('/'));
                await window.WaitForBoardAsync();
                await window.ExpectStandaloneAsync();
                await window.ExpectEditorAsync();
                await window.ExpectCountsAsync(2, 0, 1);
                await window.CaptureAsync(recorder, "u3-5", "standalone-window", "priya types the window's address: the board alone, with no header, address line or spine");

                await window.LeaveApp.ClickAsync();
                await priyaApps.ExpectTaskBoardOverviewAsync();
                await Capture(priya, "u3-5", "left-to-overview", "Leave app returns to the task board's overview in the full console");

                var oven = ids[tasks[1].Title];
                await priya.Page.GotoAsync($"apps/task-board/open/tasks/{oven}");
                await Expect(priya.Page).ToHaveURLAsync(new Regex(Regex.Escape(TaskBoardWindow.Address) + "/tasks/" + Regex.Escape(oven) + "$"));
                await window.WaitForBoardAsync();
                await window.ExpectDetailAsync(tasks[1].Title, U3Contract.Key(oven), "To do");
                await window.CaptureAsync(recorder, "u3-5", "legacy-open-redirected", "The legacy /open address redirects to the window, keeping its in-app path to the task");
                Coverage.Hit("subroute:apps/open");

                var conveyor = ids[tasks[2].Title];
                await priya.Page.GotoAsync($"apps/task-board/window?path=/tasks/{conveyor}");
                await window.WaitForBoardAsync();
                await window.ExpectDetailAsync(tasks[2].Title, U3Contract.Key(conveyor), "Done");
                await window.CaptureAsync(recorder, "u3-5", "deep-path", "A deep link with ?path= opens the window on one task");

                var press = ids[tasks[0].Title];
                await priya.Page.GotoAsync($"apps/task-board/window?path=/tasks/{press}&query=from=u3");
                await window.WaitForBoardAsync();
                await window.ExpectDetailAsync(tasks[0].Title, U3Contract.Key(press), "To do");
                await window.CaptureAsync(recorder, "u3-5", "deep-query", "A deep link with ?path= and ?query= opens the window on one task; the board reads its path past the query");
                Coverage.Hit("query:apps.query");
                Coverage.Hit("use-case:apps/open-frame");
            });
        }

        // U3.6: nina holds no role: no app is hers, and nothing lives at its addresses.
        await using var nina = await estate.SignInAsAsync(Personas.Nina);
        var ninaApps = new AppsAreaPage(nina);
        await StepAsync("U3.6", async () =>
        {
            await ninaApps.OpenAsync(expectTaskBoard: false, viaSpine: false);
            await ninaApps.ExpectNoAppsAsync();
            await Capture(nina, "u3-6", "no-apps", "nina's Apps: no enabled app grants her a role");

            await nina.Shell.GotoAsync("/apps/task-board");
            await nina.Shell.ExpectHeadingAsync(ShellPage.NotFoundHeading);
            await Capture(nina, "u3-6", "app-not-found", "The task board's address is, to nina, a page that does not exist");

            await nina.Page.GotoAsync(TaskBoardWindow.Address.TrimStart('/'));
            await Expect(nina.Page.GetByRole(AriaRole.Heading, new() { Name = ShellPage.NotFoundHeading, Exact = true })).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await Expect(nina.Page.Locator("section.appframe iframe")).ToHaveCountAsync(0);
            await Capture(nina, "u3-6", "window-not-found", "So is its window: no frame opens for her");
            Coverage.Hit("use-case:apps/no-role-no-access");
        });

        // U3.7: each persona's Apps page shows the roles they hold.
        await StepAsync("U3.7", async () =>
        {
            await priyaApps.OpenAsync(expectTaskBoard: true, viaSpine: false);
            await priyaApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.EditorRole);
            await Capture(priya, "u3-7", "priya-roles", "priya's Your roles column: editor");

            var danaApps = new AppsAreaPage(dana);
            await danaApps.OpenAsync(expectTaskBoard: true);
            await danaApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.ViewerRole);
            await Capture(dana, "u3-7", "dana-roles", "dana's Your roles column: viewer");

            await samApps.OpenAsync(expectTaskBoard: true);
            await samApps.ExpectTaskBoardRolesAsync(StoryNames.Contoso.Apps.ViewerRole);
            await Capture(sam, "u3-7", "sam-roles", "sam's Your roles column: viewer");

            await ninaApps.OpenAsync(expectTaskBoard: false, viaSpine: false);
            await ninaApps.ExpectNoAppsAsync();
            await Capture(nina, "u3-7", "nina-roles", "nina holds no app role, so her Apps page lists none");
            Coverage.Hit("use-case:apps/roles-column");
        });

        if (boardOpens)
        {
            context.Record(StoryKeys.TaskBoardTasks, (IReadOnlyList<string>)[.. tasks.Select(task => ids[task.Title])]);
        }
    }

    private static Task Capture(PersonaSession session, string step, string substep, string title) =>
        session.SubstepAsync(JourneyRecorder.For(Journey), step, substep, title);

    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope(step);
        await body();
        scope.Complete();
    }
}
