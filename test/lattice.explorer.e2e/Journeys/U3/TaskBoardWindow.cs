using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U3;

/// <summary>
/// The task board's window, <c>/apps/task-board/window</c>: the Explorer's standalone page
/// around the app's sandboxed frame, and the board the frame draws. Either a window the
/// persona opened from the console (a new browser page, checked against its own console),
/// or the persona's own page after they typed the window's address.
/// </summary>
internal sealed class TaskBoardWindow
{
    /// <summary>The address of the task board's window.</summary>
    public const string Address = "/apps/task-board/window";

    private static readonly string[] Columns = ["todo", "doing", "done"];

    private readonly Func<JourneyRecorder, string, string, string, Task> _capture;
    private readonly Func<Task<TaskBoardServerView>>? _readServer;

    private TaskBoardWindow(IPage page, Func<JourneyRecorder, string, string, string, Task> capture, Func<Task<TaskBoardServerView>>? readServer = null)
    {
        Page = page;
        _capture = capture;
        _readServer = readServer;
    }

    /// <summary>The browser page showing the window.</summary>
    public IPage Page { get; }

    /// <summary>The Explorer's frame host around the app.</summary>
    public ILocator Host => Page.Locator("section.appframe");

    /// <summary>The app's sandboxed frame element.</summary>
    public ILocator Element => Host.Locator("iframe");

    /// <summary>The board, drawn inside the frame.</summary>
    public IFrameLocator Board => Element.ContentFrame;

    /// <summary>The window's Leave app control (the frame host draws one above and one below the frame).</summary>
    public ILocator LeaveApp => Host.GetByRole(AriaRole.Button, new() { Name = "Leave app", Exact = true }).First;

    /// <summary>The board's status line, where it reports what it just did.</summary>
    public ILocator Status => Board.Locator("#tb-status");

    /// <summary>The window as the persona's own page shows it.</summary>
    /// <param name="session">The persona's session, on the window's address.</param>
    public static TaskBoardWindow InSession(PersonaSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        return new(session.Page, (recorder, step, substep, title) => session.SubstepAsync(recorder, step, substep, title));
    }

    /// <summary>
    /// Opens the window the way the console does, in a new browser window: <paramref name="open"/>
    /// clicks a control whose link targets a new window. The new page is checked against its own
    /// console and the estate's host logs at every checkpoint.
    /// </summary>
    /// <param name="session">The persona's session, whose page holds the control.</param>
    /// <param name="estate">The estate.</param>
    /// <param name="open">Clicks the control.</param>
    public static async Task<TaskBoardWindow> OpenNewWindowAsync(PersonaSession session, JourneyEstate estate, Func<Task> open)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(estate);
        ArgumentNullException.ThrowIfNull(open);
        var popup = await session.Context.RunAndWaitForPageAsync(open);
        var console = new BrowserConsoleLog();
        console.Attach(popup);
        var window = new TaskBoardWindow(
            popup,
            (recorder, step, substep, title) => JourneyCheckpoint.CaptureAsync(popup, recorder, step, substep, title, session.Persona.Id, console, estate.HostFaults),
            () => TaskBoardServerView.ReadAsync(estate, session));
        await Expect(popup).ToHaveURLAsync(new Regex(Regex.Escape(Address) + "$"));
        return window;
    }

    /// <summary>Captures a checkpoint of the window.</summary>
    /// <param name="recorder">The journey's recorder.</param>
    /// <param name="step">The step id.</param>
    /// <param name="substep">The substep id.</param>
    /// <param name="title">The caption.</param>
    public Task CaptureAsync(JourneyRecorder recorder, string step, string substep, string title) => _capture(recorder, step, substep, title);

    /// <summary>
    /// Waits until the frame has loaded the board and the board has read its tree: it names the
    /// installed version and shows every column's count. A refusal at any point before then fails
    /// at once, with what the cluster answered for the persona: every silo answers the persona's
    /// workspace from a registry snapshot current with every write that returned before the read
    /// began (#5595), so a window that refuses an app the persona holds is a defect, not a wait.
    /// </summary>
    public async Task WaitForBoardAsync()
    {
        var closed = Page.GetByRole(AriaRole.Heading, new() { NameRegex = new Regex("is not open to you yet") });
        var notFound = Page.GetByRole(AriaRole.Heading, new() { Name = ShellPage.NotFoundHeading, Exact = true });
        var refused = Host.GetByRole(AriaRole.Alert);
        var context = Board.Locator("#tb-context");
        var loaded = new Regex(@"^task-board 1\.1\.0( in \S+)?\.$");

        // The board has loaded only once it has read its tree. A window can draw the frame and then
        // give way to a refusal (the page's own read of the app is a second read), so a refusal
        // counts at any point until then.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        string? refusal;
        while (true)
        {
            if (await context.IsVisibleAsync() && loaded.IsMatch(await context.InnerTextAsync()))
            {
                break;
            }

            refusal = await refused.IsVisibleAsync() ? await refused.InnerTextAsync()
                : await notFound.IsVisibleAsync() ? ShellPage.NotFoundHeading
                : await closed.IsVisibleAsync() ? "is not open to you yet"
                : null;
            if (refusal is null && DateTime.UtcNow > deadline)
            {
                refusal = "the board did not load within 60 s" + (await Element.IsVisibleAsync() ? " (its frame is shown)" : " (no frame is shown)");
            }

            if (refusal is not null)
            {
                var server = _readServer is null ? "not read" : (await _readServer()).ToString();
                Assert.Fail($"The task board's window at {Page.Url} refused to open: {refusal.ReplaceLineEndings(" ")}. Server: {server}.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        foreach (var column in Columns)
        {
            await Expect(Board.Locator("#tb-count-" + column)).ToHaveTextAsync(new Regex(@"^\(\d+\)$"));
        }
    }

    /// <summary>Asserts the window stands alone: no header, address line or directory spine around the frame.</summary>
    public async Task ExpectStandaloneAsync()
    {
        await Expect(Page.GetByRole(AriaRole.Banner)).ToHaveCountAsync(0);
        await Expect(Page.GetByRole(AriaRole.Navigation, new() { Name = "Estate" })).ToHaveCountAsync(0);
        await Expect(LeaveApp).ToBeVisibleAsync();
    }

    /// <summary>Asserts the board offers its write controls: the caller holds a role that lets them write.</summary>
    public async Task ExpectEditorAsync()
    {
        await Expect(Board.Locator("#tb-add")).ToBeVisibleAsync();
        await Expect(Board.Locator("#tb-read-only")).ToBeHiddenAsync();
    }

    /// <summary>Asserts the board is read-only for the caller, and says so.</summary>
    public async Task ExpectReadOnlyAsync()
    {
        await Expect(Board.Locator("#tb-read-only")).ToHaveTextAsync("You can read this board. Your role does not let you change it.");
        await Expect(Board.Locator("#tb-read-only")).ToBeVisibleAsync();
        await Expect(Board.Locator("#tb-add")).ToBeHiddenAsync();
    }

    /// <summary>The card titled <paramref name="title"/> in <paramref name="column"/>.</summary>
    /// <param name="column">The column: <c>todo</c>, <c>doing</c> or <c>done</c>.</param>
    /// <param name="title">The task's title.</param>
    public ILocator Card(string column, string title) =>
        Board.Locator($"#tb-cards-{column} button.tb-card").Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(title) + "$") });

    /// <summary>Asserts how many cards each column holds.</summary>
    /// <param name="todo">The cards in To do.</param>
    /// <param name="doing">The cards in Doing.</param>
    /// <param name="done">The cards in Done.</param>
    public async Task ExpectCountsAsync(int todo, int doing, int done)
    {
        await Expect(Board.Locator("#tb-count-todo")).ToHaveTextAsync($"({todo})");
        await Expect(Board.Locator("#tb-count-doing")).ToHaveTextAsync($"({doing})");
        await Expect(Board.Locator("#tb-count-done")).ToHaveTextAsync($"({done})");
    }

    /// <summary>Adds a task through the board's New task field, and waits until its card is in To do.</summary>
    /// <param name="title">The task's title.</param>
    /// <returns>The task's id, which the board generated.</returns>
    public async Task<string> AddTaskAsync(string title)
    {
        await Board.Locator("#tb-add-title").FillAsync(title);
        await Board.Locator("#tb-add-button").ClickAsync();
        await Expect(Status).ToHaveTextAsync($"Added \"{title}\" to To do.");
        var card = Card("todo", title);
        await Expect(card).ToHaveCountAsync(1);
        return await card.GetAttributeAsync("data-id") ?? throw new InvalidOperationException($"The card '{title}' carries no task id.");
    }

    /// <summary>Selects the card titled <paramref name="title"/> in <paramref name="column"/>, opening its detail.</summary>
    /// <param name="column">The card's column.</param>
    /// <param name="title">The task's title.</param>
    public async Task SelectAsync(string column, string title)
    {
        await Card(column, title).ClickAsync();
        await Expect(Board.Locator("#tb-detail-title")).ToHaveTextAsync(title);
    }

    /// <summary>Moves the selected task to <paramref name="column"/> with the detail's Move control.</summary>
    /// <param name="title">The selected task's title.</param>
    /// <param name="column">The column to move it to.</param>
    /// <param name="columnName">The column's name, as the board writes it.</param>
    public async Task MoveSelectedAsync(string title, string column, string columnName)
    {
        await Board.Locator($"#tb-detail-actions [data-move=\"{column}\"]").ClickAsync();
        await Expect(Status).ToHaveTextAsync($"Moved \"{title}\" to {columnName}.");
        await Expect(Card(column, title)).ToHaveCountAsync(1);
        await Expect(Board.Locator("#tb-detail-column")).ToHaveTextAsync($"In {columnName}.");
    }

    /// <summary>Asserts the detail shows the task titled <paramref name="title"/>, in <paramref name="columnName"/>.</summary>
    /// <param name="title">The task's title.</param>
    /// <param name="key">The task's key in the app's tree.</param>
    /// <param name="columnName">The column's name, as the board writes it.</param>
    public async Task ExpectDetailAsync(string title, string key, string columnName)
    {
        await Expect(Board.Locator("#tb-detail")).ToBeVisibleAsync();
        await Expect(Board.Locator("#tb-detail-title")).ToHaveTextAsync(title);
        await Expect(Board.Locator("#tb-detail-key")).ToHaveTextAsync(key);
        await Expect(Board.Locator("#tb-detail-column")).ToHaveTextAsync($"In {columnName}.");
    }

    /// <summary>Asserts no task is selected: the detail is closed.</summary>
    public Task ExpectNoDetailAsync() => Expect(Board.Locator("#tb-detail")).ToBeHiddenAsync();

    /// <summary>
    /// Sends a write of <paramref name="key"/> to the app's tree from inside the frame, as a user
    /// typing into the browser's developer console on the frame would: through the same bridge
    /// the board uses, under the user's own identity. Returns <c>allowed</c> or the bridge's
    /// error code.
    /// </summary>
    /// <param name="key">The key to write.</param>
    public async Task<string> ForceWriteAsync(string key)
    {
        var frame = await FrameDocumentAsync();
        return await frame.EvaluateAsync<string>(
            "([op, args]) => lattice.ready.then(() => lattice.request(op, JSON.parse(args))).then(() => 'allowed', e => (e && e.code) || String(e))",
            new object[] { "data.write", $"{{\"action\":\"set\",\"tree\":\"tasks\",\"key\":\"{key}\",\"value\":\"e30=\"}}" });
    }

    private async Task<IFrame> FrameDocumentAsync()
    {
        var bootstrap = new Regex("_apps/frame/");
        await Expect(Element).ToHaveAttributeAsync("src", bootstrap);
        var handle = await Element.ElementHandleAsync();
        var frame = await handle.ContentFrameAsync() ?? throw new InvalidOperationException("The app frame element has no content frame.");
        await frame.WaitForURLAsync(bootstrap);
        await frame.WaitForLoadStateAsync(LoadState.Load);
        return frame;
    }
}
