using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Apps;

/// <summary>
/// The pilot app (P1) end to end, in Chromium, Firefox and WebKit: an administrator
/// finds the task board in the catalogue, reviews what it asks for, binds its roles,
/// installs and enables it; an editor opens it, adds and moves a task and deep-links to
/// it; a viewer sees no write control and is refused a write forced from the console;
/// and a user who holds no role finds the app nowhere.
/// </summary>
[TestFixture(UiBrowsers.Chromium)]
[TestFixture(UiBrowsers.Firefox)]
[TestFixture(UiBrowsers.WebKit)]
[Category("UI")]
public sealed class TaskBoardPilotTests(string engine) : UiTestBase(engine)
{
    [Test]
    public async Task The_task_board_is_installed_opened_edited_and_deep_linked_and_its_roles_hold()
    {
        var world = await UiHosts.WorldAsync();
        await world.RemoveTaskBoardAsync();

        await InstallThroughTheCatalogueAsync(world);
        var deepLink = await EditAsEditorAsync(world);
        await FollowDeepLinkAsync(world, deepLink);
        await ViewAsViewerAsync(world);
        await FindNothingAsRoleLessUserAsync(world);
    }

    private async Task InstallThroughTheCatalogueAsync(ExplorerWorld world)
    {
        var page = await OpenAsync(world.Head, "/apps/catalogue", WorldIdentities.Admin);
        var entry = Shell.Content(page).GetByRole(AriaRole.Row).Filter(new() { HasText = "Task board" });
        await Expect(entry).ToBeVisibleAsync();
        await entry.GetByRole(AriaRole.Link, new() { Name = "Review" }).ClickAsync();

        // Consent review: what it asks for, drawn from its own manifest.
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Task board");
        await Expect(Shell.Content(page)).ToContainTextAsync("tasks");
        await Expect(Shell.Content(page)).ToContainTextAsync("editor");
        await Expect(Shell.Content(page)).ToContainTextAsync("viewer");
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Install...", Exact = true }).ClickAsync();

        // Bind roles to groups.
        await Expect(Shell.Content(page).GetByRole(AriaRole.Heading, new() { Name = "Bind roles to groups" })).ToBeVisibleAsync();
        await Shell.Content(page).GetByLabel("Group for editor").FillAsync(WorldIdentities.EditorsGroup);
        await Shell.Content(page).GetByLabel("Group for viewer").FillAsync(WorldIdentities.ViewersGroup);
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Continue", Exact = true }).ClickAsync();

        // Confirm the ceiling: the consent the review drafted covers everything it asks for.
        await Expect(Shell.Content(page).GetByRole(AriaRole.Heading, new() { Name = "Confirm the ceiling" })).ToBeVisibleAsync();
        await Expect(Shell.Content(page).Locator("[data-lt-activation=\"ok\"]")).ToBeVisibleAsync();
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Install v1.0.0", Exact = true }).ClickAsync();

        // Enable it.
        await Shell.Content(page).GetByRole(AriaRole.Button, new() { Name = "Enable now", Exact = true }).ClickAsync();
        await Expect(Shell.Content(page)).ToContainTextAsync("is enabled");
    }

    private async Task<string> EditAsEditorAsync(ExplorerWorld world)
    {
        var page = await OpenAsync(world.Head, "/apps", WorldIdentities.Alice);
        await Expect(Shell.Content(page).GetByRole(AriaRole.Link, new() { Name = "Task board" }).First).ToBeVisibleAsync();

        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var board = AppFrames.Content(page);

        // An editor gets the write controls.
        var title = board.Locator("#tb-add-title");
        await Expect(title).ToBeVisibleAsync();
        await Expect(board.Locator("#tb-read-only")).ToBeHiddenAsync();

        var name = "Ship U1 " + Engine;
        await title.FillAsync(name);
        await board.Locator("#tb-add-button").ClickAsync();
        var card = board.Locator("#tb-cards-todo button.tb-card").Filter(new() { HasText = name });
        await Expect(card).ToHaveCountAsync(1);

        // Select it, and move it to Doing.
        await card.ClickAsync();
        await Expect(board.Locator("#tb-detail")).ToBeVisibleAsync();
        await board.Locator("#tb-detail-actions [data-move=\"doing\"]").ClickAsync();
        await Expect(board.Locator("#tb-cards-doing button.tb-card").Filter(new() { HasText = name })).ToHaveCountAsync(1);

        // Selecting a card writes its path into the Explorer's address.
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("/apps/task-board/open/tasks/[^/?#]+$"));
        await Expect(board.Locator("#tb-detail-title")).ToHaveTextAsync(name);
        return page.Url;
    }

    private async Task FollowDeepLinkAsync(ExplorerWorld world, string deepLink)
    {
        var page = await NewPageAsync(world.Head, WorldIdentities.Alice);
        await page.GotoAsync(deepLink);
        await Shell.WaitForInteractiveAsync(page);

        var board = AppFrames.Content(page);
        await Expect(board.Locator("#tb-detail")).ToBeVisibleAsync();
        await Expect(board.Locator("#tb-detail-title")).ToHaveTextAsync("Ship U1 " + Engine);
        await Expect(board.Locator("#tb-detail-column")).ToContainTextAsync("Doing");
    }

    private async Task ViewAsViewerAsync(ExplorerWorld world)
    {
        var page = await NewPageAsync(world.Head, WorldIdentities.Dave);
        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var board = AppFrames.Content(page);

        // The viewer reads the board; every write control is absent.
        await Expect(board.Locator("#tb-read-only")).ToBeVisibleAsync();
        await Expect(board.Locator("#tb-cards-doing button.tb-card").Filter(new() { HasText = "Ship U1 " + Engine })).ToHaveCountAsync(1);
        await Expect(board.Locator("#tb-add")).ToBeHiddenAsync();

        // A write forced from the frame's console is refused.
        var frame = await AppFrames.DocumentAsync(page);
        var outcome = await AppFrames.RequestAsync(frame, "data.write", "{\"action\":\"set\",\"tree\":\"tasks\",\"key\":\"tasks/forced\",\"value\":\"e30=\"}");
        Assert.That(outcome, Is.EqualTo("denied"), "A viewer's write forced from the console must be refused.");
    }

    private async Task FindNothingAsRoleLessUserAsync(ExplorerWorld world)
    {
        var page = await OpenAsync(world.Head, "/apps", WorldIdentities.Carol);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Apps");
        await Expect(Shell.Content(page).GetByText("Task board")).ToHaveCountAsync(0);

        // Not in the address line's completions either.
        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync("a/task");
        await Expect(Shell.Suggestions(page).Filter(new() { HasText = "task-board" })).ToHaveCountAsync(0);
        await page.Keyboard.PressAsync("Escape");

        // And its address, deep or not, is a page that does not exist.
        await Shell.GotoAsync(page, world.Head, $"/apps/{TaskBoardApp.Slug}");
        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        await Shell.GotoAsync(page, world.Head, $"/apps/{TaskBoardApp.Slug}/open");
        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        await Expect(AppFrames.Element(page)).ToHaveCountAsync(0);
    }
}
