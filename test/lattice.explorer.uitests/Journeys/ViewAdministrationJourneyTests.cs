using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5369: materialised views are administered from the Data area. An
/// administrator opens New view on a tree's Views tab, picks a registered provider
/// by its description, fills its payload form and creates the view, which reaches
/// Current and whose projection is browsable in Data; rebuilds and reconciles it as
/// tracked operations; and drops it behind the destructive confirmation. A reader
/// without administrative authority over the tree is offered none of it, and the
/// <c>?new=view</c> deep link opens nothing for them.
/// </summary>
/// <remarks>
/// Each step's state is captured in order under the run's <c>screenshots/journeys</c>
/// artifact (or under <c>LATTICE_JOURNEY_SCREENSHOTS</c> when it names a directory),
/// for the pull request's journey screenshots.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class ViewAdministrationJourneyTests : UiTestBase
{
    private const string ViewName = "line-marks";
    private int _shot;

    [Test]
    public async Task An_administrator_creates_rebuilds_reconciles_and_drops_a_view()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.ViewsTree}?tab=views", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var create = content.Locator($"button[data-lt-command=\"data.create-view\"]");

        await Expect(create).ToBeVisibleAsync();
        await ShotAsync(page, "views-tab-offers-new-view");

        await create.ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New view" });
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("new=view"));
        var provider = dialog.Locator("select[data-lt-view-provider]");
        await Expect(provider.Locator("option")).ToHaveTextAsync([WorldViewProviders.MarkedCopy, WorldViewProviders.PlainCopy]);
        await Expect(dialog).ToContainTextAsync("Copies each entry, appending a marker to its value");
        await ShotAsync(page, "new-view-lists-providers-with-descriptions");

        await provider.SelectOptionAsync(WorldViewProviders.PlainCopy);
        await Expect(dialog.Locator("[data-lt-view-payload=json]")).ToBeVisibleAsync();
        await Expect(dialog).ToContainTextAsync("Copies every entry unchanged");
        await ShotAsync(page, "a-provider-without-fields-takes-json");

        await provider.SelectOptionAsync(WorldViewProviders.MarkedCopy);
        await dialog.GetByLabel("View name").FillAsync(ViewName);
        await dialog.GetByLabel("Marker", new() { Exact = true }).FillAsync("line-a");
        await dialog.GetByLabel("Delay per entry in ms (optional)").FillAsync("400");
        await ShotAsync(page, "payload-form-filled");

        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create view" }).ClickAsync();
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Created view {ViewName}." })).ToBeVisibleAsync();
        await Expect(dialog).ToBeHiddenAsync();
        var row = content.Locator("tbody tr").Filter(new() { HasText = ViewName });
        await Expect(row).ToBeVisibleAsync();
        // No Refresh: the tab follows a view that is behind until it reads Current (#5519).
        await Expect(row).ToContainTextAsync("Current", new() { Timeout = 30_000 });
        await ShotAsync(page, "created-view-is-current");

        await row.GetByRole(AriaRole.Link, new() { Name = ViewName }).ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync(ViewName);
        await Expect(content.Locator("tbody tr").Filter(new() { HasText = "reading-000" }).First).ToBeVisibleAsync();
        await Expect(content).ToContainTextAsync("ok-000 [line-a]", new() { Timeout = 30_000 });
        await ShotAsync(page, "projection-browsable-in-data");

        await Shell.GotoAsync(page, world.Head, $"/data/{ExplorerWorld.ViewsTree}?tab=views");
        row = content.Locator("tbody tr").Filter(new() { HasText = ViewName });
        await row.GetByRole(AriaRole.Button, new() { Name = "Rebuild", Exact = true }).ClickAsync();
        var confirm = page.GetByRole(AriaRole.Alertdialog);
        await Expect(confirm).ToContainTextAsync("Rebuild this view?");
        await confirm.Locator("input").FillAsync(ViewName);
        await ShotAsync(page, "rebuild-confirmation");

        await confirm.GetByRole(AriaRole.Button, new() { Name = "Rebuild", Exact = true }).ClickAsync();
        var progress = content.Locator("[data-lt-data-operation]");
        await Expect(progress).ToContainTextAsync($"Rebuilding {ViewName}");
        await Expect(progress.GetByRole(AriaRole.Button, new() { Name = "Stop" })).ToBeVisibleAsync();
        await ShotAsync(page, "rebuild-progress-with-stop");
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Rebuilt {ViewName}." })).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await ShotAsync(page, "rebuild-finished");

        await row.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        await confirm.Locator("input").FillAsync(ViewName);
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true }).ClickAsync();
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Reconciled {ViewName}" })).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await ShotAsync(page, "reconcile-finished");

        await row.GetByRole(AriaRole.Button, new() { Name = "Drop view..." }).ClickAsync();
        await Expect(confirm).ToContainTextAsync("Drop this view?");
        await confirm.Locator("input").FillAsync(ViewName);
        await ShotAsync(page, "drop-confirmation");

        await confirm.GetByRole(AriaRole.Button, new() { Name = "Drop view", Exact = true }).ClickAsync();
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Dropped view {ViewName}." })).ToBeVisibleAsync();
        await Expect(content.Locator("tbody tr").Filter(new() { HasText = ViewName })).ToHaveCountAsync(0);
        await ShotAsync(page, "view-dropped");
    }

    [Test]
    public async Task A_reader_without_administrative_authority_is_offered_nothing()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?tab=views&new=view", WorldIdentities.Alice);
        var content = Shell.Content(page);

        await Expect(content.Locator(".lt-empty")).ToContainTextAsync("No views");
        await Expect(content.Locator("button[data-lt-command=\"data.create-view\"]")).ToHaveCountAsync(0);
        await Expect(page.GetByRole(AriaRole.Dialog)).ToHaveCountAsync(0);
        await ShotAsync(page, "reader-is-offered-nothing");
    }

    private async Task ShotAsync(IPage page, string step)
    {
        var root = Environment.GetEnvironmentVariable("LATTICE_JOURNEY_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(Directory.GetCurrentDirectory(), "screenshots", "journeys");
        var directory = Path.Combine(root, nameof(ViewAdministrationJourneyTests));
        Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions
        {
            Path = Path.Combine(directory, $"{TestContext.CurrentContext.Test.MethodName}-{++_shot:D2}-{step}.png"),
        });
    }
}
