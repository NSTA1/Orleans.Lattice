using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// G4 (issue #5368): an operator creates a tree from the Cluster area. <b>New tree...</b>
/// opens a form that starts at the library defaults; a reserved name and a name already
/// taken are each refused with the reason; the review names the tree and its sizing;
/// the create lands on the new tree's Summary with what to do next, its sizing shows on
/// Summary and Configuration, and the Data area lists it at once. An observer who may
/// not create trees is shown no control, and the palette command is refused.
/// </summary>
/// <remarks>
/// Every step waits on what the page shows, never on time. The tree is the test's own,
/// so the shared world's other trees are untouched. When
/// <c>LATTICE_UI_JOURNEY_SCREENSHOTS</c> names a directory, each step's screenshot is
/// written there in order, for the pull request that changes the flow.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class TreeCreateJourneyTests : UiTestBase
{
    private int _shot;

    /// <summary>Numbers each test's screenshots from one.</summary>
    [SetUp]
    public void RestartShots() => _shot = 0;

    [Test]
    public async Task An_operator_creates_a_sized_tree_and_lands_on_its_summary()
    {
        var world = await UiHosts.WorldAsync();
        var treeId = "journey-create-" + Guid.NewGuid().ToString("N")[..8];
        var page = await OpenAsync(world.Head, "/cluster/trees", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var create = content.Locator("[data-lt-command=\"cluster.create-tree\"]");
        await Expect(create).ToHaveTextAsync("New tree...");
        await ShotAsync(page, "affordance");

        await create.ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New tree" });
        await Expect(dialog.GetByLabel("Physical shards")).ToHaveValueAsync("64");
        await Expect(dialog.GetByLabel("Keys per leaf")).ToHaveValueAsync("128");
        await Expect(dialog.GetByLabel("Children per internal node")).ToHaveValueAsync("128");
        await ShotAsync(page, "form-defaults");

        await dialog.GetByLabel("Tree name").FillAsync("_lattice_journey");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Expect(dialog.Locator(".lt-field__error")).ToContainTextAsync("the '_lattice_' namespace is managed by the library");
        await ShotAsync(page, "reserved-refused");

        await dialog.GetByLabel("Tree name").FillAsync(ExplorerWorld.DemoTree);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Expect(dialog.Locator("[data-lt-create-tree-refusal]")).ToHaveTextAsync($"A tree named {ExplorerWorld.DemoTree} already exists. Creating it again would change nothing; choose another name.");
        await ShotAsync(page, "duplicate-refused");

        await dialog.GetByLabel("Tree name").FillAsync(treeId);
        await dialog.GetByLabel("Physical shards").FillAsync("4");
        await dialog.GetByLabel("Keys per leaf").FillAsync("64");
        await dialog.GetByLabel("Children per internal node").FillAsync("32");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Review" }).ClickAsync();
        await Expect(dialog.Locator("[data-lt-create-tree=review]")).ToContainTextAsync(
            $"Create the empty tree {treeId} with 4 physical shards, 64 keys per leaf and 32 children per internal node.");
        await ShotAsync(page, "review");

        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create tree" }).ClickAsync();
        await Expect(Shell.Heading(page)).ToHaveTextAsync(treeId);
        await Expect(content.Locator("[data-lt-tree-created]")).ToContainTextAsync($"{treeId} is new and empty, with 4 physical shards, 64 keys per leaf and 32 children per internal node.");
        await Expect(content.GetByRole(AriaRole.Tab, new() { Name = "Summary", Selected = true })).ToHaveCountAsync(1);
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Tree {treeId} created." })).ToHaveCountAsync(1);
        await Expect(page.Locator(".lt-shell-page-lede")).ToContainTextAsync("4 shards");
        await ShotAsync(page, "summary-with-next-steps");

        await content.Locator("[data-lt-tree-created-next=configuration]").ClickAsync();
        await Expect(Term(page, "Physical shards")).ToHaveTextAsync("4");
        await Expect(Term(page, "Keys per leaf")).ToHaveTextAsync("64");
        await Expect(Term(page, "Children per internal node")).ToHaveTextAsync("32");
        await ShotAsync(page, "configuration");

        await Shell.GotoAsync(page, world.Head, "/data");
        await Expect(content).ToContainTextAsync(treeId);
        await ShotAsync(page, "listed-in-data");

        await Shell.GotoAsync(page, world.Head, "/cluster/trees");
        await Expect(content.Locator("tbody")).ToContainTextAsync(treeId);
        await ShotAsync(page, "listed-in-cluster");
    }

    [Test]
    public async Task An_observer_who_may_not_create_trees_has_no_control_and_the_command_is_refused()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, "/cluster/trees", WorldIdentities.Erin);
        var content = Shell.Content(page);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Trees");
        await Expect(content.Locator("tbody")).ToContainTextAsync(ExplorerWorld.DemoTree);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Reshard tree..." })).ToHaveCountAsync(0);
        await Expect(content.Locator("[data-lt-command=\"cluster.create-tree\"]")).ToHaveCountAsync(0);
        await ShotAsync(page, "observer-hidden");

        await Shell.OpenAddressLineAsync(page);
        await Shell.AddressInput(page).FillAsync(">New tree");
        var command = Shell.Suggestions(page).Filter(new() { HasText = "New tree..." });
        await Expect(command).ToHaveCountAsync(1);
        await command.ClickAsync();

        await Expect(content.Locator("[data-lt-create-tree-hidden]")).ToHaveTextAsync("Your identity holds no grant to create trees on this cluster.");
        await Expect(page.GetByRole(AriaRole.Dialog, new() { Name = "New tree" })).ToHaveCountAsync(0);
        await ShotAsync(page, "observer-refused");
    }

    private async Task ShotAsync(IPage page, string step)
    {
        var directory = Environment.GetEnvironmentVariable("LATTICE_UI_JOURNEY_SCREENSHOTS");
        _shot++;
        if (string.IsNullOrEmpty(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
        var name = $"{TestContext.CurrentContext.Test.MethodName}-{_shot:D2}-{step}.png";
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, name), FullPage = true });
    }

    private static ILocator Term(IPage page, string term) =>
        Shell.Content(page).Locator(".lt-dl__row").Filter(new() { Has = page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }) }).Locator("dd");
}
