using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5370 (epic #5401, G6): with an identity directory configured, a group
/// created in Access is a <c>Stored</c> group that can be nested in another and named
/// as a rule subject, each picker labelling its source; a platform operator names a
/// tenant group by tenant and name, which the editor confines to that tenant's own
/// trees before saving; and a tenant administrator's page still refuses another
/// tenant's group.
/// </summary>
/// <remarks>
/// Each step keeps an ordered screenshot under <c>screenshots/journey-g6-group-pickers</c>
/// (or <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class AccessGroupSourcesJourneyTests : UiTestBase
{
    private const string Stewards = "g6-data-stewards";
    private const string Staff = "g6-factory-staff";
    private const string StewardsRule = "g6-stewards-orders";
    private const string TenantRule = "g6-acme-team-orders";

    private int _shot;

    [Test]
    public async Task An_operator_uses_stored_and_tenant_groups_in_nested_membership_and_rules()
    {
        var world = await UiHosts.DelegatedWorldAsync();
        await world.RemoveGroupAsync(Stewards);
        await world.RemoveGroupAsync(Staff);

        var page = await OpenAsync(world.Head, "/access/groups?new=true", WorldIdentities.Admin);
        var content = Shell.Content(page);

        // A stored group: the directory does not list it, and that is now allowed.
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New group" });
        await Expect(dialog).ToContainTextAsync("or a new id for a group stored here");
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true }).FillAsync(Stewards);
        await ShotAsync(page, "new-stored-group");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();
        await Expect(page).ToHaveURLAsync(new Regex($"/access/groups/{Stewards}$"));

        await Shell.GotoAsync(page, world.Head, "/access/groups?new=true");
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true }).FillAsync(Staff);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();
        await Expect(page).ToHaveURLAsync(new Regex($"/access/groups/{Staff}$"));

        // Nest the stored group: offered labelled Stored beside the directory's groups.
        await content.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("group");
        var member = content.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true });
        await member.FillAsync("g6-data");
        var stored = page.Locator(".lt-combobox__list [role=option]").Filter(new() { HasText = Stewards });
        await Expect(stored.Locator(".lt-combobox__detail")).ToHaveTextAsync("Stored");
        await ShotAsync(page, "nest-stored-group-offered");
        await stored.ClickAsync();
        await Expect(content.Locator("[data-lt-provenance=stored]")).ToContainTextAsync(Stewards);
        await content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(content.Locator("table").First).ToContainTextAsync(Stewards);
        await ShotAsync(page, "nested-stored-group");

        // A rule for the stored group.
        await Shell.GotoAsync(page, world.Head, "/access/rules?new=true");
        var editor = page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" }).Locator("form.lt-access-form");
        await editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync(StewardsRule);
        await editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("group");
        var subject = editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true });
        await subject.FillAsync("g6");
        var offered = page.Locator(".lt-combobox__list [role=option] .lt-combobox__detail");
        await Expect(offered).ToHaveCountAsync(2);
        await ShotAsync(page, "rule-subject-sources");
        await page.Locator(".lt-combobox__list [role=option]").Filter(new() { HasText = Stewards }).ClickAsync();
        await Expect(editor.Locator("[data-lt-provenance=stored]")).ToContainTextAsync(Stewards);
        await PickAsync(page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }), ExplorerWorld.DemoTree);
        await editor.GetByLabel("Read", new() { Exact = true }).CheckAsync();
        await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
        await Expect(content).ToContainTextAsync(StewardsRule);
        await ShotAsync(page, "stored-group-rule-saved");

        // A tenant group, by tenant and name, confined to its own trees.
        await Shell.GotoAsync(page, world.Head, "/access/rules?new=true");
        await editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync(TenantRule);
        var kind = editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true });
        await Expect(kind.Locator("option[value=tenant-group]")).ToHaveCountAsync(1);
        await kind.SelectOptionAsync("tenant-group");
        await PickAsync(page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject tenant", Exact = true }), ExplorerWorld.Acme);
        await PickAsync(page, subject, ExplorerWorld.AcmeGroup);
        await Expect(editor.Locator("[data-lt-provenance=tenant]")).ToContainTextAsync($"t/{ExplorerWorld.Acme}/{ExplorerWorld.AcmeGroup}");
        await ShotAsync(page, "tenant-group-chosen");
        var tree = editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await tree.FillAsync($"t/{ExplorerWorld.Globex}/{ExplorerWorld.GlobexOrdersTree}");
        await editor.GetByLabel("Read", new() { Exact = true }).CheckAsync();
        await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
        await Expect(editor).ToContainTextAsync($"may govern only tenant {ExplorerWorld.Acme}'s own trees");
        await ShotAsync(page, "tenant-group-confined");
        await tree.FillAsync($"t/{ExplorerWorld.Acme}/{ExplorerWorld.GlobexOrdersTree}");
        await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
        await Expect(content).ToContainTextAsync(TenantRule);
        await ShotAsync(page, "tenant-group-rule-saved");

        // A tenant administrator who is not an operator: another tenant's group is refused on their page.
        var tenantPage = await OpenAsync(world.Head, $"/t/{ExplorerWorld.Globex}/access/rules?new=true", WorldIdentities.GlobexAdmin);
        var tenantEditor = tenantPage.Locator($"form[data-lt-tenant-rule-editor={ExplorerWorld.Globex}]");
        await Expect(tenantEditor).ToBeVisibleAsync();
        await tenantEditor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync("g6-forged");
        await tenantEditor.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("cluster-group");
        await tenantEditor.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }).FillAsync($"t/{ExplorerWorld.Acme}/{ExplorerWorld.AcmeGroup}");
        await PickAsync(tenantPage, tenantEditor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }), ExplorerWorld.GlobexOrdersTree);
        await tenantEditor.GetByLabel("Read", new() { Exact = true }).CheckAsync();
        await tenantEditor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
        await Expect(tenantEditor).ToContainTextAsync("Another tenant's group can never be chosen.");
        await ShotAsync(tenantPage, "tenant-page-refuses-foreign-group");
    }

    /// <summary>Types <paramref name="value"/> into a picker and chooses the suggestion that names it exactly.</summary>
    private static async Task PickAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        var option = page.Locator(".lt-combobox__list [role=option]").Filter(new()
        {
            Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = new Regex($"^\\s*{Regex.Escape(value)}\\s*$") }),
        });
        await option.First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's journey screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-g6-group-pickers");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
