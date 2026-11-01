using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Accessibility;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4077: a new group's id is a plain text box, not a drop-down. An id that
/// already names a group is refused as it is typed, a directory user's id is
/// refused by name when the field is left (an id the directory does not list at
/// all names a stored group, issue #5370), and a roster group
/// that is not defined yet is created - with the dialog staying open, input kept,
/// for every refusal.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class GroupCreateJourneyTests : UiTestBase
{
    [Test]
    public async Task A_new_group_is_named_in_a_text_box_refused_when_taken_and_created_from_the_roster()
    {
        var world = await UiHosts.WorldAsync();
        await world.RemoveGroupAsync(WorldIdentities.AuditorsGroup);
        var page = await OpenAsync(world.Head, "/access/groups", WorldIdentities.Admin);

        await page.GetByRole(AriaRole.Button, new() { Name = "New group", Exact = true }).ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog);
        var id = dialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true });
        await Expect(id).ToBeVisibleAsync();
        await Expect(dialog.GetByRole(AriaRole.Combobox)).ToHaveCountAsync(0);
        await Expect(dialog.Locator(".lt-combobox__chevron")).ToHaveCountAsync(0);

        await id.FillAsync(WorldIdentities.OperatorsGroup);
        var error = dialog.Locator(".lt-field__error");
        await Expect(error).ToContainTextAsync($"A group named {WorldIdentities.OperatorsGroup} already exists.");
        await Expect(id).ToHaveAttributeAsync("aria-invalid", "true");

        await id.FillAsync(WorldIdentities.Alice);
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Display name (optional)" }).FocusAsync();
        await Expect(error).ToContainTextAsync($"{WorldIdentities.Alice} is a user in the identity directory (static roster), not a group.");

        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(id).ToHaveValueAsync(WorldIdentities.Alice);
        await AxeConformance.SweepAsync(page, "the New group dialog with a refused id");

        await id.FillAsync(WorldIdentities.AuditorsGroup);
        await Expect(error).ToHaveCountAsync(0);
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();

        await Expect(page).ToHaveURLAsync(new Regex($"/access/groups/{WorldIdentities.AuditorsGroup}$"));
        await Expect(page.GetByRole(AriaRole.Heading, new() { Name = WorldIdentities.AuditorsGroup, Level = 1 })).ToBeVisibleAsync();
    }
}
