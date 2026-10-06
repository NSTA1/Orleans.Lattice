using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #4150: an app role is held only by binding (issue #3902), so an administrator who
/// installs the task board for globex bound to a group they are not in is warned while
/// binding, is told after install that they cannot open it and how to fix that, joins the
/// group from the link offered, and then finds the app's Open entry.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class AppRoleHoldingJourneyTests : UiTestBase
{
    private const string Tenant = "globex";

    [Test]
    public async Task An_installer_bound_to_a_group_they_are_not_in_is_warned_joins_it_and_can_then_open_the_app()
    {
        var world = await UiHosts.TenantWorldAsync();

        // The tenancy world is shared, so the journey starts from no globex install and
        // leaves the world as it found it: the install's role rules would otherwise show
        // in globex's Access listing, and the administrator would stay in operators.
        await world.RemoveTaskBoardAsync(Tenant);
        await world.RemoveMemberAsync(WorldIdentities.OperatorsGroup, WorldIdentities.Admin);
        try
        {
            await InstallJoinAndOpenAsync(world);
        }
        finally
        {
            await world.RemoveTaskBoardAsync(Tenant);
            await world.RemoveMemberAsync(WorldIdentities.OperatorsGroup, WorldIdentities.Admin);
        }
    }

    private async Task InstallJoinAndOpenAsync(ExplorerWorld world)
    {
        var page = await OpenAsync(world.Head, $"/t/{Tenant}/apps/catalogue/in-image/task-board", WorldIdentities.Admin);
        var content = Shell.Content(page);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Task board");

        // Bind both roles to operators, a group the administrator is not in: warned, not blocked.
        await content.GetByRole(AriaRole.Button, new() { Name = "Install...", Exact = true }).ClickAsync();
        await content.GetByLabel("Group for editor").FillAsync(WorldIdentities.OperatorsGroup);
        await content.GetByLabel("Group for viewer").FillAsync(WorldIdentities.OperatorsGroup);
        await page.Keyboard.PressAsync("Tab");
        var warnings = content.Locator("[data-lt-holding=not-member]");
        await Expect(warnings).ToHaveCountAsync(2);
        await Expect(warnings.First).ToContainTextAsync($"You are not in {WorldIdentities.OperatorsGroup}, so you will not hold the");
        await Expect(content.Locator("[data-lt-holding-summary]")).ToContainTextAsync("You will hold no role in Task board, so you won't be able to open it.");
        await content.GetByRole(AriaRole.Button, new() { Name = "Continue", Exact = true }).ClickAsync();

        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Confirm the ceiling" })).ToBeVisibleAsync();
        await content.GetByRole(AriaRole.Button, new() { Name = "Install v1.0.0", Exact = true }).ClickAsync();
        await content.GetByRole(AriaRole.Button, new() { Name = "Enable now", Exact = true }).ClickAsync();

        // The confirmation names the tenant, links the app there, and says why it cannot be opened.
        await Expect(content).ToContainTextAsync("is enabled in tenant globex");
        await Expect(content.Locator("[data-lt-app-address]")).ToHaveTextAsync("/t/globex/apps/task-board");
        var notice = content.Locator(".lt-apps-status [data-lt-holding=none]");
        await Expect(notice).ToContainTextAsync("You hold no role in Task board, so you cannot open it.");
        await Expect(notice).ToContainTextAsync($"You are not in {WorldIdentities.OperatorsGroup}.");

        // Join the group from the link offered.
        await notice.GetByRole(AriaRole.Link, new() { Name = $"Add me to {WorldIdentities.OperatorsGroup}", Exact = true }).ClickAsync();
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex($"/access/groups/{WorldIdentities.OperatorsGroup}$"));
        var member = content.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true });
        await member.FocusAsync();
        await member.PressSequentiallyAsync(WorldIdentities.Admin);
        await Expect(page.Locator(".lt-combobox__list [role=option]").Filter(new() { HasText = WorldIdentities.Admin })).ToHaveCountAsync(1);
        await page.Keyboard.PressAsync("ArrowDown");
        await page.Keyboard.PressAsync("Enter");
        await Expect(member).ToHaveValueAsync(WorldIdentities.Admin);
        await content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(content.Locator("table").First).ToContainTextAsync(WorldIdentities.Admin);

        // Now a member of the bound group, the administrator holds its roles: Open appears.
        await Shell.GotoAsync(page, world.Head, "/t/globex/apps/task-board");
        await Expect(content.GetByRole(AriaRole.Link, new() { Name = "Open Task board (opens in a new window)", Exact = true })).ToBeVisibleAsync();
        await Expect(content.Locator("[data-lt-holding]")).ToHaveCountAsync(0);
    }
}
