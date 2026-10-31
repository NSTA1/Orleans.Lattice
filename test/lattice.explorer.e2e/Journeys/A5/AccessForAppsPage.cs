using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>
/// The parts of the Access area journey A5 drives: the rules list's App-owned filter and an
/// app-owned rule's read-only page, and creating a group and nesting groups in it.
/// </summary>
/// <param name="shell">The Explorer's chrome on the session's page.</param>
internal sealed class AccessForAppsPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The rules table's rows owned by the app <paramref name="slug"/>.</summary>
    /// <param name="slug">The app slug.</param>
    public ILocator AppRuleRows(string slug) =>
        Content.Locator("table tbody tr").Filter(new() { Has = Page.GetByRole(AriaRole.Link, new() { Name = $"app {slug}", Exact = true }) });

    /// <summary>
    /// Opens the rules list filtered to app-owned rules and asserts <paramref name="slug"/>'s are
    /// listed, each attributed to the app with a link to its roles.
    /// </summary>
    /// <param name="slug">The app slug.</param>
    public async Task ShowAppOwnedRulesAsync(string slug)
    {
        // The rules an enable compiles reach the listing a moment later: read it again until they do.
        var rows = AppRuleRows(slug);
        for (var attempt = 1; ; attempt++)
        {
            await shell.GotoAsync("/access/rules");
            await shell.ExpectHeadingAsync("Access");
            await Content.GetByRole(AriaRole.Group, new() { Name = "Show", Exact = true }).GetByRole(AriaRole.Button, new() { Name = "App-owned", Exact = true }).ClickAsync();
            var more = Content.GetByRole(AriaRole.Button, new() { Name = "Load more rules", Exact = true });
            for (var page = 0; page < 5 && await rows.CountAsync() == 0 && await more.IsVisibleAsync(); page++)
            {
                // The cluster holds more rules than one page: the filter applies to those loaded.
                await more.ClickAsync();
                await Page.WaitForTimeoutAsync(2_000);
            }

            try
            {
                await rows.First.WaitForAsync(new() { Timeout = 10_000 });
                break;
            }
            catch (System.TimeoutException) when (attempt < 6)
            {
            }
        }

        var owners = rows.GetByRole(AriaRole.Link, new() { Name = $"app {slug}", Exact = true });
        await Expect(owners.First).ToHaveAttributeAsync("href", new Regex($"/apps/{Regex.Escape(slug)}/roles$"));
        Coverage.Hit("route:/access/rules");
        Coverage.Hit("subroute:access/rules");
    }

    /// <summary>Opens the first rule <paramref name="slug"/> owns and asserts it is read-only and attributed to the app.</summary>
    /// <param name="slug">The app slug.</param>
    public async Task OpenAppOwnedRuleAsync(string slug)
    {
        await AppRuleRows(slug).First.Locator("a").First.ClickAsync();
        var notice = Content.Locator("[data-lt-app-owned=true]");
        await Expect(notice).ToContainTextAsync("This rule is owned by");
        await Expect(notice).ToContainTextAsync("so it is read-only here");
        await Expect(notice.GetByRole(AriaRole.Link, new() { Name = $"the {slug} app", Exact = true })).ToHaveAttributeAsync("href", new Regex($"/apps/{Regex.Escape(slug)}/roles$"));
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Delete rule" })).ToHaveCountAsync(0);
        Coverage.Hit("route:/access/rules/{p1}");
        Coverage.Hit("use-case:access/app-owned-rules");
    }

    /// <summary>Follows the app-owned rule's link to the app's roles.</summary>
    /// <param name="slug">The app slug.</param>
    public Task FollowToAppRolesAsync(string slug) =>
        Content.Locator("[data-lt-app-owned=true]").GetByRole(AriaRole.Link, new() { Name = $"the {slug} app", Exact = true }).ClickAsync();

    /// <summary>Creates a stored group through <b>New group</b>, landing on its page.</summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="displayName">Its display name.</param>
    /// <param name="filled">Called once the dialog is filled in, before it is submitted.</param>
    public async Task CreateGroupAsync(string groupId, string displayName, Func<Task> filled)
    {
        await shell.GotoAsync("/access/groups?new=true");
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "New group" });
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true }).FillAsync(groupId);
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Display name (optional)", Exact = true }).FillAsync(displayName);
        Coverage.Hit("query:access.new");
        Coverage.Hit("query-value:access.new=true");
        await filled();
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create group" }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/access/groups/{Regex.Escape(groupId)}$"));
        await shell.ExpectHeadingAsync(groupId);
        await Expect(Content.GetByLabel("Display name", new() { Exact = true })).ToHaveValueAsync(displayName);
        Coverage.Hit("route:/access/groups");
        Coverage.Hit("route:/access/groups/{p1}");
        Coverage.Hit("subroute:access/groups");
    }

    /// <summary>Adds the group <paramref name="member"/> to the group whose page is shown.</summary>
    /// <param name="member">The member group's id.</param>
    public async Task AddGroupMemberAsync(string member)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("group");
        await JourneyPick.PickAsync(Page, Content.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true }), member);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(DirectMembers.Filter(new() { HasText = member })).ToHaveCountAsync(1);
    }

    /// <summary>The rows of the direct members table.</summary>
    public ILocator DirectMembers => Content.GetByRole(AriaRole.Region, new() { Name = "Direct members" }).Locator("table tbody tr");
}
