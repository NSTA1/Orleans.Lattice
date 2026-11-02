using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// The Access groups (<c>/access/groups</c>) and one group's page
/// (<c>/access/groups/{id}</c>): <b>New group</b>, its direct members, the groups it
/// belongs to, and the member picker that nests a stored group (G6, #5370).
/// </summary>
/// <param name="shell">The session's shell.</param>
internal sealed class GroupPages(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The <b>New group</b> dialog.</summary>
    public ILocator NewGroupDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "New group" });

    /// <summary>The direct members table.</summary>
    public ILocator Members => Content.Locator("section[aria-labelledby=lt-access-group-members]");

    /// <summary>The <b>Member of</b> section.</summary>
    public ILocator MemberOf => Content.Locator("section[aria-labelledby=lt-access-group-parents]");

    /// <summary>The member picker.</summary>
    public ILocator MemberBox => Members.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true });

    /// <summary>The group's <b>Details</b> section, with its display name.</summary>
    public ILocator Details => Content.Locator("section[aria-labelledby=lt-access-group-details]");

    /// <summary>The <b>Save name</b> button.</summary>
    public ILocator SaveName => Details.GetByRole(AriaRole.Button, new() { Name = "Save name", Exact = true });

    /// <summary>The error shown beside the display name.</summary>
    public ILocator DisplayNameError => Details.Locator(".lt-field__error");

    /// <summary>Opens the group list through the Access area's own navigation.</summary>
    public async Task OpenListAsync()
    {
        await Content.GetByRole(AriaRole.Navigation, new() { Name = "Access" }).GetByRole(AriaRole.Link, new() { Name = "Groups", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex(@"://[^/]+/access/groups$"));
        await Expect(Content.Locator(".lt-access-count")).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>Opens <b>New group</b> and fills its id and display name.</summary>
    /// <param name="groupId">The new group's id.</param>
    /// <param name="displayName">Its display name.</param>
    public async Task FillNewGroupAsync(string groupId, string displayName)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "New group", Exact = true }).ClickAsync();
        await Expect(NewGroupDialog).ToBeVisibleAsync();
        await NewGroupDialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true }).FillAsync(groupId);
        await NewGroupDialog.GetByRole(AriaRole.Textbox, new() { Name = "Display name (optional)", Exact = true }).FillAsync(displayName);
    }

    /// <summary>Creates the group the dialog holds, expecting its page.</summary>
    /// <param name="groupId">The new group's id.</param>
    public async Task CreateAsync(string groupId)
    {
        await NewGroupDialog.GetByRole(AriaRole.Button, new() { Name = "Create group", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/access/groups/{Regex.Escape(groupId)}$"));
        await ExpectGroupAsync(groupId);
    }

    /// <summary>Expects the page of <paramref name="groupId"/>, loaded.</summary>
    /// <param name="groupId">The group id.</param>
    public async Task ExpectGroupAsync(string groupId)
    {
        await shell.ExpectHeadingAsync(groupId);
        await Expect(Members).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    /// <summary>Types <paramref name="group"/> into the member picker as a group, and waits for it to be offered.</summary>
    /// <param name="group">The group to nest.</param>
    /// <returns>The offered option.</returns>
    public async Task<ILocator> OfferGroupMemberAsync(string group)
    {
        await Members.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("group");
        await MemberBox.FillAsync(group);
        var option = Pickers.Option(Page, group);
        await Expect(option).ToHaveCountAsync(1);
        return option;
    }

    /// <summary>Chooses the offered group and adds it as a direct member.</summary>
    /// <param name="option">The offered option.</param>
    /// <param name="group">The group.</param>
    /// <param name="into">The group it is added to.</param>
    public async Task AddOfferedMemberAsync(ILocator option, string group, string into)
    {
        await option.ClickAsync();
        await Expect(MemberBox).ToHaveValueAsync(group);
        await Expect(Members.Locator(".lt-access-provenance")).ToContainTextAsync(group);
        await Members.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(shell.Toasts.Filter(new() { HasText = $"{group} added to {into}." })).ToBeVisibleAsync();
        await Expect(MemberRow(group)).ToHaveCountAsync(1);
    }

    /// <summary>The direct-member row of <paramref name="member"/>.</summary>
    /// <param name="member">The member id.</param>
    public ILocator MemberRow(string member) => Tables.Rows(Page, Members, member);

    /// <summary>Follows the link to the group <paramref name="group"/>, as a member or a parent.</summary>
    /// <param name="scope">Where the link is.</param>
    /// <param name="group">The group id.</param>
    /// <param name="text">The link's text.</param>
    public async Task FollowGroupLinkAsync(ILocator scope, string group, string text)
    {
        await scope.GetByRole(AriaRole.Link, new() { Name = text, Exact = true }).First.ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/access/groups/{Regex.Escape(group)}$"));
        await ExpectGroupAsync(group);
    }
}
