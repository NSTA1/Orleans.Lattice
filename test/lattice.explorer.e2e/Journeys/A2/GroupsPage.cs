using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// The Access area's group pages (<c>/access/groups</c> and <c>/access/groups/{id}</c>) as
/// olivia drives them: the paged catalogue, the New group dialog, a group's name and its
/// direct members. Each method records its coverage hits only after its assertion holds.
/// </summary>
/// <param name="session">The session driving the pages.</param>
internal sealed class GroupsPage(PersonaSession session)
{
    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    /// <summary>The catalogue's count line, such as <c>200 groups, more to load</c>.</summary>
    public ILocator Count => Content.Locator(".lt-access-count[role=status]");

    /// <summary>The New group dialog.</summary>
    public ILocator CreateDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "New group" });

    /// <summary>The group page's direct members: the table, or its empty state for a group with none.</summary>
    public ILocator Members => Content.Locator("section[aria-labelledby=lt-access-group-members]").Locator(".lt-table-frame, .lt-table-list, .lt-table-empty");

    /// <summary>The member picker's id field.</summary>
    public ILocator MemberBox => Content.GetByRole(AriaRole.Combobox, new() { Name = "Member", Exact = true });

    /// <summary>The member picker's field error.</summary>
    public ILocator MemberError => Content.Locator("[data-lt-subject-picker] .lt-field__error");

    /// <summary>
    /// Opens the group catalogue from the Access area's own navigation and waits for its first
    /// page. The spine opens Access at the active tenant, where Groups only says groups belong
    /// to the whole cluster, so olivia follows its link to the cluster's Groups page.
    /// </summary>
    public async Task OpenAsync()
    {
        await Content.GetByRole(AriaRole.Navigation, new() { Name = "Access" }).GetByRole(AriaRole.Link, new() { Name = "Groups", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/access/groups$"));
        if (new Regex("/t/[^/]+/access/groups$").IsMatch(Page.Url))
        {
            var caveat = Content.Locator("[data-lt-cluster-wide-groups]");
            await Expect(caveat).ToContainTextAsync("Groups belong to the whole cluster");
            await caveat.GetByRole(AriaRole.Link, new() { Name = "the cluster's Groups page", Exact = true }).ClickAsync();
            await Expect(Page).ToHaveURLAsync(new Regex(@"://[^/]+/access/groups$"));
        }

        await Expect(Count).ToBeVisibleAsync(new() { Timeout = 60_000 });
        Coverage.Hit("route:/access/groups");
        Coverage.Hit("subroute:access/groups");
    }

    /// <summary>Asserts the count line reads <paramref name="expected"/>.</summary>
    /// <param name="expected">The count line.</param>
    public Task ExpectCountAsync(Regex expected) => Expect(Count).ToHaveTextAsync(expected);

    /// <summary>Loads the catalogue's next page with <b>Load more groups</b>, and waits until no more remain.</summary>
    /// <param name="total">The groups the catalogue then holds, at least.</param>
    public async Task LoadMoreAsync(int total)
    {
        var more = Content.GetByRole(AriaRole.Button, new() { Name = "Load more groups", Exact = true });
        await more.ClickAsync();
        await Expect(more).ToBeHiddenAsync();
        await Expect(Count).Not.ToContainTextAsync("more to load");
        var loaded = int.Parse(Regex.Match(await Count.InnerTextAsync(), @"(\d+) groups").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
        Assert.That(loaded, Is.GreaterThanOrEqualTo(total), "every directory-synced group is listed once the last page is loaded");
        Coverage.Hit("use-case:access/group-pagination");
    }

    /// <summary>Opens the New group dialog through the command palette's <b>Create a group</b>.</summary>
    public async Task OpenCreateFromPaletteAsync()
    {
        await session.Shell.RunCommandAsync("Create a group");
        await Expect(Page).ToHaveURLAsync(new Regex(@"/access/groups\?new=true$"));
        await Expect(CreateDialog).ToBeVisibleAsync();
        Coverage.Hit("command:access.create-group");
        Coverage.Hit("query:access.new");
        Coverage.Hit("query-value:access.new=true");
    }

    /// <summary>Opens the New group dialog with the catalogue's <b>New group</b> button.</summary>
    public async Task OpenCreateAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "New group", Exact = true }).ClickAsync();
        await Expect(CreateDialog).ToBeVisibleAsync();
    }

    /// <summary>Fills the New group dialog.</summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="displayName">The display name, or <see langword="null"/> to leave it empty.</param>
    public async Task FillCreateAsync(string groupId, string? displayName)
    {
        var id = CreateDialog.GetByRole(AriaRole.Textbox, new() { Name = "Group id", Exact = true });
        await id.FillAsync(groupId);
        var name = CreateDialog.GetByRole(AriaRole.Textbox, new() { Name = "Display name (optional)", Exact = true });
        await name.FillAsync(displayName ?? string.Empty);
        await Expect(id).ToHaveValueAsync(groupId);
        await Expect(CreateDialog.Locator(".lt-field__error")).ToHaveCountAsync(0);
    }

    /// <summary>Creates the group the dialog holds and waits on its page.</summary>
    /// <param name="groupId">The group id.</param>
    public async Task SubmitCreateAsync(string groupId)
    {
        await CreateDialog.GetByRole(AriaRole.Button, new() { Name = "Create group", Exact = true }).ClickAsync();
        await ExpectGroupPageAsync(groupId);
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"Group {groupId} created.");
    }

    /// <summary>Waits on the page of <paramref name="groupId"/>.</summary>
    /// <param name="groupId">The group id.</param>
    public async Task ExpectGroupPageAsync(string groupId)
    {
        await Expect(Page).ToHaveURLAsync(new Regex($"/access/groups/{Regex.Escape(groupId)}$"));
        await session.Shell.ExpectHeadingAsync(groupId);
        await Expect(Members).ToBeVisibleAsync();
        Coverage.Hit("route:/access/groups/{p1}");
    }

    /// <summary>
    /// Types a member id the identity directory does not hold and asks to add it: the picker
    /// refuses it beside the field, and nothing is added.
    /// </summary>
    /// <param name="mistyped">The mistyped id.</param>
    public async Task AddMistypedUserAsync(string mistyped)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("user");
        await MemberBox.FillAsync(mistyped);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await Expect(MemberError).ToContainTextAsync(mistyped);
        await Expect(MemberBox).ToHaveAttributeAsync("aria-invalid", "true");
        await Expect(Members).Not.ToContainTextAsync(mistyped);
        Coverage.Hit("use-case:access/subject-picker");
    }

    /// <summary>Chooses a user from the directory in the member picker and adds them.</summary>
    /// <param name="groupId">The group the page shows.</param>
    /// <param name="user">The user id.</param>
    /// <param name="chosen">Runs once the user is chosen, before it is added: its screenshot.</param>
    public async Task AddUserAsync(string groupId, string user, Func<Task>? chosen = null)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Member kind", Exact = true }).SelectOptionAsync("user");
        await A2Pickers.ChooseAsync(Page, MemberBox, user);
        await Expect(MemberError).ToHaveCountAsync(0);
        if (chosen is not null)
        {
            await chosen();
        }

        await Content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true }).ClickAsync();
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"{user} added to {groupId}.");
        await Expect(Members).ToContainTextAsync(user);
        await Expect(MemberBox).ToHaveValueAsync(string.Empty);
        Coverage.Hit("use-case:access/group-create-rename-members");
    }

    /// <summary>Renames the group with <b>Save name</b>.</summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="displayName">The new display name.</param>
    /// <param name="typed">Runs once the name is typed, before it is saved: its screenshot.</param>
    public async Task RenameAsync(string groupId, string displayName, Func<Task>? typed = null)
    {
        await Content.GetByRole(AriaRole.Textbox, new() { Name = "Display name", Exact = true }).FillAsync(displayName);
        if (typed is not null)
        {
            await typed();
        }

        await Content.GetByRole(AriaRole.Button, new() { Name = "Save name", Exact = true }).ClickAsync();
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"Group {groupId} renamed.");
        await Expect(Content.Locator("p.lt-shell-page-lede")).ToHaveTextAsync(displayName);
        Coverage.Hit("use-case:access/group-create-rename-members");
    }

    /// <summary>Removes a direct member through the destructive confirmation.</summary>
    /// <param name="groupId">The group id.</param>
    /// <param name="member">The member to remove.</param>
    /// <param name="armed">Runs once the confirmation is armed: its screenshot.</param>
    public async Task RemoveMemberAsync(string groupId, string member, Func<Task> armed)
    {
        if (await session.Shell.IsCompactAsync())
        {
            await Members.GetByRole(AriaRole.Button).Filter(new() { HasText = member }).First.ClickAsync();
            var sheet = Page.GetByRole(AriaRole.Dialog, new() { Name = member, Exact = true });
            await sheet.GetByRole(AriaRole.Button, new() { Name = "Remove member", Exact = true }).ClickAsync();
        }
        else
        {
            await Members.GetByRole(AriaRole.Button, new() { Name = $"Remove {member}", Exact = true }).ClickAsync();
        }

        await A2Pickers.ConfirmDestructiveAsync(Page, "Remove this member", "Member name", member, "Remove member", armed);
        await A2Pickers.ExpectToastAsync(session.Shell.Toasts, $"{member} removed from {groupId}.");
        await Expect(Members).Not.ToContainTextAsync(member);
        Coverage.Hit("use-case:access/group-create-rename-members");
    }
}
