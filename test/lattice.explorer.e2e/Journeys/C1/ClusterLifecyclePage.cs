using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// The Cluster area as C1 drives it: the tree list and one tree's Lifecycle tab, where a tree
/// is soft-deleted and recovered behind a type-the-name confirmation.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class ClusterLifecyclePage(PersonaSession session)
{
    /// <summary>The toast the Lifecycle tab shows once a tree is soft-deleted.</summary>
    public const string DeletedToast = "Tree deleted. It can be recovered until its window closes.";

    /// <summary>The toast the Lifecycle tab shows once a tree is recovered.</summary>
    public const string RecoveredToast = "Tree recovered.";

    private IPage Page => session.Page;

    /// <summary>The Lifecycle tab's deletion section.</summary>
    public ILocator Deletion => Page.Locator("section[aria-label='Deletion']");

    /// <summary>The tab's state definition, such as <c>Live</c> or <c>Deleted, recoverable</c>.</summary>
    public ILocator State => Deletion.Locator(".lt-dl__row", new() { Has = Page.Locator("dt", new() { HasTextString = "State" }) }).Locator("dd");

    /// <summary>Opens the cluster's tree list filtered to <paramref name="tree"/>, and returns its row.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task<ILocator> OpenTreeListAsync(string tree)
    {
        await session.Shell.GotoAsync("cluster/trees");
        await session.Shell.ExpectHeadingAsync("Trees");
        var filter = Page.GetByLabel("Filter trees");
        await filter.FillAsync(tree);
        await filter.PressAsync("Enter");
        var table = Page.GetByRole(AriaRole.Table, new() { Name = "Trees" });
        var row = table.GetByRole(AriaRole.Row).Filter(new() { HasTextString = tree });
        await Expect(row).ToHaveCountAsync(1);
        return row;
    }

    /// <summary>Opens <paramref name="tree"/>'s page from its row in the tree list, then its Lifecycle tab.</summary>
    /// <param name="row">The tree's row.</param>
    /// <param name="tree">The tree id.</param>
    public async Task OpenLifecycleFromRowAsync(ILocator row, string tree)
    {
        await row.GetByRole(AriaRole.Link).Filter(new() { HasTextString = tree }).First.ClickAsync();
        await session.Shell.ExpectHeadingAsync(tree);
        await Page.GetByRole(AriaRole.Tab, new() { Name = "Lifecycle", Exact = true }).ClickAsync();
        await Expect(Deletion).ToBeVisibleAsync();
    }

    /// <summary>Opens <paramref name="tree"/>'s Lifecycle tab by its address.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenLifecycleAsync(string tree)
    {
        await session.Shell.GotoAsync($"cluster/trees/{tree}?tab=lifecycle");
        await session.Shell.ExpectHeadingAsync(tree);
        await Expect(Deletion).ToBeVisibleAsync();
    }

    /// <summary>Asks to delete the tree and returns the confirmation, not yet confirmed.</summary>
    public Task<ILocator> RequestDeleteAsync() => RequestAsync("Delete tree...", "Delete tree");

    /// <summary>Asks to recover the tree and returns the confirmation, not yet confirmed.</summary>
    public Task<ILocator> RequestRecoverAsync() => RequestAsync("Recover tree...", "Recover tree");

    /// <summary>Types the tree's name into a confirmation, leaving it ready to confirm.</summary>
    /// <param name="confirmation">The confirmation.</param>
    /// <param name="tree">The tree id.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    public static async Task TypeNameAsync(ILocator confirmation, string tree, string confirmText)
    {
        await confirmation.GetByLabel("Tree name").FillAsync(tree);
        await Expect(confirmation.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true })).ToBeEnabledAsync();
    }

    /// <summary>Confirms, and waits for the toast and the state that follow.</summary>
    /// <param name="confirmation">The filled confirmation.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    /// <param name="toast">The toast the action shows.</param>
    /// <param name="state">The state the tab reads afterwards.</param>
    public async Task ConfirmAsync(ILocator confirmation, string confirmText, string toast, string state)
    {
        await confirmation.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true }).ClickAsync();
        await Expect(confirmation).ToBeHiddenAsync();
        await Expect(session.Shell.Toasts.Filter(new() { HasTextString = toast })).ToBeVisibleAsync();
        await Expect(State).ToHaveTextAsync(state);
    }

    private async Task<ILocator> RequestAsync(string button, string title)
    {
        await Page.GetByRole(AriaRole.Button, new() { Name = button, Exact = true }).ClickAsync();
        var confirmation = Page.GetByRole(AriaRole.Alertdialog, new() { Name = title, Exact = true });
        await Expect(confirmation).ToBeVisibleAsync();
        return confirmation;
    }
}
