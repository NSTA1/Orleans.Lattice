using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// The Backups area as C1 drives it: a backup found in the catalogue, its destructive
/// point-in-time restore behind <b>Restore this backup?</b>, the restore's cluster-tracked
/// status page, and the revert staged from it behind <b>Revert this restore?</b>.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class BackupsRestorePage(PersonaSession session)
{
    /// <summary>The restore mode that replaces the tree with the backup's copy.</summary>
    public const string PointInTimeMode = "Point-in-time replace (destructive)";

    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(8);

    private IPage Page => session.Page;

    /// <summary>The cluster operation's progress, carrying its state.</summary>
    public ILocator OperationState => Page.Locator("[data-lt-operation-state]");

    /// <summary>Finds <paramref name="name"/> in the catalogue by name and opens its page.</summary>
    /// <param name="name">The backup's name.</param>
    public async Task OpenBackupAsync(string name)
    {
        await session.Shell.GotoAsync($"backups?name={Uri.EscapeDataString(name)}");
        await session.Shell.ExpectHeadingAsync("Backups");
        var table = Page.GetByRole(AriaRole.Table, new() { Name = "Backups, newest first", Exact = true });
        await table.GetByRole(AriaRole.Link, new() { Name = name, Exact = true }).First.ClickAsync();
        await session.Shell.ExpectHeadingAsync(name);
        await Expect(Page.GetByRole(AriaRole.Heading, new() { Name = "Restore", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Fills the restore form: the target tree and the point-in-time mode.</summary>
    /// <param name="target">The tree to restore into.</param>
    public async Task ChoosePointInTimeAsync(string target)
    {
        var tree = Page.GetByRole(AriaRole.Combobox, new() { Name = "Restore into tree", Exact = true });
        await tree.FillAsync(target);
        await tree.PressAsync("Tab");
        await Expect(tree).ToHaveValueAsync(target);
        await Page.GetByLabel("Restore mode", new() { Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = PointInTimeMode });
        await Expect(Page.GetByText("Every write made after the backup was taken is dropped.", new() { Exact = false }).First).ToBeVisibleAsync();
    }

    /// <summary>Asks to restore and returns <b>Restore this backup?</b>, not yet confirmed.</summary>
    public async Task<ILocator> RequestRestoreAsync()
    {
        await Page.GetByRole(AriaRole.Button, new() { Name = "Restore...", Exact = true }).ClickAsync();
        var confirmation = Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Restore this backup?", Exact = true });
        await Expect(confirmation).ToBeVisibleAsync();
        return confirmation;
    }

    /// <summary>Confirms the restore and waits for the hand-off to the cluster operation's status page.</summary>
    /// <param name="confirmation">The filled confirmation.</param>
    public async Task ConfirmRestoreAsync(ILocator confirmation)
    {
        await confirmation.GetByRole(AriaRole.Button, new() { Name = "Restore", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/operations/[^/?#]+"));
        await Expect(OperationState).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>Waits for the cluster operation to finish, and asserts it succeeded.</summary>
    public async Task ExpectSucceededAsync()
    {
        await Expect(OperationState).ToHaveAttributeAsync("data-lt-operation-state", new Regex("^(Succeeded|Failed|Cancelled)$"), new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        var state = await OperationState.GetAttributeAsync("data-lt-operation-state");
        if (state != "Succeeded")
        {
            throw new AssertionException($"The restore finished {state}: {await Page.Locator("main#lt-shell-content").InnerTextAsync()}");
        }

        await Expect(Page.GetByRole(AriaRole.Heading, new() { Name = "Revert this restore", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Asks to revert the restore and returns <b>Revert this restore?</b>, not yet confirmed.</summary>
    public async Task<ILocator> RequestRevertAsync()
    {
        await Page.GetByRole(AriaRole.Button, new() { Name = "Revert restore...", Exact = true }).ClickAsync();
        var confirmation = Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Revert this restore?", Exact = true });
        await Expect(confirmation).ToBeVisibleAsync();
        return confirmation;
    }

    /// <summary>Confirms the revert and waits for its staged operation to succeed.</summary>
    /// <param name="confirmation">The filled confirmation.</param>
    /// <param name="tree">The restored tree.</param>
    public async Task ConfirmRevertAsync(ILocator confirmation, string tree)
    {
        await confirmation.GetByRole(AriaRole.Button, new() { Name = "Revert", Exact = true }).ClickAsync();
        await session.Shell.ExpectHeadingAsync($"Revert the restore of {tree}");
        await Expect(Page.GetByText($"Reverted {tree} to the tree it held before the restore.")).ToBeVisibleAsync(new() { Timeout = 120_000 });
    }

    /// <summary>Types the tree's name into a confirmation, leaving it ready to confirm.</summary>
    /// <param name="confirmation">The confirmation.</param>
    /// <param name="tree">The tree id.</param>
    /// <param name="confirmText">The confirm button's text.</param>
    public static async Task TypeTreeAsync(ILocator confirmation, string tree, string confirmText)
    {
        await confirmation.GetByLabel("Tree name").FillAsync(tree);
        await Expect(confirmation.GetByRole(AriaRole.Button, new() { Name = confirmText, Exact = true })).ToBeEnabledAsync();
    }
}
