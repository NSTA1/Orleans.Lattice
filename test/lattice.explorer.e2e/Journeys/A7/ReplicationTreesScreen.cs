using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// The Replication area's enrolled-trees page, <c>/replication/trees</c>: each enrolled tree's
/// state, merge mode, enrolment source and owner, and the <b>Enable replication</b> dialog.
/// </summary>
internal sealed class ReplicationTreesScreen(ShellPage shell)
{
    /// <summary>The dialog's title, and its submit button's text.</summary>
    public const string EnableTitle = "Enable replication";

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The button that opens the dialog for a tree not yet enrolled.</summary>
    public ILocator EnableForATree => Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Enable replication for a tree", Exact = true });

    /// <summary>The <b>Enable replication</b> dialog.</summary>
    public ILocator EnableDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = EnableTitle, Exact = true });

    /// <summary>The dialog's tree picker.</summary>
    public ILocator TreeField => EnableDialog.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });

    /// <summary>The dialog's merge-mode choice.</summary>
    public ILocator MergeModeField => EnableDialog.GetByLabel("Merge mode", new() { Exact = true });

    /// <summary>The dialog's bootstrap source.</summary>
    public ILocator BootstrapField => EnableDialog.GetByRole(AriaRole.Combobox, new() { Name = "Bootstrap from cluster (optional)", Exact = true });

    /// <summary>The dialog's submit button.</summary>
    public ILocator EnableButton => EnableDialog.GetByRole(AriaRole.Button, new() { Name = EnableTitle, Exact = true });

    /// <summary>The dialog's <b>Cancel</b>.</summary>
    public ILocator CancelButton => EnableDialog.GetByRole(AriaRole.Button, new() { Name = "Cancel", Exact = true });

    /// <summary>Waits for the page to have read the enrolment.</summary>
    public async Task ExpectLoadedAsync()
    {
        await Shell.ExpectHeadingAsync("Enrolled trees");
        await Expect(Page).ToHaveURLAsync(new Regex("/replication/trees(\\?|$)"));
        await Expect(Shell.Content.GetByRole(AriaRole.Group, new() { Name = "Filter replication links" })).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>The row of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree id.</param>
    /// <param name="compact">Whether the page draws its compact chrome.</param>
    public ILocator Row(string treeId, bool compact) => ReplicationTables.Rows(Shell.Content, treeId, compact);

    /// <summary>Opens the dialog for a tree not yet enrolled.</summary>
    public async Task OpenEnableAsync()
    {
        await EnableForATree.ClickAsync();
        await Expect(EnableDialog).ToBeVisibleAsync();
    }

    /// <summary>Names <paramref name="treeId"/> in the tree picker, choosing it from the list when the list offers it.</summary>
    /// <param name="treeId">The tree id.</param>
    /// <returns>Whether the picker offered the tree.</returns>
    public async Task<bool> NameTreeAsync(string treeId)
    {
        await TreeField.FillAsync(treeId);
        var option = EnableDialog.GetByRole(AriaRole.Option).Filter(new() { HasText = treeId });
        try
        {
            await Expect(option.First).ToBeVisibleAsync(new() { Timeout = 10_000 });
        }
        catch (PlaywrightException)
        {
            await Expect(TreeField).ToHaveValueAsync(treeId);
            return false;
        }

        await option.First.ClickAsync();
        await Expect(TreeField).ToHaveValueAsync(treeId);
        return true;
    }

    /// <summary>Chooses the merge mode by the name the dialog shows, such as <c>LWW register</c>.</summary>
    /// <param name="label">The merge mode's name.</param>
    public async Task ChooseMergeModeAsync(string label)
    {
        await MergeModeField.SelectOptionAsync(new SelectOptionValue { Label = label });
        await Expect(MergeModeField.Locator("option:checked")).ToHaveTextAsync(label);
    }

    /// <summary>Names <paramref name="cluster"/> as the bootstrap source, choosing it from the suggestions when offered.</summary>
    /// <param name="cluster">The cluster (region) id.</param>
    public async Task NameBootstrapAsync(string cluster)
    {
        await BootstrapField.FillAsync(cluster);
        var option = EnableDialog.GetByRole(AriaRole.Option).Filter(new() { HasText = cluster });
        try
        {
            await Expect(option.First).ToBeVisibleAsync(new() { Timeout = 10_000 });
            await option.First.ClickAsync();
        }
        catch (PlaywrightException)
        {
            // Any id is accepted: a region the cluster does not list is used as typed.
        }

        await Expect(BootstrapField).ToHaveValueAsync(cluster);
    }
}
