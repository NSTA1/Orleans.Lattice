using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// <c>/cluster/orphans</c> as A12 drives it: choose a tree, audit it, show each orphaned leaf,
/// repair them, and read the audit the page runs again after the repair.
/// </summary>
/// <param name="shell">The shell of the session driving it.</param>
internal sealed class ClusterOrphansPage(ShellPage shell)
{
    /// <summary>The section holding the pass over the chosen tree.</summary>
    public ILocator Pass => shell.Content.GetByRole(AriaRole.Region, new() { NameRegex = new Regex("^Pass over ") });

    /// <summary>Opens the page with no tree chosen.</summary>
    public async Task OpenAsync()
    {
        await shell.GotoAsync("/cluster/orphans");
        await shell.ExpectHeadingAsync("Orphaned leaves");
        await Expect(shell.Content.GetByRole(AriaRole.Heading, new() { Name = "Choose a tree" })).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/orphans");
    }

    /// <summary>Names <paramref name="tree"/> in the picker.</summary>
    /// <param name="tree">The tree.</param>
    public async Task NameTreeAsync(string tree)
    {
        var picker = shell.Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await picker.FillAsync(tree);
        await picker.PressAsync("Tab");
    }

    /// <summary>Chooses the named tree, which opens its pass.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ChooseAsync(string tree)
    {
        await shell.Content.GetByRole(AriaRole.Button, new() { Name = "Choose", Exact = true }).ClickAsync();
        await Expect(shell.Page).ToHaveURLAsync(new Regex(@"/cluster/orphans\?tree=" + Regex.Escape(Uri.EscapeDataString(tree)) + "$"));
        await Expect(Pass).ToBeVisibleAsync();
        await Expect(Pass.Locator("[data-lt-cluster=audit]")).ToBeVisibleAsync();
        Coverage.Hit("query:cluster.tree");
    }

    /// <summary>Audits the tree and waits for the totals to read <paramref name="label"/>, with <paramref name="sentence"/>.</summary>
    /// <param name="label">The totals pill's text.</param>
    /// <param name="sentence">The totals sentence.</param>
    public async Task AuditAsync(string label, string sentence)
    {
        await Pass.Locator("[data-lt-cluster=audit]").ClickAsync();
        await ExpectTotalsAsync(label, sentence);
    }

    /// <summary>Waits for the pass's totals to read <paramref name="label"/>, with <paramref name="sentence"/>.</summary>
    /// <param name="label">The totals pill's text.</param>
    /// <param name="sentence">The totals sentence.</param>
    public async Task ExpectTotalsAsync(string label, string sentence)
    {
        await Expect(Pass.GetByText(label, new() { Exact = true })).ToBeVisibleAsync(ClusterParts.PatientlyVisible);
        await Expect(Pass).ToContainTextAsync(sentence, ClusterParts.PatientlyContaining);
    }

    /// <summary>Shows each orphaned leaf and asserts there is one, repairable.</summary>
    public async Task ShowEachLeafAsync()
    {
        await Pass.Locator("[data-lt-cluster=findings]").ClickAsync();
        await Expect(Pass).ToContainTextAsync("1 orphaned leaf, 1 repairable.", ClusterParts.PatientlyContaining);
        await Expect(ClusterParts.Rows(shell)).ToHaveCountAsync(1);
        await Expect(ClusterParts.Rows(shell).First).ToContainTextAsync("Repairable");
    }

    /// <summary>Opens the repair confirmation and types the tree's name.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ComposeRepairAsync(string tree)
    {
        await Pass.Locator("[data-lt-cluster=repair]").ClickAsync();
        await ClusterParts.TypeConfirmationAsync(shell, "Repair orphaned leaves", tree, "Repair");
    }

    /// <summary>Repairs, and waits for the audit the page runs afterwards to read clean.</summary>
    public async Task RepairAsync()
    {
        await ClusterParts.ConfirmAsync(shell, "Repair orphaned leaves", "Repair");
        await Expect(ClusterParts.Toast(shell, "Repair finished: 1 leaf unspliced. Auditing again.")).ToHaveCountAsync(1, new() { Timeout = (float)ClusterParts.Structural.TotalMilliseconds });
        await ExpectTotalsAsync("Clean", "No orphaned leaves, and every region was judged");
    }
}
