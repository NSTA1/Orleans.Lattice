using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// WAL placement at <c>/cluster/wal</c>: a tree's audit and partitions table, then a move
/// plan at its own address (<c>?tree=&amp;partition=&amp;target=</c>), the move, and the reclaim
/// of the source it leaves behind.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class ClusterWalPage(ShellPage shell) : A8PageObject(shell)
{
    /// <summary>What the page says when a move fails.</summary>
    public const string MoveFailedText = "The WAL move failed.";

    /// <summary>The "Plan a WAL move" dialog.</summary>
    public ILocator PlanDialog => Page.GetByRole(AriaRole.Dialog, new() { Name = "Plan a WAL move", Exact = true });

    /// <summary>The audit section.</summary>
    public ILocator Audit => Content.Locator("section").Filter(new() { Has = Page.Locator("#lt-cluster-wal-audit") });

    /// <summary>The move plan section.</summary>
    public ILocator Plan => Content.Locator("section").Filter(new() { Has = Page.Locator("#lt-cluster-wal-plan") });

    /// <summary>The receipt of the last move or reclaim.</summary>
    public ILocator Receipt => Content.Locator(".lt-cluster-result");

    /// <summary>The "Move partition..." button.</summary>
    public ILocator Execute => Content.Locator("button[data-lt-cluster=\"execute\"]");

    /// <summary>The "Reclaim the source..." button.</summary>
    public ILocator Reclaim => Content.Locator("button[data-lt-cluster=\"reclaim\"]");

    /// <summary>Sees the page with no tree chosen.</summary>
    public async Task ExpectChooseTreeAsync()
    {
        await Expect(Page).ToHaveURLAsync(new Regex("/cluster/wal(\\?|$)"));
        await Shell.ExpectHeadingAsync("WAL placement");
        await Expect(Content.GetByText("Name a tree above to audit its WAL placement.")).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/wal");
    }

    /// <summary>Names <paramref name="tree"/> and audits it: the partitions table, every key resolving.</summary>
    /// <param name="tree">The tree.</param>
    public async Task AuditAsync(string tree)
    {
        var box = Content.GetByRole(AriaRole.Form, new() { Name = "Choose a tree" }).GetByRole(AriaRole.Combobox, new() { Name = "Tree" });
        await box.FillAsync(tree);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Audit", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/cluster/wal\\?(.*&)?tree=" + Regex.Escape(tree) + "(&|$)"));
        await Expect(Audit.Locator("h2")).ToHaveTextAsync("Audit of " + tree);
        await Expect(Audit).ToContainTextAsync("Every key resolves");
        await Expect(Audit).ToContainTextAsync(new Regex(@"\d+ partitions, placement version [\d,]+\."));
        Coverage.Hit("query:cluster.tree");
    }

    /// <summary>The provider key the audit shows for <paramref name="partition"/> (desktop table only; <see langword="null"/> when compact).</summary>
    /// <param name="partition">The partition.</param>
    public async Task<string?> PartitionKeyAsync(int partition)
    {
        if (await Shell.IsCompactAsync())
        {
            return null;
        }

        var row = Audit.Locator("tbody tr").Filter(new() { Has = Page.Locator("th", new() { HasTextRegex = new Regex("^" + partition + "$") }) });
        return (await row.Locator("td").First.TextContentAsync())?.Trim();
    }

    /// <summary>Opens the plan dialog from "Plan a move...", and fills it.</summary>
    /// <param name="tree">The tree, which the dialog takes from the audit.</param>
    /// <param name="partition">The partition.</param>
    /// <param name="target">The target provider key.</param>
    public async Task ComposePlanAsync(string tree, int partition, string target)
    {
        await Content.Locator("button[data-lt-command=\"cluster.plan-wal-move\"]").ClickAsync();
        await Expect(PlanDialog).ToBeVisibleAsync();
        await Expect(PlanDialog.GetByRole(AriaRole.Combobox, new() { Name = "Tree" })).ToHaveValueAsync(tree);
        await PlanDialog.GetByLabel("Partition").FillAsync(partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var key = PlanDialog.GetByRole(AriaRole.Combobox, new() { Name = "Target provider key" });
        await key.FillAsync(target);
        await Expect(key).ToHaveValueAsync(target);
    }

    /// <summary>Plans the move and sees the read-only preview at the plan's own address.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="partition">The partition.</param>
    /// <param name="target">The target provider key.</param>
    public async Task PlanAsync(string tree, int partition, string target)
    {
        await PlanDialog.GetByRole(AriaRole.Button, new() { Name = "Plan", Exact = true }).ClickAsync();
        await Expect(PlanDialog).ToBeHiddenAsync();
        await Expect(Page).ToHaveURLAsync(new Regex($"/cluster/wal\\?(.*&)?partition={partition}(&|$)"));
        await Expect(Page).ToHaveURLAsync(new Regex($"[?&]target={Regex.Escape(target)}(&|$)"));
        await Expect(Page).ToHaveURLAsync(new Regex($"[?&]tree={Regex.Escape(tree)}(&|$)"));
        await Expect(Plan.Locator("h2")).ToHaveTextAsync($"Move plan: partition {partition} to {target}");
        await Expect(Term("To")).ToHaveTextAsync(target);
        await Expect(Term("Target resolves here")).ToHaveTextAsync("Yes");
        await Expect(Term("Entries to copy")).ToHaveTextAsync(new Regex(@"^[\d,]+$"));
        await Expect(Execute).ToBeEnabledAsync();
        Coverage.Hit("command:cluster.plan-wal-move");
        Coverage.Hit("query:cluster.partition");
        Coverage.Hit("query:cluster.target");
    }

    /// <summary>Presses "Move partition..." and types the tree's name.</summary>
    /// <param name="tree">The tree.</param>
    public async Task<ConfirmDialog> AskToMoveAsync(string tree)
    {
        await Execute.ClickAsync();
        var dialog = new ConfirmDialog(Page, "Move WAL partition");
        await dialog.TypeAsync(tree, "Move");
        return dialog;
    }

    /// <summary>
    /// Confirms the move and follows it on the cluster to its end.
    /// </summary>
    /// <param name="dialog">The open confirmation.</param>
    /// <returns><see langword="true"/> when the page shows the move's receipt; <see langword="false"/> when it says the move failed.</returns>
    public async Task<bool> MoveAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Move");
        var moved = Receipt.Filter(new() { HasTextRegex = new Regex("^Moved: ") });
        var failed = Toast(MoveFailedText);
        await Expect(moved.Or(failed).First).ToBeVisibleAsync(new() { Timeout = (float)Structural.TotalMilliseconds });
        if (await moved.IsVisibleAsync())
        {
            await Expect(moved).ToContainTextAsync("The source is retained until you reclaim it.");
            await Expect(Reclaim).ToBeEnabledAsync();
            return true;
        }

        await Expect(Execute).ToBeEnabledAsync(new() { Timeout = 60_000 });
        return false;
    }

    /// <summary>Presses "Reclaim the source..." and types the tree's name.</summary>
    /// <param name="tree">The tree.</param>
    public async Task<ConfirmDialog> AskToReclaimAsync(string tree)
    {
        await Reclaim.ClickAsync();
        var dialog = new ConfirmDialog(Page, "Reclaim WAL source");
        await dialog.TypeAsync(tree, "Reclaim");
        return dialog;
    }

    /// <summary>Confirms the reclaim and sees its receipt: the move can no longer be reverted.</summary>
    /// <param name="dialog">The open confirmation.</param>
    public async Task ReclaimAsync(ConfirmDialog dialog)
    {
        await dialog.ConfirmAsync("Reclaim");
        await Expect(Receipt).ToContainTextAsync("was reclaimed. The move can no longer be reverted.", new() { Timeout = 120_000 });
        await Expect(Toast("WAL source reclaimed.").First).ToBeVisibleAsync();
        await Expect(Reclaim).ToHaveCountAsync(0);
        Coverage.Hit("use-case:cluster/wal-plan-move-reclaim");
    }
}
