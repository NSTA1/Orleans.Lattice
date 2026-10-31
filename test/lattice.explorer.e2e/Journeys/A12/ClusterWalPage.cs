using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// <c>/cluster/wal</c> as A12 drives it: a tree's placement audit, a planned move stopped
/// before its flip, and the WAL reclamation verdict.
/// </summary>
/// <param name="shell">The shell of the session driving it.</param>
internal sealed class ClusterWalPage(ShellPage shell)
{
    /// <summary>The placement audit of the tree on screen.</summary>
    public ILocator Audit => shell.Content.GetByRole(AriaRole.Region, new() { NameRegex = new Regex("^Audit of ") });

    /// <summary>The move plan.</summary>
    public ILocator Plan => shell.Content.GetByRole(AriaRole.Region, new() { NameRegex = new Regex("^Move plan: ") });

    /// <summary>The running move's progress.</summary>
    public ILocator Progress => shell.Content.Locator("[data-lt-cluster=move-progress]");

    /// <summary>The WAL reclamation verdict: its pill and the sentence that explains it.</summary>
    public ILocator Verdict => shell.Content.Locator("[data-lt-cluster=wal-reclamation-verdict]");

    /// <summary>Opens the WAL page of <paramref name="tree"/> and waits for its audit and reclamation.</summary>
    /// <param name="tree">The tree.</param>
    public async Task OpenAsync(string tree)
    {
        await shell.GotoAsync($"/cluster/wal?tree={Uri.EscapeDataString(tree)}");
        await shell.ExpectHeadingAsync("WAL placement");
        await Expect(Audit).ToBeVisibleAsync();
        await Expect(Verdict).ToBeVisibleAsync();
        Coverage.Hit("subroute:cluster/wal");
        Coverage.Hit("query:cluster.tree");
    }

    /// <summary>The placement audit's text: every partition's provider key and the placement version.</summary>
    public async Task<string> AuditTextAsync() => (await Audit.InnerTextAsync()).Trim();

    /// <summary>Plans a move of <paramref name="partition"/> of the tree on screen to <paramref name="target"/>.</summary>
    /// <param name="partition">The partition.</param>
    /// <param name="target">The target provider key.</param>
    public async Task PlanAsync(int partition, string target)
    {
        await shell.Content.Locator("[data-lt-command=\"cluster.plan-wal-move\"]").ClickAsync();
        var dialog = shell.Page.GetByRole(AriaRole.Dialog, new() { Name = "Plan a WAL move" });
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.GetByLabel("Partition").FillAsync(partition.ToString(System.Globalization.CultureInfo.InvariantCulture));
        var key = dialog.GetByRole(AriaRole.Combobox, new() { Name = "Target provider key", Exact = true });
        await key.FillAsync(target);
        await key.PressAsync("Tab");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Plan", Exact = true }).ClickAsync();
        await Expect(Plan).ToBeVisibleAsync();
        await Expect(Plan.GetByRole(AriaRole.Heading)).ToHaveTextAsync($"Move plan: partition {partition} to {target}");
        await Expect(ClusterParts.Term(shell, "Target resolves here")).ToHaveTextAsync("Yes");
        await Expect(shell.Page).ToHaveURLAsync(new Regex("[?&]partition=" + partition + "(&|$)"));
        Coverage.Hit("command:cluster.plan-wal-move");
        Coverage.Hit("query:cluster.partition");
        Coverage.Hit("query:cluster.target");
    }

    /// <summary>Opens the move confirmation and types the tree's name.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ComposeMoveAsync(string tree)
    {
        await Plan.Locator("[data-lt-cluster=execute]").ClickAsync();
        await ClusterParts.TypeConfirmationAsync(shell, "Move WAL partition", tree, "Move");
    }

    /// <summary>Starts the move and waits for it to be running, with <b>Stop</b> offered.</summary>
    public async Task StartMoveAsync()
    {
        await ClusterParts.ConfirmAsync(shell, "Move WAL partition", "Move");
        await Expect(Progress).ToBeVisibleAsync();
        await Expect(Progress.Locator("[data-lt-operation-state=Running]")).ToBeVisibleAsync(ClusterParts.PatientlyVisible);
        await Expect(Progress.Locator("[data-lt-cluster=stop-move]")).ToBeEnabledAsync();
        await Expect(shell.Content).ToContainTextAsync("Stopping is honoured only before the placement flips.");
    }

    /// <summary>Stops the move and waits for it to read stopped, the partition left on its source.</summary>
    public async Task StopMoveAsync()
    {
        await Progress.Locator("[data-lt-cluster=stop-move]").ClickAsync();
        await Expect(Progress.Locator("[data-lt-operation-state=Cancelled]")).ToBeVisibleAsync(ClusterParts.PatientlyVisible);
        await Expect(Progress.Locator("[data-lt-cluster=stop-move]")).ToHaveCountAsync(0);
        await Expect(ClusterParts.Toast(shell, "The move was stopped before its flip: the partition stays on its source.")).ToHaveCountAsync(1);
    }

    /// <summary>Waits for the reclamation verdict to read <paramref name="label"/>, explained by <paramref name="explanation"/>.</summary>
    /// <param name="label">The verdict pill's text.</param>
    /// <param name="explanation">A part of the sentence that explains it.</param>
    public async Task ExpectVerdictAsync(string label, string explanation)
    {
        await Expect(Verdict.GetByText(label, new() { Exact = true })).ToBeVisibleAsync();
        await Expect(Verdict).ToContainTextAsync(explanation);
    }
}
