using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>C3.5 to C3.7: replication, schedules, the baseline and a storage-side deletion.</summary>
public sealed partial class OffboardingJourney
{
    /// <summary>
    /// C3.5: olivia disables <c>factory-orders</c>' replication from the Replication area of each
    /// region's console, west and then east, since each region holds its own enrolment.
    /// </summary>
    private static async Task DisableReplicationAsync(ChapterContext context, PersonaSession olivia)
    {
        using var step = C3Step.Begin(5, "olivia disables factory-orders' replication in both regions");
        await DisableReplicationOnAsync(step, olivia, JourneyEstate.West);

        await using var oliviaEast = await context.Estate.SignInAsAsync(Personas.Olivia, region: JourneyEstate.East);
        await DisableReplicationOnAsync(step, oliviaEast, JourneyEstate.East);
        Coverage.Hit("use-case:replication/disable");
        step.Complete();
    }

    private static async Task DisableReplicationOnAsync(C3Step step, PersonaSession olivia, string region)
    {
        var tree = Names.Trees.FactoryOrders;
        await olivia.Shell.GotoAsync("/replication/trees");
        var disable = olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Disable replication for {tree}", Exact = true });
        await Expect(disable).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await step.ShotAsync(olivia, $"{region}-replicated", $"Replication in {region}: factory-orders is enrolled and enabled");

        await disable.ClickAsync();
        var confirm = new ConfirmDialog(olivia.Page, "Disable replication");
        await confirm.TypeAsync(tree);
        await step.ShotAsync(olivia, $"{region}-confirm-disable", $"Disable replication in {region}: new changes stop shipping; data on peers is kept; olivia types factory-orders");
        await confirm.ConfirmAsync("Disable replication");
        await Expect(olivia.Shell.Toasts.Filter(new() { HasTextRegex = new Regex($"^Replication (was already )?disabled for {Regex.Escape(tree)}\\.$") })).ToHaveCountAsync(1, new() { Timeout = 60_000 });
        await Expect(olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = $"Enable replication for {tree}", Exact = true })).ToBeVisibleAsync();
        await step.ShotAsync(olivia, $"{region}-disabled", $"factory-orders' replication is disabled in {region}");
    }

    /// <summary>
    /// C3.6: olivia cancels every schedule on <c>factory-telemetry</c> and <c>factory-lines</c>,
    /// then deletes <c>orders-baseline</c>, typing its name.
    /// </summary>
    private static async Task CancelSchedulesAndDeleteBaselineAsync(PersonaSession olivia)
    {
        using var step = C3Step.Begin(6, "olivia cancels the telemetry and lines schedules and deletes orders-baseline");
        await CancelSchedulesAsync(step, olivia, Names.Trees.FactoryTelemetry, "telemetry");
        await CancelSchedulesAsync(step, olivia, Names.Trees.FactoryLines, "lines");
        Coverage.Hit("use-case:backups/schedule-cancel");

        var baseline = Names.Backups.OrdersBaseline;
        await olivia.Shell.GotoAsync($"/backups?tree={Names.Trees.FactoryOrders}&name={baseline}");
        var link = olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = baseline, Exact = true });
        await Expect(link).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await link.ClickAsync();
        await olivia.Shell.ExpectHeadingAsync(baseline);
        await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Delete backup...", Exact = true }).ClickAsync();
        var confirm = new ConfirmDialog(olivia.Page, "Delete this backup?");
        await confirm.TypeAsync(baseline);
        await step.ShotAsync(olivia, "confirm-delete-baseline", "Delete this backup?: it leaves the catalogue and the store; olivia types orders-baseline");
        await confirm.ConfirmAsync("Delete");
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"Deleted backup {baseline}." })).ToHaveCountAsync(1, new() { Timeout = 60_000 });
        await olivia.Shell.GotoAsync($"/backups?tree={Names.Trees.FactoryOrders}&name={baseline}");
        await Expect(olivia.Shell.Heading).ToBeVisibleAsync();
        await Expect(olivia.Shell.Content.GetByRole(AriaRole.Link, new() { Name = baseline, Exact = true })).ToHaveCountAsync(0, new() { Timeout = 30_000 });
        await step.ShotAsync(olivia, "baseline-deleted", "orders-baseline is no longer in the catalogue");
        Coverage.Hit("use-case:backups/delete");
        step.Complete();
    }

    private static async Task CancelSchedulesAsync(C3Step step, PersonaSession olivia, string tree, string label)
    {
        await olivia.Shell.GotoAsync($"/backups/schedules?tree={tree}");
        var table = olivia.Shell.Content.GetByRole(AriaRole.Table, new() { Name = "The tree's schedules" });
        await Expect(table).ToBeVisibleAsync(new() { Timeout = 60_000 });
        var cancel = table.GetByRole(AriaRole.Button, new() { Name = "Cancel schedule", Exact = true });
        await Expect(cancel.First).ToBeVisibleAsync();
        await step.ShotAsync(olivia, $"{label}-schedules", $"Schedules of {tree}: a schedule is registered");

        var first = true;
        while (await cancel.CountAsync() > 0)
        {
            var registered = await cancel.CountAsync();
            await cancel.First.ClickAsync();
            var dialog = olivia.Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Cancel this schedule?" });
            await Expect(dialog).ToContainTextAsync(tree);
            if (first)
            {
                await step.ShotAsync(olivia, $"{label}-confirm-cancel", $"Cancel this schedule?: the schedule of {tree} stops; its backups are kept");
                first = false;
            }

            await dialog.GetByRole(AriaRole.Button, new() { Name = "Cancel schedule", Exact = true }).ClickAsync();
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Status).Filter(new() { HasTextRegex = new Regex("^Cancelled the (full|incremental) schedule\\.$") })).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await Expect(cancel).ToHaveCountAsync(registered - 1, new() { Timeout = 60_000 });
        }

        await Expect(table.GetByText("Registered", new() { Exact = true })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, $"{label}-no-schedule", $"{tree} has no schedule left");
    }

    /// <summary>
    /// C3.7: off-UI, the backup store loses <c>lines-full</c>'s manifest (the harness, as the
    /// storage incident). olivia's catalogue check finds the orphan row, she removes it, and a
    /// re-check is clean.
    /// </summary>
    private static async Task ReconcileStorageDeletionAsync(ChapterContext context, PersonaSession olivia)
    {
        using var step = C3Step.Begin(7, "A storage-side deletion is found by the catalogue check, removed, and re-checked clean");
        var linesFull = await ContractCalls.FindBackupAsync(ContractFacades.Of(context).Backup, Names.Backups.LinesFull, Names.Trees.FactoryLines)
            ?? throw new InvalidOperationException($"{Names.Backups.LinesFull} is not catalogued: C3 consumes A10's produces.");
        await context.Estate.DeleteBackupArtifactAsync(linesFull.Id);

        await olivia.Shell.GotoAsync("/backups/maintenance");
        await olivia.Shell.ExpectHeadingAsync("Maintenance");
        var check = olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Check catalogue", Exact = true });
        var lastRun = olivia.Shell.Content.Locator("section[aria-labelledby=lt-backups-scrub-title] .lt-backups-last-run a.lt-backups-link");
        await check.ClickAsync();
        await Expect(lastRun).ToHaveTextAsync(new Regex(@"^Found \d+ orphan rows?\."), new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await step.ShotAsync(olivia, "orphan-found", "Check catalogue finds the orphan row lines-full left behind");

        await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Remove orphan rows...", Exact = true }).ClickAsync();
        var confirm = new ConfirmDialog(olivia.Page, "Remove orphan rows?");
        await confirm.TypeAsync("backup catalogue");
        await step.ShotAsync(olivia, "confirm-remove-orphans", "Remove orphan rows?: the store is untouched; olivia types backup catalogue");
        await confirm.ConfirmAsync("Remove");
        await Expect(lastRun).ToHaveTextAsync(new Regex(@"^Removed \d+ orphan rows? from the catalogue\.$"), new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await step.ShotAsync(olivia, "orphans-removed", "The orphan row is removed from the catalogue");

        await check.ClickAsync();
        await Expect(lastRun).ToHaveTextAsync("Every catalogue row has its backup in the store.", new() { Timeout = (float)OperationPatience.TotalMilliseconds });
        await step.ShotAsync(olivia, "recheck-clean", "A re-check is clean: every catalogue row has its backup in the store");
        step.Complete();
    }
}
