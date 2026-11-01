using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U4;

/// <summary>Steps 6 to 11: every other area as a reader sees it, and nina's empty console.</summary>
public sealed partial class AuditorsReviewChapter
{
    /// <summary>What Backups says to a caller who holds no backup grant.</summary>
    private const string NoBackupGrant = "You do not hold a backup grant on this cluster. Ask an administrator for one.";

    /// <summary>What a Telemetry board says when none of its charts is admitted.</summary>
    private const string NothingOnThisBoard = "Nothing on this board is available to you";

    // U4.6: the schema is readable - policy, versions, a compliance scan, dead letters - and
    // nothing in it can be changed.
    private static async Task SchemaIsReadOnlyAsync(PersonaSession dana)
    {
        var shell = dana.Shell;
        var content = shell.Content;
        await shell.GoToAreaAsync("schema");
        var link = content.GetByRole(AriaRole.Link, new() { Name = Names.Trees.FactoryOrders, Exact = true }).First;
        await Expect(link).ToBeVisibleAsync();
        await link.ClickAsync();
        await Expect(shell.Heading).ToHaveTextAsync(Names.Trees.FactoryOrders);

        var tab = (string title) => content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });
        await Expect(tab("Policy")).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(content.GetByRole(AriaRole.Columnheader, new() { Name = "Requirement", Exact = true })).ToBeVisibleAsync();
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Edit policy", Exact = true })).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Clear policy", Exact = true })).ToHaveCountAsync(0);
        await SnapAsync(dana, "6", "policy", "factory-orders' policy, readable, with no Edit policy or Clear policy");

        await tab("Versions").ClickAsync();
        await Expect(content.GetByText("Target version", new() { Exact = true })).ToBeVisibleAsync();
        await Expect(content.Locator(".lt-schema-heading").First).ToContainTextAsync("version 2");
        await Expect(content.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^(Advance target version|Migrate stored values|Change config|Turn off versioning)") })).ToHaveCountAsync(0);
        await SnapAsync(dana, "6", "versions", "Versions: target version 2, with nothing to advance, migrate or turn off");

        await tab("Compliance").ClickAsync();
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "You may not scan this tree", Exact = true })).ToHaveCountAsync(0);
        await content.GetByRole(AriaRole.Button, new() { Name = "Scan compliance", Exact = true }).ClickAsync();
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Last scan", Exact = true })).ToBeVisibleAsync(new() { Timeout = 180_000 });
        await Expect(content.Locator(".lt-schema-result")).ToContainTextAsync(new Regex("Compliant|Non-compliant"));
        await SnapAsync(dana, "6", "compliance", "Compliance: read authority is enough to scan, and the scan changes nothing");
        Coverage.Hit("tab:schema/compliance");

        await tab("Remediation").ClickAsync();
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Start a remediation", Exact = true })).ToHaveCountAsync(0);
        await tab("Dead letters").ClickAsync();
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "No dead letters", Exact = true })).ToBeVisibleAsync();
        await SnapAsync(dana, "6", "dead-letters", "Dead letters: none, and no remediation to start");
        Coverage.Hit("use-case:schema/availability");
    }

    // U4.7: Backups is drawn, but a reader without a backup grant cannot open it.
    private static async Task BackupsAreUnavailableAsync(PersonaSession dana)
    {
        var shell = dana.Shell;
        var spine = await shell.ReadSpineAsync();
        Assert.That(spine.Unavailable, Does.Contain("backups"), $"Backups is drawn but not open: {spine}");
        await shell.GoToAreaAsync("backups");
        await Expect(shell.Content).ToContainTextAsync("Not available");
        await Expect(shell.Content).ToContainTextAsync(NoBackupGrant);
        await Expect(shell.Content.Locator("[data-lt-command=\"backups.capture\"]")).ToHaveCountAsync(0);
        await Expect(shell.Content.GetByRole(AriaRole.Link, new() { NameRegex = new Regex("Restore|Health|Schedules") })).ToHaveCountAsync(0);
        await SnapAsync(dana, "7", "unavailable", "Backups: not available - no backup grant - so no catalogue, Capture, Restore or Delete");
        Coverage.Hit("use-case:backups/availability");
    }

    // U4.8: Replication opens, but every link and enrolment is filtered to trees she may
    // manage, which is none.
    private static async Task ReplicationShowsHerNothingAsync(PersonaSession dana)
    {
        var shell = dana.Shell;
        var content = shell.Content;
        await shell.GoToAreaAsync("replication");
        await Expect(shell.Heading).ToHaveTextAsync("Replication");
        await Expect(content).ToContainTextAsync("No peer region is linked to this region");
        await SnapAsync(dana, "8", "estate", "Replication: no link she may see, since she holds no Replication grant");

        await content.Locator("[data-lt-command=\"replication.trees\"]").ClickAsync();
        await Expect(shell.Heading).ToHaveTextAsync("Enrolled trees");
        await Expect(content).ToContainTextAsync("No tree you may manage is enrolled for replication.");
        await Expect(content.GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^(Enable|Disable) replication for ") })).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Enable replication for a tree", Exact = true })).ToHaveCountAsync(0);
        await SnapAsync(dana, "8", "enrolled-trees", "Enrolled trees: none she may manage, and no Enable or Disable control for a caller without the Replication grant");
    }

    // U4.9: every Telemetry board is hers: the cluster-wide Telemetry grant and a read-all allow-list.
    private static async Task TelemetryBoardsAsync(PersonaSession dana)
    {
        var shell = dana.Shell;
        var content = shell.Content;
        await shell.GoToAreaAsync("telemetry");
        await Expect(shell.Heading).ToHaveTextAsync("Telemetry");
        foreach (var board in new[] { "Throughput", "Latency", "Storage", "Pressure" })
        {
            await Expect(content.GetByRole(AriaRole.Link, new() { Name = board, Exact = true })).ToBeVisibleAsync();
        }

        await Expect(content.GetByText(new Regex("^Not shown: "))).ToHaveCountAsync(0);
        await SnapAsync(dana, "9", "boards", "Telemetry: every board, none of its charts withheld");

        await content.GetByRole(AriaRole.Link, new() { Name = "Throughput", Exact = true }).ClickAsync();
        await Expect(shell.Heading).ToHaveTextAsync("Throughput");
        await Expect(content.Locator(".lt-telemetry-board").First).ToBeVisibleAsync();
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = NothingOnThisBoard, Exact = true })).ToHaveCountAsync(0);
        await SnapAsync(dana, "9", "throughput", "The Throughput board, drawn for her");
        Coverage.Hit("subroute:telemetry/throughput");
    }

    // U4.10: Access is hidden: no spine stop, and its address renders the not-found page. The
    // Cluster area is hers to read through the cluster-wide Telemetry capability (#5602): the
    // estate and the tree list, with no New tree.
    private static async Task ClusterAndAccessAreWithheldAsync(PersonaSession dana)
    {
        var shell = dana.Shell;
        var content = shell.Content;
        await shell.GoToAreaAsync("cluster");
        await Expect(shell.Heading).ToHaveTextAsync("Cluster");
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "Estate", Exact = true })).ToBeVisibleAsync();
        await Expect(content.GetByText("Cluster id", new() { Exact = true }).First).ToBeVisibleAsync();
        await SnapAsync(dana, "10", "cluster", "Cluster: the estate, readable through her cluster-wide Telemetry grant");

        await shell.GotoAsync("/cluster/trees");
        await Expect(shell.Heading).ToHaveTextAsync("Trees");
        await Expect(content.GetByText(Names.Trees.FactoryOrders, new() { Exact = true }).First).ToBeVisibleAsync();
        await Expect(content.Locator("[data-lt-command=\"cluster.create-tree\"]")).ToHaveCountAsync(0);
        await Expect(content.Locator("[data-lt-command=\"cluster.reshard-tree\"]")).ToHaveCountAsync(0);
        await SnapAsync(dana, "10", "cluster-trees", "Cluster trees: every tree listed, and no New tree or Reshard tree for a reader");

        await shell.ExpectAreaWithheldAsync("access");
        await shell.ExpectHeadingAsync(ShellPage.NotFoundHeading);
        await SnapAsync(dana, "10", "access", "Access, and with it Effective permissions: hidden from anyone who does not administer access");
        Coverage.Hit("use-case:access/availability");
    }

    // U4.11: nina, with no grant at all, gets a coherent console of empty states, not errors.
    private static async Task NinaFindsAnEmptyConsoleAsync(PersonaSession nina)
    {
        var shell = nina.Shell;
        var content = shell.Content;
        await shell.ExpectSignedInAsync(nina.Persona.Id);
        var spine = await shell.ReadSpineAsync();
        Assert.Multiple(() =>
        {
            Assert.That(spine.CanOpen("data") && spine.CanOpen("telemetry"), Is.True, $"nina can open Data and Telemetry, to find them empty: {spine}");
            Assert.That(spine.Unavailable, Does.Contain("backups"), $"Backups is drawn with its reason: {spine}");
            Assert.That(spine.Stops, Has.None.EqualTo("access").And.None.EqualTo("cluster").And.None.EqualTo("schema"), $"nothing she would be refused is offered: {spine}");
        });
        await SnapAsync(nina, "11", "signed-in", "nina's console: Data and the areas every signed-in user sees, Backups with its reason");

        await shell.GoToAreaAsync("data");
        await Expect(shell.Heading).ToHaveTextAsync("Data");
        await Expect(content.GetByRole(AriaRole.Heading, new() { NameRegex = new Regex("^No trees") })).ToBeVisibleAsync();
        await Expect(content).ToContainTextAsync("There is no tree or view you can reach here.");
        await Expect(content).ToContainTextAsync("you hold a grant to read it");
        await Expect(content.Locator("[data-lt-data-new-from-file]")).ToHaveCountAsync(0);
        await SnapAsync(nina, "11", "data", "Data: an empty directory that says why, and nothing to create");

        await shell.GoToAreaAsync("apps");
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "No apps yet", Exact = true })).ToBeVisibleAsync();
        await SnapAsync(nina, "11", "apps", "Apps: no app grants her a role yet");

        await shell.GoToAreaAsync("telemetry");
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = "No metrics to show", Exact = true })).ToBeVisibleAsync();
        await shell.GotoAsync("/telemetry/throughput");
        await Expect(content.GetByRole(AriaRole.Heading, new() { Name = NothingOnThisBoard, Exact = true })).ToBeVisibleAsync();
        await SnapAsync(nina, "11", "telemetry-board", "Telemetry: no metric is hers, so a board says nothing on it is available to her");

        await shell.GoToAreaAsync("backups");
        await Expect(content).ToContainTextAsync(NoBackupGrant);
        await SnapAsync(nina, "11", "backups", "Backups: the reason, and what would change it");
        Coverage.Hit("use-case:shell/no-grants-console");
    }
}
