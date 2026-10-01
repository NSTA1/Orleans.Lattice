using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// An operator captures a backup and follows it as a cluster operation (#4122).
/// The capture is accept-then-poll: the page hands off to the cluster's operation,
/// whose status - not the circuit's - drives the page, so a reload (a new
/// circuit, as after closing the tab) still finds it, and the catalogue lists it
/// among the caller's backup and restore operations.
/// </summary>
/// <remarks>
/// Every step waits on what the page shows, never on time. The tree is the test's
/// own, so the shared world's other trees are untouched.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class BackupOperationJourneyTests : UiTestBase
{
    [Test]
    public async Task An_operator_captures_a_backup_and_still_finds_its_operation_after_a_reload()
    {
        var world = await UiHosts.WorldAsync();
        var treeId = "journey-backup-" + Guid.NewGuid().ToString("N")[..8];
        await world.SeedTreeAsync(treeId, entries: 6);

        var page = await OpenAsync(world.Head, "/backups/new", WorldIdentities.Admin);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Capture a backup");
        var content = Shell.Content(page);
        await content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }).FillAsync(treeId);
        await content.GetByRole(AriaRole.Textbox, new() { Name = "Name", Exact = true }).FillAsync("journey");
        await content.GetByRole(AriaRole.Button, new() { Name = "Capture backup" }).ClickAsync();

        // The staged page hands off to the cluster operation's own address.
        await Expect(page).ToHaveURLAsync(new Regex("/backups/operations/(?!\\d+$)[^/]+$"));
        var operationPath = new Uri(page.Url).AbsolutePath;
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Capture a full backup of " + treeId);
        await Expect(content.Locator(".lt-operation-progress")).ToHaveAttributeAsync("data-lt-operation-state", "Succeeded");
        await Expect(content.GetByRole(AriaRole.Link, new() { Name = "The captured backup" })).ToHaveCountAsync(1);

        // A reload is a new circuit: only the cluster still knows the operation.
        await Shell.GotoAsync(page, world.Head, operationPath);
        await Expect(Shell.Heading(page)).ToHaveTextAsync("Capture a full backup of " + treeId);
        await Expect(content.Locator(".lt-operation-progress")).ToHaveAttributeAsync("data-lt-operation-state", "Succeeded");

        await Shell.GotoAsync(page, world.Head, "/backups");
        var listed = content.Locator("#lt-backups-cluster-operations-title + ul li")
            .Filter(new() { HasText = "Capture a full backup of " + treeId });
        await Expect(listed).ToHaveCountAsync(1);
        await Expect(listed).ToContainTextAsync("Succeeded");
        await Expect(listed.GetByRole(AriaRole.Link)).ToHaveAttributeAsync("href", new Regex(Regex.Escape(operationPath.TrimStart('/')) + "$"));
    }
}
