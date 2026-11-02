using System.Text;
using Grpc.Core;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.UiTests.Accessibility;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5367: a data steward creates, edits and deletes a single entry from the
/// Data area. The editor validates JSON and previews the tree's schema policy, the
/// facade's refusal is reported, a write that lands while the editor is open is
/// shown before it is overwritten, deletion is confirmed by name, and the history
/// keeps every revision and the deletion. A caller who may only read sees no write
/// control, and a write forged on their behalf is refused by the facade.
/// </summary>
/// <remarks>
/// Every step writes a screenshot to <c>screenshots/journey-g3-entry-edit/</c>
/// under the test output, which the UI lane uploads, so the pull request walks the
/// flow from the head commit's own run.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class EntryEditJourneyTests : UiTestBase
{
    private const string ScreenshotFolder = "journey-g3-entry-edit";

    [Test]
    public async Task A_steward_creates_edits_and_deletes_an_entry_and_its_history_keeps_every_revision()
    {
        var world = await UiHosts.WorldAsync();
        var tree = "g3-orders-" + Guid.NewGuid().ToString("N")[..8];
        await world.SeedTreeAsync(tree, entries: 2);
        await world.SetPolicyAsync(tree, new LatticeSchemaPolicy([LatticeSchemaRule.Json(), LatticeSchemaRule.MaxLength(64)]));

        var page = await OpenAsync(world.Head, $"/data/{tree}", WorldIdentities.Admin);
        var content = Shell.Content(page);
        var newEntry = content.GetByRole(AriaRole.Button, new() { Name = "New entry...", Exact = true });
        await Expect(newEntry).ToHaveAttributeAsync("data-lt-command", "data.new-entry");
        await StepAsync(page, 1, "keys-with-new-entry");

        // Create: the key is new, the draft is JSON and passes the policy.
        await newEntry.ClickAsync();
        var dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "New entry" });
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Key" }).FillAsync("order/0001");
        var value = dialog.GetByRole(AriaRole.Textbox, new() { Name = "Value" });
        await value.FillAsync("{\"total\":12}");
        await Expect(dialog.Locator(".lt-data-editor__schema-ok")).ToContainTextAsync("Passes this tree's schema policy (2 rules)");
        await StepAsync(page, 2, "new-entry-dialog");
        await AxeConformance.SweepAsync(page, "the New entry dialog");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Create entry" }).ClickAsync();

        await Expect(Shell.ToastMessages(page)).ToContainTextAsync(["Entry order/0001 created."]);
        await Expect(page).ToHaveURLAsync(new System.Text.RegularExpressions.Regex("key=order%2F0001"));
        var entry = content.Locator(".lt-data-entry");
        await Expect(entry.Locator(".lt-data-value")).ToContainTextAsync("12");
        await StepAsync(page, 3, "entry-created");

        // Edit: invalid JSON, then a draft the policy refuses, which the facade refuses too.
        await entry.GetByRole(AriaRole.Button, new() { Name = "Edit", Exact = true }).ClickAsync();
        dialog = page.GetByRole(AriaRole.Dialog, new() { Name = "Edit entry" });
        value = dialog.GetByRole(AriaRole.Textbox, new() { Name = "Value" });
        await value.FillAsync("{\"total\":");
        await Expect(dialog.Locator(".lt-field__error")).ToContainTextAsync("This is not valid JSON");
        await StepAsync(page, 4, "edit-invalid-json");

        await value.FillAsync("{\"total\":15,\"note\":\"" + new string('x', 64) + "\"}");
        await Expect(dialog.Locator(".lt-data-editor__schema-fail")).ToContainTextAsync("The cluster will refuse this value");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Save", Exact = true }).ClickAsync();
        await Expect(dialog.Locator(".lt-data-editor__error")).ToContainTextAsync("rejected by this tree's schema policy");
        await StepAsync(page, 5, "edit-refused-by-schema");

        // A write lands behind the open editor: the newer value is shown, then overwritten.
        await world.Data().SetAsync(new DataSetRequest { TreeId = tree, Key = "order/0001", Value = Encoding.UTF8.GetBytes("{\"total\":40}") });
        await value.FillAsync("{\"total\":15}");
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Save", Exact = true }).ClickAsync();
        var conflict = dialog.Locator(".lt-data-editor__conflict");
        await Expect(conflict).ToContainTextAsync("This entry changed since you opened the editor.");
        await Expect(conflict.Locator("pre")).ToContainTextAsync("40");
        await StepAsync(page, 6, "edit-concurrent-change");

        await conflict.GetByRole(AriaRole.Button, new() { Name = "Overwrite with my value" }).ClickAsync();
        await Expect(Shell.ToastMessages(page)).ToContainTextAsync(["Entry order/0001 saved."]);
        await Expect(dialog).ToHaveCountAsync(0);
        await Expect(entry.Locator(".lt-data-value")).ToContainTextAsync("15");
        await StepAsync(page, 7, "entry-saved");

        // Delete, confirmed by typing the key.
        await entry.GetByRole(AriaRole.Button, new() { Name = "Delete...", Exact = true }).ClickAsync();
        var confirm = page.GetByRole(AriaRole.Alertdialog, new() { Name = "Delete this entry" });
        await Expect(confirm).ToContainTextAsync("order/0001");
        await confirm.GetByRole(AriaRole.Textbox).FillAsync("order/0001");
        await StepAsync(page, 8, "delete-confirmation");
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Delete entry" }).ClickAsync();

        await Expect(Shell.ToastMessages(page)).ToContainTextAsync(["Entry order/0001 deleted."]);
        await Expect(entry).ToContainTextAsync("No entry at this key");
        await StepAsync(page, 9, "entry-deleted");

        // The history keeps the create, both saves and the deletion.
        await entry.GetByRole(AriaRole.Link, new() { Name = "History of this key" }).ClickAsync();
        var kinds = content.Locator(".lt-data-timeline__kind");
        await Expect(kinds.First).ToHaveTextAsync("Deleted");
        Assert.That(await kinds.CountAsync(), Is.GreaterThanOrEqualTo(4), "the create, the write behind the editor, the overwrite and the deletion");
        await StepAsync(page, 10, "history-with-tombstone");
    }

    [Test]
    public async Task A_reader_sees_no_write_control_and_a_forged_write_is_refused()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}?key=machine-003", WorldIdentities.Alice);
        var content = Shell.Content(page);

        await Expect(content.Locator(".lt-data-entry .lt-data-value")).ToContainTextAsync("status-003");
        await Expect(content.Locator("[data-lt-command^=\"data.\"][data-lt-command$=\"-entry\"]")).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Edit", Exact = true })).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Delete...", Exact = true })).ToHaveCountAsync(0);
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "New entry...", Exact = true })).ToHaveCountAsync(0);
        await StepAsync(page, 11, "reader-sees-no-write-control");

        var forged = Assert.ThrowsAsync<RpcException>(() => world.Data(WorldIdentities.Alice).SetAsync(
            new DataSetRequest { TreeId = ExplorerWorld.DemoTree, Key = "machine-003", Value = Encoding.UTF8.GetBytes("forged") }));
        var probe = await world.Data(WorldIdentities.Alice).ProbeCapabilitiesAsync(new DataProbeRequest { TreeId = ExplorerWorld.DemoTree, Key = "machine-003" });

        Assert.Multiple(() =>
        {
            Assert.That(forged!.StatusCode, Is.EqualTo(StatusCode.PermissionDenied));
            Assert.That(probe.CanWrite || probe.CanDelete, Is.False);
        });
        await page.ReloadAsync();
        await Expect(content.Locator(".lt-data-entry .lt-data-value")).ToContainTextAsync("status-003");
    }

    private static async Task StepAsync(IPage page, int step, string name)
    {
        var directory = Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", ScreenshotFolder);
        System.IO.Directory.CreateDirectory(directory);
        await Shell.WaitForMotionToSettleAsync(page);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{step:D2}-{name}.png") });
    }
}
