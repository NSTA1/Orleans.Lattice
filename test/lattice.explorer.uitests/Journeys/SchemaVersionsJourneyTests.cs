using Microsoft.Extensions.DependencyInjection;
using Microsoft.Playwright;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5234: the schema Versions tab against a cluster that installs enforcement,
/// versioning and the runtime catalog, end to end through the real schema facade. An
/// administrator turns versioning on, registers a version and the migration hop to it,
/// reads what the hop changes and how each version reads, and advances the target; a
/// caller with no schema authority who follows a direct link sees no area at all.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class SchemaVersionsJourneyTests : UiTestBase
{
    [Test]
    public async Task An_administrator_registers_a_version_reads_what_it_changes_and_advances_to_it()
    {
        var world = await UiHosts.WorldAsync();
        await ResetAsync(world);
        var page = await OpenAsync(world.Head, $"/schema/{ExplorerWorld.VersionsTree}?tab=versions", WorldIdentities.Admin);
        var content = Shell.Content(page);

        await Expect(content.Locator("aside[aria-label='How versioning works'] dt")).ToHaveTextAsync(
            ["Stamping", "Advancing the target", "Upgrading on read", "Migrating", "Enforcing"]);

        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Turn on versioning", Exact = true }), "Enter");
        await content.GetByRole(AriaRole.Button, new() { Name = "Save config", Exact = true }).ClickAsync();
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"The version config of {ExplorerWorld.VersionsTree} is saved." })).ToHaveCountAsync(1);

        await content.Locator("summary").Filter(new() { HasText = "Register versions and migration paths" }).ClickAsync();
        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Register definitions", Exact = true }), "Enter");
        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = "Schema definitions are registered." })).ToHaveCountAsync(1);

        var hops = content.GetByRole(AriaRole.List, new() { Name = "What changes between versions of family 1", Exact = true });
        await Expect(hops.Locator(".lt-schema-hops__label")).ToHaveTextAsync(["Version 1 to 2"]);
        await Expect(hops.Locator("ul .lt-schema-hops__step")).ToHaveTextAsync(["Renames qty to quantity."]);
        var versions = content.Locator("table").Filter(new() { HasText = "Renamed quantity" }).Locator("tbody tr");
        await Expect(versions).ToHaveCountAsync(2);
        await Expect(versions.Nth(0)).ToContainTextAsync("Reads as stored: this is the target version");
        await Expect(versions.Nth(1)).ToContainTextAsync("Cannot be read: it is newer than the target");

        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Advance target version...", Exact = true }), "Enter");
        await Expect(content.GetByLabel("New target version", new() { Exact = true })).ToHaveValueAsync("2");
        await PressAsync(content.GetByRole(AriaRole.Button, new() { Name = "Advance...", Exact = true }), "Enter");
        var confirm = page.GetByRole(AriaRole.Alertdialog);
        await Expect(confirm).ToContainTextAsync("Materialised views over this tree are not refreshed");
        await Expect(confirm.Locator(".lt-schema-error")).ToHaveCountAsync(0);
        await confirm.GetByLabel("Tree name", new() { Exact = true }).FillAsync(ExplorerWorld.VersionsTree);
        await PressAsync(confirm.GetByRole(AriaRole.Button, new() { Name = "Advance", Exact = true }), "Enter");

        await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"{ExplorerWorld.VersionsTree} now stamps new writes with version 2." })).ToHaveCountAsync(1);
        await Expect(versions.Nth(0)).ToContainTextAsync("Upgraded as it is read, through versions 1, 2");
        await Expect(versions.Nth(1)).ToContainTextAsync("Reads as stored: this is the target version");

        var applied = await Admin(world).GetVersionConfigAsync(ExplorerWorld.VersionsTree);
        Assert.That(applied?.TargetVersion, Is.EqualTo(2u));
        await ResetAsync(world);
    }

    [Test]
    public async Task A_caller_without_schema_authority_following_a_direct_link_sees_no_schema_area()
    {
        var world = await UiHosts.WorldAsync();
        var page = await OpenAsync(world.Head, $"/schema/{ExplorerWorld.VersionsTree}?tab=versions", WorldIdentities.Carol);

        await Expect(Shell.Heading(page)).ToHaveTextAsync(ExplorerAreas.NotFoundHeading);
        await Expect(Shell.Content(page).GetByRole(AriaRole.Tab, new() { Name = "Versions" })).ToHaveCountAsync(0);
        await Expect(Shell.Content(page).GetByText("Versions and migration paths")).ToHaveCountAsync(0);
    }

    private static ILatticeSchemaVersionAdmin Admin(ExplorerWorld world) => world.Head.Services.GetRequiredService<ILatticeSchemaVersionAdmin>();

    private static Task ResetAsync(ExplorerWorld world) => Admin(world).ClearVersionConfigAsync(ExplorerWorld.VersionsTree);

    private static async Task PressAsync(ILocator control, string key)
    {
        await Expect(control).ToBeVisibleAsync();
        await control.FocusAsync();
        await control.PressAsync(key);
    }
}
