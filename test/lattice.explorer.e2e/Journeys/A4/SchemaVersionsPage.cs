using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A4;

/// <summary>
/// A tree's Versions tab as A4 drives it: turning versioning on, registering version
/// definitions and the migration hop between them, and refreshing the definitions.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
/// <param name="tree">The tree whose Versions tab is open.</param>
internal sealed class SchemaVersionsPage(ShellPage shell, string tree)
{
    private ILocator Content => shell.Content;

    private ILocator Panel => Content.GetByRole(AriaRole.Tabpanel);

    /// <summary>The registered versions' rows, at either width.</summary>
    private ILocator VersionRows => Panel.Locator("tr.lt-table__row, li.lt-table-list__row");

    /// <summary>Asserts the tree is unversioned, with <b>Turn on versioning</b> offered.</summary>
    public async Task ExpectUnversionedAsync()
    {
        await Expect(Panel.GetByRole(AriaRole.Complementary, new() { Name = "How versioning works", Exact = true }).Locator("dt"))
            .ToHaveTextAsync(["Stamping", "Advancing the target", "Upgrading on read", "Migrating", "Enforcing"]);
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Unversioned", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByRole(AriaRole.Button, new() { Name = "Turn on versioning", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Versions and migration paths", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByText("No versions or migration paths are registered.")).ToBeVisibleAsync();
        Coverage.Hit("tab:schema/versions");
    }

    /// <summary>Opens the <b>Turn on versioning</b> form and asserts its defaults: family 1, target 1, not strict.</summary>
    public async Task OpenTurnOnAsync()
    {
        await Panel.GetByRole(AriaRole.Button, new() { Name = "Turn on versioning", Exact = true }).ClickAsync();
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Turn on versioning", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByLabel("Schema family", new() { Exact = true })).ToHaveValueAsync("1");
        await Expect(Panel.GetByLabel("Target version", new() { Exact = true })).ToHaveValueAsync("1");
        await Expect(Panel.GetByRole(AriaRole.Switch, new() { Name = "Strict ingest", Exact = true })).ToHaveAttributeAsync("aria-checked", "false");
    }

    /// <summary>Saves the config, and asserts the tree now stamps family 1 at version 1.</summary>
    public async Task SaveConfigAsync()
    {
        await Panel.GetByRole(AriaRole.Button, new() { Name = "Save config", Exact = true }).ClickAsync();
        await Expect(shell.Toasts.Filter(new() { HasText = $"The version config of {tree} is saved." })).ToHaveCountAsync(1);
        await ExpectTargetAsync(1);
    }

    /// <summary>Asserts the config reads family 1, at target <paramref name="version"/>, strict ingest off.</summary>
    /// <param name="version">The target version.</param>
    public async Task ExpectTargetAsync(uint version)
    {
        await Expect(Definition("Schema family")).ToHaveTextAsync("1");
        await Expect(Definition("Target version")).ToHaveTextAsync(version.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Expect(Definition("Strict ingest")).ToContainTextAsync("Off:");
        await Expect(Panel.GetByRole(AriaRole.Button, new() { Name = "Advance target version...", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Opens <b>Register versions and migration paths</b> and pastes <paramref name="json"/> into the Register box.</summary>
    /// <param name="json">The registration JSON.</param>
    public async Task PasteRegistrationAsync(string json)
    {
        await Panel.Locator("summary").Filter(new() { HasText = "Register versions and migration paths" }).ClickAsync();
        var box = Panel.GetByLabel("Registration JSON", new() { Exact = true });
        await Expect(box).ToBeVisibleAsync();
        await box.FillAsync(json);
        await box.DispatchEventAsync("change");
        await Expect(box).ToHaveValueAsync(json);
    }

    /// <summary>Registers the pasted definitions.</summary>
    public async Task RegisterAsync()
    {
        await Panel.GetByRole(AriaRole.Button, new() { Name = "Register definitions", Exact = true }).ClickAsync();
        await Expect(shell.Toasts.Filter(new() { HasText = "Schema definitions are registered." })).ToHaveCountAsync(1);
        await Expect(Panel.Locator(".lt-schema-error")).ToHaveCountAsync(0);
    }

    /// <summary>Reads the definitions again, as <b>Refresh definitions</b> does.</summary>
    public async Task RefreshDefinitionsAsync()
    {
        var refresh = Panel.GetByRole(AriaRole.Button, new() { Name = "Refresh definitions", Exact = true });
        await refresh.ClickAsync();
        await Expect(refresh).ToBeEnabledAsync();
    }

    /// <summary>
    /// Asserts family 1 lists version 1 (<paramref name="v1"/>) as the target and version 2
    /// (<paramref name="v2"/>) as newer than it, with the hop from 1 to 2 setting <paramref name="member"/>.
    /// </summary>
    /// <param name="v1">Version 1's name.</param>
    /// <param name="v2">Version 2's name.</param>
    /// <param name="member">The member the hop sets.</param>
    public async Task ExpectRegisteredAsync(string v1, string v2, string member)
    {
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Family 1, which this tree stamps", Exact = true })).ToBeVisibleAsync();
        await Expect(VersionRows).ToHaveCountAsync(2);
        await Expect(VersionRows.Nth(0)).ToContainTextAsync(v1);
        await Expect(VersionRows.Nth(0)).ToContainTextAsync("Reads as stored: this is the target version");
        await Expect(VersionRows.Nth(1)).ToContainTextAsync(v2);
        await Expect(VersionRows.Nth(1)).ToContainTextAsync("Cannot be read: it is newer than the target");
        var hops = Panel.GetByRole(AriaRole.List, new() { Name = "What changes between versions of family 1", Exact = true });
        await Expect(hops.Locator(":scope > li")).ToHaveCountAsync(1);
        await Expect(hops).ToContainTextAsync("Version 1 to 2");
        await Expect(hops).ToContainTextAsync(member);
    }

    /// <summary>Records the versioning use case, once its whole flow has been asserted.</summary>
    public static void RecordVersioning() => Coverage.Hit("use-case:schema/versioning");

    private ILocator Definition(string term) =>
        Panel.Locator("dt").Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }).Locator("xpath=following-sibling::dd[1]");
}
