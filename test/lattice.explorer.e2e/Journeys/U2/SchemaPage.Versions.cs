using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>The Versions tab: advance, migrate, change the config, and turn versioning on and off.</summary>
internal sealed partial class SchemaPage
{
    /// <summary>Asserts the tab shows the tree versioned in <paramref name="family"/> at <paramref name="target"/>.</summary>
    /// <param name="family">The schema family.</param>
    /// <param name="target">The target version.</param>
    public async Task ExpectVersionedAsync(uint family, uint target)
    {
        await Expect(Definition(Panel, "Schema family")).ToHaveTextAsync(family.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Expect(Definition(Panel, "Target version")).ToHaveTextAsync(target.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Opens <b>Advance target version...</b>, and asserts it proposes <paramref name="target"/>.</summary>
    /// <param name="target">The next version.</param>
    public async Task OpenAdvanceAsync(uint target)
    {
        await Button("Advance target version...").ClickAsync();
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Advance the target version", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByLabel("New target version", new() { Exact = true })).ToHaveValueAsync(target.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Reviews the advance: its confirmation names the version and warns of no stranded version.</summary>
    /// <param name="target">The new target version.</param>
    public async Task ReviewAdvanceAsync(uint target)
    {
        await Button("Advance...").ClickAsync();
        var dialog = AlertDialog.Filter(new() { HasText = $"Advance to version {target}" });
        await Expect(dialog).ToContainTextAsync("The target version cannot be lowered again.");
        await Expect(dialog.Locator(".lt-schema-error")).ToHaveCountAsync(0);
    }

    /// <summary>Opens <b>Migrate stored values...</b> and its review.</summary>
    /// <param name="target">The version the review says values are rewritten at.</param>
    /// <returns>The review dialog.</returns>
    public async Task<ILocator> ReviewMigrationAsync(uint target)
    {
        await Button("Migrate stored values...").ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Migrate stored values", Exact = true });
        await Expect(dialog).ToContainTextAsync($"Every stored value is rewritten at the current target version, {target}, in the background.");
        return dialog;
    }

    /// <summary>Opens <b>Change config</b> on a versioned tree, and asserts it is filled with the current config.</summary>
    /// <param name="family">The current family.</param>
    /// <param name="target">The current target.</param>
    public async Task OpenChangeConfigAsync(uint family, uint target)
    {
        await Button("Change config").ClickAsync();
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Change the version config", Exact = true })).ToBeVisibleAsync();
        await Expect(Panel.GetByLabel("Schema family", new() { Exact = true })).ToHaveValueAsync(family.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Expect(Panel.GetByLabel("Target version", new() { Exact = true })).ToHaveValueAsync(target.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Opens <b>Turn on versioning</b> on an unversioned tree, and fills in the config.</summary>
    /// <param name="family">The family.</param>
    /// <param name="target">The target version.</param>
    public async Task OpenTurnOnVersioningAsync(uint family, uint target)
    {
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Unversioned", Exact = true })).ToBeVisibleAsync();
        await Button("Turn on versioning").ClickAsync();
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Turn on versioning", Exact = true })).ToBeVisibleAsync();
        await Panel.GetByLabel("Schema family", new() { Exact = true }).FillAsync(family.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Panel.GetByLabel("Target version", new() { Exact = true }).FillAsync(target.ToString(System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>Saves the version config being edited.</summary>
    /// <param name="tree">The tree.</param>
    public async Task SaveConfigAsync(string tree)
    {
        await Button("Save config").ClickAsync();
        await ExpectToastAsync($"The version config of {tree} is saved.");
        await Expect(Button("Save config")).ToHaveCountAsync(0);
    }

    /// <summary>Turns versioning off through its destructive confirmation, and asserts the tree is unversioned.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="beforeConfirm">Called with the confirmation filled in, before confirming.</param>
    public async Task TurnOffVersioningAsync(string tree, Func<Task> beforeConfirm)
    {
        await Button("Turn off versioning").ClickAsync();
        await ConfirmAsync("Turn off versioning", tree, "Turn off versioning", beforeConfirm);
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "Unversioned", Exact = true })).ToBeVisibleAsync();
    }
}
