using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C1;

/// <summary>
/// The Schema area as C1 drives it: a tree's Versions tab, where <b>Migrate stored values</b>
/// starts a migration followed on the Remediation tab, and its Dead letters tab.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class SchemaTreePage(PersonaSession session)
{
    /// <summary>The Remediation tab's headline once the latest operation finished and its result is served.</summary>
    public const string CompletedHeadline = "The last operation finished, and the tree serves its result.";

    private static readonly TimeSpan MigrationPatience = TimeSpan.FromMinutes(10);

    private IPage Page => session.Page;

    /// <summary>The Remediation tab's background-operation section.</summary>
    public ILocator Operation => Page.GetByRole(AriaRole.Region, new() { Name = "Background operation", Exact = true });

    /// <summary>Opens one of <paramref name="tree"/>'s tabs by its address.</summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="tab">The tab's query value, such as <c>versions</c>.</param>
    /// <param name="title">The tab's title, such as <c>Versions</c>.</param>
    public async Task OpenAsync(string tree, string tab, string title)
    {
        await session.Shell.GotoAsync($"schema/{tree}?tab={tab}");
        await Expect(Page.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
    }

    /// <summary>Asks to migrate the stored values and returns the dialog, not yet started.</summary>
    /// <param name="target">The target version the dialog names.</param>
    public async Task<ILocator> RequestMigrationAsync(uint target)
    {
        await Page.GetByRole(AriaRole.Button, new() { Name = "Migrate stored values...", Exact = true }).ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Migrate stored values", Exact = true });
        await Expect(dialog).ToBeVisibleAsync();
        await Expect(dialog).ToContainTextAsync($"at the current target version, {target}, in the background");
        return dialog;
    }

    /// <summary>Starts the migration and waits on the Remediation tab for it to finish.</summary>
    /// <param name="dialog">The migration dialog.</param>
    /// <param name="target">The target version.</param>
    public async Task StartMigrationAsync(ILocator dialog, uint target)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Start migration", Exact = true }).ClickAsync();
        await Expect(dialog).ToBeHiddenAsync();
        await Expect(Operation).ToBeVisibleAsync(new() { Timeout = 60_000 });
        await Expect(Operation).ToContainTextAsync($"Migrating every value to version {target}");
    }

    /// <summary>Waits for the migration this page started to finish, and asserts its result is served.</summary>
    public async Task ExpectMigrationFinishedAsync()
    {
        await Expect(Operation.Locator("dt", new() { HasTextString = "Finished" })).ToBeVisibleAsync(new() { Timeout = (float)MigrationPatience.TotalMilliseconds });
        await Expect(Operation.GetByText(CompletedHeadline, new() { Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Loads the dead letters and returns their table.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task<ILocator> LoadDeadLettersAsync(string tree)
    {
        var load = Page.GetByRole(AriaRole.Button, new() { Name = "Load dead letters", Exact = true });
        await Expect(load).ToBeEnabledAsync();
        await load.ClickAsync();
        var table = Page.GetByRole(AriaRole.Table, new() { Name = $"Dead letters of {tree}", Exact = true });
        await Expect(table).ToBeVisibleAsync();
        return table;
    }

    /// <summary>The button that pages through more dead letters, drawn only when more remain.</summary>
    public ILocator LoadMore => Page.GetByRole(AriaRole.Button, new() { Name = "Load more dead letters", Exact = true });
}
