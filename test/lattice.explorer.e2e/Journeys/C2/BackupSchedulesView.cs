using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>The Backups area's Schedules page as C2 drives it: one tree's Full schedule, read and changed.</summary>
/// <param name="session">The persona's session.</param>
internal sealed class BackupSchedulesView(PersonaSession session)
{
    private IPage Page => session.Page;

    private ILocator Content => session.Shell.Content;

    /// <summary>The Full schedule's row.</summary>
    public ILocator FullRow => Content.Locator("table tbody tr").Filter(new() { Has = Page.Locator("th, td").GetByText("Full", new() { Exact = true }) });

    /// <summary>Opens the Schedules page for <paramref name="tree"/>: Backups, then <b>Schedules</b>, then the tree.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenAsync(string tree)
    {
        await session.Shell.GoToAreaAsync("backups");
        await Content.GetByRole(AriaRole.Link, new() { Name = "Schedules", Exact = true }).First.ClickAsync();
        await session.Shell.ExpectHeadingAsync("Schedules");
        await ShowAsync(tree);
    }

    /// <summary>Shows <paramref name="tree"/>'s schedules again, which reads their status afresh.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task ShowAsync(string tree)
    {
        var box = Content.GetByLabel("Tree", new() { Exact = true });
        await box.FillAsync(tree);
        var option = Page.GetByRole(AriaRole.Option, new() { Name = tree, Exact = true });
        if (await WaitVisibleAsync(option, TimeSpan.FromSeconds(5)))
        {
            await option.First.ClickAsync();
        }

        await Content.GetByRole(AriaRole.Button, new() { Name = "Show schedules", Exact = true }).ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Schedules of" })).ToContainTextAsync(tree, new() { Timeout = 30_000 });
        await Expect(FullRow).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    /// <summary>The Full row's cells, by column: Kind, Schedule, Interval, Last run, Last success.</summary>
    public async Task<IReadOnlyList<string>> FullCellsAsync() =>
        [.. (await FullRow.First.Locator("th, td").AllInnerTextsAsync()).Select(text => text.Trim())];

    /// <summary>Registers the Full schedule at <paramref name="every"/> through the form, and waits for its confirmation.</summary>
    /// <param name="every">The interval, in whole minutes.</param>
    public async Task ScheduleFullAsync(TimeSpan every)
    {
        await Content.GetByLabel("Kind", new() { Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = "Full" });
        await Content.GetByLabel("Every, hours", new() { Exact = true }).FillAsync(((int)every.TotalHours).ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Content.GetByLabel("Every, minutes", new() { Exact = true }).FillAsync(every.Minutes.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save schedule", Exact = true }).ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Status).Filter(new() { HasTextString = "Scheduled a full backup every" })).ToBeVisibleAsync(new() { Timeout = 30_000 });
    }

    private static async Task<bool> WaitVisibleAsync(ILocator locator, TimeSpan timeout)
    {
        try
        {
            await locator.First.WaitForAsync(new() { State = WaitForSelectorState.Visible, Timeout = (float)timeout.TotalMilliseconds });
            return true;
        }
        catch (System.TimeoutException)
        {
            return false;
        }
    }
}
