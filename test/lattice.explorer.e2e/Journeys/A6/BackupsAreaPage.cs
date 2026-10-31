using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A6;

/// <summary>
/// The Backups area as a journey drives it (#5377): the catalogue, the capture form, an
/// operation's status page, one backup's page, the schedules, the health page and maintenance.
/// Every method asserts what it drove before it records the feature it proves.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class BackupsAreaPage(PersonaSession session)
{
    /// <summary>How long a capture, check or rebuild on the cluster is given to finish, in milliseconds.</summary>
    public const float OperationTimeout = 180_000;

    private const float LoadTimeout = 60_000;

    private IPage Page => session.Page;

    private ShellPage Shell => session.Shell;

    private ILocator Content => Shell.Content;

    /// <summary>The area's row of page links.</summary>
    public ILocator NavRow => Content.GetByRole(AriaRole.Navigation, new() { Name = "Backups pages" });

    /// <summary>The empty state of the catalogue or of the health list.</summary>
    public ILocator NoBackupsYet => Content.GetByRole(AriaRole.Heading, new() { Name = "No backups yet", Exact = true });

    /// <summary>The cluster operation's progress block on the page.</summary>
    public ILocator OperationProgress => Content.Locator(".lt-operation-progress").First;

    /// <summary>The catalogue's list of recent backup and restore operations.</summary>
    public ILocator ClusterOperations => Content.Locator("#lt-backups-cluster-operations-title + ul > li");

    /// <summary>The focused backup's health status pill on the health page.</summary>
    public ILocator HealthStatus => Definition("Status").Locator(".lt-pill");

    /// <summary>Goes to the Backups area from its stop on the directory spine.</summary>
    public async Task OpenFromSpineAsync()
    {
        await Shell.GoToAreaAsync("backups");
        await ExpectCatalogueAsync();
    }

    /// <summary>Goes to one of the area's pages from its row of page links.</summary>
    /// <param name="link">The link's text: Catalogue, Schedules, Health or Maintenance.</param>
    /// <param name="heading">The page's heading.</param>
    public async Task GoToAsync(string link, string heading)
    {
        var target = NavRow.GetByRole(AriaRole.Link, new() { Name = link, Exact = true });
        await target.ClickAsync();
        await Shell.ExpectHeadingAsync(heading);
        await Expect(NavRow.GetByRole(AriaRole.Link, new() { Name = link, Exact = true })).ToHaveAttributeAsync("aria-current", "page");
        Coverage.Hit("use-case:backups/navigation-row");
    }

    /// <summary>Goes back to the catalogue from a page below the row (a backup, an operation, the capture form).</summary>
    public async Task BackToCatalogueAsync()
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = "Back to the catalogue", Exact = true }).ClickAsync();
        await ExpectCatalogueAsync();
    }

    /// <summary>Waits for the catalogue page.</summary>
    public async Task ExpectCatalogueAsync()
    {
        await Shell.ExpectHeadingAsync("Backups");
        await Expect(Page).ToHaveURLAsync(new Regex("/backups(\\?|$)"));
        Coverage.Hit("route:/backups");
    }

    /// <summary>Asserts the catalogue lists no backup.</summary>
    public async Task ExpectCatalogueEmptyAsync()
    {
        await Expect(NoBackupsYet).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        await Expect(Content.GetByText("Capture one to start the catalogue.", new() { Exact = true })).ToBeVisibleAsync();
        Coverage.Hit("use-case:backups/area");
    }

    /// <summary>Filters the catalogue by a name prefix and waits for the backup named <paramref name="name"/>.</summary>
    /// <param name="name">The prefix, and the name of the backup expected.</param>
    public async Task FilterByNameAsync(string name)
    {
        var filter = Content.GetByRole(AriaRole.Searchbox, new() { Name = "Filter by name prefix" });
        await filter.FillAsync(name);
        await filter.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]name=" + Regex.Escape(Uri.EscapeDataString(name)) + "(&|$)"));
        await Expect(Row(name)).ToHaveCountAsync(1, new() { Timeout = LoadTimeout });
        Coverage.Hit("query:backups.name");
    }

    /// <summary>The table row (or compact row) holding the exact text <paramref name="text"/>.</summary>
    /// <param name="text">The text, such as a backup's name.</param>
    public ILocator Row(string text) =>
        Content.Locator("tr.lt-table__row, li.lt-table-list__row").Filter(new() { Has = Page.GetByText(text, new() { Exact = true }) });

    /// <summary>Opens the backup named <paramref name="name"/> from its catalogue row.</summary>
    /// <param name="name">The backup's name.</param>
    public async Task OpenBackupFromCatalogueAsync(string name)
    {
        await OpenRowAsync(name, "Open backup");
        await Shell.ExpectHeadingAsync(name);
    }

    /// <summary>Opens the capture form from the catalogue's toolbar.</summary>
    public async Task OpenCaptureFromToolbarAsync()
    {
        await Content.Locator("[data-lt-command=\"backups.capture\"]").ClickAsync();
        await ExpectCaptureFormAsync();
    }

    /// <summary>Opens the capture form with the palette's Capture backup... command.</summary>
    public async Task OpenCaptureFromPaletteAsync()
    {
        await Shell.RunCommandAsync("Capture backup...");
        await ExpectCaptureFormAsync();
        Coverage.Hit("command:backups.capture");
    }

    /// <summary>Fills the capture form for a capture of a whole tree.</summary>
    /// <param name="kind">Full or Incremental.</param>
    /// <param name="name">The backup's name, or <see langword="null"/> to leave it empty.</param>
    /// <param name="tree">The tree.</param>
    public async Task FillCaptureAsync(string kind, string? name, string tree)
    {
        await SelectAsync("Kind", kind);
        if (name is not null)
        {
            var nameBox = Content.GetByRole(AriaRole.Textbox, new() { Name = "Name", Exact = true });
            await nameBox.FillAsync(name);
            await Expect(nameBox).ToHaveValueAsync(name);
        }

        var treeBox = Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await treeBox.FillAsync(tree);
        await Expect(treeBox).ToHaveValueAsync(tree);
        await Expect(Content.GetByLabel("Scope", new() { Exact = true })).ToHaveValueAsync("whole-tree");
    }

    /// <summary>Submits the capture form and waits for the capture's status page.</summary>
    /// <param name="tree">The tree captured, as the page's title names it.</param>
    public async Task SubmitCaptureAsync(string tree)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Capture backup", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/operations/[^/?]+$"), new() { Timeout = LoadTimeout });
        await Shell.ExpectHeadingAsync("Capture a full backup of " + tree);
        await Expect(OperationProgress).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        Coverage.Hit("route:/backups/operations/{p1}");
        Coverage.Hit("subroute:backups/operations");
    }

    /// <summary>Waits until the operation on the page reaches <paramref name="state"/>.</summary>
    /// <param name="state">Running, Succeeded, Failed or Cancelled.</param>
    public Task ExpectOperationStateAsync(string state) =>
        Expect(OperationProgress).ToHaveAttributeAsync("data-lt-operation-state", state, new() { Timeout = OperationTimeout });

    /// <summary>
    /// Submits the capture form and waits for the staged capture's status page to report it
    /// refused at its Check access stage, with <paramref name="message"/>.
    /// </summary>
    /// <param name="tree">The tree whose capture was refused.</param>
    /// <param name="message">The refusal the page shows.</param>
    public async Task SubmitCaptureExpectingRefusalAsync(string tree, string message)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Capture backup", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/operations/[^/?]+$"), new() { Timeout = LoadTimeout });
        await Shell.ExpectHeadingAsync("Capture a full backup of " + tree);
        var status = Content.Locator("p.lt-backups-status");
        await Expect(status.Locator(".lt-pill")).ToHaveTextAsync("Failed", new() { Timeout = LoadTimeout });
        await Expect(status).ToContainTextAsync(message);
        var checkAccess = Content.GetByRole(AriaRole.List, new() { Name = "Stages" }).GetByRole(AriaRole.Listitem).Filter(new() { HasText = "Check access" });
        await Expect(checkAccess).ToContainTextAsync("Failed");
        await Expect(OperationProgress).ToHaveCountAsync(0);
    }

    /// <summary>Asserts the schedules page offers the shown tree's schedules and the form to change them.</summary>
    public async Task ExpectSchedulesOfferedAsync()
    {
        await Expect(ScheduleRow("Full")).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Save schedule", Exact = true })).ToBeVisibleAsync();
        await Expect(Content.GetByText("You may not schedule backups of this tree.", new() { Exact = true })).ToHaveCountAsync(0);
    }

    /// <summary>The value a definition list on the page gives <paramref name="term"/>.</summary>
    /// <param name="term">The term.</param>
    public ILocator Definition(string term) =>
        Content.Locator(".lt-dl__row").Filter(new() { Has = Page.Locator("dt", new() { HasTextRegex = new Regex("^\\s*" + Regex.Escape(term) + "\\s*$") }) }).Locator("dd");

    /// <summary>Waits for the finished capture and returns the backup id its "The captured backup" link names.</summary>
    public async Task<string> CapturedBackupIdAsync()
    {
        await ExpectOperationStateAsync("Succeeded");
        var link = Content.GetByRole(AriaRole.Link, new() { Name = "The captured backup", Exact = true });
        await Expect(link).ToBeVisibleAsync();
        var href = await link.GetAttributeAsync("href") ?? throw new InvalidOperationException("The captured backup's link has no address.");
        var id = Uri.UnescapeDataString(href.Split('?')[0].TrimEnd('/').Split('/')[^1]);
        Coverage.Hit("use-case:backups/operation-status");
        return id;
    }

    /// <summary>Asks the running operation on the page to stop, and waits for the button to go.</summary>
    public async Task CancelOperationAsync()
    {
        var cancel = Content.GetByRole(AriaRole.Button, new() { Name = "Cancel operation", Exact = true });
        await Expect(cancel).ToBeEnabledAsync();
        await cancel.ClickAsync();
        await Expect(cancel).ToBeHiddenAsync(new() { Timeout = LoadTimeout });
    }

    /// <summary>Asks the capture form for the full bases of its tree, and returns the base picker.</summary>
    public async Task<ILocator> FindFullBasesAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Find full backups of this tree", Exact = true }).ClickAsync();
        var bases = Content.GetByLabel("Base backup", new() { Exact = true });
        await Expect(bases).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        return bases;
    }

    /// <summary>Leaves the capture form without capturing, by its Cancel link.</summary>
    public async Task LeaveCaptureAsync()
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = "Cancel", Exact = true }).ClickAsync();
        await ExpectCatalogueAsync();
    }

    /// <summary>Exports one of the open backup's artifacts and returns the downloaded file.</summary>
    /// <param name="artifactId">The artifact id.</param>
    /// <param name="directory">Where the download is saved.</param>
    public async Task<FileInfo> ExportArtifactAsync(string artifactId, string directory)
    {
        IDownload download;
        if (await Shell.IsCompactAsync())
        {
            await Row(artifactId).GetByRole(AriaRole.Button).ClickAsync();
            var export = Page.GetByRole(AriaRole.Dialog).GetByRole(AriaRole.Button, new() { Name = "Export artifact", Exact = true });
            download = await Page.RunAndWaitForDownloadAsync(() => export.ClickAsync(), new() { Timeout = OperationTimeout });
            await Page.Keyboard.PressAsync("Escape");
        }
        else
        {
            var export = Content.GetByRole(AriaRole.Button, new() { Name = "Export artifact " + artifactId, Exact = true });
            download = await Page.RunAndWaitForDownloadAsync(() => export.ClickAsync(), new() { Timeout = OperationTimeout });
        }

        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, download.SuggestedFilename);
        await download.SaveAsAsync(path);
        Assert.That(await download.FailureAsync(), Is.Null, $"the download of artifact {artifactId} completed");
        return new FileInfo(path);
    }

    /// <summary>Shows a tree's schedules on the schedules page.</summary>
    /// <param name="tree">The tree.</param>
    public async Task ShowSchedulesAsync(string tree)
    {
        var treeBox = Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true });
        await treeBox.FillAsync(tree);
        await Expect(treeBox).ToHaveValueAsync(tree);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Show schedules", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/schedules\\?tree=" + Regex.Escape(Uri.EscapeDataString(tree)) + "$"), new() { Timeout = LoadTimeout });
        await Expect(Content.Locator("h2").Filter(new() { HasText = "Schedules of" })).ToContainTextAsync(tree, new() { Timeout = LoadTimeout });
        await Expect(ScheduleRow("Full")).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        Coverage.Hit("query:backups.tree");
    }

    /// <summary>The schedules table's row (or compact row) for a kind.</summary>
    /// <param name="kind">Full or Incremental.</param>
    public ILocator ScheduleRow(string kind) =>
        Content.Locator("tr.lt-table__row, li.lt-table-list__row").Filter(new() { HasTextRegex = new Regex("^\\s*" + Regex.Escape(kind) + "\\b") });

    /// <summary>Registers or changes a schedule of the shown tree, and waits for the page to confirm it.</summary>
    /// <param name="kind">Full or Incremental.</param>
    /// <param name="hours">The interval's hours.</param>
    /// <param name="minutes">The interval's minutes.</param>
    /// <param name="confirmation">The confirmation the page shows.</param>
    public async Task SaveScheduleAsync(string kind, int hours, int minutes, string confirmation)
    {
        await SelectAsync("Kind", kind);
        await FillDurationAsync("Every", hours, minutes);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save schedule", Exact = true }).ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Status).Filter(new() { HasText = confirmation })).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
    }

    /// <summary>Reads the shown tree's full schedule as the page draws it: whether a run and a success are recorded.</summary>
    public async Task<(bool Ran, bool Succeeded, string Text)> ReadFullScheduleAsync()
    {
        var row = ScheduleRow("Full");
        var text = (await row.InnerTextAsync()).Trim();
        if (await Shell.IsCompactAsync())
        {
            var succeeded = !text.Contains("last success Never", StringComparison.Ordinal);
            return (succeeded, succeeded, text);
        }

        var cells = row.Locator("td");
        var lastRun = (await cells.Nth(2).InnerTextAsync()).Trim();
        var lastSuccess = (await cells.Nth(3).InnerTextAsync()).Trim();
        return (lastRun != "Never", lastSuccess != "Never", text);
    }

    /// <summary>Reloads the shown schedules until the full schedule's Last run and Last success fill.</summary>
    /// <param name="timeout">How long the schedule is given to fire.</param>
    public async Task WaitForFullScheduleToSucceedAsync(TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (true)
        {
            var (ran, succeeded, text) = await ReadFullScheduleAsync();
            if (ran && succeeded)
            {
                return;
            }

            if (DateTimeOffset.UtcNow > deadline)
            {
                Assert.Fail($"The full schedule did not succeed within {timeout}: its row reads '{text}'.");
            }

            await Task.Delay(TimeSpan.FromSeconds(10));
            await Page.ReloadAsync();
            await Expect(ScheduleRow("Full")).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        }
    }

    /// <summary>Opens a backup's health from the health list.</summary>
    /// <param name="name">The backup's name.</param>
    public async Task OpenHealthOfAsync(string name)
    {
        await OpenRowAsync(name, "Check or configure");
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/health\\?backup=[^&]+$"));
        await Expect(Content.Locator("h2").Filter(new() { HasText = name + " of" })).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        Coverage.Hit("query:backups.backup");
        Coverage.Hit("query-value:backups.backup=id");
    }

    /// <summary>Turns a backup's periodic monitoring on at an interval and saves it.</summary>
    /// <param name="hours">The interval's hours.</param>
    /// <param name="minutes">The interval's minutes.</param>
    public async Task SaveMonitoringAsync(int hours, int minutes)
    {
        var monitor = Content.GetByRole(AriaRole.Switch, new() { Name = "Verify this backup periodically" });
        if (await monitor.GetAttributeAsync("aria-checked") != "true")
        {
            await monitor.ClickAsync();
        }

        await Expect(monitor).ToHaveAttributeAsync("aria-checked", "true");
        await FillDurationAsync("Verify every", hours, minutes);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save monitoring", Exact = true }).ClickAsync();
        await Expect(Content.GetByRole(AriaRole.Status).Filter(new() { HasText = "Monitoring on: this backup is verified every" }))
            .ToBeVisibleAsync(new() { Timeout = LoadTimeout });
    }

    /// <summary>Checks the focused backup's health now and waits for the check to finish.</summary>
    public async Task CheckNowAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Check now", Exact = true }).ClickAsync();
        await ExpectOperationStateAsync("Succeeded");
    }

    /// <summary>Starts a check of the catalogue against the store, and waits for it on its status page.</summary>
    public async Task CheckCatalogueAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Check catalogue", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/operations/[^/?]+$"), new() { Timeout = LoadTimeout });
        await Shell.ExpectHeadingAsync("Check the catalogue against the backup store");
        await ExpectOperationStateAsync("Succeeded");
    }

    /// <summary>Opens the rebuild's confirmation from the maintenance page.</summary>
    public async Task<ILocator> AskRebuildAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Rebuild catalogue...", Exact = true }).ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Rebuild the catalogue?" });
        await Expect(dialog).ToBeVisibleAsync();
        return dialog;
    }

    /// <summary>Confirms the rebuild and waits for it on its status page.</summary>
    /// <param name="dialog">The confirmation.</param>
    public async Task ConfirmRebuildAsync(ILocator dialog)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Rebuild", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/operations/[^/?]+$"), new() { Timeout = LoadTimeout });
        await Shell.ExpectHeadingAsync("Rebuild the catalogue from the backup store");
        await ExpectOperationStateAsync("Succeeded");
    }

    private async Task ExpectCaptureFormAsync()
    {
        await Shell.ExpectHeadingAsync("Capture a backup");
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/new(\\?|$)"));
        Coverage.Hit("route:/backups/new");
        Coverage.Hit("subroute:backups/new");
    }

    private async Task OpenRowAsync(string text, string detailAction)
    {
        var row = Row(text).First;
        await Expect(row).ToBeVisibleAsync(new() { Timeout = LoadTimeout });
        if (await Shell.IsCompactAsync())
        {
            await row.GetByRole(AriaRole.Button).ClickAsync();
            await Page.GetByRole(AriaRole.Dialog).GetByRole(AriaRole.Link, new() { Name = detailAction, Exact = true }).ClickAsync();
        }
        else
        {
            await row.GetByRole(AriaRole.Link, new() { Name = text, Exact = true }).ClickAsync();
        }
    }

    private async Task SelectAsync(string label, string option)
    {
        var select = Content.GetByLabel(label, new() { Exact = true });
        await select.SelectOptionAsync(new[] { new SelectOptionValue { Label = option } });
        await Expect(select.Locator("option:checked")).ToHaveTextAsync(option);
    }

    private async Task FillDurationAsync(string label, int hours, int minutes)
    {
        var hoursBox = Content.GetByRole(AriaRole.Textbox, new() { Name = label + ", hours", Exact = true });
        var minutesBox = Content.GetByRole(AriaRole.Textbox, new() { Name = label + ", minutes", Exact = true });
        // Ordered so the boxes never pass through a duration under the one-minute minimum.
        var order = hours > 0 ? new[] { (hoursBox, hours), (minutesBox, minutes) } : [(minutesBox, minutes), (hoursBox, hours)];
        foreach (var (box, value) in order)
        {
            await box.FillAsync(value.ToString(CultureInfo.InvariantCulture));
            await box.BlurAsync();
        }
        await Expect(hoursBox).ToHaveValueAsync(hours.ToString(CultureInfo.InvariantCulture));
        await Expect(minutesBox).ToHaveValueAsync(minutes.ToString(CultureInfo.InvariantCulture));
    }
}
