using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A10;

/// <summary>
/// The Backups area as A10 drives it (#5378): the catalogue and its filters and pager, the
/// capture form, a backup's details page with its restore form, an operation's status page,
/// and the schedules page. Every locator is a role, an accessible name or a
/// <c>data-lt-*</c> hook, and works at the desktop and the compact width alike.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class BackupsArea(ShellPage shell)
{
    /// <summary>How long a capture, restore or cold restore may take on the estate.</summary>
    public static readonly TimeSpan OperationTimeout = TimeSpan.FromMinutes(5);

    /// <summary>The catalogue's rows per page, as the Explorer's guide documents them.</summary>
    public const int CataloguePageSize = 25;

    // A staged (session) operation id is a number; the cluster's own operation ids are not.
    private static readonly Regex ClusterOperationAddress = new("/backups/operations/(?!\\d+$)[^/?#]+$");

    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The catalogue's backup rows on screen: table rows, or the compact list's rows.</summary>
    public ILocator CatalogueRows => Content.Locator("table.lt-table tbody tr, ul.lt-table-list__rows > li");

    /// <summary>The catalogue pager's position, such as <c>Page 2</c>.</summary>
    public ILocator PagerPosition => Content.GetByText(new Regex("^Page \\d+$"));

    /// <summary>The catalogue pager's <b>Next page</b> button.</summary>
    public ILocator NextPage => Content.GetByRole(AriaRole.Button, new() { Name = "Next page", Exact = true });

    /// <summary>The catalogue pager's <b>Previous page</b> button.</summary>
    public ILocator PreviousPage => Content.GetByRole(AriaRole.Button, new() { Name = "Previous page", Exact = true });

    /// <summary>The cluster operation's progress, whose <c>data-lt-operation-state</c> names its state.</summary>
    public ILocator OperationProgress => Content.Locator(".lt-operation-progress");

    /// <summary>The <b>Restore chain</b> section's entries on a backup's details page.</summary>
    public ILocator RestoreChain => Content.GetByRole(AriaRole.Region, new() { Name = "Restore chain" }).GetByRole(AriaRole.Listitem);

    /// <summary>The restore confirmation, <b>Restore this backup?</b>.</summary>
    public ILocator RestoreConfirmation => Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Restore this backup?" });

    /// <summary>The trees listed in the set being captured.</summary>
    public ILocator SetTrees => Content.GetByRole(AriaRole.List, new() { Name = "Trees in the set" }).GetByRole(AriaRole.Listitem);

    /// <summary>The set form's <b>Capture every tree at one consistency fence</b> checkbox.</summary>
    public ILocator ConsistencyFence => Content.GetByRole(AriaRole.Checkbox, new() { Name = "Capture every tree at one consistency fence", Exact = true });

    /// <summary>The restore form's <b>Restore mode</b> select.</summary>
    public ILocator RestoreMode => Content.GetByRole(AriaRole.Combobox, new() { Name = "Restore mode", Exact = true });

    /// <summary>The restore form's <b>Restore point</b> select, offered when the backup's chain has more than one backup.</summary>
    public ILocator RestorePoint => Content.GetByRole(AriaRole.Combobox, new() { Name = "Restore point", Exact = true });

    /// <summary>The restore form's cold-restore checkbox.</summary>
    public ILocator ColdRestore => Content.GetByRole(AriaRole.Checkbox, new() { Name = "Restore from the backup store alone (cold restore)", Exact = true });

    private ILocator ScheduleForm => Content.GetByRole(AriaRole.Region, new() { Name = "Register or change a schedule" });

    /// <summary>Opens the catalogue at <c>/backups</c>.</summary>
    public async Task OpenCatalogueAsync()
    {
        await shell.GotoAsync("/backups");
        await shell.ExpectHeadingAsync("Backups");
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Filters the catalogue by <paramref name="tree"/> through its <b>Filter by tree</b> field.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task FilterByTreeAsync(string tree)
    {
        var field = Content.GetByRole(AriaRole.Combobox, new() { Name = "Filter by tree", Exact = true });
        await field.FillAsync(tree);
        await field.PressAsync("Enter");
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]tree=" + Regex.Escape(Uri.EscapeDataString(tree)) + "(&|$)"));
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Schedule status of " + tree })).ToBeVisibleAsync();
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Filters the catalogue by kind through its <b>Kind</b> select.</summary>
    /// <param name="label">The kind's label, such as <c>Full</c>.</param>
    /// <param name="query">The kind's query value, such as <c>full</c>.</param>
    public async Task FilterByKindAsync(string label, string query)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Kind", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = label });
        await Expect(Page).ToHaveURLAsync(new Regex("[?&]kind=" + query + "(&|$)"));
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Waits until the catalogue shows a page of rows rather than its loading skeleton.</summary>
    public Task ExpectCatalogueLoadedAsync() => Expect(CatalogueRows.First).ToBeVisibleAsync();

    /// <summary>The catalogue row for the backup named <paramref name="name"/>.</summary>
    /// <param name="name">The backup's name.</param>
    public ILocator CatalogueRow(string name) => CatalogueRows.Filter(new() { Has = Page.GetByText(name, new() { Exact = true }) });

    /// <summary>Moves the pager to the next page and waits for page <paramref name="expected"/>.</summary>
    /// <param name="expected">The page number expected after the move.</param>
    public async Task NextPageAsync(int expected)
    {
        await NextPage.ClickAsync();
        await Expect(PagerPosition).ToHaveTextAsync($"Page {expected}");
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Moves the pager to the previous page and waits for page <paramref name="expected"/>.</summary>
    /// <param name="expected">The page number expected after the move.</param>
    public async Task PreviousPageAsync(int expected)
    {
        await PreviousPage.ClickAsync();
        await Expect(PagerPosition).ToHaveTextAsync($"Page {expected}");
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Opens the capture form from the catalogue's <b>Capture backup...</b> link.</summary>
    public async Task OpenCaptureFormAsync()
    {
        await OpenCatalogueAsync();
        var capture = Content.Locator("[data-lt-command=\"backups.capture\"]");
        await Expect(capture).ToHaveTextAsync("Capture backup...");
        await capture.ClickAsync();
        await shell.ExpectHeadingAsync("Capture a backup");
    }

    /// <summary>Chooses the capture kind: <c>Full</c>, <c>Incremental</c> or <c>Set of trees</c>.</summary>
    /// <param name="kind">The kind's label.</param>
    public Task ChooseCaptureKindAsync(string kind) =>
        Content.GetByRole(AriaRole.Combobox, new() { Name = "Kind", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = kind });

    /// <summary>Gives the capture its name.</summary>
    /// <param name="name">The backup's name.</param>
    public Task NameCaptureAsync(string name) =>
        Content.GetByRole(AriaRole.Textbox, new() { Name = "Name", Exact = true }).FillAsync(name);

    /// <summary>Names the tree to capture in the form's <b>Tree</b> field.</summary>
    /// <param name="tree">The tree id.</param>
    public Task ChooseCaptureTreeAsync(string tree) =>
        FillTreeFieldAsync(Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }), tree);

    /// <summary>Narrows a full capture to the keys under <paramref name="prefix"/>.</summary>
    /// <param name="prefix">The key prefix.</param>
    public async Task ScopeToPrefixAsync(string prefix)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = "Keys under a prefix" });
        await Content.GetByRole(AriaRole.Textbox, new() { Name = "Key prefix", Exact = true }).FillAsync(prefix);
    }

    /// <summary>Narrows a full capture to the one key <paramref name="key"/>.</summary>
    /// <param name="key">The key.</param>
    public async Task ScopeToKeyAsync(string key)
    {
        await Content.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = "One key" });
        await Content.GetByRole(AriaRole.Textbox, new() { Name = "Key", Exact = true }).FillAsync(key);
    }

    /// <summary>Finds the tree's full backups and chooses the one named <paramref name="baseName"/> as the incremental's base.</summary>
    /// <param name="baseName">The base backup's name.</param>
    public async Task ChooseBaseAsync(string baseName)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Find full backups of this tree", Exact = true }).ClickAsync();
        var select = Content.GetByRole(AriaRole.Combobox, new() { Name = "Base backup", Exact = true });
        var option = select.Locator("option").Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(baseName) + ", ") });
        await Expect(option).ToHaveCountAsync(1);
        await select.SelectOptionAsync(await option.GetAttributeAsync("value") ?? throw new InvalidOperationException($"The base option {baseName} has no value."));
    }

    /// <summary>Adds <paramref name="tree"/> to the set being captured.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task AddSetTreeAsync(string tree)
    {
        await FillTreeFieldAsync(Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree to add", Exact = true }), tree);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Add tree", Exact = true }).ClickAsync();
        await Expect(SetTrees.Filter(new() { HasText = tree })).ToHaveCountAsync(1);
    }

    /// <summary>Submits the capture form, and waits for the cluster's operation page to take over; returns its path.</summary>
    public async Task<string> SubmitCaptureAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Capture backup", Exact = true }).ClickAsync();
        return await ExpectClusterOperationAsync();
    }

    /// <summary>
    /// Waits for the page to be a cluster-tracked operation's status page (a staged start hands
    /// over to it), and returns its head-relative path.
    /// </summary>
    public async Task<string> ExpectClusterOperationAsync()
    {
        await Expect(Page).ToHaveURLAsync(ClusterOperationAddress, new() { Timeout = 120_000 });
        await Expect(OperationProgress).ToBeVisibleAsync();
        return new Uri(Page.Url).PathAndQuery;
    }

    /// <summary>Waits for the followed operation to succeed.</summary>
    public Task ExpectOperationSucceededAsync() =>
        Expect(OperationProgress).ToHaveAttributeAsync("data-lt-operation-state", "Succeeded", new() { Timeout = (float)OperationTimeout.TotalMilliseconds });

    /// <summary>The value of the page's figure <paramref name="term"/>, such as <c>Backups captured</c>.</summary>
    /// <param name="term">The figure's term.</param>
    public ILocator Fact(string term) =>
        Content.Locator(".lt-dl__row").Filter(new() { Has = Page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }) }).Locator("dd");

    /// <summary>A link the finished operation offers to what it produced, such as <c>The captured backup</c>.</summary>
    /// <param name="text">The link's text.</param>
    public ILocator ResultLink(string text) => Content.GetByRole(AriaRole.Link, new() { Name = text, Exact = true });

    /// <summary>The backup id a link to <c>/backups/{id}</c> points at.</summary>
    /// <param name="link">The link.</param>
    public static async Task<string> BackupIdOfAsync(ILocator link)
    {
        var href = await link.GetAttributeAsync("href") ?? throw new InvalidOperationException("The backup link has no address.");
        var path = href.Split('?', '#')[0].TrimEnd('/');
        return Uri.UnescapeDataString(path[(path.LastIndexOf('/') + 1)..]);
    }

    /// <summary>Opens the catalogue filtered to <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenCatalogueOfAsync(string tree)
    {
        await shell.GotoAsync("/backups?tree=" + Uri.EscapeDataString(tree));
        await shell.ExpectHeadingAsync("Backups");
        await ExpectCatalogueLoadedAsync();
    }

    /// <summary>Opens the backup named <paramref name="name"/> from the catalogue page on screen.</summary>
    /// <param name="name">The backup's name.</param>
    public async Task OpenListedBackupAsync(string name)
    {
        var row = CatalogueRow(name);
        await Expect(row).ToHaveCountAsync(1);
        var link = row.GetByRole(AriaRole.Link, new() { Name = name, Exact = true });
        if (await link.CountAsync() == 0)
        {
            // The compact list opens a row's details sheet, whose action opens the backup.
            await row.ClickAsync();
            link = Page.GetByRole(AriaRole.Dialog, new() { Name = name }).GetByRole(AriaRole.Link, new() { Name = "Open backup", Exact = true });
        }

        await link.ClickAsync();
        await shell.ExpectHeadingAsync(name);
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Restore chain" })).ToBeVisibleAsync();
        await Expect(RestoreMode.Or(Content.GetByText("You may not restore this backup's tree."))).ToBeVisibleAsync();
    }

    /// <summary>Names the tree the restore writes into.</summary>
    /// <param name="tree">The target tree id.</param>
    public Task ChooseRestoreTargetAsync(string tree) =>
        FillTreeFieldAsync(Content.GetByRole(AriaRole.Combobox, new() { Name = "Restore into tree", Exact = true }), tree);

    /// <summary>Chooses the restore mode by its label.</summary>
    /// <param name="label">The mode's label.</param>
    public Task ChooseRestoreModeAsync(string label) => RestoreMode.SelectOptionAsync(new SelectOptionValue { Label = label });

    /// <summary>Chooses the chain's restore point that is this backup itself (the chain's newest).</summary>
    public async Task ChooseThisBackupAsRestorePointAsync()
    {
        var option = RestorePoint.Locator("option").Filter(new() { HasTextRegex = new Regex(", this backup$") });
        await Expect(option).ToHaveCountAsync(1);
        await RestorePoint.SelectOptionAsync(await option.GetAttributeAsync("value") ?? throw new InvalidOperationException("The restore point has no value."));
    }

    /// <summary>Presses <b>Restore...</b> and waits for the confirmation.</summary>
    public async Task RequestRestoreAsync()
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Restore...", Exact = true }).ClickAsync();
        await Expect(RestoreConfirmation).ToBeVisibleAsync();
    }

    /// <summary>Types <paramref name="tree"/> into the confirmation, which only then enables <b>Restore</b>.</summary>
    /// <param name="tree">The target tree id.</param>
    public async Task TypeRestoreConfirmationAsync(string tree)
    {
        var confirm = RestoreConfirmation.GetByRole(AriaRole.Button, new() { Name = "Restore", Exact = true });
        await Expect(confirm).ToBeDisabledAsync();
        await RestoreConfirmation.GetByRole(AriaRole.Textbox, new() { Name = "Tree name", Exact = true }).FillAsync(tree);
        await Expect(confirm).ToBeEnabledAsync();
    }

    /// <summary>Confirms the restore and waits for its cluster operation page; returns that page's path.</summary>
    public async Task<string> ConfirmRestoreAsync()
    {
        await RestoreConfirmation.GetByRole(AriaRole.Button, new() { Name = "Restore", Exact = true }).ClickAsync();
        return await ExpectClusterOperationAsync();
    }

    /// <summary>Opens the schedules page and shows <paramref name="tree"/>'s schedules.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenSchedulesAsync(string tree)
    {
        await shell.GotoAsync("/backups/schedules");
        await shell.ExpectHeadingAsync("Schedules");
        await FillTreeFieldAsync(Content.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true }), tree);
        await Content.GetByRole(AriaRole.Button, new() { Name = "Show schedules", Exact = true }).ClickAsync();
        await Expect(Page).ToHaveURLAsync(new Regex("/backups/schedules\\?tree=" + Regex.Escape(Uri.EscapeDataString(tree)) + "$"));
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Schedules of " + tree })).ToBeVisibleAsync();
        await Expect(ScheduleRow("Incremental")).ToHaveCountAsync(1);
    }

    /// <summary>The schedules table's row of <paramref name="kind"/> (<c>Full</c> or <c>Incremental</c>).</summary>
    /// <param name="kind">The kind.</param>
    public ILocator ScheduleRow(string kind) =>
        Content.Locator("table.lt-table tbody tr, ul.lt-table-list__rows > li")
            .Filter(new() { HasTextRegex = new Regex("^\\s*" + kind) });

    /// <summary>Fills the schedule form: the kind, and the interval in hours and minutes.</summary>
    /// <param name="kind">The kind's label.</param>
    /// <param name="hours">Whole hours.</param>
    /// <param name="minutes">Whole minutes.</param>
    public async Task FillScheduleAsync(string kind, int hours, int minutes)
    {
        await ScheduleForm.GetByRole(AriaRole.Combobox, new() { Name = "Kind", Exact = true }).SelectOptionAsync(new SelectOptionValue { Label = kind });
        await ScheduleForm.GetByRole(AriaRole.Textbox, new() { Name = "Every, hours", Exact = true }).FillAsync(hours.ToString(CultureInfo.InvariantCulture));
        await ScheduleForm.GetByRole(AriaRole.Textbox, new() { Name = "Every, minutes", Exact = true }).FillAsync(minutes.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>Saves the schedule and waits for the page to confirm it with <paramref name="message"/>.</summary>
    /// <param name="message">The confirmation the page shows.</param>
    public async Task SaveScheduleAsync(string message)
    {
        await ScheduleForm.GetByRole(AriaRole.Button, new() { Name = "Save schedule", Exact = true }).ClickAsync();
        await Expect(ScheduleForm.GetByRole(AriaRole.Status)).ToHaveTextAsync(message);
    }

    // A tree field lists matching trees as the operator types; leaving it with Tab closes the
    // list (so it never covers the control below it) and checks the tree it names.
    private static async Task FillTreeFieldAsync(ILocator field, string tree)
    {
        await field.FillAsync(tree);
        await field.PressAsync("Tab");
        await Expect(field).ToHaveAttributeAsync("aria-expanded", "false");
        await Expect(field).ToHaveValueAsync(tree);
    }
}
