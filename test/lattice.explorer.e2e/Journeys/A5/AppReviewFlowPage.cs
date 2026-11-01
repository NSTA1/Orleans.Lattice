using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>
/// An app's review page (<c>/apps/catalogue/{source}/{slug}</c>) and the staged flows it hosts,
/// as journey A5 drives them: install (bind roles, confirm the ceiling, install, enable),
/// changing the installed version's role bindings, editing its consent, and upgrading.
/// </summary>
/// <param name="shell">The Explorer's chrome on the session's page.</param>
internal sealed class AppReviewFlowPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The flow's row of stops.</summary>
    public ILocator Steps => Content.Locator("ol.lt-apps-steps");

    /// <summary>Opens the review of <paramref name="slug"/> as <paramref name="source"/> offers it.</summary>
    /// <param name="source">The source key.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="displayName">The app's display name.</param>
    public async Task OpenAsync(string source, string slug, string displayName)
    {
        await shell.GotoAsync($"/apps/catalogue/{source}/{slug}");
        await shell.ExpectHeadingAsync(displayName);
        await ExpectReviewAsync();
    }

    /// <summary>Asserts the review is drawn: what the app creates and what it asks for.</summary>
    public async Task ExpectReviewAsync()
    {
        await Expect(Section("What it asks for")).ToBeVisibleAsync();
        await Expect(Section("What it creates")).ToBeVisibleAsync();
        await Expect(Section("Identity and provenance")).ToBeVisibleAsync();
        Coverage.Hit("route:/apps/catalogue/{p2}/{p3}");
    }

    /// <summary>The page's lede, which says whether the app is installed and from where.</summary>
    public ILocator Lede => Content.Locator("p.lt-shell-page-lede").First;

    /// <summary>One titled section of the review.</summary>
    /// <param name="title">The section heading.</param>
    public ILocator Section(string title) => Content.GetByRole(AriaRole.Region, new() { Name = title, Exact = true });

    /// <summary>The review's integration section: replication, schema, change feeds and tools.</summary>
    public ILocator Integration => Section("Replication, schema, change feeds and tools");

    /// <summary>Clicks a button by its exact text.</summary>
    /// <param name="text">The button's text.</param>
    public Task ClickAsync(string text) => Content.GetByRole(AriaRole.Button, new() { Name = text, Exact = true }).ClickAsync();

    /// <summary>Asserts the flow's current stop.</summary>
    /// <param name="stop">The stop's text, such as <c>Bind roles</c>.</param>
    public Task ExpectStopAsync(string stop) =>
        Expect(Steps.Locator("[aria-current=step]")).ToContainTextAsync(stop);

    /// <summary>The group picker of <paramref name="role"/> on the binding step.</summary>
    /// <param name="role">The role name.</param>
    public ILocator GroupFor(string role) => Content.GetByRole(AriaRole.Combobox, new() { Name = $"Group for {role}", Exact = true });

    /// <summary>Binds <paramref name="role"/> to <paramref name="group"/> through its picker.</summary>
    /// <param name="role">The role name.</param>
    /// <param name="group">The group id.</param>
    public Task BindAsync(string role, string group) => JourneyPick.PickAsync(Page, GroupFor(role), group);

    /// <summary>The binding step's line saying whether the caller is in the group <paramref name="group"/>.</summary>
    /// <param name="group">The bound group.</param>
    public ILocator NotMemberOf(string group) =>
        Content.Locator("[data-lt-holding=not-member]").Filter(new() { Has = Page.Locator($"[data-lt-join='{group}']") });

    /// <summary>The operation checkbox of the ceiling step labelled <paramref name="operation"/>, such as <c>range delete</c>.</summary>
    /// <param name="operation">The operation's words.</param>
    public ILocator Operation(string operation) =>
        Content.GetByRole(AriaRole.Group, new() { Name = "Operations", Exact = true }).GetByLabel(operation, new() { Exact = true });

    /// <summary>The ceiling step's activation verdict: <c>ok</c> or <c>fails</c>.</summary>
    /// <param name="verdict">The verdict.</param>
    public ILocator Activation(string verdict) => Content.Locator($"[data-lt-activation={verdict}]");

    /// <summary>The flow's status block (installed, enabled, rebound).</summary>
    public ILocator Status => Content.Locator(".lt-apps-status[role=status]");

    /// <summary>Waits for the flow's status to say <paramref name="text"/>.</summary>
    /// <param name="text">The status text.</param>
    /// <param name="timeout">How long to wait, in milliseconds.</param>
    public Task ExpectStatusAsync(string text, float timeout = 90_000) => Expect(Status).ToContainTextAsync(text, new() { Timeout = timeout });

    /// <summary>
    /// How long an enable may take to report: the flow follows a pending enable for up to five
    /// minutes (AppLifecycleFollow.Limit) before it gives up, so wait a little past that.
    /// </summary>
    public const float EnableTimeout = 360_000;

    /// <summary>The address the app's page lives at, as the flow's result shows it.</summary>
    public ILocator AppAddress => Content.Locator("[data-lt-app-address]");

    /// <summary>Waits for the binding step of an install or upgrade.</summary>
    public async Task ExpectBindingStepAsync(string heading)
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = heading, Exact = true })).ToBeVisibleAsync();
        await ExpectStopAsync("Bind roles");
    }

    /// <summary>Waits for the ceiling step.</summary>
    public async Task ExpectCeilingStepAsync()
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Confirm the ceiling", Exact = true })).ToBeVisibleAsync();
        await ExpectStopAsync("Confirm ceiling");
    }

    /// <summary>Asserts the drafted ceiling checks exactly <paramref name="checkedOperations"/> of <paramref name="shown"/>.</summary>
    /// <param name="shown">The operations to inspect.</param>
    /// <param name="checkedOperations">Those that are checked.</param>
    public async Task ExpectCeilingAsync(IEnumerable<string> shown, IReadOnlyCollection<string> checkedOperations)
    {
        foreach (var operation in shown)
        {
            if (checkedOperations.Contains(operation))
            {
                await Expect(Operation(operation)).ToBeCheckedAsync();
            }
            else
            {
                await Expect(Operation(operation)).Not.ToBeCheckedAsync();
            }
        }
    }

    /// <summary>The upgrade's difference section.</summary>
    public ILocator UpgradeDiff => Content.GetByRole(AriaRole.Region, new() { NameRegex = new Regex("^What changes from v") });

    /// <summary>The confirmation step's row for <paramref name="role"/>.</summary>
    /// <param name="role">The role name.</param>
    public ILocator BindingChange(string role) => Content.Locator($"[data-lt-binding='{role}']");

    /// <summary>The address of the current page.</summary>
    public string Url => Page.Url;

    /// <summary>Waits until the page's address matches <paramref name="pattern"/>.</summary>
    /// <param name="pattern">The pattern.</param>
    public Task ExpectUrlAsync(string pattern) => Expect(Page).ToHaveURLAsync(new Regex(pattern));
}
