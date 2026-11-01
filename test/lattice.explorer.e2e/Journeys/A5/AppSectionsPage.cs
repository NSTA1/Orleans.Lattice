using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>
/// An installed app's own page (<c>/apps/{slug}/{section}</c>) as journey A5 drives it: its
/// heading, and its Overview, Trees, Roles, Tools, Subscriptions, Replication and Consent tabs.
/// </summary>
/// <param name="shell">The Explorer's chrome on the session's page.</param>
internal sealed class AppSectionsPage(ShellPage shell)
{
    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The section panel shown.</summary>
    public ILocator Panel => Content.GetByRole(AriaRole.Tabpanel);

    /// <summary>Opens <paramref name="slug"/>'s page at <paramref name="section"/> by its address.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="section">The section.</param>
    public async Task OpenAsync(string slug, string displayName, string section)
    {
        await shell.GotoAsync($"/apps/{slug}/{section}");
        await ExpectSectionAsync(slug, displayName, section);
    }

    /// <summary>Chooses the tab <paramref name="title"/> and waits for its section's address.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="title">The tab's title.</param>
    public async Task ChooseTabAsync(string slug, string displayName, string title)
    {
        await Content.GetByRole(AriaRole.Tablist, new() { Name = "App sections" }).GetByRole(AriaRole.Tab, new() { Name = title, Exact = true }).ClickAsync();
        await ExpectSectionAsync(slug, displayName, title.ToLowerInvariant());
    }

    /// <summary>Asserts the page shows <paramref name="section"/> of the app.</summary>
    /// <param name="slug">The app slug.</param>
    /// <param name="displayName">The app's display name.</param>
    /// <param name="section">The section.</param>
    public async Task ExpectSectionAsync(string slug, string displayName, string section)
    {
        await Expect(Page).ToHaveURLAsync(new Regex($"/apps/{Regex.Escape(slug)}/{section}$"));
        await shell.ExpectHeadingAsync(displayName);
        await Expect(Content.GetByRole(AriaRole.Tab, new() { Name = Title(section), Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        Coverage.Hit("route:/apps/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
        Coverage.Hit($"subroute:apps/{{app}}/{section}");
    }

    private static string Title(string section) => char.ToUpperInvariant(section[0]) + section[1..];
}
