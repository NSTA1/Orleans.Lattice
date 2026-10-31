using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U3;

/// <summary>
/// The Data area as dana watches the task board's tree: the directory, whose App column names
/// the app that owns a tree, and the tree's Keys tab with live updates on.
/// </summary>
/// <param name="session">The persona's session.</param>
/// <param name="tree">The task board's tree, <c>a/task-board/tasks</c>.</param>
internal sealed class DataTaskTreePage(PersonaSession session, string tree)
{
    private const string Marker = "window.__u3KeysDocument";

    /// <summary>The directory table.</summary>
    public ILocator Directory => session.Shell.Content.GetByRole(AriaRole.Table, new() { Name = "Trees and views" });

    /// <summary>The directory's row for the tree: the row whose App column names the task board.</summary>
    public ILocator DirectoryRow => Directory.GetByRole(AriaRole.Row)
        .Filter(new() { Has = session.Page.GetByRole(AriaRole.Link, new() { Name = "task-board", Exact = true }) });

    /// <summary>The tree's own link, in the row header of <see cref="DirectoryRow"/>.</summary>
    public ILocator TreeLink => DirectoryRow.Locator("th a");

    /// <summary>The keys the Keys tab lists, as links whose title is the full key.</summary>
    public ILocator Keys => session.Shell.Content.Locator("a.lt-data-key");

    /// <summary>The Keys tab's live-updates switch.</summary>
    public ILocator LiveUpdates => session.Shell.Content.GetByRole(AriaRole.Switch, new() { Name = "Live updates" });

    /// <summary>Opens the Data directory through the spine and filters it to the task board's trees.</summary>
    public async Task OpenDirectoryAsync()
    {
        await session.Shell.GoToAreaAsync("data");
        await session.Shell.ExpectHeadingAsync("Data");
        await session.Shell.Content.GetByLabel("Filter trees and views").FillAsync("task-board");
        await Expect(DirectoryRow).ToHaveCountAsync(1, new() { Timeout = 60_000 });
    }

    /// <summary>Asserts the directory's App column names the task board for the tree, linking to the app.</summary>
    public async Task ExpectAppColumnAsync()
    {
        await Expect(Directory.GetByRole(AriaRole.Columnheader, new() { Name = "App" })).ToBeVisibleAsync();
        await Expect(TreeLink).ToHaveTextAsync(new Regex(Regex.Escape(tree) + "$|^tasks$"));
        var app = DirectoryRow.GetByRole(AriaRole.Link, new() { Name = "task-board", Exact = true });
        await Expect(app).ToHaveAttributeAsync("href", new Regex("/apps/task-board$"));
    }

    /// <summary>
    /// Opens the tree from the directory on its Keys tab, with live updates following a live
    /// scan, and marks the document, so <see cref="ExpectSameDocumentAsync"/> can prove that
    /// later rows arrived without a reload.
    /// </summary>
    public async Task OpenKeysAsync()
    {
        await TreeLink.ClickAsync();
        await Expect(session.Page).ToHaveURLAsync(new Regex("/data/" + Regex.Escape(tree) + @"(\?|$)"));
        await Expect(session.Shell.Content.GetByRole(AriaRole.Tab, new() { Name = "Keys", Exact = true })).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(LiveUpdates).ToHaveAttributeAsync("aria-checked", "true");
        await session.Page.EvaluateAsync($"() => {{ {Marker} = true; }}");
    }

    /// <summary>Asserts the page is still the document <see cref="OpenKeysAsync"/> marked: nothing reloaded it.</summary>
    public async Task ExpectSameDocumentAsync() =>
        Assert.That(await session.Page.EvaluateAsync<bool>($"() => {Marker} === true"), Is.True, "The Keys tab was reloaded: its rows did not arrive live.");

    /// <summary>Asserts the Keys tab lists exactly <paramref name="keys"/>, waiting for live updates to bring them in.</summary>
    /// <param name="keys">The keys, in any order.</param>
    public async Task ExpectKeysAsync(IReadOnlyCollection<string> keys)
    {
        await Expect(Keys).ToHaveCountAsync(keys.Count, new() { Timeout = 60_000 });
        foreach (var key in keys)
        {
            await Expect(session.Shell.Content.Locator($"a.lt-data-key[title=\"{key}\"]")).ToHaveCountAsync(1);
        }
    }
}
