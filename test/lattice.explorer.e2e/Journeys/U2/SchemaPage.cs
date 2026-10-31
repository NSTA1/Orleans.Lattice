using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>
/// The Schema area as a schema owner drives it: the directory, one tree's workspace and its
/// five tabs. Each method acts, asserts the state it leads to, and only then records the
/// catalogue features it proved.
/// </summary>
internal sealed partial class SchemaPage(ShellPage shell)
{
    /// <summary>How long a background schema operation (a remediation or a migration) is given to finish.</summary>
    public static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(6);

    /// <summary>The shell.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The page's main content.</summary>
    public ILocator Content => Shell.Content;

    /// <summary>A button of the content, by its exact name.</summary>
    /// <param name="name">The name.</param>
    public ILocator Button(string name) => Content.GetByRole(AriaRole.Button, new() { Name = name, Exact = true });

    /// <summary>A destructive confirmation or alert dialog.</summary>
    public ILocator AlertDialog => Page.GetByRole(AriaRole.Alertdialog);

    /// <summary>Opens <paramref name="tree"/>'s workspace on <paramref name="tab"/> by its address, and waits for the tab.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="tab">The tab id, or <see langword="null"/> for the default (the policy).</param>
    public async Task OpenTreeAsync(string tree, string? tab = null)
    {
        await Shell.GotoAsync(tab is null ? $"/schema/{tree}" : $"/schema/{tree}?tab={tab}");
        await Expect(Shell.Heading).ToHaveTextAsync(tree);
        await Expect(Tab(TitleOf(tab ?? "policy"))).ToHaveAttributeAsync("aria-selected", "true");
        Coverage.Hit("route:/schema/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
    }

    /// <summary>A tab of the open workspace, by its title.</summary>
    /// <param name="title">The tab's title, such as <c>Compliance</c>.</param>
    public ILocator Tab(string title) => Content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });

    /// <summary>The open tab's panel.</summary>
    public ILocator Panel => Content.GetByRole(AriaRole.Tabpanel);

    /// <summary>Chooses a tab of the open workspace, and asserts the address names it.</summary>
    /// <param name="tab">The tab id, such as <c>compliance</c>.</param>
    public async Task ChooseTabAsync(string tab)
    {
        await Tab(TitleOf(tab)).ClickAsync();
        await Expect(Tab(TitleOf(tab))).ToHaveAttributeAsync("aria-selected", "true");
        if (tab != "policy")
        {
            await Expect(Page).ToHaveURLAsync(new Regex($"[?&]tab={Regex.Escape(tab)}(&|$)"));
            Coverage.Hit("query:schema.tab");
        }

        Coverage.Hit("tab:schema/" + tab);
    }

    /// <summary>Waits for a toast saying exactly <paramref name="message"/>.</summary>
    /// <param name="message">The toast's text.</param>
    public Task ExpectToastAsync(string message) =>
        Expect(Shell.Toasts.Filter(new() { HasText = message })).Not.ToHaveCountAsync(0);

    /// <summary>Completes an open destructive confirmation: types the tree's name, and confirms.</summary>
    /// <param name="title">The confirmation's title.</param>
    /// <param name="tree">The tree it names.</param>
    /// <param name="confirm">The confirming button's name.</param>
    /// <param name="beforeConfirm">Called once the name is typed, before confirming (to photograph it), or <see langword="null"/>.</param>
    public async Task ConfirmAsync(string title, string tree, string confirm, Func<Task>? beforeConfirm = null)
    {
        var dialog = AlertDialog.Filter(new() { HasText = title });
        await Expect(dialog).ToBeVisibleAsync();
        var button = dialog.GetByRole(AriaRole.Button, new() { Name = confirm, Exact = true });
        await Expect(button).ToBeDisabledAsync();
        await dialog.GetByLabel("Tree name", new() { Exact = true }).FillAsync(tree);
        await Expect(button).ToBeEnabledAsync();
        if (beforeConfirm is not null)
        {
            await beforeConfirm();
        }

        await button.ClickAsync();
        await Expect(dialog).ToHaveCountAsync(0);
    }

    private static string TitleOf(string tab) => tab switch
    {
        "policy" => "Policy",
        "versions" => "Versions",
        "compliance" => "Compliance",
        "remediation" => "Remediation",
        "dead-letters" => "Dead letters",
        _ => throw new ArgumentOutOfRangeException(nameof(tab), tab, "Not a schema tab."),
    };
}
