using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U6;

/// <summary>
/// The Schema area as U6 (#5385) drives it: a tree's schema workspace, its Policy tab's rule
/// builder (the rule set as sentences, the composer with its member shape and card gallery, the
/// sample check and the Advanced view), and its Versions and Remediation tabs.
/// </summary>
/// <param name="shell">The session's shell.</param>
internal sealed class SchemaWorkshopPage(ShellPage shell)
{
    /// <summary>How long a background schema operation is given to finish, in milliseconds.</summary>
    public const float OperationPatienceMs = 6 * 60 * 1000;

    /// <summary>The session's shell.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The main content.</summary>
    public ILocator Content => Shell.Content;

    /// <summary>The rule builder.</summary>
    public ILocator Builder => Content.Locator("section.lt-schema-rulebuilder");

    /// <summary>The sample check beside the builder.</summary>
    public ILocator Preview => Content.Locator(".lt-schema-rulebuilder__aside");

    /// <summary>The rules of the draft policy, in order.</summary>
    public ILocator Rules => Content.Locator("ol.lt-schema-ruleset > li.lt-schema-ruleset__rule");

    /// <summary>The composer, while a rule is being written.</summary>
    public ILocator Composer => Content.Locator("section.lt-schema-composer");

    /// <summary>The composer's "Reads:" sentence.</summary>
    public ILocator Reads => Composer.Locator(".lt-schema-composer__reads");

    /// <summary>The Advanced view's exact policy, as JSON.</summary>
    public ILocator ExactPolicy => Content.Locator("pre[aria-label='The exact policy, as JSON']");

    /// <summary>The Advanced view's raw rule composer.</summary>
    public ILocator RawComposer => Content.Locator("fieldset.lt-schema-builder");

    /// <summary>The Advanced switch.</summary>
    public ILocator AdvancedSwitch => Builder.GetByRole(AriaRole.Switch, new() { Name = "Advanced", Exact = true });

    /// <summary>The headline of the tree's background operation.</summary>
    public ILocator OperationHeadline => Content.Locator("section.lt-schema-operation p.lt-schema-status");

    /// <summary>A tab of the tree's workspace.</summary>
    /// <param name="title">The tab's title, such as <c>Versions</c>.</param>
    public ILocator Tab(string title) => Content.GetByRole(AriaRole.Tab, new() { Name = title, Exact = true });

    /// <summary>A button in the content, by its exact accessible name.</summary>
    /// <param name="name">The name.</param>
    public ILocator Button(string name) => Content.GetByRole(AriaRole.Button, new() { Name = name, Exact = true });

    /// <summary>The sentence of rule <paramref name="number"/> (1-based).</summary>
    /// <param name="number">The rule's number.</param>
    public ILocator Sentence(int number) => Rules.Nth(number - 1).Locator(":scope > .lt-schema-ruleset__sentence");

    /// <summary>The gallery option of a card kind in the composer's top-level gallery.</summary>
    /// <param name="kind">The card kind's name, such as <c>NumberRange</c>.</param>
    public ILocator Card(string kind) => Composer.Locator($"label[for='schema-kind-root-{kind}']");

    /// <summary>The radio of a card kind in the composer's top-level gallery.</summary>
    /// <param name="kind">The card kind's name.</param>
    public ILocator CardRadio(string kind) => Composer.Locator($"#schema-kind-root-{kind}");

    /// <summary>The live example (or the reason it is disabled) under a card kind.</summary>
    /// <param name="kind">The card kind's name.</param>
    public ILocator CardExample(string kind) => Card(kind).Locator(".lt-schema-gallery__example");

    /// <summary>A definition's value (<c>dd</c>) in the content, by its exact term.</summary>
    /// <param name="term">The term, such as <c>Target version</c>.</param>
    public ILocator Definition(string term) =>
        Content.Locator(".lt-dl__row").Filter(new()
        {
            Has = Page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }),
        }).Locator("dd");

    /// <summary>Opens the Schema area from the spine, and then <paramref name="tree"/>'s workspace from its directory.</summary>
    /// <param name="tree">The tree.</param>
    public async Task OpenTreeAsync(string tree)
    {
        await Shell.GoToAreaAsync("schema");
        await Shell.ExpectHeadingAsync("Schema");
        await Content.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true }).First.ClickAsync();
        await Shell.ExpectHeadingAsync(tree);
        await Expect(Tab("Policy")).ToHaveAttributeAsync("aria-selected", "true");
    }

    /// <summary>Opens a tab of the tree's workspace.</summary>
    /// <param name="title">The tab's title.</param>
    public async Task OpenTabAsync(string title)
    {
        await Tab(title).ClickAsync();
        await Expect(Tab(title)).ToHaveAttributeAsync("aria-selected", "true");
    }

    /// <summary>Opens the rule builder on the tree's existing policy, and waits until the sample is checked.</summary>
    public async Task EditPolicyAsync()
    {
        await Button("Edit policy").ClickAsync();
        await Expect(Builder.GetByRole(AriaRole.Heading, new() { Name = "Edit the policy", Exact = true })).ToBeVisibleAsync();
        await WaitForSampleAsync();
    }

    /// <summary>Waits until the sample check reports what it checked.</summary>
    public Task WaitForSampleAsync() =>
        Expect(Preview).ToContainTextAsync("Checked", new() { Timeout = 30_000 });

    /// <summary>Opens the composer for a new rule.</summary>
    public async Task OpenComposerAsync()
    {
        await Button("Add a rule").ClickAsync();
        await Expect(Composer.GetByRole(AriaRole.Heading, new() { Name = "Add a rule", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Chooses <paramref name="member"/> in the composer's member shape (<c>The whole value</c> for the whole value).</summary>
    /// <param name="member">The member's name as the shape shows it.</param>
    public async Task ChooseMemberAsync(string member)
    {
        var button = Composer.Locator(".lt-schema-shape__member").Filter(new()
        {
            Has = Page.Locator(".lt-schema-shape__name", new() { HasTextRegex = new Regex("^" + Regex.Escape(member) + "$") }),
        }).First;
        await button.ClickAsync();
        await Expect(button).ToHaveAttributeAsync("aria-pressed", "true");
    }

    /// <summary>Chooses a card kind in the composer's gallery.</summary>
    /// <param name="kind">The card kind's name.</param>
    public async Task ChooseCardAsync(string kind)
    {
        await Expect(CardRadio(kind)).ToBeEnabledAsync();
        await Card(kind).ClickAsync();
        await Expect(CardRadio(kind)).ToBeCheckedAsync();
    }

    /// <summary>Fills a text field by its exact label.</summary>
    /// <param name="scope">Where the field is.</param>
    /// <param name="label">The label.</param>
    /// <param name="value">The value.</param>
    public static async Task FillAsync(ILocator scope, string label, string value)
    {
        var field = scope.GetByLabel(label, new() { Exact = true });
        await field.FillAsync(value);
        await Expect(field).ToHaveValueAsync(value);
    }

    /// <summary>Chooses an option of a select by its exact label.</summary>
    /// <param name="scope">Where the select is.</param>
    /// <param name="label">The label.</param>
    /// <param name="value">The option's value.</param>
    public static async Task SelectAsync(ILocator scope, string label, string value)
    {
        var select = scope.GetByLabel(label, new() { Exact = true });
        await select.SelectOptionAsync(value);
        await Expect(select).ToHaveValueAsync(value);
    }

    /// <summary>Turns a switch of the composer on or off by its exact label.</summary>
    /// <param name="label">The label.</param>
    /// <param name="on">Whether it should be on.</param>
    public async Task SetSwitchAsync(string label, bool on)
    {
        var toggle = Composer.GetByRole(AriaRole.Switch, new() { Name = label, Exact = true });
        if (await toggle.GetAttributeAsync("aria-checked") != (on ? "true" : "false"))
        {
            await toggle.ClickAsync();
        }

        await Expect(toggle).ToHaveAttributeAsync("aria-checked", on ? "true" : "false");
    }

    /// <summary>Commits the composer's rule with <paramref name="commit"/>, and waits for the rule set to hold <paramref name="rules"/> rules.</summary>
    /// <param name="commit">The commit button's name: <c>Add rule</c> or <c>Add alternative</c>.</param>
    /// <param name="rules">How many rules the set then holds.</param>
    public async Task CommitAsync(string commit, int rules)
    {
        await Composer.GetByRole(AriaRole.Button, new() { Name = commit, Exact = true }).ClickAsync();
        await Expect(Composer).ToHaveCountAsync(0);
        await Expect(Rules).ToHaveCountAsync(rules);
    }

    /// <summary>Switches the builder's Advanced view on or off.</summary>
    /// <param name="on">Whether it should be on.</param>
    public async Task SetAdvancedAsync(bool on)
    {
        if (await AdvancedSwitch.GetAttributeAsync("aria-checked") != (on ? "true" : "false"))
        {
            await AdvancedSwitch.ClickAsync();
        }

        await Expect(AdvancedSwitch).ToHaveAttributeAsync("aria-checked", on ? "true" : "false");
        await Expect(Builder.GetByRole(AriaRole.Heading, new() { Name = on ? "Rules, as the policy stores them" : "Rules", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Waits for a toast saying <paramref name="message"/>.</summary>
    /// <param name="message">The toast's text, or part of it.</param>
    public Task ExpectToastAsync(string message) =>
        Expect(Shell.Toasts.Filter(new() { HasText = message }).First).ToBeVisibleAsync();

    /// <summary>Saves the policy, and waits for the confirmation and the read-only policy view.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="rules">How many rules the saved policy holds.</param>
    public async Task SaveAsync(string tree, int rules)
    {
        await Button("Save policy").ClickAsync();
        await ExpectToastAsync($"The policy of {tree} is saved.");
        await Expect(Builder).ToHaveCountAsync(0);
        await Expect(Definition("Rules")).ToContainTextAsync($"{rules} rules, all of which a value must satisfy");
    }
}
