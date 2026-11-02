using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A4;

/// <summary>
/// The schema rule builder as A4 drives it: rules composed from cards (a member, a kind from the
/// gallery, the kind's details), the live check against the tree's sample, strict ingest, and
/// saving through the warning that some sampled values would not comply.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class SchemaRuleBuilderPage(ShellPage shell)
{
    /// <summary>The title of the warning raised by saving rules some sampled values fail.</summary>
    public const string NonCompliantSampleTitle = "Some sampled values would not comply";

    private IPage Page => shell.Page;

    private ILocator Content => shell.Content;

    /// <summary>The live check against a sample of the tree's values.</summary>
    public ILocator Preview => Content.GetByRole(AriaRole.Complementary, new() { Name = "Check against a sample", Exact = true });

    /// <summary>The draft rules, as sentences.</summary>
    public ILocator Rules => Content.GetByRole(AriaRole.List, new() { Name = "Rules in the new policy", Exact = true }).Locator(":scope > li");

    /// <summary>The composer of the rule being added.</summary>
    public ILocator Composer => Content.GetByRole(AriaRole.Region, new() { Name = "Add a rule", Exact = true });

    /// <summary>The builder's strict-ingest switch.</summary>
    public ILocator StrictIngest => Content.GetByRole(AriaRole.Switch, new() { Name = "Strict ingest", Exact = true });

    /// <summary>The warning raised when saving rules some sampled values fail.</summary>
    public ILocator SaveWarning => Page.GetByRole(AriaRole.Alertdialog, new() { Name = NonCompliantSampleTitle, Exact = true });

    /// <summary>Waits for the builder, with its sample read and no rules yet.</summary>
    public async Task ExpectOpenAsync()
    {
        await Expect(Content.GetByRole(AriaRole.Heading, new() { Name = "Set a policy", Level = 2, Exact = true })).ToBeVisibleAsync();
        await Expect(Content.GetByText("No rules yet. Add the first one: pick what it checks, then what it must be.")).ToBeVisibleAsync();
        await Expect(Preview.Locator("dt").Filter(new() { HasTextRegex = new Regex("^Checked$") })).ToBeVisibleAsync();
        await Expect(Content.GetByRole(AriaRole.Button, new() { Name = "Save policy", Exact = true })).ToBeDisabledAsync();
    }

    /// <summary>The preview's definition <paramref name="term"/>.</summary>
    /// <param name="term">The term, such as <c>Checked</c>.</param>
    public ILocator PreviewDefinition(string term) =>
        Preview.Locator("dt").Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }).Locator("xpath=following-sibling::dd[1]");

    /// <summary>Opens the composer and picks <paramref name="member"/> as what the new rule checks.</summary>
    /// <param name="member">The member path.</param>
    public async Task AddARuleAsync(string member)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Add a rule", Exact = true }).ClickAsync();
        await Expect(Composer).ToBeVisibleAsync();

        var path = Composer.GetByRole(AriaRole.Combobox, new() { Name = "Member path", Exact = true });
        await path.FillAsync(member);
        await Expect(path).ToHaveValueAsync(member);
        await path.PressAsync("Escape");
        await Expect(Composer.GetByRole(AriaRole.Group, new() { Name = "Members seen in the tree's values", Exact = true })
            .GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^" + Regex.Escape(member) + @"\b") })).ToHaveAttributeAsync("aria-pressed", "true");
    }

    /// <summary>Chooses the gallery's card <paramref name="title"/> for the rule being added.</summary>
    /// <param name="title">The card's title, such as <c>Number range</c>.</param>
    public async Task ChooseCardAsync(string title)
    {
        var card = Composer.GetByRole(AriaRole.Radiogroup, new() { Name = "Constraint", Exact = true })
            .GetByRole(AriaRole.Radio, new() { NameRegex = new Regex("^" + Regex.Escape(title)) });
        await card.CheckAsync();
        await Expect(card).ToBeCheckedAsync();
    }

    /// <summary>Sets the Number range card's bounds; an empty bound is open.</summary>
    /// <param name="smallest">The smallest number.</param>
    /// <param name="largest">The largest number.</param>
    public async Task SetRangeAsync(string smallest, string largest)
    {
        await Composer.GetByLabel("Smallest", new() { Exact = true }).FillAsync(smallest);
        await Composer.GetByLabel("Largest", new() { Exact = true }).FillAsync(largest);
        var whole = Composer.GetByRole(AriaRole.Switch, new() { Name = "Whole numbers only", Exact = true });
        if (await whole.GetAttributeAsync("aria-checked") == "true")
        {
            await whole.ClickAsync();
        }

        await Expect(whole).ToHaveAttributeAsync("aria-checked", "false");
    }

    /// <summary>
    /// Makes the One of a set card's allowed values exactly <paramref name="values"/>, in that
    /// order: the values the sample seeded are removed, then each is typed and added.
    /// </summary>
    /// <param name="values">The allowed values.</param>
    public async Task SetAllowedValuesAsync(IReadOnlyList<string> values)
    {
        var chosen = Composer.GetByRole(AriaRole.List, new() { Name = "Allowed values, chosen", Exact = true });
        foreach (var seeded in await chosen.Locator(":scope > li").AllInnerTextsAsync())
        {
            await Composer.GetByRole(AriaRole.Button, new() { Name = "Remove " + seeded.Trim(), Exact = true }).ClickAsync();
        }

        await Expect(chosen).ToHaveCountAsync(0);
        var field = Composer.GetByRole(AriaRole.Combobox, new() { Name = "Allowed values", Exact = true });
        foreach (var value in values)
        {
            await field.FillAsync(value);
            await Expect(field).ToHaveValueAsync(value);
            await field.PressAsync("Enter");
            await Expect(field).ToHaveValueAsync(string.Empty);
        }

        await field.PressAsync("Escape");
        await Expect(chosen.Locator(":scope > li")).ToHaveTextAsync([.. values]);
        var numbers = Composer.GetByRole(AriaRole.Switch, new() { Name = "Compare as numbers", Exact = true });
        await Expect(numbers).ToHaveAttributeAsync("aria-checked", "false");
    }

    /// <summary>Asserts the composer reads <paramref name="sentence"/> and states how the sample fares against it.</summary>
    /// <param name="sentence">The rule as a sentence.</param>
    /// <param name="sample">The sample's outcome, such as <c>99 of 100 values pass</c>.</param>
    public async Task ExpectComposerReadsAsync(string sentence, string sample)
    {
        await Expect(Composer.Locator(".lt-schema-composer__reads")).ToContainTextAsync(sentence);
        await Expect(Composer.Locator(".lt-schema-composer__example")).ToContainTextAsync(sample);
    }

    /// <summary>Commits the composer's card as the next rule, and asserts it reads <paramref name="sentence"/>.</summary>
    /// <param name="sentence">The rule as a sentence.</param>
    public async Task CommitRuleAsync(string sentence)
    {
        var before = await Rules.CountAsync();
        await Composer.GetByRole(AriaRole.Button, new() { Name = "Add rule", Exact = true }).ClickAsync();
        await Expect(Composer).ToHaveCountAsync(0);
        await Expect(Rules).ToHaveCountAsync(before + 1);
        await Expect(Rules.Nth(before)).ToContainTextAsync(sentence);
    }

    /// <summary>
    /// Asserts the sample check: <paramref name="failing"/> of <paramref name="checkedCount"/>
    /// sampled values fail, the first failing one being <paramref name="firstFailure"/>.
    /// </summary>
    /// <param name="failing">How many sampled values fail.</param>
    /// <param name="checkedCount">How many values were checked.</param>
    /// <param name="firstFailure">The key of the first failing value.</param>
    public async Task ExpectSampleCheckAsync(int failing, int checkedCount, string firstFailure)
    {
        await Expect(Preview).ToContainTextAsync($"{failing} would fail");
        await Expect(PreviewDefinition("Checked")).ToHaveTextAsync($"{checkedCount} values");
        await Expect(PreviewDefinition("Fail")).ToHaveTextAsync(failing == 1 ? "1 value" : $"{failing} values");
        await Expect(Preview.GetByRole(AriaRole.Heading, new() { Name = "First failing values", Exact = true })).ToBeVisibleAsync();
        await Expect(Preview.GetByRole(AriaRole.List).Locator(":scope > li code")).ToHaveTextAsync([firstFailure]);
        Coverage.Hit("use-case:schema/rule-builder");
    }

    /// <summary>Turns the builder's strict-ingest switch on.</summary>
    public async Task TurnStrictIngestOnAsync()
    {
        await Expect(StrictIngest).ToHaveAttributeAsync("aria-checked", "false");
        await StrictIngest.ClickAsync();
        await Expect(StrictIngest).ToHaveAttributeAsync("aria-checked", "true");
    }

    /// <summary>Saves the policy, which warns that <paramref name="share"/> sampled values fail it.</summary>
    /// <param name="share">The failing share, such as <c>1 of 100</c>.</param>
    public async Task SaveIntoWarningAsync(string share)
    {
        await Content.GetByRole(AriaRole.Button, new() { Name = "Save policy", Exact = true }).ClickAsync();
        await Expect(SaveWarning).ToBeVisibleAsync();
        await Expect(SaveWarning).ToContainTextAsync($"{share} sampled values fail the new rules.");
        await Expect(SaveWarning.GetByRole(AriaRole.Link, new() { Name = "Plan a remediation", Exact = true })).ToBeVisibleAsync();
    }

    /// <summary>Saves anyway, past the warning, and waits for the saved policy.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task SaveAnywayAsync(string tree)
    {
        await SaveWarning.GetByRole(AriaRole.Button, new() { Name = "Save anyway", Exact = true }).ClickAsync();
        await Expect(SaveWarning).ToHaveCountAsync(0);
        await Expect(shell.Toasts.Filter(new() { HasText = $"The policy of {tree} is saved." })).ToHaveCountAsync(1);
        Coverage.Hit("use-case:schema/sample-check");
    }
}
