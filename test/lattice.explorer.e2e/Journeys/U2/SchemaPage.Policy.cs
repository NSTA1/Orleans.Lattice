using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>The Policy tab: the rule builder, the regex take-over, saving, and clearing.</summary>
internal sealed partial class SchemaPage
{
    /// <summary>The rule builder.</summary>
    public ILocator RuleBuilder => Content.Locator("section.lt-schema-rulebuilder");

    /// <summary>The rules in the builder, read as sentences.</summary>
    public ILocator RuleSentences => RuleBuilder.Locator(".lt-schema-ruleset__rule > .lt-schema-ruleset__sentence");

    /// <summary>The composer writing a rule.</summary>
    public ILocator Composer => RuleBuilder.Locator("section.lt-schema-composer");

    /// <summary>The rules of the saved policy, as the Policy tab's table lists them.</summary>
    public ILocator PolicyRules => Panel.Locator("table tbody tr");

    /// <summary>
    /// Opens the rule builder with <b>Edit policy</b>, or <b>Set a policy</b> on a tree with
    /// none, and waits until its sample of the tree's values has been read.
    /// </summary>
    /// <param name="existing">Whether the tree has a policy to edit.</param>
    public async Task OpenRuleBuilderAsync(bool existing)
    {
        await Button(existing ? "Edit policy" : "Set a policy").ClickAsync();
        await Expect(RuleBuilder.GetByRole(AriaRole.Heading, new() { Name = existing ? "Edit the policy" : "Set a policy", Exact = true })).ToBeVisibleAsync();
        await Expect(RuleBuilder.Locator(".lt-schema-rulebuilder__aside")).ToContainTextAsync("Checked");
    }

    /// <summary>Opens the composer for a new rule on <paramref name="member"/>, and picks the card titled <paramref name="card"/>.</summary>
    /// <param name="member">The member path.</param>
    /// <param name="card">The card's title, such as <c>One of a set</c>.</param>
    public async Task ComposeRuleAsync(string member, string card)
    {
        await RuleBuilder.GetByRole(AriaRole.Button, new() { Name = "Add a rule", Exact = true }).ClickAsync();
        await Expect(Composer.GetByRole(AriaRole.Heading, new() { Name = "Add a rule", Exact = true })).ToBeVisibleAsync();
        await SetComposerPathAsync(member);
        await ChooseCardAsync(card);
    }

    /// <summary>Opens the composer on the builder's rule about <paramref name="member"/>.</summary>
    /// <param name="member">The member the rule checks.</param>
    public async Task EditRuleAboutAsync(string member)
    {
        var sentences = await RuleSentences.AllInnerTextsAsync();
        var index = sentences.ToList().FindIndex(sentence => sentence.Contains(member + " ", StringComparison.Ordinal));
        Assert.That(index, Is.GreaterThanOrEqualTo(0), $"no rule about {member}: {string.Join(" | ", sentences)}");
        await RuleBuilder.GetByRole(AriaRole.Button, new() { Name = $"Edit rule {index + 1}", Exact = true }).ClickAsync();
        await Expect(Composer.GetByRole(AriaRole.Heading, new() { Name = $"Edit rule {index + 1}", Exact = true })).ToBeVisibleAsync();
        await Expect(Composer.GetByRole(AriaRole.Combobox, new() { Name = "Member path", Exact = true })).ToHaveValueAsync(member);
    }

    /// <summary>Picks the card titled <paramref name="card"/> in the composer's gallery.</summary>
    /// <param name="card">The card's title.</param>
    public async Task ChooseCardAsync(string card)
    {
        var radio = CardRadio(card);
        await radio.CheckAsync();
        await Expect(radio).ToBeCheckedAsync();
    }

    /// <summary>The gallery's radio for the card titled <paramref name="card"/>.</summary>
    /// <param name="card">The card's title.</param>
    public ILocator CardRadio(string card) =>
        Composer.Locator(".lt-schema-gallery__option")
            .Filter(new() { Has = Page.Locator(".lt-schema-gallery__title", new() { HasTextRegex = new Regex($"^{Regex.Escape(card)}$") }) })
            .Locator("input[type=radio]")
            .First;

    /// <summary>Adds each of <paramref name="values"/> to a One of a set card's allowed values.</summary>
    /// <param name="values">The values.</param>
    public async Task AllowValuesAsync(params string[] values)
    {
        var input = Composer.GetByRole(AriaRole.Combobox, new() { Name = "Allowed values", Exact = true });
        foreach (var value in values)
        {
            await input.FillAsync(value);
            await input.PressAsync("Enter");
        }

        await Expect(Composer.Locator(".lt-combobox__chip")).ToHaveTextAsync(values);
    }

    /// <summary>Turns a switch of the composer on.</summary>
    /// <param name="label">The switch's label.</param>
    public async Task TurnOnAsync(string label)
    {
        var toggle = Composer.GetByRole(AriaRole.Switch, new() { Name = label });
        await toggle.ClickAsync();
        await Expect(toggle).ToHaveAttributeAsync("aria-checked", "true");
    }

    /// <summary>What the composer reads the rule being written as.</summary>
    public ILocator ComposerReads => Composer.Locator(".lt-schema-composer__reads");

    /// <summary>Commits the composer (<b>Add rule</b> or <b>Update rule</b>) and asserts the builder holds <paramref name="rules"/> rules.</summary>
    /// <param name="commit">The commit button's name.</param>
    /// <param name="rules">How many rules the builder then holds.</param>
    public async Task CommitRuleAsync(string commit, int rules)
    {
        await Composer.GetByRole(AriaRole.Button, new() { Name = commit, Exact = true }).ClickAsync();
        await Expect(Composer).ToHaveCountAsync(0);
        await Expect(RuleSentences).ToHaveCountAsync(rules);
    }

    /// <summary>
    /// On a Starts, ends or contains card: writes a prefix, reads the same check back as a
    /// pattern, and takes it over with <b>Edit as regex</b>, which turns the card into a Custom
    /// pattern card holding that expression.
    /// </summary>
    /// <param name="prefix">The prefix.</param>
    public async Task TakeOverPrefixAsRegexAsync(string prefix)
    {
        await Composer.GetByLabel("Where", new() { Exact = true }).SelectOptionAsync("StartsWith");
        await Composer.GetByLabel("Text", new() { Exact = true }).FillAsync(prefix);
        await Expect(Composer.Locator(".lt-schema-regex__pattern")).ToHaveTextAsync("^" + prefix);
        await Composer.GetByRole(AriaRole.Button, new() { Name = "Edit as regex", Exact = true }).ClickAsync();
        await Expect(CardRadio("Custom pattern")).ToBeCheckedAsync();
        await Expect(PatternInput).ToHaveValueAsync("^" + prefix);
    }

    /// <summary>The Custom pattern card's pattern.</summary>
    public ILocator PatternInput => Composer.GetByLabel("Pattern (a regular expression)", new() { Exact = true });

    /// <summary>Tries <paramref name="value"/> against the Custom pattern card's pattern, and asserts the tester's verdict.</summary>
    /// <param name="value">The value.</param>
    /// <param name="matches">Whether the pattern should accept it.</param>
    public async Task TryPatternAsync(string value, bool matches)
    {
        await Composer.GetByLabel("Try a value", new() { Exact = true }).FillAsync(value);
        await Expect(Composer.Locator(".lt-schema-details__example")).ToContainTextAsync(matches ? "Matches." : "Does not match.");
    }

    /// <summary>Saves the policy (every sampled value passing), and asserts the Policy tab lists <paramref name="rules"/> rules.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="rules">How many rules the saved policy holds.</param>
    public async Task SavePolicyAsync(string tree, int rules)
    {
        await Expect(RuleBuilder.Locator(".lt-schema-rulebuilder__aside").GetByText("All pass", new() { Exact = true })).ToBeVisibleAsync();
        await RuleBuilder.GetByRole(AriaRole.Button, new() { Name = "Save policy", Exact = true }).ClickAsync();
        await ExpectToastAsync($"The policy of {tree} is saved.");
        await Expect(RuleBuilder).ToHaveCountAsync(0);
        await Expect(PolicyRules).ToHaveCountAsync(rules);
    }

    /// <summary>Clears the tree's policy through its destructive confirmation, and asserts the tree has none.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="beforeConfirm">Called with the confirmation filled in, before confirming.</param>
    public async Task ClearPolicyAsync(string tree, Func<Task> beforeConfirm)
    {
        await Button("Clear policy").ClickAsync();
        await ConfirmAsync("Clear this tree's policy", tree, "Clear policy", beforeConfirm);
        await ExpectToastAsync($"The policy of {tree} is cleared.");
        await Expect(Panel.GetByRole(AriaRole.Heading, new() { Name = "No policy", Exact = true })).ToBeVisibleAsync();
    }

    private async Task SetComposerPathAsync(string member)
    {
        var path = Composer.GetByRole(AriaRole.Combobox, new() { Name = "Member path", Exact = true });
        await path.FillAsync(member);
        await path.PressAsync("Escape");
        await Expect(path).ToHaveValueAsync(member);
    }
}
