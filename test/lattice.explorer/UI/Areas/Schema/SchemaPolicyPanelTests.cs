using Bunit;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The Policy tab: get (rules and strict ingest), set (an editor seeded from the
/// applied policy), clear (a destructive confirmation naming the tree), a reader
/// who may not change it, faults, and the compact rule list.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaPolicyPanelTests : SchemaTestContext
{
    private IRenderedComponent<SchemaTreePage> Open(string tree = "orders", LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}", band);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static void ClickWhenShown(IRenderedComponent<SchemaTreePage> cut, string text)
    {
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == text), Is.EqualTo(1)));
        Button(cut, text).Click();
    }

    [Test]
    public void It_shows_the_rules_a_value_must_satisfy_and_strict_ingest()
    {
        UseEstate();

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll("tbody tr")[1].Children.Select(cell => cell.TextContent.Trim()),
                Is.EqualTo(new[] { "2", "Size", "The value is at most 4,096 bytes - fits a page" }));
            Assert.That(cut.Find("dl.lt-dl").TextContent, Does.Contain("3 rules, all of which a value must satisfy"));
            Assert.That(cut.Find("dl.lt-dl").TextContent, Does.Contain("Off: replicated and restored values are trusted"));
            Assert.That(Button(cut, "Edit policy").HasAttribute("disabled"), Is.False);
            Assert.That(Button(cut, "Clear policy").ClassList, Does.Contain("lt-btn--destructive"));
        });
    }

    [Test]
    public void A_tree_without_a_policy_accepts_everything_and_offers_to_set_one()
    {
        UseTrees("scratch");

        var cut = Open("scratch");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No policy"));
            Assert.That(Button(cut, "Set a policy"), Is.Not.Null);
        });
    }

    [Test]
    public void Editing_starts_from_the_applied_policy_and_saves_the_whole_policy()
    {
        UseEstate();
        var cut = Open();
        ClickWhenShown(cut, "Edit policy");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-ruleset__rule"), Has.Count.EqualTo(3), "seeded from the applied policy"));
        cut.Find("button[aria-label='Remove rule 1']").Click();
        Button(cut, "Add a rule").Click();
        cut.FindAll("input[type=radio]").Single(radio => radio.GetAttribute("value") == nameof(SchemaCardKind.Encoding)).Change(nameof(SchemaCardKind.Encoding));
        Button(cut, "Add rule").Click();
        cut.FindAll("[role=switch]").Single(control => control.TextContent.Contains("Strict ingest", StringComparison.Ordinal)).Click();
        Button(cut, "Save policy").Click();

        cut.WaitUntil(() =>
        {
            var saved = Schema.Policies["orders"];
            Assert.That(saved.Rules.Select(SchemaFormat.RuleKind), Is.EqualTo(new[] { "Size", "Pattern", "JSON" }));
            Assert.That(saved.StrictIngest, Is.True);
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("The policy of orders is saved."));
            Assert.That(cut.FindAll(".lt-schema-rulebuilder"), Is.Empty);
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.Find("dl.lt-dl").TextContent, Does.Contain("On: replicated and restored values are checked too"));
        });
    }

    [Test]
    public void A_rule_written_but_not_added_is_saved_rather_than_lost()
    {
        UseTrees("scratch");
        var cut = Open("scratch");
        ClickWhenShown(cut, "Set a policy");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-schema-rulebuilder"), Has.Count.EqualTo(1)));
        Button(cut, "Add a rule").Click();
        cut.FindAll("input[type=radio]").Single(radio => radio.GetAttribute("value") == nameof(SchemaCardKind.Pattern)).Change(nameof(SchemaCardKind.Pattern));
        SchemaRuleBuilderTestBase.Type(cut, "Pattern (a regular expression)", "^[a-z]+$");
        Button(cut, "Save policy").Click();

        cut.WaitUntil(() =>
        {
            var rule = Schema.Policies["scratch"].Rules.Single();
            Assert.That(rule.Kind, Is.EqualTo(LatticeSchemaRuleKind.Regex));
            Assert.That(rule.RegexPattern, Is.EqualTo("^[a-z]+$"));
        });
    }

    [Test]
    public void A_policy_needs_a_rule()
    {
        UseTrees("scratch");
        var cut = Open("scratch");
        ClickWhenShown(cut, "Set a policy");

        cut.WaitUntil(() =>
        {
            Assert.That(Button(cut, "Save policy").HasAttribute("disabled"), Is.True, "a policy with no rules is not offered for saving");
            Assert.That(cut.Find("#lt-schema-save-note").TextContent, Is.EqualTo("Add a rule to save the policy."));
            Assert.That(Schema.CountOf("SetPolicy"), Is.Zero);
        });
    }

    [Test]
    public void An_invalid_rule_is_explained_and_not_added()
    {
        UseTrees("scratch");
        var cut = Open("scratch");
        ClickWhenShown(cut, "Set a policy");

        ClickWhenShown(cut, "Add a rule");
        cut.FindAll("input[type=radio]").Single(radio => radio.GetAttribute("value") == nameof(SchemaCardKind.MaxSize)).Change(nameof(SchemaCardKind.MaxSize));
        SchemaRuleBuilderTestBase.Type(cut, "Largest size, in bytes", "big");
        Button(cut, "Add rule").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-composer .lt-schema-error").TextContent, Does.Contain("Enter the largest size, in bytes, as a whole number."));
            Assert.That(cut.FindAll(".lt-schema-ruleset__rule"), Is.Empty);
        });

        cut.FindAll(".lt-schema-rulebuilder > .lt-schema-actions button").Single(button => button.TextContent.Trim() == "Cancel").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No policy")));
    }

    [Test]
    public void A_structured_rule_is_shown_and_kept_as_it_is()
    {
        UseTrees("orders");
        var structured = new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured, Description = "has an id" };
        Schema.Policies["orders"] = new LatticeSchemaPolicy([structured]);
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr")[0].TextContent, Does.Contain("not edited")));

        Button(cut, "Edit policy").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-ruleset__rule").TextContent, Does.Contain("kept as it is")));
        Button(cut, "Add a rule").Click();
        cut.FindAll("input[type=radio]").Single(radio => radio.GetAttribute("value") == nameof(SchemaCardKind.Encoding)).Change(nameof(SchemaCardKind.Encoding));
        Button(cut, "Add rule").Click();
        Button(cut, "Save policy").Click();

        cut.WaitUntil(() => Assert.That(Schema.Policies["orders"].Rules[0], Is.EqualTo(structured)));
    }
    [Test]
    public void A_refused_save_is_explained_in_the_editor()
    {
        UseEstate();
        Schema.Faults["SetPolicy"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        ClickWhenShown(cut, "Edit policy");

        Button(cut, "Save policy").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-error").TextContent, Is.EqualTo("You are not permitted to set the policy.")));
    }

    [Test]
    public void Clearing_the_policy_needs_the_tree_named()
    {
        UseEstate();
        var cut = Open();
        ClickWhenShown(cut, "Clear policy");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("Every value will be accepted from now on"));
            Assert.That(cut.Find("[role=alertdialog] button[type=submit]").HasAttribute("disabled"), Is.True, "the tree must be named first");
        });

        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.Policies.ContainsKey("orders"), Is.False);
            Assert.That(ToastService.Toasts.Single().Message, Is.EqualTo("The policy of orders is cleared."));
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No policy"));
        });
    }

    [Test]
    public void A_refused_clear_is_reported()
    {
        UseEstate();
        Schema.Faults["ClearPolicy"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        ClickWhenShown(cut, "Clear policy");

        cut.Find("[role=alertdialog] input").Input("orders");
        cut.Find("[role=alertdialog] form").Submit();

        cut.WaitUntil(() =>
        {
            var toast = ToastService.Toasts.Single();
            Assert.That(toast.Message, Is.EqualTo("You are not permitted to clear the policy."));
            Assert.That(toast.Tone, Is.EqualTo(LtToastTone.Danger));
        });
    }

    [Test]
    public void A_reader_sees_the_policy_but_no_change_controls()
    {
        UseEstate();
        Schema.Capabilities["orders"] = FakeSchemaControl.ReadOnly;

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Has.None.EqualTo("Edit policy").And.None.EqualTo("Clear policy"));
        });
    }

    [Test]
    public void A_caller_who_may_not_read_the_policy_is_told_so()
    {
        UseEstate();
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.ReadOnly(tree) with { CanViewPolicy = false };

        var cut = Open();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("You may not read this tree's policy")));
    }

    [Test]
    public void A_policy_that_does_not_load_can_be_tried_again()
    {
        UseEstate();
        Schema.Faults["GetPolicy"] = new NotSupportedException("x");

        var cut = RenderAt<SchemaTreePage>("schema/orders");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__body").TextContent, Is.EqualTo(SchemaFailure.NotServed)));

        Schema.Faults.Remove("GetPolicy");
        cut.Find("[role=tabpanel] .lt-empty__actions button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel] tbody tr"), Has.Count.EqualTo(3)));
    }

    [Test]
    public void Below_the_small_breakpoint_the_rules_are_two_line_rows()
    {
        UseEstate();

        var cut = Open(band: LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("[role=tabpanel] table"), Is.Empty);
            Assert.That(cut.FindAll("li.lt-table-list__row .lt-compact-row__primary").Select(primary => primary.TextContent.Trim()),
                Is.EqualTo(new[] { "1. UTF-8", "2. Size", "3. Pattern" }));
        });
    }
}
