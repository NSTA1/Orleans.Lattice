using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The rule builder around its cards: the shape picked from a sample, the live
/// check of the draft against that sample, the save warning that links to
/// remediation, the advanced view with the exact policy, reordering and
/// removing, and a sample that cannot be read.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaRuleBuilderTests : SchemaRuleBuilderTestBase
{
    private void UseOrders()
    {
        UseTrees("orders");
        Data.Use(
            "orders",
            ("order/1", "{\"total\":129.9,\"status\":\"shipped\",\"email\":\"a@example.com\",\"lines\":[{\"sku\":\"SKU-1\",\"qty\":1}]}"),
            ("order/2", "{\"total\":18,\"status\":\"open\",\"email\":\"b@example.com\",\"lines\":[{\"sku\":\"SKU-2\",\"qty\":3},{\"sku\":\"SKU-9\",\"qty\":1}]}"),
            ("order/3", "{\"total\":-5,\"status\":\"open\",\"email\":\"c@example.com\",\"lines\":[]}"),
            ("order/4", "{\"status\":\"cancelled\",\"email\":\"d@example.com\",\"lines\":[{\"sku\":\"SKU-3\",\"qty\":0}]}"));
    }

    [Test]
    public void The_shape_lists_the_members_seen_in_the_sample_with_their_types()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);

        cut.WaitUntil(() =>
        {
            var members = cut.FindAll(".lt-schema-shape__member .lt-schema-shape__name").Select(Text).ToArray();
            Assert.That(members, Is.EqualTo(new[] { "The whole value", "email", "lines", "each item", "qty", "sku", "status", "total" }));
            Assert.That(cut.Find(".lt-schema-shape__caption").TextContent, Does.Contain("From 4 sampled values"));
        });
    }

    [Test]
    public void Picking_a_member_makes_it_the_rule_path_and_seeds_the_gallery_from_it()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);

        cut.FindAll(".lt-schema-shape__member").Single(member => member.TextContent.Contains("total", StringComparison.Ordinal)).Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Field(cut, "Member path").GetAttribute("value"), Is.EqualTo("total"));
            Assert.That(cut.FindAll(".lt-schema-shape__member[aria-pressed='true']").Single().TextContent, Does.Contain("total"));
            var range = cut.FindAll(".lt-schema-gallery__option").Single(option => option.TextContent.Contains("Number range", StringComparison.Ordinal));
            Assert.That(range.TextContent, Does.Contain("Seen -5 to 129.9."));
        });
    }

    [Test]
    public void Picking_a_member_of_a_list_item_writes_an_every_item_rule()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);

        cut.FindAll(".lt-schema-shape__member").Single(member => member.TextContent.Contains("qty", StringComparison.Ordinal)).Click();
        Kind(cut, SchemaCardKind.NumberRange, gallery: 1);
        Type(cut, "Smallest", "1");
        Type(cut, "Largest", string.Empty);
        Commit(cut);

        cut.WaitUntil(() => Assert.That(Sentences(cut).Single(), Does.StartWith("1. lines must be a list in which: qty must be a")));
        Click(cut, "Save policy");
        cut.WaitUntil(() => Assert.That(cut.Find("[role=alertdialog]").TextContent, Does.Contain("1 of 4 sampled values fail"), "order/4 has a line of quantity 0"));
        Click(cut, "Save anyway");
        cut.WaitUntil(() => Assert.That(Schema.CountOf("SetPolicy"), Is.EqualTo(1)));
        Assert.That(Schema.Policies["orders"].Rules.Single().Predicate!.Value.Kind, Is.EqualTo(LatticePredicateNodeKind.Every));
    }

    [Test]
    public void One_of_is_seeded_with_the_values_seen()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "status");
        Kind(cut, SchemaCardKind.OneOf);

        cut.WaitUntil(() => Assert.That(
            cut.FindAll(".lt-combobox__chip").Select(chip => Text(chip)),
            Is.EqualTo(new[] { "open", "cancelled", "shipped" })));
    }

    [Test]
    public void A_format_every_value_matches_is_suggested_in_the_gallery()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "email");

        cut.WaitUntil(() => Assert.That(
            cut.FindAll(".lt-schema-gallery__option").Single(option => option.TextContent.Contains("Common format", StringComparison.Ordinal)).TextContent,
            Does.Contain("Every value seen is an email address.")));
    }

    [Test]
    public void The_draft_is_checked_live_against_the_sample()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "total");
        Kind(cut, SchemaCardKind.NumberRange);
        Type(cut, "Smallest", "0");
        Type(cut, "Largest", string.Empty);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-composer__example").TextContent, Does.Contain("Against the sample: 2 of 4 values pass.")));
        Commit(cut);

        cut.WaitUntil(() =>
        {
            var aside = cut.Find(".lt-schema-rulebuilder__aside");
            Assert.That(aside.TextContent, Does.Contain("2 would fail"));
            Assert.That(aside.QuerySelectorAll(".lt-schema-failures__item code").Select(Text), Is.EqualTo(new[] { "order/3", "order/4" }));
            Assert.That(aside.QuerySelector(".lt-schema-failures__reason")!.TextContent, Does.StartWith("Rule 1: The value did not satisfy"));
            Assert.That(cut.Find(".lt-schema-ruleset__rule").TextContent, Does.Contain("2 of 4 fail"));
        });
    }

    [Test]
    public void A_value_cut_short_by_the_scan_is_read_in_full()
    {
        UseOrders();
        Data.Truncated.Add("order/2");
        var cut = OpenEditor();

        cut.WaitUntil(() => Assert.That(Collapse(cut.Find(".lt-schema-rulebuilder__aside").TextContent), Does.Contain("Checked 4 values Pass 4 values")));
    }

    [Test]
    public void Saving_a_draft_that_fails_sampled_values_warns_and_links_to_remediation()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "total");
        Commit(cut);
        Click(cut, "Save policy");

        cut.WaitUntil(() =>
        {
            var dialog = cut.Find("[role=alertdialog]");
            Assert.That(dialog.TextContent, Does.Contain("1 of 4 sampled values fail the new rules."));
            Assert.That(dialog.QuerySelector("a")!.GetAttribute("href"), Does.Contain("schema/orders?tab=remediation"));
            Assert.That(Schema.CountOf("SetPolicy"), Is.Zero, "nothing is saved until the operator confirms");
        });

        Click(cut, "Keep editing");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty));
        Click(cut, "Save policy");
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Has.Count.EqualTo(1)));
        Click(cut, "Save anyway");

        cut.WaitUntil(() => Assert.That(Schema.Policies["orders"].Rules.Single().Kind, Is.EqualTo(LatticeSchemaRuleKind.Structured)));
    }

    [Test]
    public void A_draft_every_sampled_value_passes_saves_without_a_warning()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "status");
        Commit(cut);
        Save(cut);

        Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty);
    }

    [Test]
    public void Rules_can_be_reordered_and_removed_with_buttons()
    {
        UseTrees("orders");
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Utf8(), LatticeSchemaRule.Json(), LatticeSchemaRule.MaxLength(10)]);
        var cut = OpenEditor();
        cut.WaitUntil(() => Assert.That(Sentences(cut), Has.Count.EqualTo(3)));

        Assert.That(cut.Find("button[aria-label='Move rule 1 up']").HasAttribute("disabled"), Is.True);
        Assert.That(cut.Find("button[aria-label='Move rule 3 down']").HasAttribute("disabled"), Is.True);
        ClickLabelled(cut, "Move rule 3 up");
        ClickLabelled(cut, "Remove rule 1");

        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. The value must be at most 10 bytes", "2. The value must be one JSON document" })));
        Save(cut);
        Assert.That(Schema.Policies["orders"].Rules, Is.EqualTo(new[] { LatticeSchemaRule.MaxLength(10), LatticeSchemaRule.Json() }));
    }

    [Test]
    public void A_rule_is_edited_in_place()
    {
        UseTrees("orders");
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.MaxLength(10)]);
        var cut = OpenEditor();
        ClickLabelled(cut, "Edit rule 1");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-composer h3").TextContent, Is.EqualTo("Edit rule 1")));
        Type(cut, "Largest size, in bytes", "20");
        Commit(cut);

        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. The value must be at most 20 bytes" })));
    }

    [Test]
    public void The_advanced_view_shows_the_exact_policy_and_keeps_the_raw_editor()
    {
        UseTrees("orders");
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Regex("^[A-Z]{3}\\z", "currency")], strictIngest: true);
        var cut = OpenEditor();
        cut.FindAll("[role=switch]").Single(control => control.TextContent.Contains("Advanced", StringComparison.Ordinal)).Click();

        cut.WaitUntil(() =>
        {
            var json = cut.Find(".lt-schema-json").TextContent;
            Assert.That(json, Does.Contain("\"strictIngest\": true"));
            Assert.That(json, Does.Contain("\"member\": \"currency\""));
            Assert.That(cut.FindAll(".lt-schema-rules__item"), Has.Count.EqualTo(1));
        });

        Choose(cut, "Rule", nameof(SchemaRuleDraftKind.Json));
        Click(cut, "Add rule");
        cut.FindAll("[role=switch]").Single(control => control.TextContent.Contains("Advanced", StringComparison.Ordinal)).Click();

        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. currency must be a three-letter currency code", "2. The value must be one JSON document" })));
    }

    [Test]
    public void A_sample_that_cannot_be_read_says_so_and_can_be_read_again()
    {
        UseOrders();
        Data.ScanFault = new UnauthorizedAccessException("no");
        var cut = OpenEditor();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-rulebuilder__aside .lt-schema-note").TextContent, Is.Not.Empty));
        Data.ScanFault = null;
        cut.FindAll(".lt-schema-rulebuilder__aside button").Single(button => Text(button) == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-rulebuilder__aside").TextContent, Does.Contain("Checked")));
    }

    [Test]
    public void The_sample_scan_releases_its_cursor()
    {
        UseTrees("orders");
        Data.Use("orders", [.. Enumerable.Range(0, SchemaSampleReader.SampleSize + 5).Select(index => ($"k/{index:D4}", "{}"))]);

        var cut = OpenEditor();

        cut.WaitUntil(() =>
        {
            Assert.That(Data.Released, Is.EqualTo(new[] { "more" }));
            Assert.That(cut.Find(".lt-schema-rulebuilder__aside").TextContent, Does.Contain("of more"));
        });
    }

    [Test]
    public void Cancelling_the_composer_adds_nothing()
    {
        UseOrders();
        var cut = OpenEditor();
        StartRule(cut);
        Path(cut, "total");
        cut.FindAll(".lt-schema-composer button").Single(button => Text(button) == "Cancel").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-composer"), Is.Empty);
            Assert.That(Sentences(cut), Is.Empty);
        });
    }

    [Test]
    public void The_host_passes_a_save_error_and_a_busy_state_and_hears_cancel_and_save()
    {
        var cancelled = false;
        LatticeSchemaPolicy? saved = null;
        var cut = Render<SchemaRuleBuilder>(parameters => parameters
            .Add(builder => builder.Policy, new LatticeSchemaPolicy([LatticeSchemaRule.Json()]))
            .Add(builder => builder.SaveError, "The cluster refused it.")
            .Add(builder => builder.Busy, true)
            .Add(builder => builder.OnCancel, () => cancelled = true)
            .Add(builder => builder.OnSave, policy => saved = policy));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-schema-rulebuilder > .lt-schema-error").TextContent, Is.EqualTo("The cluster refused it."));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent.Trim() == "Save policy").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("Edit the policy"));
        });

        cut.FindAll(".lt-schema-rulebuilder > .lt-schema-actions button").Single(button => button.TextContent.Trim() == "Cancel").Click();
        Assert.That(cancelled, Is.True);

        cut.Render(parameters => parameters.Add(builder => builder.Busy, false));
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Save policy").Click();
        cut.WaitUntil(() => Assert.That(saved?.Rules, Is.EqualTo(new[] { LatticeSchemaRule.Json() })));
    }

    [Test]
    public void An_alternative_can_be_removed_and_the_group_collapses_back_to_a_rule()
    {
        UseTrees("orders");
        var open = LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("s"), LatticePredicateNode.Const(LatticeConstant.Text("a")));
        var list = LatticePredicateNode.TypeOf("s", LatticeValueKind.Array);
        Schema.Policies["orders"] = new LatticeSchemaPolicy([LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.Or, open, list), "either")]);
        var cut = OpenEditor();
        cut.WaitUntil(() => Assert.That(Sentences(cut).Single(), Does.StartWith("1. At least one of these must hold:")));

        ClickLabelled(cut, "Remove alternative 2 of rule 1");
        Save(cut);

        Assert.That(Schema.Policies["orders"].Rules.Single(), Is.EqualTo(LatticeSchemaRule.Structured(open, "either")));
    }
}
