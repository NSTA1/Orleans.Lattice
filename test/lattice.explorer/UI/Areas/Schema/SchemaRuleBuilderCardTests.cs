using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Every card type built in the rule builder, as an operator builds it, compiles
/// to the policy model the cluster enforces - and every such rule opens back in
/// the builder as the same card and saves unchanged.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaRuleBuilderCardTests : SchemaRuleBuilderTestBase
{
    private static LatticePredicateNode Member(string path) => LatticePredicateNode.Member(path);

    private static LatticePredicateNode Int(long value) => LatticePredicateNode.Const(LatticeConstant.Integer(value));

    private LatticeSchemaRule Build(Action<IRenderedComponent<SchemaTreePage>> configure, string expectedSentence)
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        configure(cut);
        Commit(cut);
        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. " + expectedSentence })));
        Save(cut);
        return Schema.Policies["scratch"].Rules.Single();
    }

    [Test]
    public void Required_compiles_to_a_presence_check()
    {
        var rule = Build(cut => Path(cut, "id"), "id must be present as text, a number or true or false");

        Assert.That(rule, Is.EqualTo(LatticeSchemaRule.Structured(
            LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, Member("id"), LatticePredicateNode.Const(LatticeConstant.Null())))));
    }

    [Test]
    public void Required_on_an_object_compiles_to_a_structural_presence_check()
    {
        var rule = Build(cut =>
        {
            Path(cut, "address");
            Tick(cut, "It holds an object or a list");
        }, "address must be present, as any value");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.TypeOf("address", LatticeValueKind.Present)));
    }

    [TestCase("Text", "must be text")]
    [TestCase("Number", "must be a number")]
    [TestCase("Boolean", "must be true or false")]
    [TestCase("Object", "must be an object")]
    [TestCase("List", "must be a list")]
    public void Type_compiles_to_a_check_the_cluster_evaluates(string type, string words)
    {
        var rule = Build(cut =>
        {
            Path(cut, "field");
            Kind(cut, SchemaCardKind.Type);
            Choose(cut, "Type", type);
        }, "field " + words);

        var value = type switch
        {
            "Text" => "{\"field\":\"x\"}",
            "Number" => "{\"field\":2.5}",
            "Boolean" => "{\"field\":false}",
            "Object" => "{\"field\":{}}",
            _ => "{\"field\":[]}",
        };
        var validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy([rule]));
        Assert.That(validator.Validate(System.Text.Encoding.UTF8.GetBytes(value)), Is.Null, value);
        Assert.That(validator.Validate(System.Text.Encoding.UTF8.GetBytes(type == "Text" ? "{\"field\":1}" : "{\"field\":\"x\"}")), Is.Not.Null);
    }

    [Test]
    public void One_of_compiles_to_equalities_joined_by_or()
    {
        var rule = Build(cut =>
        {
            Path(cut, "status");
            Kind(cut, SchemaCardKind.OneOf);
            foreach (var value in new[] { "open", "shipped" })
            {
                Field(cut, "Allowed values").Input(value);
                Field(cut, "Allowed values").KeyDown("Enter");
                cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-combobox__chip").Select(chip => chip.TextContent.Trim()), Does.Contain(value)));
            }
        }, "status must be one of \"open\" or \"shipped\"");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Bool(
            LatticeBooleanOperator.Or,
            LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("open"))),
            LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("shipped"))))));
    }

    [Test]
    public void Number_range_compiles_to_bounds_and_an_optional_whole_number_test()
    {
        var rule = Build(cut =>
        {
            Path(cut, "order.total");
            Kind(cut, SchemaCardKind.NumberRange);
            Type(cut, "Smallest", "0");
            Type(cut, "Largest", "10,000");
            Tick(cut, "Whole numbers only");
        }, "order.total must be a whole number between 0 and 10,000");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("order.total", LatticeValueKind.Integer),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, Member("order.total"), Int(0)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, Member("order.total"), Int(10000)))));
    }

    [Test]
    public void Text_length_compiles_to_a_text_test_and_length_bounds()
    {
        var rule = Build(cut =>
        {
            Path(cut, "name");
            Kind(cut, SchemaCardKind.TextLength);
            Type(cut, "Fewest characters", "2");
            Type(cut, "Most characters", "40");
        }, "name must be text of 2 to 40 characters");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("name", LatticeValueKind.String),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf("name"), Int(2)),
            LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.LengthOf("name"), Int(40)))));
    }

    [Test]
    public void A_format_compiles_to_its_pattern_and_shows_it_read_only()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "email");
        Kind(cut, SchemaCardKind.Format);
        Choose(cut, "Format", nameof(SchemaTextFormat.Email));

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-regex__pattern").TextContent, Is.EqualTo(SchemaFormatPatterns.PatternOf(SchemaTextFormat.Email))));
        Assert.That(cut.FindAll(".lt-schema-regex input"), Is.Empty, "the generated pattern is read-only");

        Commit(cut);
        Save(cut);
        Assert.That(Schema.Policies["scratch"].Rules.Single(), Is.EqualTo(LatticeSchemaRule.Regex(SchemaFormatPatterns.PatternOf(SchemaTextFormat.Email), "email")));
    }

    [Test]
    public void A_format_can_be_taken_over_as_a_regex()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "code");
        Kind(cut, SchemaCardKind.Format);
        Choose(cut, "Format", nameof(SchemaTextFormat.CurrencyCode));
        Click(cut, "Edit as regex");

        cut.WaitUntil(() => Assert.That(Field(cut, "Pattern (a regular expression)").GetAttribute("value"), Is.EqualTo("^[A-Z]{3}$")));
        Type(cut, "Pattern (a regular expression)", "^[A-Z]{3}-[0-9]$");
        Commit(cut);
        Save(cut);

        Assert.That(Schema.Policies["scratch"].Rules.Single(), Is.EqualTo(LatticeSchemaRule.Regex("^[A-Z]{3}-[0-9]$", "code")));
    }

    [Test]
    public void Starts_with_compiles_to_a_string_method_and_offers_its_regex()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "sku");
        Kind(cut, SchemaCardKind.TextMatch);
        Choose(cut, "Where", nameof(SchemaTextMatch.StartsWith));
        Type(cut, "Text", "SKU-");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-regex__pattern").TextContent, Is.EqualTo("^SKU-")));
        Commit(cut);
        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. sku must start with \"SKU-\"" })));
        Save(cut);

        Assert.That(Schema.Policies["scratch"].Rules.Single().Predicate, Is.EqualTo(
            LatticePredicateNode.StringCall(LatticeStringMethod.StartsWith, Member("sku"), LatticePredicateNode.Const(LatticeConstant.Text("SKU-")))));
    }

    [Test]
    public void List_length_compiles_to_a_list_test_and_item_count_bounds()
    {
        var rule = Build(cut =>
        {
            Path(cut, "lines");
            Kind(cut, SchemaCardKind.ListLength);
            Type(cut, "Fewest items", "1");
        }, "lines must be a list of at least 1 item");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf("lines", LatticeValueKind.Array),
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf("lines"), Int(1)))));
    }

    [Test]
    public void Every_item_composes_another_card_for_each_item()
    {
        var rule = Build(cut =>
        {
            Path(cut, "lines");
            Kind(cut, SchemaCardKind.EveryItem);
            Type(cut, "Member of each item", "qty");
            Kind(cut, SchemaCardKind.NumberRange, gallery: 1);
            Type(cut, "Smallest", "1");
        }, "lines must be a list in which: qty must be a number of at least 1");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Every(
            "lines",
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, Member("qty"), Int(1)))));
    }

    [Test]
    public void Every_item_nests_for_the_item_itself()
    {
        var rule = Build(cut =>
        {
            Path(cut, "tags");
            Kind(cut, SchemaCardKind.EveryItem);
            Kind(cut, SchemaCardKind.Type, gallery: 1);
            Choose(cut, "Type", "Text");
        }, "tags must be a list in which: Each item must be text");

        Assert.That(rule.Predicate, Is.EqualTo(LatticePredicateNode.Every(
            "tags",
            LatticePredicateNode.StringCall(LatticeStringMethod.StartsWith, LatticePredicateNode.Self(), LatticePredicateNode.Const(LatticeConstant.Text(string.Empty))))));
    }

    [Test]
    public void A_custom_pattern_has_a_live_tester()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "code");
        Kind(cut, SchemaCardKind.Pattern);
        Type(cut, "Pattern (a regular expression)", "^[A-Z]{3}$");
        Type(cut, "Try a value", "EUR");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-details__example").TextContent, Does.Contain("Matches.")));

        Type(cut, "Try a value", "euro");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-details__example").TextContent, Does.Contain("Does not match.")));
    }

    [Test]
    public void A_pattern_the_cluster_cannot_run_is_explained_and_not_added()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Kind(cut, SchemaCardKind.Pattern);
        Type(cut, "Pattern (a regular expression)", "(a)\\1");
        Commit(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-composer .lt-schema-error").TextContent, Does.StartWith("The cluster cannot run this pattern"));
            Assert.That(Sentences(cut), Is.Empty);
        });
    }

    [Test]
    public void The_whole_value_cards_compile_to_encoding_rules()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Kind(cut, SchemaCardKind.Encoding);
        Choose(cut, "The value must be", "Utf8");
        Commit(cut);
        StartRule(cut);
        Kind(cut, SchemaCardKind.MaxSize);
        Type(cut, "Largest size, in bytes", "4096");
        Commit(cut);

        cut.WaitUntil(() => Assert.That(Sentences(cut), Is.EqualTo(new[] { "1. The value must be well-formed UTF-8", "2. The value must be at most 4,096 bytes" })));
        Save(cut);
        Assert.That(Schema.Policies["scratch"].Rules, Is.EqualTo(new[] { LatticeSchemaRule.Utf8(), LatticeSchemaRule.MaxLength(4096) }));
    }

    [Test]
    public void A_whole_value_card_is_not_offered_for_a_member()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "total");

        cut.WaitUntil(() => Assert.That(
            cut.Find($"input[value='{SchemaCardKind.MaxSize}']").HasAttribute("disabled"), Is.True));
    }

    [Test]
    public void An_optional_card_accepts_a_missing_member()
    {
        var rule = Build(cut =>
        {
            Path(cut, "note");
            Kind(cut, SchemaCardKind.TextLength);
            Type(cut, "Most characters", "200");
            Tick(cut, "Also accept a missing value");
        }, "note must be text of at most 200 characters, when present");

        var validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy([rule]));
        Assert.That(validator.Validate("{}"u8.ToArray()), Is.Null);
        Assert.That(validator.Validate("{\"note\":null}"u8.ToArray()), Is.Null);
        Assert.That(validator.Validate("{\"note\":5}"u8.ToArray()), Is.Not.Null);
    }

    [Test]
    public void An_alternative_turns_a_rule_into_an_any_of_group()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "discount");
        Kind(cut, SchemaCardKind.NumberRange);
        Type(cut, "Smallest", "0");
        Commit(cut);
        ClickLabelled(cut, "Add an alternative to rule 1");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-composer h3").TextContent, Is.EqualTo("Add an alternative to rule 1")));
        Path(cut, "discount");
        Kind(cut, SchemaCardKind.Type);
        Choose(cut, "Type", "Boolean");
        Commit(cut);

        cut.WaitUntil(() => Assert.That(Sentences(cut).Single(), Does.StartWith("1. At least one of these must hold:")));
        Save(cut);

        Assert.That(Schema.Policies["scratch"].Rules.Single().Predicate!.Value.BooleanOperator, Is.EqualTo(LatticeBooleanOperator.Or));
        Assert.That(Schema.Policies["scratch"].Rules.Single().Predicate!.Value.Children, Has.Length.EqualTo(2));
    }

    [Test]
    public void A_pattern_card_is_not_offered_as_an_alternative()
    {
        UseTrees("scratch");
        var cut = OpenEditor("scratch");
        StartRule(cut);
        Path(cut, "a");
        Commit(cut);
        ClickLabelled(cut, "Add an alternative to rule 1");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find($"input[value='{SchemaCardKind.Pattern}']").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find($"input[value='{SchemaCardKind.Format}']").HasAttribute("disabled"), Is.True);
        });
    }

    [Test]
    public void A_card_message_is_saved_as_the_rule_description()
    {
        var rule = Build(cut =>
        {
            Path(cut, "id");
            Type(cut, "Message when a value fails (optional)", "every order has an id");
        }, "id must be present as text, a number or true or false");

        Assert.That(rule.Description, Is.EqualTo("every order has an id"));
    }

    [Test]
    public void Every_card_the_builder_writes_opens_as_the_same_card_and_saves_unchanged()
    {
        UseTrees("orders");
        LatticeSchemaRule[] rules =
        [
            LatticeSchemaRule.Structured(LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, Member("id"), LatticePredicateNode.Const(LatticeConstant.Null())), "has an id"),
            LatticeSchemaRule.Structured(LatticePredicateNode.TypeOf("lines", LatticeValueKind.Array)),
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("open"))),
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("shipped"))))),
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.And,
                LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, Member("total"), Int(0)),
                LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, Member("total"), LatticePredicateNode.Const(LatticeConstant.Real(9999.5))))),
            LatticeSchemaRule.Structured(LatticePredicateNode.Every("lines", LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, Member("note"), LatticePredicateNode.Const(LatticeConstant.Null())),
                LatticePredicateNode.StringCall(LatticeStringMethod.Contains, Member("note"), LatticePredicateNode.Const(LatticeConstant.Text("x")))))),
            LatticeSchemaRule.Regex(SchemaFormatPatterns.PatternOf(SchemaTextFormat.Uuid), "id"),
            LatticeSchemaRule.Regex("^[a-z]+$", "slug"),
            LatticeSchemaRule.Json(),
            LatticeSchemaRule.MaxLength(65536, "fits"),
        ];
        Schema.Policies["orders"] = new LatticeSchemaPolicy(rules, strictIngest: true);

        var cut = OpenEditor();
        cut.WaitUntil(() =>
        {
            Assert.That(Sentences(cut), Has.Count.EqualTo(rules.Length));
            Assert.That(cut.FindAll(".lt-schema-ruleset__rule .lt-pill").Select(pill => Text(pill)), Has.None.EqualTo("kept as it is"));
        });
        Save(cut);

        Assert.That(Schema.Policies["orders"].Rules, Is.EqualTo(rules));
        Assert.That(Schema.Policies["orders"].StrictIngest, Is.True);
    }

    [Test]
    public void A_rule_no_card_writes_is_kept_as_a_read_only_custom_card()
    {
        UseTrees("orders");
        var handWritten = LatticeSchemaRule.Structured(
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThan, Member("total"), Member("paid")),
            "total above paid");
        Schema.Policies["orders"] = new LatticeSchemaPolicy([handWritten, LatticeSchemaRule.Json()]);

        var cut = OpenEditor();
        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll(".lt-schema-ruleset__rule");
            Assert.That(Collapse(rows[0].TextContent), Does.Contain("A custom rule: total > paid"));
            Assert.That(rows[0].TextContent, Does.Contain("kept as it is"));
            Assert.That(rows[0].QuerySelectorAll("button").Select(Text), Has.None.EqualTo("Edit"), "a custom card is read-only");
            Assert.That(rows[1].QuerySelectorAll("button").Select(Text), Has.Some.EqualTo("Edit"), "the mapped rule stays a card");
        });

        ClickLabelled(cut, "Move rule 1 down");
        Save(cut);
        Assert.That(Schema.Policies["orders"].Rules, Is.EqualTo(new[] { LatticeSchemaRule.Json(), handWritten }));
    }
}
