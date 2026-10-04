using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The card model below the builder: what each card compiles to and refuses,
/// that every card reads back from its own output, that anything else becomes a
/// custom card holding the rule unchanged, and the sentence each card reads as.
/// </summary>
[TestFixture]
public sealed class SchemaCardModelTests
{
    private static SchemaRuleCard Card(SchemaCardKind kind, string path = "", Action<SchemaRuleCard>? configure = null)
    {
        var card = SchemaRuleCard.Of(kind, path);
        configure?.Invoke(card);
        return card;
    }

    private static IEnumerable<TestCaseData> EveryCard()
    {
        yield return new TestCaseData("required", Card(SchemaCardKind.Required, "id"), "id must be present as text, a number or true or false");
        yield return new TestCaseData("required object", Card(SchemaCardKind.Required, "a", card => card.Structural = true), "a must be present, as any value");
        foreach (var type in Enum.GetValues<SchemaValueType>())
        {
            yield return new TestCaseData("type " + type, Card(SchemaCardKind.Type, "t", card => card.ValueType = type), "t must be " + SchemaCardText.TypePhrase(type));
        }

        yield return new TestCaseData("one of text", Card(SchemaCardKind.OneOf, "s", card => card.Values = ["a", "b", "c"]), "s must be one of \"a\", \"b\" or \"c\"");
        yield return new TestCaseData("one of number", Card(SchemaCardKind.OneOf, "n", card => { card.Values = ["1", "2.5"]; card.ValuesAreNumbers = true; }), "n must be one of 1 or 2.5");
        yield return new TestCaseData("one value", Card(SchemaCardKind.OneOf, "s", card => card.Values = ["x"]), "s must be \"x\"");
        yield return new TestCaseData("range", Card(SchemaCardKind.NumberRange, "n", card => { card.Minimum = "-1.5"; card.Maximum = "10000"; }), "n must be a number between -1.5 and 10,000");
        yield return new TestCaseData("minimum", Card(SchemaCardKind.NumberRange, "n", card => card.Minimum = "0"), "n must be a number of at least 0");
        yield return new TestCaseData("maximum", Card(SchemaCardKind.NumberRange, "n", card => card.Maximum = "9"), "n must be a number of at most 9");
        yield return new TestCaseData("whole", Card(SchemaCardKind.NumberRange, "n", card => card.IntegerOnly = true), "n must be a whole number");
        yield return new TestCaseData("text length", Card(SchemaCardKind.TextLength, "s", card => { card.Minimum = "1"; card.Maximum = "5"; }), "s must be text of 1 to 5 characters");
        yield return new TestCaseData("list length", Card(SchemaCardKind.ListLength, "l", card => card.Maximum = "1"), "l must be a list of at most 1 item");
        yield return new TestCaseData("format", Card(SchemaCardKind.Format, "e", card => card.Format = SchemaTextFormat.Email), "e must be an email address");
        yield return new TestCaseData("starts", Card(SchemaCardKind.TextMatch, "s", card => card.MatchText = "x"), "s must start with \"x\"");
        yield return new TestCaseData("ends", Card(SchemaCardKind.TextMatch, "s", card => { card.Match = SchemaTextMatch.EndsWith; card.MatchText = "x"; }), "s must end with \"x\"");
        yield return new TestCaseData("contains", Card(SchemaCardKind.TextMatch, "s", card => { card.Match = SchemaTextMatch.Contains; card.MatchText = "x"; }), "s must contain \"x\"");
        yield return new TestCaseData("every", Card(SchemaCardKind.EveryItem, "l", card => card.Item = Card(SchemaCardKind.Type)), "l must be a list in which: Each item must be text");
        yield return new TestCaseData("nested every", Card(SchemaCardKind.EveryItem, "g", card => card.Item = Card(SchemaCardKind.EveryItem, string.Empty, inner => inner.Item = Card(SchemaCardKind.NumberRange, string.Empty, cell => cell.IntegerOnly = true))), "g must be a list in which: Each item must be a list in which: Each item must be a whole number");
        yield return new TestCaseData("pattern", Card(SchemaCardKind.Pattern, "c", card => card.Pattern = "^x$"), "c must match the pattern ^x$");
        yield return new TestCaseData("whole pattern", Card(SchemaCardKind.Pattern, string.Empty, card => card.Pattern = "^x$"), "The value must match the pattern ^x$");
        yield return new TestCaseData("utf8", Card(SchemaCardKind.Encoding, string.Empty, card => card.Encoding = LatticeSchemaEncodingKind.Utf8), "The value must be well-formed UTF-8");
        yield return new TestCaseData("json", Card(SchemaCardKind.Encoding), "The value must be one JSON document");
        yield return new TestCaseData("size", Card(SchemaCardKind.MaxSize, string.Empty, card => card.MaxBytes = "1024"), "The value must be at most 1,024 bytes");
        yield return new TestCaseData("one byte", Card(SchemaCardKind.MaxSize, string.Empty, card => card.MaxBytes = "1"), "The value must be at most 1 byte");
        yield return new TestCaseData("optional", Card(SchemaCardKind.TextLength, "s", card => { card.Maximum = "3"; card.Optional = true; }), "s must be text of at most 3 characters, when present");
        yield return new TestCaseData("any of", Card(SchemaCardKind.AnyOf, string.Empty, card => card.Alternatives = [Card(SchemaCardKind.Type, "a"), Card(SchemaCardKind.Required, "b")]), "At least one of these must hold: a must be text or b must be present as text, a number or true or false");
    }

    [TestCaseSource(nameof(EveryCard))]
    public void Every_card_compiles_reads_back_as_itself_and_reads_as_a_sentence(string name, object boxed, string sentence)
    {
        var card = (SchemaRuleCard)boxed;
        card.Description = "because";

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, $"{name}: {error}");
        var read = SchemaCardDecompiler.Decompile(rule);

        Assert.Multiple(() =>
        {
            Assert.That(read.Kind, Is.EqualTo(card.Kind), name);
            Assert.That(read.Description, Is.EqualTo("because"));
            Assert.That(SchemaCardCompiler.TryCompile(read, out var again, out _), Is.True);
            Assert.That(again, Is.EqualTo(rule), name);
            Assert.That(SchemaCardText.Plain(card), Is.EqualTo(sentence), name);
            Assert.That(SchemaCardText.Plain(read), Is.EqualTo(sentence), name);
        });
    }

    [Test]
    public void The_forms_an_older_cluster_evaluates_are_used_where_they_exist()
    {
        static LatticePredicateNodeKind[] Kinds(LatticePredicateNode node) =>
            [node.Kind, .. (node.Children ?? []).SelectMany(Kinds)];

        foreach (var card in new[]
        {
            Card(SchemaCardKind.Required, "a"),
            Card(SchemaCardKind.Type, "a", card => card.ValueType = SchemaValueType.Number),
            Card(SchemaCardKind.Type, "a", card => card.ValueType = SchemaValueType.Boolean),
            Card(SchemaCardKind.Type, "a"),
            Card(SchemaCardKind.OneOf, "a", card => card.Values = ["x"]),
            Card(SchemaCardKind.NumberRange, "a", card => card.Minimum = "1"),
            Card(SchemaCardKind.TextMatch, "a", card => card.MatchText = "x"),
        })
        {
            Assert.That(SchemaCardCompiler.TryCompilePredicate(card, out var node, out _), Is.True);
            Assert.That(Kinds(node), Has.None.EqualTo(LatticePredicateNodeKind.TypeOf).And.None.EqualTo(LatticePredicateNodeKind.Length).And.None.EqualTo(LatticePredicateNodeKind.Every), card.Kind.ToString());
        }
    }

    [TestCase("NumberRange", "", "", "Enter a smallest number, a largest number or both, or require a whole number.")]
    [TestCase("NumberRange", "x", "", "Enter the smallest allowed number, such as 0 or -2.5.")]
    [TestCase("NumberRange", "5", "1", "The smallest number is larger than the largest.")]
    [TestCase("TextLength", "", "", "Enter the fewest characters, the most characters or both.")]
    [TestCase("TextLength", "-1", "", "Enter the fewest characters as a whole number.")]
    [TestCase("ListLength", "3", "2", "The fewest items is more than the most.")]
    public void A_range_that_does_not_make_sense_is_explained(string kind, string minimum, string maximum, string message)
    {
        var card = Card(Enum.Parse<SchemaCardKind>(kind), "a", card => { card.Minimum = minimum; card.Maximum = maximum; });

        Assert.That(SchemaCardCompiler.TryCompile(card, out _, out var error), Is.False);
        Assert.That(error, Is.EqualTo(message));
    }

    [Test]
    public void Other_incomplete_cards_are_explained()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Error(Card(SchemaCardKind.OneOf, "a")), Is.EqualTo("Add at least one allowed value."));
            Assert.That(Error(Card(SchemaCardKind.OneOf, "a", card => { card.Values = ["x"]; card.ValuesAreNumbers = true; })), Does.StartWith("\"x\" is not a number."));
            Assert.That(Error(Card(SchemaCardKind.TextMatch, "a")), Is.EqualTo("Enter the text to look for."));
            Assert.That(Error(Card(SchemaCardKind.EveryItem, "a")), Is.EqualTo("Say what every item must be."));
            Assert.That(Error(Card(SchemaCardKind.EveryItem, "a", card => card.Item = Card(SchemaCardKind.Pattern, string.Empty, item => item.Pattern = "x"))), Does.StartWith("Every item: A format or pattern"));
            Assert.That(Error(Card(SchemaCardKind.AnyOf)), Is.EqualTo("An \"any of\" group needs at least two alternatives."));
            Assert.That(Error(Card(SchemaCardKind.MaxSize)), Is.EqualTo("Enter the largest size, in bytes, as a whole number."));
            Assert.That(Error(Card(SchemaCardKind.MaxSize, string.Empty, card => card.MaxBytes = "99999999999")), Does.StartWith("The largest size can be at most"));
            Assert.That(Error(Card(SchemaCardKind.Required, "a..b")), Does.StartWith("A member path is member names joined by dots"));
            Assert.That(Error(Card(SchemaCardKind.Required, "a b")), Does.StartWith("A member path"));
            Assert.That(Error(Card(SchemaCardKind.Custom)), Is.EqualTo("This rule has nothing to keep."));
        });

        static string? Error(SchemaRuleCard card) => SchemaCardCompiler.TryCompile(card, out _, out var error) ? null : error;
    }

    [Test]
    public void Only_predicate_cards_can_be_grouped_or_made_optional()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardCompiler.IsPredicate(SchemaCardKind.TextMatch), Is.True);
            Assert.That(SchemaCardCompiler.IsPredicate(SchemaCardKind.Format), Is.False);
            Assert.That(SchemaCardCompiler.IsPredicate(SchemaCardKind.Custom), Is.False);
            Assert.That(SchemaCardCompiler.CanBeOptional(SchemaCardKind.Required), Is.False);
            Assert.That(SchemaCardCompiler.CanBeOptional(SchemaCardKind.AnyOf), Is.False);
            Assert.That(SchemaCardCompiler.CanBeOptional(SchemaCardKind.NumberRange), Is.True);
            Assert.That(SchemaCardCompiler.IsWholeValueOnly(SchemaCardKind.MaxSize), Is.True);
            Assert.That(SchemaCardCompiler.TryCompilePredicate(Card(SchemaCardKind.Encoding), out _, out var error), Is.False);
            Assert.That(error, Does.StartWith("A whole-value check"));
        });
    }

    [Test]
    public void Numbers_are_read_as_integers_when_whole()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardCompiler.TryNumber("10,000", out var grouped), Is.True);
            Assert.That(grouped, Is.EqualTo(LatticeConstant.Integer(10000)));
            Assert.That(SchemaCardCompiler.TryNumber("2.50", out var real), Is.True);
            Assert.That(real, Is.EqualTo(LatticeConstant.Real(2.5)));
            Assert.That(SchemaCardCompiler.TryNumber("NaN", out _), Is.False);
            Assert.That(SchemaCardCompiler.TryNumber("x", out _), Is.False);
        });
    }

    [Test]
    public void A_predicate_no_card_writes_becomes_a_custom_card_holding_the_rule()
    {
        LatticeSchemaRule[] rules =
        [
            LatticeSchemaRule.Structured(LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThan, LatticePredicateNode.Member("a"), LatticePredicateNode.Member("b"))),
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.Not, LatticePredicateNode.Member("a"))),
            LatticeSchemaRule.Structured(LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Member("a"), LatticePredicateNode.Const(LatticeConstant.Integer(1)))),
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.And,
                LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("a"), LatticePredicateNode.Const(LatticeConstant.Integer(1))),
                LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("b"), LatticePredicateNode.Const(LatticeConstant.Integer(1))))),
            new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured },
            new LatticeSchemaRule { Kind = (LatticeSchemaRuleKind)42 },
        ];

        foreach (var rule in rules)
        {
            var card = SchemaCardDecompiler.Decompile(rule);
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.Custom), rule.ToString());
            Assert.That(SchemaCardCompiler.TryCompile(card, out var kept, out _), Is.True);
            Assert.That(kept, Is.EqualTo(rule));
        }
    }

    [Test]
    public void A_pattern_rule_reads_as_a_format_when_it_is_one()
    {
        Assert.That(SchemaCardDecompiler.Decompile(LatticeSchemaRule.Regex(SchemaFormatPatterns.PatternOf(SchemaTextFormat.Slug), "s")).Kind, Is.EqualTo(SchemaCardKind.Format));
        Assert.That(SchemaCardDecompiler.Decompile(LatticeSchemaRule.Regex("^s")).Kind, Is.EqualTo(SchemaCardKind.Pattern));
        Assert.That(SchemaCardDecompiler.Decompile([LatticeSchemaRule.Json(), LatticeSchemaRule.Utf8()]).Select(card => card.Encoding), Is.EqualTo(new[] { LatticeSchemaEncodingKind.Json, LatticeSchemaEncodingKind.Utf8 }));
    }

    [Test]
    public void A_legacy_dollar_anchored_rule_is_kept_verbatim_as_a_pattern_card()
    {
        // The format patterns moved from "$" to "\z" because "$" also matches
        // before a trailing line feed, so a stored "$" rule is a weaker rule
        // than the card that wrote it. Reading it back as a format card would
        // silently rewrite the operator's policy on the next save, so it reads
        // back as a pattern card holding the stored pattern unchanged.
        var legacy = LatticeSchemaRule.Regex("^[a-z0-9]+(-[a-z0-9]+)*$", "s");
        var card = SchemaCardDecompiler.Decompile(legacy);

        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.Pattern));
            Assert.That(card.Pattern, Is.EqualTo("^[a-z0-9]+(-[a-z0-9]+)*$"));
            Assert.That(SchemaCardCompiler.TryCompile(card, out var kept, out _), Is.True);
            Assert.That(kept, Is.EqualTo(legacy), "opening a policy must never rewrite it");
        });
    }

    [Test]
    public void A_clone_is_deep_and_has_its_own_id()
    {
        var card = Card(SchemaCardKind.AnyOf, string.Empty, group => group.Alternatives = [Card(SchemaCardKind.EveryItem, "l", every => every.Item = Card(SchemaCardKind.Type))]);

        var clone = card.Clone();
        clone.Alternatives[0].Item!.ValueType = SchemaValueType.Number;

        Assert.That(clone.Id, Is.Not.EqualTo(card.Id));
        Assert.That(card.Alternatives[0].Item!.ValueType, Is.EqualTo(SchemaValueType.Text));
        Assert.That(card.IsWholeValue, Is.True);
    }

    [Test]
    public void A_custom_rule_reads_as_an_expression()
    {
        var predicate = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThan, LatticePredicateNode.Member("total"), LatticePredicateNode.Member("paid")),
            LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("s"), LatticePredicateNode.Const(LatticeConstant.Text("a\"b"))),
                LatticePredicateNode.Bool(LatticeBooleanOperator.Not, LatticePredicateNode.Compare(LatticeComparisonOperator.NotEqual, LatticePredicateNode.Member("f"), LatticePredicateNode.Const(LatticeConstant.Bool(true))))),
            LatticePredicateNode.Every("l", LatticePredicateNode.Compare(LatticeComparisonOperator.LessThanOrEqual, LatticePredicateNode.LengthOf(null), LatticePredicateNode.Const(LatticeConstant.Real(2.5)))),
            LatticePredicateNode.TypeOf("o", LatticeValueKind.Object),
            LatticePredicateNode.StringCall(LatticeStringMethod.EndsWith, LatticePredicateNode.Self(), LatticePredicateNode.Const(LatticeConstant.Null())));

        Assert.That(SchemaPredicateText.Expression(predicate), Is.EqualTo(
            "total > paid and (s == \"a\\\"b\" or not (f != true)) and every item of l: (length of it <= 2.5) and o is an object and it ends with null"));
    }

    [Test]
    public void The_exact_policy_is_written_as_json()
    {
        var policy = new LatticeSchemaPolicy(
        [
            LatticeSchemaRule.Regex("^x$", "a", "why"),
            LatticeSchemaRule.MaxLength(5),
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.And,
                LatticePredicateNode.TypeOf("t", LatticeValueKind.Integer),
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.LengthOf("t"), LatticePredicateNode.Const(LatticeConstant.Real(1.5))),
                LatticePredicateNode.StringCall(LatticeStringMethod.Contains, LatticePredicateNode.Member("m"), LatticePredicateNode.Const(LatticeConstant.Bool(true))),
                LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Self(), LatticePredicateNode.Const(LatticeConstant.Null())))),
        ], strictIngest: true);

        using var document = System.Text.Json.JsonDocument.Parse(SchemaPolicyJson.Write(policy));
        var root = document.RootElement;
        var rules = root.GetProperty("rules");

        Assert.Multiple(() =>
        {
            Assert.That(root.GetProperty("strictIngest").GetBoolean(), Is.True);
            Assert.That(rules[0].GetProperty("pattern").GetString(), Is.EqualTo("^x$"));
            Assert.That(rules[0].GetProperty("member").GetString(), Is.EqualTo("a"));
            Assert.That(rules[0].GetProperty("description").GetString(), Is.EqualTo("why"));
            Assert.That(rules[1].GetProperty("maxByteLength").GetInt32(), Is.EqualTo(5));
            var parts = rules[2].GetProperty("predicate").GetProperty("children");
            Assert.That(parts[0].GetProperty("valueKind").GetString(), Is.EqualTo("Integer"));
            Assert.That(parts[1].GetProperty("children")[1].GetProperty("value").GetDouble(), Is.EqualTo(1.5));
            Assert.That(parts[2].GetProperty("method").GetString(), Is.EqualTo("Contains"));
            Assert.That(parts[3].GetProperty("children")[1].GetProperty("value").ValueKind, Is.EqualTo(System.Text.Json.JsonValueKind.Null));
        });
        Assert.That(() => SchemaPolicyJson.Write(null!), Throws.ArgumentNullException);
    }
}
