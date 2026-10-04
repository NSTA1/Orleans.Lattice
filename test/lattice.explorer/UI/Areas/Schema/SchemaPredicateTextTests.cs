using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The predicate-to-expression writer, arm by arm: every node kind, every
/// comparison operator, every string method, every value kind, every constant
/// kind, the bracketing rule, and the two ways a predicate is written as
/// something other than itself - the depth elision and the "(unreadable)"
/// fallback for a node whose child arity does not fit its kind.
/// </summary>
/// <remarks>
/// The writer is reached in production only through <see cref="SchemaCardDecompiler"/>,
/// which hands it the predicates the card model failed to decompile. That caller
/// cannot produce a malformed node (its input came from a compiled card), nor one
/// nested past <see cref="SchemaPredicateText.MaximumDepth"/>, nor a value outside
/// an enum's declared members - so the elision, the fallback and every "unknown"
/// arm are unreachable from it however many card tests are added. They are only
/// reachable by writing against this type directly.
/// </remarks>
[TestFixture]
public sealed class SchemaPredicateTextTests
{
    private static readonly LatticePredicateNode Total = LatticePredicateNode.Member("total");

    [Test]
    public void A_member_is_written_as_its_path()
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Member("lines.sku")), Is.EqualTo("lines.sku"));
    }

    [Test]
    public void A_member_with_no_path_is_written_as_a_question_mark()
    {
        var node = new LatticePredicateNode { Kind = LatticePredicateNodeKind.Member, MemberPath = null };

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("?"));
    }

    [Test]
    public void The_value_itself_is_written_as_it()
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Self()), Is.EqualTo("it"));
    }

    [Test]
    [TestCase(LatticeComparisonOperator.Equal, "==")]
    [TestCase(LatticeComparisonOperator.NotEqual, "!=")]
    [TestCase(LatticeComparisonOperator.LessThan, "<")]
    [TestCase(LatticeComparisonOperator.LessThanOrEqual, "<=")]
    [TestCase(LatticeComparisonOperator.GreaterThan, ">")]
    [TestCase(LatticeComparisonOperator.GreaterThanOrEqual, ">=")]
    public void Every_comparison_operator_has_its_own_symbol(LatticeComparisonOperator op, string symbol)
    {
        var node = LatticePredicateNode.Compare(op, Total, LatticePredicateNode.Const(LatticeConstant.Integer(0)));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo($"total {symbol} 0"));
    }

    [Test]
    public void An_operator_outside_the_enum_is_written_as_a_question_mark()
    {
        var node = LatticePredicateNode.Compare(
            (LatticeComparisonOperator)99,
            Total,
            LatticePredicateNode.Const(LatticeConstant.Integer(0)));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("total ? 0"));
    }

    [Test]
    [TestCase(LatticeStringMethod.StartsWith, " starts with ")]
    [TestCase(LatticeStringMethod.EndsWith, " ends with ")]
    [TestCase(LatticeStringMethod.Contains, " contains ")]
    [TestCase(LatticeStringMethod.Equals, " equals ")]
    [TestCase((LatticeStringMethod)99, " equals ")]
    public void Every_string_method_has_its_own_phrase(LatticeStringMethod method, string phrase)
    {
        var node = LatticePredicateNode.StringCall(
            method,
            LatticePredicateNode.Member("status"),
            LatticePredicateNode.Const(LatticeConstant.Text("open")));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo($"status{phrase}\"open\""));
    }

    [Test]
    [TestCase(LatticeValueKind.Present, "present")]
    [TestCase(LatticeValueKind.Null, "null")]
    [TestCase(LatticeValueKind.Boolean, "true or false")]
    [TestCase(LatticeValueKind.Number, "a number")]
    [TestCase(LatticeValueKind.Integer, "a whole number")]
    [TestCase(LatticeValueKind.String, "text")]
    [TestCase(LatticeValueKind.Object, "an object")]
    [TestCase(LatticeValueKind.Array, "a list")]
    [TestCase((LatticeValueKind)99, "of an unknown kind")]
    public void Every_value_kind_is_named_in_words(LatticeValueKind kind, string words)
    {
        var node = LatticePredicateNode.TypeOf("total", kind);

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo($"total is {words}"));
    }

    [Test]
    public void A_type_test_on_the_value_itself_names_it_as_it()
    {
        Assert.That(
            SchemaPredicateText.Expression(LatticePredicateNode.TypeOf(null, LatticeValueKind.Object)),
            Is.EqualTo("it is an object"));
    }

    [Test]
    [TestCase("")]
    [TestCase(null)]
    public void A_length_of_the_value_itself_names_it_as_it(string? path)
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.LengthOf(path)), Is.EqualTo("length of it"));
    }

    [Test]
    public void A_length_of_a_member_names_the_member()
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.LengthOf("sku")), Is.EqualTo("length of sku"));
    }

    [Test]
    public void A_length_comparison_reads_as_a_sentence()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.LessThanOrEqual,
            LatticePredicateNode.LengthOf("sku"),
            LatticePredicateNode.Const(LatticeConstant.Integer(16)));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("length of sku <= 16"));
    }

    [Test]
    public void Every_item_wraps_its_body_in_brackets_and_writes_it_at_top_level()
    {
        var body = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThan, LatticePredicateNode.Member("qty"), LatticePredicateNode.Const(LatticeConstant.Integer(0))),
            LatticePredicateNode.TypeOf("sku", LatticeValueKind.String));

        var node = LatticePredicateNode.Every("lines", body);

        Assert.That(
            SchemaPredicateText.Expression(node),
            Is.EqualTo("every item of lines: (qty > 0 and sku is text)"),
            "the body is written at top level, so it is bracketed once by the every-item wrapper and not again by itself");
    }

    [Test]
    public void Every_item_of_the_value_itself_names_it_as_it()
    {
        var node = LatticePredicateNode.Every(null, LatticePredicateNode.Self());

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("every item of it: (it)"));
    }

    [Test]
    [TestCase(LatticeBooleanOperator.And, " and ")]
    [TestCase(LatticeBooleanOperator.Or, " or ")]
    public void A_boolean_group_is_unbracketed_at_top_level(LatticeBooleanOperator op, string joiner)
    {
        var node = LatticePredicateNode.Bool(op, Total, LatticePredicateNode.Member("tax"));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo($"total{joiner}tax"));
    }

    [Test]
    public void A_nested_boolean_group_is_bracketed()
    {
        var inner = LatticePredicateNode.Bool(
            LatticeBooleanOperator.Or,
            LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("open"))),
            LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("status"), LatticePredicateNode.Const(LatticeConstant.Text("shipped"))));

        var node = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, Total, LatticePredicateNode.Const(LatticeConstant.Integer(0))),
            inner);

        Assert.That(
            SchemaPredicateText.Expression(node),
            Is.EqualTo("total >= 0 and (status == \"open\" or status == \"shipped\")"));
    }

    [Test]
    public void Not_brackets_its_operand_and_writes_it_at_top_level()
    {
        var node = LatticePredicateNode.Bool(
            LatticeBooleanOperator.Not,
            LatticePredicateNode.Bool(LatticeBooleanOperator.And, Total, LatticePredicateNode.Member("tax")));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("not (total and tax)"));
    }

    [Test]
    public void A_boolean_operator_other_than_or_joins_with_and()
    {
        var node = LatticePredicateNode.Bool((LatticeBooleanOperator)99, Total, LatticePredicateNode.Member("tax"));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("total and tax"));
    }

    [Test]
    public void Null_is_written_as_the_word()
    {
        var node = LatticePredicateNode.Const(LatticeConstant.Null());

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("null"));
    }

    [Test]
    [TestCase(true, "true")]
    [TestCase(false, "false")]
    public void A_boolean_constant_is_written_as_the_word(bool value, string written)
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Const(LatticeConstant.Bool(value))), Is.EqualTo(written));
    }

    [Test]
    [TestCase(0L, "0")]
    [TestCase(42L, "42")]
    [TestCase(-7L, "-7")]
    [TestCase(long.MaxValue, "9223372036854775807")]
    public void A_whole_number_constant_is_written_invariantly(long value, string written)
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Const(LatticeConstant.Integer(value))), Is.EqualTo(written));
    }

    [Test]
    [TestCase(1.5d, "1.5")]
    [TestCase(-0.25d, "-0.25")]
    public void A_real_constant_is_written_round_trippably(double value, string written)
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Const(LatticeConstant.Real(value))), Is.EqualTo(written));
    }

    [Test]
    public void A_text_constant_is_quoted_and_its_own_quotes_escaped()
    {
        var node = LatticePredicateNode.Const(LatticeConstant.Text("say \"hello\""));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("\"say \\\"hello\\\"\""));
    }

    // A quote is written \" , so a backslash must be written \\ or the text is ambiguous:
    // C:\new would read as a newline, and a\" as a backslash followed by a stray quote.
    [Test]
    [TestCase("C:\\new", "\"C:\\\\new\"")]
    [TestCase("a\\\"", "\"a\\\\\\\"\"")]
    [TestCase("\\d+", "\"\\\\d+\"")]
    public void A_text_constant_has_its_backslashes_escaped(string value, string written)
    {
        Assert.That(SchemaPredicateText.Expression(LatticePredicateNode.Const(LatticeConstant.Text(value))), Is.EqualTo(written));
    }

    [Test]
    [TestCase("line\nbreak", "\"line\\nbreak\"")]
    [TestCase("cr\rlf", "\"cr\\rlf\"")]
    [TestCase("tab\there", "\"tab\\there\"")]
    [TestCase("bell\u0007", "\"bell\\u0007\"")]
    [TestCase("next\u0085line", "\"next\\u0085line\"")]
    [TestCase("para\u2029graph", "\"para\\u2029graph\"")]
    public void A_text_constant_never_breaks_the_expression_across_lines(string value, string written)
    {
        var expression = SchemaPredicateText.Expression(LatticePredicateNode.Const(LatticeConstant.Text(value)));

        Assert.Multiple(() =>
        {
            Assert.That(expression, Is.EqualTo(written));
            Assert.That(expression.Any(ch => char.IsControl(ch) || ch is '\u2028' or '\u2029'), Is.False);
        });
    }

    [Test]
    public void A_text_constant_keeps_characters_outside_ascii_as_written()
    {
        var node = LatticePredicateNode.Const(LatticeConstant.Text("caf\u00e9 \u4e2d\u6587 \U0001F600"));

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("\"caf\u00e9 \u4e2d\u6587 \U0001F600\""));
    }

    [Test]
    public void A_text_constant_with_no_value_is_written_as_empty_quotes()
    {
        var node = LatticePredicateNode.Const(new LatticeConstant { Kind = LatticeConstantKind.String, StringValue = null });

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("\"\""));
    }

    [Test]
    public void A_constant_kind_outside_the_enum_is_written_as_a_question_mark()
    {
        var node = LatticePredicateNode.Const(new LatticeConstant { Kind = (LatticeConstantKind)99 });

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("?"));
    }

    [Test]
    [TestCase(LatticePredicateNodeKind.Compare, 1)]
    [TestCase(LatticePredicateNodeKind.Compare, 3)]
    [TestCase(LatticePredicateNodeKind.StringMethod, 1)]
    [TestCase(LatticePredicateNodeKind.StringMethod, 3)]
    [TestCase(LatticePredicateNodeKind.Every, 2)]
    public void A_node_whose_child_arity_does_not_fit_its_kind_is_unreadable(LatticePredicateNodeKind kind, int children)
    {
        var node = new LatticePredicateNode { Kind = kind, Children = [.. Enumerable.Repeat(Total, children)] };

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("(unreadable)"));
    }

    [Test]
    public void A_boolean_group_with_no_operands_is_unreadable()
    {
        var node = new LatticePredicateNode { Kind = LatticePredicateNodeKind.Boolean, Children = [] };

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("(unreadable)"));
    }

    [Test]
    public void A_not_with_more_than_one_operand_falls_back_to_the_group_form()
    {
        var node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, Total, LatticePredicateNode.Member("tax"));

        Assert.That(
            SchemaPredicateText.Expression(node),
            Is.EqualTo("total and tax"),
            "Not is written only for a single operand; anything else is a plain group");
    }

    [Test]
    public void A_node_kind_outside_the_enum_is_unreadable()
    {
        var node = new LatticePredicateNode { Kind = (LatticePredicateNodeKind)99 };

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("(unreadable)"));
    }

    [Test]
    public void A_node_with_no_children_at_all_is_treated_as_having_none()
    {
        var node = new LatticePredicateNode { Kind = LatticePredicateNodeKind.Compare, Children = null };

        Assert.That(SchemaPredicateText.Expression(node), Is.EqualTo("(unreadable)"));
    }

    [Test]
    public void Nesting_past_the_maximum_depth_is_elided()
    {
        var node = Total;
        for (var i = 0; i < SchemaPredicateText.MaximumDepth + 1; i++)
        {
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        }

        var expression = SchemaPredicateText.Expression(node);

        Assert.Multiple(() =>
        {
            Assert.That(expression, Does.Contain("..."), "the innermost node sits past the depth bound and is elided");
            Assert.That(expression, Does.Not.Contain("total"), "nothing past the bound is written out");
        });
    }

    [Test]
    public void Nesting_up_to_the_maximum_depth_is_written_out_in_full()
    {
        var node = Total;
        for (var i = 0; i < SchemaPredicateText.MaximumDepth; i++)
        {
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        }

        var expression = SchemaPredicateText.Expression(node);

        Assert.Multiple(() =>
        {
            Assert.That(expression, Does.Contain("total"), "the bound is inclusive, so the last node still fits");
            Assert.That(expression, Does.Not.Contain("..."));
        });
    }
}
