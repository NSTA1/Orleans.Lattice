using System.Text;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// Branch-coverage unit tests for <see cref="LatticePredicateEvaluator"/> that
/// exercise edge shapes the translator never emits, by constructing raw
/// <see cref="LatticePredicateNode"/> trees directly. These deliberately hit
/// defensive paths (malformed arity, unknown operators, mismatched operand
/// kinds, empty / dotted member paths, missing members, and both the
/// JsonDocument slow path and the Utf8JsonReader fast path) that the
/// expression-oracle suite cannot reach.
/// </summary>
[TestFixture]
public class LatticePredicateEvaluatorRawNodeTests
{
    private static byte[] Json(string json) => Encoding.UTF8.GetBytes(json);

    private static bool Match(byte[] value, LatticePredicateNode node) =>
        LatticePredicateEvaluator.Matches(value, node);

    private static LatticePredicateNode Raw(
        LatticePredicateNodeKind kind,
        params LatticePredicateNode[] children) =>
        new() { Kind = kind, Children = children };

    // ===== Slow path (member-free constant-only, or dotted member paths) =====

    [Test]
    public void Matches_constant_only_predicate_on_malformed_json_returns_false()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Integer(1)));

        Assert.That(Match(Json("{not valid"), node), Is.False);
    }

    [Test]
    public void Matches_constant_only_equal_predicate_on_valid_json_evaluates()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Integer(1)));

        Assert.That(Match(Json("{}"), node), Is.True);
    }

    [Test]
    public void Matches_boolean_operator_with_no_children_returns_false()
    {
        var node = LatticePredicateNode.Bool(LatticeBooleanOperator.And);
        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_unknown_boolean_operator_returns_false()
    {
        var node = LatticePredicateNode.Bool(
            (LatticeBooleanOperator)99,
            LatticePredicateNode.Const(LatticeConstant.Bool(true)));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_slow_path_and_with_dotted_members_returns_true_when_all_match()
    {
        var node = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("Address.City"),
                LatticePredicateNode.Const(LatticeConstant.Text("London"))),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("Address.Country"),
                LatticePredicateNode.Const(LatticeConstant.Text("UK"))));

        Assert.That(Match(Json("{\"Address\":{\"City\":\"London\",\"Country\":\"UK\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_slow_path_not_over_dotted_member_negates()
    {
        var node = LatticePredicateNode.Bool(
            LatticeBooleanOperator.Not,
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.Equal,
                LatticePredicateNode.Member("Address.City"),
                LatticePredicateNode.Const(LatticeConstant.Text("Paris"))));

        Assert.That(Match(Json("{\"Address\":{\"City\":\"London\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_comparison_with_wrong_arity_returns_false()
    {
        var node = Raw(LatticePredicateNodeKind.Compare,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_string_method_with_wrong_arity_returns_false()
    {
        var node = Raw(LatticePredicateNodeKind.StringMethod,
            LatticePredicateNode.Const(LatticeConstant.Text("abc")));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_dotted_string_method_startswith_matches()
    {
        var node = LatticePredicateNode.StringCall(
            LatticeStringMethod.StartsWith,
            LatticePredicateNode.Member("Address.City"),
            LatticePredicateNode.Const(LatticeConstant.Text("Lon")));

        Assert.That(Match(Json("{\"Address\":{\"City\":\"London\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_string_method_on_non_string_operand_returns_false()
    {
        var node = LatticePredicateNode.StringCall(
            LatticeStringMethod.StartsWith,
            LatticePredicateNode.Const(LatticeConstant.Integer(5)),
            LatticePredicateNode.Const(LatticeConstant.Text("5")));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_unknown_string_method_returns_false()
    {
        var node = LatticePredicateNode.StringCall(
            (LatticeStringMethod)99,
            LatticePredicateNode.Const(LatticeConstant.Text("abc")),
            LatticePredicateNode.Const(LatticeConstant.Text("a")));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_unknown_comparison_operator_returns_false()
    {
        var node = LatticePredicateNode.Compare(
            (LatticeComparisonOperator)99,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Integer(2)));

        Assert.That(Match(Json("{}"), node), Is.False);
    }

    [Test]
    public void Matches_boolean_equality_of_two_constants_evaluates()
    {
        var equalTrue = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Bool(true)),
            LatticePredicateNode.Const(LatticeConstant.Bool(true)));
        var equalMismatch = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Bool(true)),
            LatticePredicateNode.Const(LatticeConstant.Bool(false)));

        Assert.That(Match(Json("{}"), equalTrue), Is.True);
        Assert.That(Match(Json("{}"), equalMismatch), Is.False);
    }

    [Test]
    public void Matches_equality_of_mismatched_kinds_is_false_and_inequality_true()
    {
        var eq = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Text("1")));
        var ne = LatticePredicateNode.Compare(
            LatticeComparisonOperator.NotEqual,
            LatticePredicateNode.Const(LatticeConstant.Integer(1)),
            LatticePredicateNode.Const(LatticeConstant.Text("1")));

        Assert.That(Match(Json("{}"), eq), Is.False);
        Assert.That(Match(Json("{}"), ne), Is.True);
    }

    [Test]
    public void Matches_string_ordering_comparison_evaluates()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.LessThan,
            LatticePredicateNode.Const(LatticeConstant.Text("a")),
            LatticePredicateNode.Const(LatticeConstant.Text("b")));

        Assert.That(Match(Json("{}"), node), Is.True);
    }

    [Test]
    public void Matches_nested_boolean_in_operand_position_resolves_to_boolean()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Bool(
                LatticeBooleanOperator.Or,
                LatticePredicateNode.Compare(
                    LatticeComparisonOperator.Equal,
                    LatticePredicateNode.Const(LatticeConstant.Integer(1)),
                    LatticePredicateNode.Const(LatticeConstant.Integer(1)))),
            LatticePredicateNode.Const(LatticeConstant.Bool(true)));

        Assert.That(Match(Json("{}"), node), Is.True);
    }

    [Test]
    public void Matches_empty_member_path_resolves_to_missing_and_equals_null()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member(string.Empty),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{}"), node), Is.True);
    }

    [Test]
    public void Matches_dotted_member_absent_segment_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Address.Nope"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"Address\":{\"City\":\"London\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_dotted_member_through_non_object_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Name.Sub"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"Name\":\"Bob\"}"), node), Is.True);
    }

    [Test]
    public void Matches_dotted_member_case_insensitive_fallback_resolves()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Address.City"),
            LatticePredicateNode.Const(LatticeConstant.Text("Berlin")));

        Assert.That(Match(Json("{\"address\":{\"city\":\"Berlin\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_dotted_member_true_miss_after_fallback_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Address.Zzz"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"address\":{\"city\":\"Berlin\"}}"), node), Is.True);
    }

    [Test]
    public void Matches_dotted_member_json_kinds_resolve_via_from_json()
    {
        var doc = Json("{\"Meta\":{\"Nil\":null,\"Flag\":true,\"Off\":false,\"Word\":\"hi\",\"Num\":7,\"Obj\":{}}}");

        var nil = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Nil"),
            LatticePredicateNode.Const(LatticeConstant.Null()));
        var flag = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Flag"),
            LatticePredicateNode.Const(LatticeConstant.Bool(true)));
        var off = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Off"),
            LatticePredicateNode.Const(LatticeConstant.Bool(false)));
        var word = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Word"),
            LatticePredicateNode.Const(LatticeConstant.Text("hi")));
        var num = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Num"),
            LatticePredicateNode.Const(LatticeConstant.Integer(7)));
        var obj = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Meta.Obj"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(doc, nil), Is.True);
        Assert.That(Match(doc, flag), Is.True);
        Assert.That(Match(doc, off), Is.True);
        Assert.That(Match(doc, word), Is.True);
        Assert.That(Match(doc, num), Is.True);
        Assert.That(Match(doc, obj), Is.True);
    }

    [Test]
    public void Matches_bare_dotted_member_in_boolean_position_is_truthy()
    {
        var node = LatticePredicateNode.Member("Meta.Flag");
        Assert.That(Match(Json("{\"Meta\":{\"Flag\":true}}"), node), Is.True);
        Assert.That(Match(Json("{\"Meta\":{\"Flag\":false}}"), node), Is.False);
    }

    // ===== Fast path (single top-level member) =====

    [Test]
    public void Matches_fast_path_unknown_boolean_operator_returns_false()
    {
        var node = LatticePredicateNode.Bool(
            (LatticeBooleanOperator)99,
            LatticePredicateNode.Member("Age"));

        Assert.That(Match(Json("{\"Age\":5}"), node), Is.False);
    }

    [Test]
    public void Matches_fast_path_comparison_wrong_arity_returns_false()
    {
        var node = Raw(LatticePredicateNodeKind.Compare, LatticePredicateNode.Member("Age"));
        Assert.That(Match(Json("{\"Age\":5}"), node), Is.False);
    }

    [Test]
    public void Matches_fast_path_string_method_wrong_arity_returns_false()
    {
        var node = Raw(LatticePredicateNodeKind.StringMethod, LatticePredicateNode.Member("Name"));
        Assert.That(Match(Json("{\"Name\":\"Bob\"}"), node), Is.False);
    }

    [Test]
    public void Matches_fast_path_member_against_non_object_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("[1,2,3]"), node), Is.True);
    }

    [Test]
    public void Matches_fast_path_null_member_equals_null_constant()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Nickname"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"Nickname\":null}"), node), Is.True);
    }

    [Test]
    public void Matches_fast_path_bare_member_boolean_truthiness()
    {
        var node = LatticePredicateNode.Member("Active");
        Assert.That(Match(Json("{\"Active\":true}"), node), Is.True);
        Assert.That(Match(Json("{\"Active\":false}"), node), Is.False);
    }

    [Test]
    public void Matches_fast_path_object_member_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Addr"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"Addr\":{\"x\":1}}"), node), Is.True);
    }

    [Test]
    public void Matches_fast_path_absent_member_is_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Ghost"),
            LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(Json("{\"Age\":5}"), node), Is.True);
    }

    [Test]
    public void Matches_fast_path_nested_boolean_in_operand_position_resolves()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Bool(
                LatticeBooleanOperator.Or,
                LatticePredicateNode.Compare(
                    LatticeComparisonOperator.GreaterThanOrEqual,
                    LatticePredicateNode.Member("Age"),
                    LatticePredicateNode.Const(LatticeConstant.Integer(0)))),
            LatticePredicateNode.Const(LatticeConstant.Bool(true)));

        Assert.That(Match(Json("{\"Age\":5}"), node), Is.True);
    }

    [Test]
    public void Matches_null_string_constant_resolves_to_null()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Nickname"),
            LatticePredicateNode.Const(new LatticeConstant { Kind = LatticeConstantKind.String, StringValue = null }));

        Assert.That(Match(Json("{\"Nickname\":null}"), node), Is.True);
    }

    [Test]
    public void Matches_unknown_constant_kind_resolves_to_missing()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal,
            LatticePredicateNode.Member("Age"),
            LatticePredicateNode.Const(new LatticeConstant { Kind = (LatticeConstantKind)99 }));

        Assert.That(Match(Json("{\"Age\":5}"), node), Is.False);
    }

    [Test]
    public void Matches_double_constant_ordering_evaluates()
    {
        var node = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual,
            LatticePredicateNode.Member("Score"),
            LatticePredicateNode.Const(LatticeConstant.Real(0.5)));

        Assert.That(Match(Json("{\"Score\":0.9}"), node), Is.True);
        Assert.That(Match(Json("{\"Score\":0.1}"), node), Is.False);
    }
}
