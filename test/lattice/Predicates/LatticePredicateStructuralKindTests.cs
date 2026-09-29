using System.Text;

namespace Orleans.Lattice.Tests.Predicates;

/// <summary>
/// The structural predicate kinds a schema rule builder writes and the
/// translator never emits: <see cref="LatticePredicateNodeKind.TypeOf"/>,
/// <see cref="LatticePredicateNodeKind.Length"/>,
/// <see cref="LatticePredicateNodeKind.Every"/> and
/// <see cref="LatticePredicateNodeKind.Self"/> - what each accepts and refuses,
/// that they never take the forward-only fast path, that nesting is bounded,
/// and that the node's equality and hash see <see cref="LatticePredicateNode.ValueKind"/>.
/// </summary>
[TestFixture]
public class LatticePredicateStructuralKindTests
{
    private static bool Match(string json, LatticePredicateNode node) =>
        LatticePredicateEvaluator.Matches(Encoding.UTF8.GetBytes(json), node);

    private static LatticePredicateNode Int(long value) => LatticePredicateNode.Const(LatticeConstant.Integer(value));

    [TestCase("{\"a\":\"x\"}", "String", true)]
    [TestCase("{\"a\":1}", "String", false)]
    [TestCase("{\"a\":1.5}", "Number", true)]
    [TestCase("{\"a\":1.5}", "Integer", false)]
    [TestCase("{\"a\":3.0}", "Integer", true)]
    [TestCase("{\"a\":1e20}", "Integer", true)]
    [TestCase("{\"a\":7}", "Integer", true)]
    [TestCase("{\"a\":true}", "Boolean", true)]
    [TestCase("{\"a\":false}", "Boolean", true)]
    [TestCase("{\"a\":{}}", "Object", true)]
    [TestCase("{\"a\":[]}", "Array", true)]
    [TestCase("{\"a\":[]}", "Object", false)]
    [TestCase("{\"a\":null}", "Null", true)]
    [TestCase("{\"a\":null}", "Present", false)]
    [TestCase("{\"a\":{}}", "Present", true)]
    [TestCase("{\"b\":1}", "Present", false)]
    [TestCase("{\"b\":1}", "Null", false)]
    public void TypeOf_tests_the_kind_of_the_member(string json, string kind, bool expected)
    {
        var node = LatticePredicateNode.TypeOf("a", Enum.Parse<LatticeValueKind>(kind));

        Assert.That(Match(json, node), Is.EqualTo(expected));
    }

    [Test]
    public void TypeOf_follows_a_dotted_path_case_insensitively()
    {
        Assert.That(Match("{\"Order\":{\"Lines\":[1]}}", LatticePredicateNode.TypeOf("order.lines", LatticeValueKind.Array)), Is.True);
        Assert.That(Match("{\"order\":1}", LatticePredicateNode.TypeOf("order.lines", LatticeValueKind.Array)), Is.False);
    }

    [Test]
    public void TypeOf_with_no_path_tests_the_whole_document()
    {
        Assert.That(Match("[1,2]", LatticePredicateNode.TypeOf(null, LatticeValueKind.Array)), Is.True);
        Assert.That(Match("\"text\"", LatticePredicateNode.TypeOf(string.Empty, LatticeValueKind.String)), Is.True);
        Assert.That(Match("{}", LatticePredicateNode.TypeOf(null, LatticeValueKind.Array)), Is.False);
    }

    [Test]
    public void An_unknown_value_kind_never_matches()
    {
        Assert.That(Match("{\"a\":1}", LatticePredicateNode.TypeOf("a", (LatticeValueKind)99)), Is.False);
    }

    [TestCase("{\"a\":\"h\\u00e9llo\"}", 5)]
    [TestCase("{\"a\":\"\\ud83d\\ude00x\"}", 2)]
    [TestCase("{\"a\":[1,2,3]}", 3)]
    [TestCase("{\"a\":{\"x\":1,\"y\":2}}", 2)]
    [TestCase("{\"a\":\"\"}", 0)]
    public void Length_counts_scalars_items_or_members(string json, long length)
    {
        var node = LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.LengthOf("a"), Int(length));

        Assert.That(Match(json, node), Is.True);
    }

    [TestCase("{\"a\":5}")]
    [TestCase("{\"a\":true}")]
    [TestCase("{\"b\":\"xyz\"}")]
    public void Length_of_anything_else_is_missing_and_orders_against_nothing(string json)
    {
        var atLeastZero = LatticePredicateNode.Compare(LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.LengthOf("a"), Int(0));
        var isNull = LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.LengthOf("a"), LatticePredicateNode.Const(LatticeConstant.Null()));

        Assert.That(Match(json, atLeastZero), Is.False);
        Assert.That(Match(json, isNull), Is.True, "a missing length equals null, as a missing member does");
    }

    [Test]
    public void Length_in_boolean_position_is_false()
    {
        Assert.That(Match("{\"a\":\"x\"}", LatticePredicateNode.LengthOf("a")), Is.False);
    }

    [Test]
    public void Every_holds_when_every_item_satisfies_the_item_predicate()
    {
        var qtyAtLeastOne = LatticePredicateNode.Compare(
            LatticeComparisonOperator.GreaterThanOrEqual, LatticePredicateNode.Member("qty"), Int(1));
        var node = LatticePredicateNode.Every("lines", qtyAtLeastOne);

        Assert.Multiple(() =>
        {
            Assert.That(Match("{\"lines\":[{\"qty\":1},{\"qty\":4}]}", node), Is.True);
            Assert.That(Match("{\"lines\":[{\"qty\":1},{\"qty\":0}]}", node), Is.False);
            Assert.That(Match("{\"lines\":[]}", node), Is.True, "an empty list has no failing item");
            Assert.That(Match("{\"lines\":{\"qty\":1}}", node), Is.False, "an object is not a list");
            Assert.That(Match("{\"other\":[]}", node), Is.False, "a missing list is not a list");
        });
    }

    [Test]
    public void Self_names_the_item_an_enclosing_every_visits()
    {
        var tag = LatticePredicateNode.StringCall(
            LatticeStringMethod.StartsWith, LatticePredicateNode.Self(), LatticePredicateNode.Const(LatticeConstant.Text("#")));
        var node = LatticePredicateNode.Every("tags", tag);

        Assert.That(Match("{\"tags\":[\"#a\",\"#b\"]}", node), Is.True);
        Assert.That(Match("{\"tags\":[\"#a\",\"b\"]}", node), Is.False);
    }

    [Test]
    public void Every_nests_for_lists_of_lists()
    {
        var cell = LatticePredicateNode.TypeOf(null, LatticeValueKind.Integer);
        var node = LatticePredicateNode.Every("grid", LatticePredicateNode.Every(null, cell));

        Assert.That(Match("{\"grid\":[[1,2],[3]]}", node), Is.True);
        Assert.That(Match("{\"grid\":[[1,2],[3.5]]}", node), Is.False);
    }

    [Test]
    public void Every_with_the_wrong_arity_never_matches()
    {
        var node = new LatticePredicateNode { Kind = LatticePredicateNodeKind.Every, MemberPath = "a", Children = [] };

        Assert.That(Match("{\"a\":[]}", node), Is.False);
    }

    [Test]
    public void Self_in_boolean_position_is_true_only_for_true()
    {
        Assert.That(Match("true", LatticePredicateNode.Self()), Is.True);
        Assert.That(Match("1", LatticePredicateNode.Self()), Is.False);
    }

    [Test]
    public void Self_compares_as_the_whole_scalar_document()
    {
        var node = LatticePredicateNode.Compare(LatticeComparisonOperator.LessThan, LatticePredicateNode.Self(), Int(10));

        Assert.That(Match("7", node), Is.True);
        Assert.That(Match("{\"a\":7}", node), Is.False);
    }

    [Test]
    public void No_structural_kind_takes_the_forward_only_path()
    {
        var member = LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("a"), Int(1));

        Assert.Multiple(() =>
        {
            Assert.That(LatticePredicateEvaluator.IsFastPathEligible(member), Is.True, "control: a top-level member is eligible");
            Assert.That(LatticePredicateEvaluator.IsFastPathEligible(LatticePredicateNode.Bool(LatticeBooleanOperator.And, member, LatticePredicateNode.TypeOf("a", LatticeValueKind.Number))), Is.False);
            Assert.That(LatticePredicateEvaluator.IsFastPathEligible(LatticePredicateNode.Bool(LatticeBooleanOperator.And, member, LatticePredicateNode.Every("a", member))), Is.False);
            Assert.That(LatticePredicateEvaluator.IsFastPathEligible(LatticePredicateNode.Bool(LatticeBooleanOperator.And, member, LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.LengthOf("a"), Int(1)))), Is.False);
            Assert.That(LatticePredicateEvaluator.IsFastPathEligible(LatticePredicateNode.Bool(LatticeBooleanOperator.And, member, LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Self(), Int(1)))), Is.False);
        });
    }

    [Test]
    public void A_structural_kind_beside_a_top_level_member_is_still_evaluated()
    {
        var node = LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.Compare(LatticeComparisonOperator.Equal, LatticePredicateNode.Member("a"), Int(1)),
            LatticePredicateNode.TypeOf("b", LatticeValueKind.Array));

        Assert.That(Match("{\"a\":1,\"b\":[]}", node), Is.True);
        Assert.That(Match("{\"a\":1,\"b\":{}}", node), Is.False);
    }

    [Test]
    public void Nested_quantifiers_follow_nested_lists_within_the_document_depth_limit()
    {
        static (LatticePredicateNode Node, string Json) Nest(int levels)
        {
            var node = LatticePredicateNode.TypeOf(null, LatticeValueKind.Number);
            var json = new StringBuilder("1");
            for (var level = 0; level < levels; level++)
            {
                node = LatticePredicateNode.Every(null, node);
                json.Insert(0, '[').Append(']');
            }

            return (node, json.ToString());
        }

        var (shallow, shallowJson) = Nest(10);
        var (deep, deepJson) = Nest(70);

        Assert.That(Match(shallowJson, shallow), Is.True);
        Assert.That(Match(deepJson, deep), Is.False, "a document nested past the parser's depth limit never matches, so a quantifier chain cannot be driven deeper by data");
    }

    [Test]
    public void Value_kind_takes_part_in_equality_and_hashing()
    {
        var number = LatticePredicateNode.TypeOf("a", LatticeValueKind.Number);
        var text = LatticePredicateNode.TypeOf("a", LatticeValueKind.String);

        Assert.That(number, Is.Not.EqualTo(text));
        Assert.That(number, Is.EqualTo(LatticePredicateNode.TypeOf("a", LatticeValueKind.Number)));
        Assert.That(number.GetHashCode(), Is.EqualTo(LatticePredicateNode.TypeOf("a", LatticeValueKind.Number).GetHashCode()));
    }

    [Test]
    public void The_public_evaluation_helper_answers_the_structural_kinds()
    {
        Assert.That(LatticePredicateEvaluation.Matches(Encoding.UTF8.GetBytes("{\"a\":[1]}"), LatticePredicateNode.TypeOf("a", LatticeValueKind.Array)), Is.True);
    }
}
