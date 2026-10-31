using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaShapeTests
{
    [Test]
    public void Find_case_colliding_members_prefers_the_exact_path_at_every_level()
    {
        const string json = "{\"Order\":{\"Total\":1,\"total\":2},\"order\":{\"Total\":3,\"total\":4}}";
        var rule = LatticeSchemaRule.Structured(LatticePredicateNode.Compare(
            LatticeComparisonOperator.Equal, LatticePredicateNode.Member("order.total"),
            LatticePredicateNode.Const(LatticeConstant.Integer(4))));
        var shape = SchemaShape.Infer([Encoding.UTF8.GetBytes(json)], [rule]);
        var node = shape.Find("order.total");
        var validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy([rule]));

        Assert.Multiple(() =>
        {
            Assert.That(node, Is.SameAs(shape.Root.Children["order"].Children["total"]));
            Assert.That(node!.Numbers, Is.EqualTo((4d, 4d)));
            Assert.That(node.NamedByPolicy, Is.True);
            Assert.That(shape.Root.Children["Order"].Children["Total"].NamedByPolicy, Is.False);
            Assert.That(validator.ValidateRule(0, Encoding.UTF8.GetBytes(json)), Is.Null);
        });
    }

    [Test]
    public void Infer_policy_path_preserves_an_exact_unseen_member_beside_a_case_variant()
    {
        var shape = SchemaShape.Infer(
            [Encoding.UTF8.GetBytes("{\"Amount\":1,\"amount\":2}")],
            [LatticeSchemaRule.Regex("x", "amount.missing")]);

        Assert.Multiple(() =>
        {
            Assert.That(shape.Root.Children["amount"].Children["missing"].NamedByPolicy, Is.True);
            Assert.That(shape.Root.Children["Amount"].Children, Is.Empty);
            Assert.That(shape.Find("amount.missing")!.Unseen, Is.True);
        });
    }

    [Test]
    public void Find_without_an_exact_case_match_keeps_the_case_insensitive_fallback()
    {
        var shape = Infer("{\"Order\":{\"Total\":4}}");

        Assert.That(shape.Find("ORDER.TOTAL"), Is.SameAs(shape.Root.Children["Order"].Children["Total"]));
    }

    [Test]
    public void Infer_object_at_depth_limit_records_its_type_without_visiting_children()
    {
        var json = "{\"hidden\":1}";
        for (var depth = 0; depth < SchemaShape.MaximumDepth; depth++)
        {
            json = "{\"child\":" + json + "}";
        }

        var shape = Infer(json);
        var path = string.Join('.', Enumerable.Repeat("child", SchemaShape.MaximumDepth));
        var node = shape.Find(path)!;

        Assert.Multiple(() =>
        {
            Assert.That(shape.Truncated, Is.True);
            Assert.That(node.Types[SchemaValueType.Object], Is.EqualTo(1));
            Assert.That(node.Children, Is.Empty);
            Assert.That(shape.Find(path + ".hidden"), Is.Null);
            Assert.That(shape.Find(string.Empty), Is.SameAs(shape.Root));
        });
    }

    [Test]
    public void Infer_array_at_depth_limit_records_its_length_without_visiting_items()
    {
        var json = "[1,2]";
        for (var depth = 0; depth < SchemaShape.MaximumDepth; depth++)
        {
            json = "{\"child\":" + json + "}";
        }

        var shape = Infer(json);
        var node = shape.Find(string.Join('.', Enumerable.Repeat("child", SchemaShape.MaximumDepth)))!;

        Assert.Multiple(() =>
        {
            Assert.That(shape.Truncated, Is.True);
            Assert.That(node.ItemCounts, Is.EqualTo((2, 2)));
            Assert.That(node.Types[SchemaValueType.List], Is.EqualTo(1));
            Assert.That(node.Items, Is.Null);
        });
    }

    [Test]
    public void Infer_global_node_budget_refuses_new_members_and_array_items_but_keeps_observing_existing_nodes()
    {
        var children = Enumerable.Range(0, SchemaShape.MaximumChildren).ToDictionary(index => $"m{index:D2}", _ => 1);
        var document = new Dictionary<string, object>();
        for (var index = 0; index < 6; index++)
        {
            document[$"group{index}"] = children;
        }

        document["tail"] = Enumerable.Range(0, 7).ToDictionary(index => $"m{index:D2}", _ => 1);
        document["overflow"] = new[] { 1, 2 };
        var shape = Infer(JsonSerializer.Serialize(document), "{\"group0\":{\"m00\":9},\"overflow\":[3],\"new\":1}");

        Assert.Multiple(() =>
        {
            Assert.That(Count(shape.Root), Is.EqualTo(SchemaShape.MaximumNodes));
            Assert.That(shape.Truncated, Is.True);
            Assert.That(shape.Find("tail.m06"), Is.Not.Null);
            Assert.That(shape.Find("overflow")!.ItemCounts, Is.EqualTo((1, 2)));
            Assert.That(shape.Find("overflow")!.Items, Is.Null);
            Assert.That(shape.Find("new"), Is.Null);
            Assert.That(shape.Find("group0.m00")!.Numbers, Is.EqualTo((1d, 9d)));
            Assert.That(shape.Find("group0.m00")!.Seen, Is.EqualTo(2));
        });
    }

    [TestCase(".member")]
    [TestCase("member.")]
    [TestCase("member..child")]
    public void Infer_invalid_policy_path_does_not_mark_a_partial_path_as_named(string path)
    {
        var shape = SchemaShape.Infer([], [LatticeSchemaRule.Regex("x", path)]);

        Assert.Multiple(() =>
        {
            Assert.That(shape.Find(path), Is.Null);
            Assert.That(shape.Root.NamedByPolicy, Is.False);
            Assert.That(shape.Find("member")?.NamedByPolicy ?? false, Is.False);
        });
    }

    [Test]
    public void Infer_policy_path_cannot_grow_past_the_object_width_limit()
    {
        var rules = Enumerable.Range(0, SchemaShape.MaximumChildren + 1)
            .Select(index => LatticeSchemaRule.Regex("x", $"m{index:D2}.child"));

        var shape = SchemaShape.Infer([], rules);

        Assert.Multiple(() =>
        {
            Assert.That(shape.Truncated, Is.True);
            Assert.That(shape.Root.Children, Has.Count.EqualTo(SchemaShape.MaximumChildren));
            Assert.That(shape.Find("m63.child")!.NamedByPolicy, Is.True);
            Assert.That(shape.Find("m64"), Is.Null);
        });
    }

    [Test]
    public void Infer_policy_predicate_depth_limit_stops_before_a_deeper_member()
    {
        var node = LatticePredicateNode.TypeOf("hidden", LatticeValueKind.Object);
        for (var depth = 0; depth <= SchemaPredicateText.MaximumDepth; depth++)
        {
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        }

        var shape = SchemaShape.Infer([], [LatticeSchemaRule.Structured(node)]);

        Assert.Multiple(() =>
        {
            Assert.That(shape.Find("hidden"), Is.Null);
            Assert.That(shape.IsEmpty, Is.True);
        });
    }

    [Test]
    public void Infer_policy_predicate_at_depth_limit_still_names_its_member()
    {
        var node = LatticePredicateNode.TypeOf("visible", LatticeValueKind.Object);
        for (var depth = 0; depth < SchemaPredicateText.MaximumDepth; depth++)
        {
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        }

        var shape = SchemaShape.Infer([], [LatticeSchemaRule.Structured(node)]);

        Assert.That(shape.Find("visible")!.NamedByPolicy, Is.True);
    }

    [Test]
    public void Infer_scalar_observations_count_false_and_unicode_scalars_and_bound_text_examples()
    {
        var values = Enumerable.Range(0, SchemaShape.TextLimit + 2).Select(index => $"\"value{index:D2}\"")
            .Prepend("\"\\ud83d\\ude00\"").Append("\"value00\"").Append("false");

        var shape = SchemaShape.Infer(values.Select(Encoding.UTF8.GetBytes), []);

        Assert.Multiple(() =>
        {
            Assert.That(shape.Root.TextLengths, Is.EqualTo((1, 7)));
            Assert.That(shape.Root.Texts, Has.Count.EqualTo(SchemaShape.TextLimit));
            Assert.That(shape.Root.Types[SchemaValueType.Boolean], Is.EqualTo(1));
            Assert.That(shape.Root.ManyValues, Is.True);
            Assert.That(shape.Root.Texts.Count(value => value == "value00"), Is.EqualTo(1));
        });
    }

    private static SchemaShape Infer(params string[] documents) => SchemaShape.Infer(documents.Select(Encoding.UTF8.GetBytes), []);

    private static int Count(SchemaShapeNode node) =>
        1 + node.Children.Values.Sum(Count) + (node.Items is { } items ? Count(items) : 0);
}
