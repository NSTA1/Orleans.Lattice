using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaPolicyJsonTests
{
    [Test]
    public void Write_preserves_constant_types_and_values()
    {
        var policy = new LatticeSchemaPolicy(
        [
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.Or,
                LatticePredicateNode.Const(LatticeConstant.Null()),
                LatticePredicateNode.Const(LatticeConstant.Bool(false)),
                LatticePredicateNode.Const(LatticeConstant.Bool(true)),
                LatticePredicateNode.Const(LatticeConstant.Integer(long.MinValue)),
                LatticePredicateNode.Const(LatticeConstant.Real(1.25)),
                LatticePredicateNode.Const(LatticeConstant.Text("quoted \"text\"\n<value>")))),
        ]);

        using var document = JsonDocument.Parse(SchemaPolicyJson.Write(policy));
        var children = document.RootElement.GetProperty("rules")[0].GetProperty("predicate").GetProperty("children");

        Assert.Multiple(() =>
        {
            Assert.That(document.RootElement.GetProperty("strictIngest").GetBoolean(), Is.False);
            Assert.That(children.GetArrayLength(), Is.EqualTo(6));
            Assert.That(children[0].GetProperty("value").ValueKind, Is.EqualTo(JsonValueKind.Null));
            Assert.That(children[1].GetProperty("value").GetBoolean(), Is.False);
            Assert.That(children[2].GetProperty("value").GetBoolean(), Is.True);
            Assert.That(children[3].GetProperty("value").GetInt64(), Is.EqualTo(long.MinValue));
            Assert.That(children[4].GetProperty("value").GetDouble(), Is.EqualTo(1.25));
            Assert.That(children[5].GetProperty("value").GetString(), Is.EqualTo("quoted \"text\"\n<value>"));
        });
    }

    [Test]
    public void Write_omits_inapplicable_optional_fields()
    {
        var policy = new LatticeSchemaPolicy(
        [
            LatticeSchemaRule.Regex("^x$"),
            LatticeSchemaRule.Utf8(),
            new LatticeSchemaRule { Kind = LatticeSchemaRuleKind.Structured },
            new LatticeSchemaRule { Kind = (LatticeSchemaRuleKind)99, Description = "future rule" },
        ], strictIngest: true);

        using var document = JsonDocument.Parse(SchemaPolicyJson.Write(policy));
        var rules = document.RootElement.GetProperty("rules");

        Assert.Multiple(() =>
        {
            Assert.That(document.RootElement.GetProperty("strictIngest").GetBoolean(), Is.True);
            Assert.That(Properties(rules[0]), Is.EqualTo(new[] { "kind", "pattern" }));
            Assert.That(rules[0].GetProperty("pattern").GetString(), Is.EqualTo("^x$"));
            Assert.That(Properties(rules[1]), Is.EqualTo(new[] { "kind", "encoding" }));
            Assert.That(rules[1].GetProperty("encoding").GetString(), Is.EqualTo("Utf8"));
            Assert.That(Properties(rules[2]), Is.EqualTo(new[] { "kind" }));
            Assert.That(Properties(rules[3]), Is.EqualTo(new[] { "kind", "description" }));
            Assert.That(rules[3].GetProperty("kind").GetString(), Is.EqualTo("99"));
            Assert.That(rules[3].GetProperty("description").GetString(), Is.EqualTo("future rule"));
        });
    }

    [Test]
    public void Write_keeps_current_document_nodes_distinct_from_named_members()
    {
        var policy = new LatticeSchemaPolicy(
        [
            LatticeSchemaRule.Structured(LatticePredicateNode.Bool(LatticeBooleanOperator.And,
                LatticePredicateNode.TypeOf(null, LatticeValueKind.Object),
                LatticePredicateNode.TypeOf("customer", LatticeValueKind.Object),
                LatticePredicateNode.LengthOf(null),
                LatticePredicateNode.LengthOf("items"),
                LatticePredicateNode.Every(null, LatticePredicateNode.Self()),
                LatticePredicateNode.Every("items", LatticePredicateNode.Self()),
                LatticePredicateNode.Member("id"),
                new LatticePredicateNode { Kind = LatticePredicateNodeKind.Member, Children = [] })),
        ]);

        using var document = JsonDocument.Parse(SchemaPolicyJson.Write(policy));
        var children = document.RootElement.GetProperty("rules")[0].GetProperty("predicate").GetProperty("children");

        Assert.Multiple(() =>
        {
            Assert.That(Properties(children[0]), Is.EqualTo(new[] { "kind", "valueKind" }));
            Assert.That(children[0].GetProperty("valueKind").GetString(), Is.EqualTo("Object"));
            Assert.That(children[1].GetProperty("member").GetString(), Is.EqualTo("customer"));
            Assert.That(Properties(children[2]), Is.EqualTo(new[] { "kind" }));
            Assert.That(children[3].GetProperty("member").GetString(), Is.EqualTo("items"));
            Assert.That(Properties(children[4]), Is.EqualTo(new[] { "kind", "children" }));
            Assert.That(children[4].GetProperty("children")[0].GetProperty("kind").GetString(), Is.EqualTo("Self"));
            Assert.That(children[5].GetProperty("member").GetString(), Is.EqualTo("items"));
            Assert.That(children[6].GetProperty("member").GetString(), Is.EqualTo("id"));
            Assert.That(Properties(children[7]), Is.EqualTo(new[] { "kind" }));
        });
    }

    [TestCase(32, false)]
    [TestCase(33, true)]
    public void Write_elides_only_nodes_beyond_the_depth_limit(int depth, bool elided)
    {
        var node = LatticePredicateNode.Const(LatticeConstant.Text("leaf"));
        for (var i = 0; i < depth; i++)
        {
            node = LatticePredicateNode.Bool(LatticeBooleanOperator.Not, node);
        }

        // Each predicate level adds both an object and a children array.
        using var document = JsonDocument.Parse(SchemaPolicyJson.Write(
            new LatticeSchemaPolicy([LatticeSchemaRule.Structured(node)])),
            new JsonDocumentOptions { MaxDepth = 128 });
        var leaf = document.RootElement.GetProperty("rules")[0].GetProperty("predicate");
        for (var i = 0; i < depth; i++)
        {
            Assert.That(leaf.GetProperty("operator").GetString(), Is.EqualTo("Not"));
            Assert.That(leaf.GetProperty("children").GetArrayLength(), Is.EqualTo(1));
            leaf = leaf.GetProperty("children")[0];
        }

        Assert.Multiple(() =>
        {
            Assert.That(leaf.GetProperty("kind").GetString(), Is.EqualTo("Constant"));
            Assert.That(Properties(leaf), Is.EqualTo(elided ? new[] { "kind", "elided" } : new[] { "kind", "value" }));
            Assert.That(leaf.GetProperty(elided ? "elided" : "value").GetString(),
                Is.EqualTo(elided ? "nested too deeply to show" : "leaf"));
        });
    }

    private static string[] Properties(JsonElement element) =>
        element.EnumerateObject().Select(property => property.Name).ToArray();
}
