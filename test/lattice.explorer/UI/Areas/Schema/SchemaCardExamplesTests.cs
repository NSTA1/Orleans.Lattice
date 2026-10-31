using System.Text;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaCardExamplesTests
{
    [Test]
    public void Seed_one_of_text_distinguishes_strings_from_boolean_samples()
    {
        byte[][] bodies = ["true"u8.ToArray(), "false"u8.ToArray(), "\"true\""u8.ToArray(), "\"other\""u8.ToArray()];
        var shape = SchemaShape.Infer(bodies, []);
        var card = SchemaCardExamples.Seed(SchemaCardKind.OneOf, shape.Root);

        Assert.That(card.ValuesAreNumbers, Is.False);
        Assert.That(card.Values, Is.EquivalentTo(new[] { "true", "other" }));
    }

    [TestCase("true")]
    [TestCase("false")]
    public void Seed_one_of_text_keeps_boolean_looking_strings(string text)
    {
        var bodies = new[] { JsonSerializer.SerializeToUtf8Bytes(text), JsonSerializer.SerializeToUtf8Bytes("other") };
        var shape = SchemaShape.Infer(bodies, []);
        var card = SchemaCardExamples.Seed(SchemaCardKind.OneOf, shape.Root);

        Assert.Multiple(() =>
        {
            Assert.That(card.ValuesAreNumbers, Is.False);
            Assert.That(card.Values, Does.Contain(text));
        });
        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        var validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy([rule]));
        Assert.That(bodies.Select(body => validator.ValidateRule(0, body)), Is.All.Null);
    }

    [TestCase("shared-surrogate")]
    [TestCase("length-limit")]
    [TestCase("complete-character")]
    public void Seed_text_prefix_survives_utf8_and_matches_every_sample(string scenario)
    {
        string[] texts = scenario switch
        {
            "shared-surrogate" => ["sku-\ud83d\ude00-a", "sku-\ud83d\ude01-b"],
            "length-limit" => [new string('x', 39) + "\ud83d\ude00-a", new string('x', 39) + "\ud83d\ude00-b"],
            _ => ["sku-\ud83d\ude00-a", "sku-\ud83d\ude00-b"],
        };
        var expected = scenario switch
        {
            "shared-surrogate" => "sku-",
            "length-limit" => new string('x', 39),
            _ => "sku-\ud83d\ude00-",
        };
        var bodies = texts.Select(text => JsonSerializer.SerializeToUtf8Bytes(text)).ToArray();
        var shape = SchemaShape.Infer(bodies, []);
        var card = SchemaCardExamples.Seed(SchemaCardKind.TextMatch, shape.Root);

        Assert.That(card.MatchText, Is.EqualTo(expected));
        var wireText = new UTF8Encoding(false, true).GetString(new UTF8Encoding(false, true).GetBytes(card.MatchText));
        card.MatchText = wireText;
        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        var validator = new LatticeSchemaPolicyValidator(new LatticeSchemaPolicy([rule]));
        Assert.That(bodies.Select(body => validator.ValidateRule(0, body)), Is.All.Null);
    }
}
