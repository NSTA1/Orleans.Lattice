using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>Format and numeric cards must enforce what the operator entered without rewriting saved policies.</summary>
[TestFixture]
public sealed class SchemaInputRegressionTests
{
    [TestCase("::ffff:192.0.2.1", true)]
    [TestCase("::192.0.2.1", true)]
    [TestCase("2001:db8::192.0.2.1", true)]
    [TestCase("1:2:3:4:5:6:192.0.2.1", true)]
    [TestCase("::1:2:3:4:5:192.0.2.1", true)]
    [TestCase("1::2:3:4:5:192.0.2.1", true)]
    [TestCase("1:2::3:4:5:192.0.2.1", true)]
    [TestCase("1:2:3::4:5:192.0.2.1", true)]
    [TestCase("1:2:3:4::5:192.0.2.1", true)]
    [TestCase("1:2:3:4:5::192.0.2.1", true)]
    [TestCase("::ffff:256.0.2.1", false)]
    [TestCase("::ffff:192.0.2", false)]
    [TestCase(":192.0.2.1", false)]
    [TestCase("1:2:3:4:5:6::192.0.2.1", false)]
    [TestCase("1:2:3:4:5:192.0.2.1", false)]
    [TestCase("::1:2:3:4:5:6:192.0.2.1", false)]
    [TestCase("1::2::192.0.2.1", false)]
    [TestCase("::ffff:192.0.2.1\n", false)]
    public void Ipv6_mixed_notation_is_enforced_by_the_compiled_card(string value, bool expected) =>
        AssertFormat(SchemaTextFormat.Ipv6, value, expected);

    [TestCase(".dana@example.com", false)]
    [TestCase("dana.@example.com", false)]
    [TestCase("dana..smith@example.com", false)]
    [TestCase("dana@-example.com", false)]
    [TestCase("dana@example-.com", false)]
    [TestCase("dana@mail.-example.com", false)]
    [TestCase("dana@mail.example-.com", false)]
    [TestCase("dana.smith+tag@mail.example.com", true)]
    [TestCase("dana@my-example.com", true)]
    [TestCase("a@b.co", true)]
    public void Email_dot_atoms_and_domain_labels_are_enforced_by_the_compiled_card(string value, bool expected) =>
        AssertFormat(SchemaTextFormat.Email, value, expected);

    [TestCase("1,2")]
    [TestCase("1,,000")]
    [TestCase(",100")]
    [TestCase("100,")]
    [TestCase("1234,567")]
    [TestCase("1.2,3")]
    [TestCase("1e1,0")]
    [TestCase("-,1")]
    public void Numeric_cards_refuse_malformed_grouping_without_producing_a_rule(string value)
    {
        foreach (var kind in new[] { SchemaCardKind.NumberRange, SchemaCardKind.OneOf, SchemaCardKind.TextLength, SchemaCardKind.ListLength, SchemaCardKind.MaxSize })
        {
            var card = SchemaRuleCard.Of(kind, "amount");
            card.Maximum = value;
            card.ValuesAreNumbers = true;
            card.Values = [value];
            card.MaxBytes = value;

            Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.False, kind.ToString());
            Assert.Multiple(() =>
            {
                Assert.That(rule, Is.EqualTo(default(LatticeSchemaRule)), kind.ToString());
                Assert.That(error, Is.Not.Null.And.Not.Empty, kind.ToString());
            });
        }
    }

    [TestCase("1,000", 1000d)]
    [TestCase("  -12,345.5  ", -12345.5d)]
    [TestCase("+1,000.5e-2", 10.005d)]
    [TestCase("1234567", 1234567d)]
    [TestCase(".5", 0.5d)]
    public void Numeric_cards_preserve_valid_grouped_and_ungrouped_values(string value, double expected)
    {
        Assert.That(SchemaCardCompiler.TryNumber(value, out var parsed), Is.True);
        Assert.That(parsed.Kind == LatticeConstantKind.Int64 ? parsed.Int64Value : parsed.DoubleValue, Is.EqualTo(expected));
    }

    [TestCase("TextLength")]
    [TestCase("ListLength")]
    [TestCase("MaxSize")]
    public void Whole_count_cards_preserve_valid_grouping(string name)
    {
        var card = SchemaRuleCard.Of(Enum.Parse<SchemaCardKind>(name), "value");
        card.Maximum = "1,024";
        card.MaxBytes = "1,024";

        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        Assert.That(rule, Is.Not.EqualTo(default(LatticeSchemaRule)));
    }

    [Test]
    public void Legacy_email_policy_reopens_as_an_editable_pattern_without_rewriting()
    {
        const string legacy = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}\\z";
        AssertLegacyPattern(legacy);
    }

    [Test]
    public void Legacy_ipv6_policy_reopens_as_an_editable_pattern_without_rewriting()
    {
        const string hex = "[0-9A-Fa-f]{1,4}";
        var legacy = "^((" + hex + ":){7}" + hex
            + "|(" + hex + ":){1,7}:"
            + "|(" + hex + ":){1,6}:" + hex
            + "|(" + hex + ":){1,5}(:" + hex + "){1,2}"
            + "|(" + hex + ":){1,4}(:" + hex + "){1,3}"
            + "|(" + hex + ":){1,3}(:" + hex + "){1,4}"
            + "|(" + hex + ":){1,2}(:" + hex + "){1,5}"
            + "|" + hex + ":(:" + hex + "){1,6}"
            + "|:((:" + hex + "){1,7}|:))\\z";
        AssertLegacyPattern(legacy);
    }

    private static void AssertFormat(SchemaTextFormat format, string value, bool expected)
    {
        var card = SchemaRuleCard.Of(SchemaCardKind.Format, "value");
        card.Format = format;
        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        var sample = new SchemaSample("t", null,
            [new SchemaSampleValue("k", JsonSerializer.SerializeToUtf8Bytes(new { value }))], 0, false);
        var result = SchemaPreviewResult.Evaluate([rule], sample);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormatPatterns.IsMatch(format, value), Is.EqualTo(expected));
            Assert.That(result.Passed, Is.EqualTo(expected ? 1 : 0));
            Assert.That(SchemaFormatPatterns.Matching([value]).Contains(format), Is.EqualTo(expected));
        });
    }

    private static void AssertLegacyPattern(string pattern)
    {
        var original = LatticeSchemaRule.Regex(pattern, "value");
        var card = SchemaCardDecompiler.Decompile(original);
        Assert.Multiple(() =>
        {
            Assert.That(card.Kind, Is.EqualTo(SchemaCardKind.Pattern));
            Assert.That(card.Pattern, Is.EqualTo(pattern));
            Assert.That(SchemaCardCompiler.TryCompile(card, out var compiled, out var error), Is.True, error);
            Assert.That(compiled, Is.EqualTo(original));
        });
    }
}
