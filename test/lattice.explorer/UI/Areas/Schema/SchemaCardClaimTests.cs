using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.UI.Areas.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Issue #3985: what a gallery card claims about the sample, the sentence it
/// reads as, and the check against the sample must agree. Every card that states
/// a count (Required, Type) is seeded on every kind of subject the builder can
/// show - the whole value and a member, holding text, numbers, true or false,
/// objects, lists, a mix, nulls or nothing - compiled, and checked against the
/// same sample; the count it claims must be the count that passes.
/// </summary>
[TestFixture]
public sealed class SchemaCardClaimTests
{
    private static readonly Dictionary<string, string[]> Values = new(StringComparer.Ordinal)
    {
        ["text"] = ["\"a\"", "\"bc\"", "\"d\""],
        ["numbers"] = ["1", "2.5", "-3"],
        ["booleans"] = ["true", "false", "true"],
        ["objects"] = ["{\"title\":\"a\"}", "{\"title\":\"b\"}", "{}"],
        ["lists"] = ["[1]", "[]", "[\"x\",2]"],
        ["objects and text"] = ["{\"title\":\"a\"}", "\"b\"", "\"c\""],
        ["text and lists"] = ["\"a\"", "[1]", "[2]"],
        ["objects with a null"] = ["{\"title\":\"a\"}", "null", "{}"],
        ["text with a null"] = ["\"a\"", "null", "\"c\""],
    };

    private static IEnumerable<TestCaseData> Combinations()
    {
        foreach (var shape in Values.Keys)
        {
            foreach (var subject in new[] { "the whole value", "a member", "a member missing from one value" })
            {
                yield return new TestCaseData(nameof(SchemaCardKind.Required), shape, subject, null).SetArgDisplayNames("Required", shape, subject, "seeded");
                yield return new TestCaseData(nameof(SchemaCardKind.Required), shape, subject, true).SetArgDisplayNames("Required", shape, subject, "structural");
                yield return new TestCaseData(nameof(SchemaCardKind.Required), shape, subject, false).SetArgDisplayNames("Required", shape, subject, "older form");
                yield return new TestCaseData(nameof(SchemaCardKind.Type), shape, subject, null).SetArgDisplayNames("Type", shape, subject, "seeded");
            }
        }
    }

    [TestCaseSource(nameof(Combinations))]
    public void The_count_a_card_claims_is_the_count_that_passes_the_sample(string kindName, string shape, string subject, bool? structural)
    {
        var kind = Enum.Parse<SchemaCardKind>(kindName);
        var (documents, path) = Documents(Values[shape], subject);
        var sample = new SchemaSample("t", null, [.. documents.Select((json, index) => new SchemaSampleValue("k/" + index, Encoding.UTF8.GetBytes(json)))], 0, false);
        var tree = SchemaShape.Infer(sample.Values.Select(value => value.Value), []);
        var node = tree.Find(path);

        var card = SchemaCardExamples.Seed(kind, node);
        card.Path = path;
        if (structural is { } forced)
        {
            card.Structural = forced;
        }

        var claim = SchemaCardExamples.Example(kind, node, card);
        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);
        var preview = SchemaPreviewResult.Evaluate([rule], sample);

        Assert.That(Claimed(kind, card, claim), Is.EqualTo(preview.Passed),
            $"the card claims \"{claim}\" and reads \"{SchemaCardText.Plain(card)}\", but {preview.Passed} of {preview.Checked} pass");
    }

    [Test]
    public void A_seeded_required_card_uses_the_structural_form_when_any_object_or_list_was_seen()
    {
        var tree = SchemaShape.Infer(new[] { "{\"m\":\"a\"}", "{\"m\":\"b\"}", "{\"m\":{}}" }.Select(Encoding.UTF8.GetBytes), []);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.Required, tree.Find("m")).Structural, Is.True, "text is dominant, but one object was seen");
            Assert.That(SchemaCardExamples.Seed(SchemaCardKind.Required, tree.Root).Structural, Is.True);
        });
    }

    [Test]
    public void Holds_structure_is_true_only_where_an_object_or_a_list_was_seen()
    {
        var tree = SchemaShape.Infer(new[] { "{\"t\":\"a\",\"l\":[1],\"o\":{\"x\":1},\"n\":null}" }.Select(Encoding.UTF8.GetBytes), []);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.HoldsStructure(tree.Root), Is.True);
            Assert.That(SchemaCardExamples.HoldsStructure(tree.Find("l")), Is.True);
            Assert.That(SchemaCardExamples.HoldsStructure(tree.Find("o")), Is.True);
            Assert.That(SchemaCardExamples.HoldsStructure(tree.Find("t")), Is.False);
            Assert.That(SchemaCardExamples.HoldsStructure(tree.Find("n")), Is.False);
            Assert.That(SchemaCardExamples.HoldsStructure(null), Is.False);
        });
    }

    [Test]
    public void The_older_presence_check_on_an_object_says_it_counts_only_text_numbers_and_true_or_false()
    {
        var tree = SchemaShape.Infer(new[] { "{}", "{}", "\"a\"" }.Select(Encoding.UTF8.GetBytes), []);
        var card = SchemaRuleCard.Of(SchemaCardKind.Required);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Required, tree.Root, card),
                Is.EqualTo("Set as text, a number or true or false in 1 of 3 sampled values."));
            Assert.That(SchemaCardText.Plain(card), Is.EqualTo("The value must be present as text, a number or true or false"));
            card.Structural = true;
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Required, tree.Root, card), Is.EqualTo("Set in 3 of 3 sampled values."));
            Assert.That(SchemaCardText.Plain(card), Is.EqualTo("The value must be present, as any value"));
        });
    }

    [Test]
    public void Without_a_current_card_the_required_example_describes_the_card_it_would_seed()
    {
        var tree = SchemaShape.Infer(new[] { "{}", "{}", "{}" }.Select(Encoding.UTF8.GetBytes), []);

        Assert.Multiple(() =>
        {
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Required, tree.Root), Is.EqualTo("Set in 3 of 3 sampled values."));
            Assert.That(SchemaCardExamples.Example(SchemaCardKind.Required, tree.Root, SchemaRuleCard.Of(SchemaCardKind.Type)), Is.EqualTo("Set in 3 of 3 sampled values."));
        });
    }

    private static (string[] Documents, string Path) Documents(string[] values, string subject) => subject switch
    {
        "the whole value" => (values, string.Empty),
        "a member" => ([.. values.Select(value => "{\"m\":" + value + "}")], "m"),
        _ => ([.. values.Select(value => "{\"m\":" + value + "}"), "{\"other\":1}"], "m"),
    };

    private static int Claimed(SchemaCardKind kind, SchemaRuleCard card, string claim)
    {
        if (kind == SchemaCardKind.Required)
        {
            var match = Regex.Match(claim, @"^Set (?:as text, a number or true or false )?in (\d+) of \d+ sampled values\.$");
            Assert.That(match.Success, Is.True, $"a required card states a count: \"{claim}\"");
            return int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
        }

        var seen = Regex.Match(claim, Regex.Escape(SchemaCardText.TypePhrase(card.ValueType)) + @" \((\d+)\)");
        Assert.That(seen.Success, Is.True, $"a type card states how often its type was seen: \"{claim}\"");
        return int.Parse(seen.Groups[1].Value, CultureInfo.InvariantCulture);
    }
}
