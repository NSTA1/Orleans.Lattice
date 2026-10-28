using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

[TestFixture]
public sealed class SchemaFormatTests
{
    [Test]
    public void Rule_unknown_encoding_keeps_a_generic_requirement_and_the_description()
    {
        var rule = new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = (LatticeSchemaEncodingKind)99,
            Description = "A requirement from a newer server",
        };

        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.RuleKind(rule), Is.EqualTo("Encoding"));
            Assert.That(SchemaFormat.RuleDetail(rule),
                Is.EqualTo("An encoding requirement - A requirement from a newer server"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase(" \t")]
    public void Rule_empty_description_does_not_append_a_separator(string? description)
    {
        var rule = new LatticeSchemaRule
        {
            Kind = LatticeSchemaRuleKind.Encoding,
            EncodingKind = LatticeSchemaEncodingKind.MaxByteLength,
            Description = description,
        };

        Assert.That(SchemaFormat.RuleDetail(rule), Is.EqualTo("The value is at most 0 bytes"));
    }

    [Test]
    public void Count_irregular_plural_is_used_only_for_counts_other_than_one()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.Count(0, "entry", "entries"), Is.EqualTo("0 entries"));
            Assert.That(SchemaFormat.Count(1, "entry", "entries"), Is.EqualTo("1 entry"));
            Assert.That(SchemaFormat.Count(1204, "entry", "entries"), Is.EqualTo("1,204 entries"));
        });
    }

    [Test]
    public void Rule_recognised_structured_predicate_uses_the_card_sentence()
    {
        var card = new SchemaRuleCard { Kind = SchemaCardKind.Type, Path = "name", ValueType = SchemaValueType.Text };
        Assert.That(SchemaCardCompiler.TryCompile(card, out var rule, out var error), Is.True, error);

        Assert.That(SchemaFormat.RuleDetail(rule), Is.EqualTo("name must be text"));
    }

    [Test]
    public void Version_and_policy_show_strict_ingest_only_when_enabled()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.Version(new LatticeSchemaVersionConfig(7, 3)),
                Is.EqualTo("family 7 at version 3"));
            Assert.That(SchemaFormat.Policy(new LatticeSchemaPolicy([], strictIngest: true)),
                Is.EqualTo("0 rules, strict ingest"));
        });
    }

    [Test]
    public void Unknown_phase_and_source_preserve_the_wire_value()
    {
        Assert.Multiple(() =>
        {
            Assert.That(SchemaFormat.Phase((LatticeSchemaRemediationPhase)99), Is.EqualTo("99"));
            Assert.That(SchemaFormat.Source((LatticeSchemaDeadLetterSource)99), Is.EqualTo("99"));
        });
    }

    [TestCase(SchemaOperationPhases.Advance, "Advancing the target version")]
    [TestCase(SchemaOperationPhases.DryRun, "Checking every value")]
    [TestCase(SchemaOperationPhases.Build, "Building the remediated copy")]
    [TestCase(SchemaOperationPhases.Cutover, "Cutting over")]
    [TestCase("future-phase", "future-phase")]
    [TestCase("", "")]
    public void OperationPhase_known_phases_are_words_and_unknown_phases_are_preserved(string phase, string expected) =>
        Assert.That(SchemaFormat.OperationPhase(phase), Is.EqualTo(expected));
}
