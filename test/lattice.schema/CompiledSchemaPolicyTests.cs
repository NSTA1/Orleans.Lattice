using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledSchemaPolicy"/>: conjunctive multi-rule
/// evaluation, empty-policy acceptance, first-failure reason reporting, and the
/// strict-ingest flag carry-through.
/// </summary>
public class CompiledSchemaPolicyTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public void Compile_empty_policy_accepts_every_value()
    {
        var compiled = CompiledSchemaPolicy.Compile(new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>()));
        Assert.That(compiled.RuleCount, Is.Zero);
        Assert.That(compiled.Validate(Utf8("anything")), Is.Null);
    }

    [Test]
    public void Validate_requires_every_rule_to_pass()
    {
        var policy = new LatticeSchemaPolicy(new[]
        {
            LatticeSchemaRule.Json(),
            LatticeSchemaRule.MaxLength(32),
        });
        var compiled = CompiledSchemaPolicy.Compile(policy);

        Assert.That(compiled.Validate(Utf8("{\"a\":1}")), Is.Null);
    }

    [Test]
    public void Validate_returns_reason_of_first_failing_rule()
    {
        var policy = new LatticeSchemaPolicy(new[]
        {
            LatticeSchemaRule.Json("must be json"),
            LatticeSchemaRule.MaxLength(4, "too long"),
        });
        var compiled = CompiledSchemaPolicy.Compile(policy);

        // First rule (JSON) fails first, so its reason wins even though the value
        // also violates the max-length rule.
        Assert.That(compiled.Validate(Utf8("not json at all")), Is.EqualTo("must be json"));
    }

    [Test]
    public void Validate_reports_later_rule_when_earlier_passes()
    {
        var policy = new LatticeSchemaPolicy(new[]
        {
            LatticeSchemaRule.Json("must be json"),
            LatticeSchemaRule.MaxLength(4, "too long"),
        });
        var compiled = CompiledSchemaPolicy.Compile(policy);

        Assert.That(compiled.Validate(Utf8("{\"aaaaa\":1}")), Is.EqualTo("too long"));
    }

    [Test]
    public void Compile_carries_strict_ingest_flag()
    {
        var compiled = CompiledSchemaPolicy.Compile(
            new LatticeSchemaPolicy(Array.Empty<LatticeSchemaRule>(), strictIngest: true));
        Assert.That(compiled.StrictIngest, Is.True);
    }

    [Test]
    public void Compile_null_policy_throws()
    {
        Assert.That(() => CompiledSchemaPolicy.Compile(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Compile_propagates_uncompilable_regex_rejection()
    {
        var policy = new LatticeSchemaPolicy(new[] { LatticeSchemaRule.Regex("(unclosed") });
        Assert.That(() => CompiledSchemaPolicy.Compile(policy), Throws.ArgumentException);
    }
}
