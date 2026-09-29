using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// <see cref="LatticeSchemaPolicyValidator"/>: it judges values exactly as
/// enforcement does, per policy and per rule, refuses what setting the policy
/// would refuse, and guards its arguments.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaPolicyValidatorTests
{
    private static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

    private static LatticeSchemaPolicy Policy() => new(
    [
        LatticeSchemaRule.Json("must be JSON"),
        LatticeSchemaRule.Regex("^[A-Z]{3}$", "currency", "currency code"),
        LatticeSchemaRule.Structured(
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.GreaterThanOrEqual,
                LatticePredicateNode.Member("total"),
                LatticePredicateNode.Const(LatticeConstant.Integer(0))),
            "total not negative"),
    ]);

    [Test]
    public void A_compliant_value_passes_every_rule()
    {
        var validator = new LatticeSchemaPolicyValidator(Policy());

        Assert.That(validator.Validate(Utf8("{\"currency\":\"EUR\",\"total\":3}")), Is.Null);
        Assert.That(validator.RuleCount, Is.EqualTo(3));
    }

    [Test]
    public void A_failing_value_reports_the_first_rule_it_fails()
    {
        var validator = new LatticeSchemaPolicyValidator(Policy());

        Assert.That(validator.Validate(Utf8("{\"currency\":\"euro\",\"total\":-1}")), Is.EqualTo("currency code"));
        Assert.That(validator.Validate(Utf8("not json")), Is.EqualTo("must be JSON"));
    }

    [Test]
    public void Each_rule_can_be_judged_on_its_own()
    {
        var validator = new LatticeSchemaPolicyValidator(Policy());
        var value = Utf8("{\"currency\":\"euro\",\"total\":-1}");

        Assert.Multiple(() =>
        {
            Assert.That(validator.ValidateRule(0, value), Is.Null);
            Assert.That(validator.ValidateRule(1, value), Is.EqualTo("currency code"));
            Assert.That(validator.ValidateRule(2, value), Is.EqualTo("total not negative"));
        });
    }

    [Test]
    public void It_judges_the_same_way_a_policy_set_compiles()
    {
        var policy = Policy();
        var validator = new LatticeSchemaPolicyValidator(policy);
        var compiled = CompiledSchemaPolicy.Compile(policy);
        string[] values = ["{}", "{\"currency\":\"GBP\",\"total\":1}", "[]", "\"x\"", "{\"currency\":\"GB\"}"];

        foreach (var value in values)
        {
            Assert.That(validator.Validate(Utf8(value)), Is.EqualTo(compiled.Validate(Utf8(value))), value);
        }

        Assert.That(validator.Policy, Is.SameAs(policy));
    }

    [Test]
    public void A_pattern_the_cluster_cannot_compile_is_refused_up_front()
    {
        var policy = new LatticeSchemaPolicy([LatticeSchemaRule.Regex("(a)\\1")]);

        Assert.That(() => new LatticeSchemaPolicyValidator(policy), Throws.ArgumentException);
    }

    [Test]
    public void Arguments_are_guarded()
    {
        var validator = new LatticeSchemaPolicyValidator(Policy());

        Assert.Multiple(() =>
        {
            Assert.That(() => new LatticeSchemaPolicyValidator(null!), Throws.ArgumentNullException);
            Assert.That(() => validator.Validate(null!), Throws.ArgumentNullException);
            Assert.That(() => validator.ValidateRule(0, null!), Throws.ArgumentNullException);
            Assert.That(() => validator.ValidateRule(-1, Utf8("{}")), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => validator.ValidateRule(3, Utf8("{}")), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
