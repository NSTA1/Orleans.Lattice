namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantOwnedRuleException"/>: the framework
/// constructors, the <see cref="LatticeTenantOwnedRuleException.Rejected"/>
/// factory's guards and context, and an operator-actionable message that points
/// at authoring an operator rule outside the tenant-tier prefix.
/// </summary>
[TestFixture]
public sealed class LatticeTenantOwnedRuleExceptionTests
{
    private const string TenantRuleId = LatticeTenantRuleIds.Prefix + "contoso:readers";

    [Test]
    public void Rejected_carries_the_rule_id_and_parameter_name()
    {
        var ex = LatticeTenantOwnedRuleException.Rejected(TenantRuleId, "rule");

        Assert.That(ex.RuleId, Is.EqualTo(TenantRuleId));
        Assert.That(ex.ParamName, Is.EqualTo("rule"));
        Assert.That(ex, Is.InstanceOf<ArgumentException>(), "transport bindings map ArgumentException to invalid-argument");
    }

    [Test]
    public void Rejected_message_directs_the_operator_to_an_operator_rule_outside_the_prefix()
    {
        var ex = LatticeTenantOwnedRuleException.Rejected(TenantRuleId, "ruleId");

        Assert.That(ex.Message, Does.Contain(TenantRuleId));
        Assert.That(ex.Message, Does.Contain("'" + LatticeTenantRuleIds.Prefix + "'"));
        Assert.That(ex.Message, Does.Contain("Author an operator rule"));
        Assert.That(ex.Message, Does.Contain("tenant policy administration surface"));
    }

    [Test]
    public void Rejected_throws_for_a_null_rule_id()
    {
        Assert.That(() => LatticeTenantOwnedRuleException.Rejected(null!, "rule"), Throws.ArgumentNullException);
    }

    [Test]
    public void Rejected_throws_for_a_null_parameter_name()
    {
        Assert.That(() => LatticeTenantOwnedRuleException.Rejected(TenantRuleId, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Parameterless_constructor_has_an_empty_rule_id()
    {
        var ex = new LatticeTenantOwnedRuleException();

        Assert.That(ex.RuleId, Is.Empty);
    }

    [Test]
    public void Message_constructor_keeps_the_message_and_an_empty_rule_id()
    {
        var ex = new LatticeTenantOwnedRuleException("custom");

        Assert.That(ex.Message, Is.EqualTo("custom"));
        Assert.That(ex.RuleId, Is.Empty);
    }

    [Test]
    public void Inner_exception_constructor_keeps_the_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new LatticeTenantOwnedRuleException("custom", inner);

        Assert.That(ex.Message, Is.EqualTo("custom"));
        Assert.That(ex.InnerException, Is.SameAs(inner));
        Assert.That(ex.RuleId, Is.Empty);
    }
}
