namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeAppOwnedRuleException"/>: the framework
/// constructors, the <see cref="LatticeAppOwnedRuleException.Rejected"/> factory's
/// guards and context, and an operator-actionable message that points at authoring
/// a separate rule outside the app-owned prefix.
/// </summary>
[TestFixture]
public sealed class LatticeAppOwnedRuleExceptionTests
{
    private const string AppRuleId = LatticeAppRuleIds.Prefix + "billing/reader/orders";

    [Test]
    public void Rejected_carries_the_rule_id_and_parameter_name()
    {
        var ex = LatticeAppOwnedRuleException.Rejected(AppRuleId, "rule");

        Assert.That(ex.RuleId, Is.EqualTo(AppRuleId));
        Assert.That(ex.ParamName, Is.EqualTo("rule"));
        Assert.That(ex, Is.InstanceOf<ArgumentException>(), "transport bindings map ArgumentException to invalid-argument");
    }

    [Test]
    public void Rejected_message_directs_the_operator_to_a_separate_rule_outside_the_prefix()
    {
        var ex = LatticeAppOwnedRuleException.Rejected(AppRuleId, "ruleId");

        Assert.That(ex.Message, Does.Contain(AppRuleId));
        Assert.That(ex.Message, Does.Contain("'" + LatticeAppRuleIds.Prefix + "'"));
        Assert.That(ex.Message, Does.Contain("Author a separate rule"));
        Assert.That(ex.Message, Does.Contain("app compiler"));
    }

    [Test]
    public void Rejected_throws_for_a_null_rule_id()
    {
        Assert.That(() => LatticeAppOwnedRuleException.Rejected(null!, "rule"), Throws.ArgumentNullException);
    }

    [Test]
    public void Rejected_throws_for_a_null_parameter_name()
    {
        Assert.That(() => LatticeAppOwnedRuleException.Rejected(AppRuleId, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Parameterless_constructor_has_an_empty_rule_id()
    {
        var ex = new LatticeAppOwnedRuleException();

        Assert.That(ex.RuleId, Is.Empty);
    }

    [Test]
    public void Message_constructor_keeps_the_message_and_an_empty_rule_id()
    {
        var ex = new LatticeAppOwnedRuleException("custom");

        Assert.That(ex.Message, Is.EqualTo("custom"));
        Assert.That(ex.RuleId, Is.Empty);
    }

    [Test]
    public void Inner_exception_constructor_keeps_the_message_and_inner_exception()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new LatticeAppOwnedRuleException("custom", inner);

        Assert.That(ex.Message, Is.EqualTo("custom"));
        Assert.That(ex.InnerException, Is.SameAs(inner));
        Assert.That(ex.RuleId, Is.Empty);
    }
}
