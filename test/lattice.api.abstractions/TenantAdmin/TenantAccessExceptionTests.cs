using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Api.Abstractions.Tests.TenantAdmin;

/// <summary>
/// Tests for the delegated tenant-access typed failures:
/// <see cref="TenantAccessAdministrationDisabledException"/> (the feature is off)
/// and <see cref="TenantAccessConfinementException"/> (a write would reach outside
/// the tenant), including the same-silo deep copy of the latter.
/// </summary>
[TestFixture]
public sealed class TenantAccessExceptionTests
{
    [Test]
    public void Disabled_exception_carries_the_tenant_and_names_the_flag()
    {
        var exception = new TenantAccessAdministrationDisabledException("acme");

        Assert.Multiple(() =>
        {
            Assert.That(exception.TenantId, Is.EqualTo("acme"));
            Assert.That(exception.Message, Does.Contain("acme"));
            Assert.That(exception.Message, Does.Contain("DelegatedAccessAdministrationEnabled"));
            Assert.That(exception.GetType().BaseType, Is.EqualTo(typeof(Exception)));
        });
    }

    [Test]
    public void Confinement_exception_carries_the_tenant_rule_and_argument()
    {
        var exception = new TenantAccessConfinementException(
            "acme", TenantAccessConfinementRule.RuleTree, "'a/inventory' is an app-owned tree.", "rule");

        Assert.Multiple(() =>
        {
            Assert.That(exception.TenantId, Is.EqualTo("acme"));
            Assert.That(exception.Rule, Is.EqualTo(TenantAccessConfinementRule.RuleTree));
            Assert.That(exception.ParamName, Is.EqualTo("rule"));
            Assert.That(exception.Message, Does.StartWith("'a/inventory' is an app-owned tree."));
            Assert.That(exception, Is.InstanceOf<ArgumentException>(), "bindings map it to an invalid-argument status");
        });
    }

    [Test]
    public void Confinement_exception_accepts_no_argument_name()
    {
        var exception = new TenantAccessConfinementException("acme", TenantAccessConfinementRule.GroupNesting, "nested");

        Assert.Multiple(() =>
        {
            Assert.That(exception.ParamName, Is.Null);
            Assert.That(exception.Message, Is.EqualTo("nested"));
        });
    }

    [Test]
    public void Confinement_exception_rejects_a_null_message() =>
        Assert.That(
            () => new TenantAccessConfinementException("acme", TenantAccessConfinementRule.ReservedRuleId, null!),
            Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName)).EqualTo("message"));

    [Test]
    public void Confinement_exception_survives_a_same_silo_deep_copy_as_the_same_instance()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copier = services.GetRequiredService<DeepCopier<TenantAccessConfinementException>>();
        var exception = new TenantAccessConfinementException("acme", TenantAccessConfinementRule.RuleOperations, "telemetry");

        Assert.That(copier.Copy(exception), Is.SameAs(exception));
    }
}
