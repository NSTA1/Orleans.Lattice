using ModelContextProtocol;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAccessToolFaults"/>: each delegated tenant
/// access typed failure maps onto the MCP error shape with an actionable message,
/// caller mistakes are marked as client errors, an authorization denial and every
/// unrelated fault pass through unchanged, and no mapped message echoes the
/// facade's caller-derived text where a fixed description is available.
/// </summary>
[TestFixture]
public sealed class TenantAccessToolFaultsTests
{
    [Test]
    public void Feature_disabled_maps_to_the_clear_message_and_is_not_a_client_error()
    {
        var source = new TenantAccessAdministrationDisabledException("acme");

        Assert.That(TenantAccessToolFaults.TryTranslate(source, out var mapped), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(mapped.Message, Is.EqualTo(TenantAccessToolFaults.DisabledMessage));
            Assert.That(mapped.Message, Does.Contain("Delegated tenant access administration is not enabled on this cluster"));
            Assert.That(mapped.InnerException, Is.SameAs(source));
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out _), Is.False);
        });
    }

    [TestCase(TenantAccessConfinementRule.GroupNesting)]
    [TestCase(TenantAccessConfinementRule.ForeignTenantGroup)]
    [TestCase(TenantAccessConfinementRule.RuleTree)]
    [TestCase(TenantAccessConfinementRule.RuleOperations)]
    [TestCase(TenantAccessConfinementRule.ReservedRuleId)]
    public void Confinement_maps_to_a_rejected_content_client_error_naming_the_rule(TenantAccessConfinementRule rule)
    {
        var source = new TenantAccessConfinementException("acme", rule, "caller-text\nforged-line", "subjectId");

        Assert.That(TenantAccessToolFaults.TryTranslate(source, out var mapped), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.RejectedContent));
            Assert.That(mapped.Message, Does.Contain($"({rule})"));
            Assert.That(mapped.Message, Does.Not.Contain("caller-text"));
        });
    }

    [Test]
    public void An_unknown_confinement_rule_still_maps_to_a_fixed_message()
    {
        Assert.That(
            TenantAccessToolFaults.DescribeConfinement((TenantAccessConfinementRule)99),
            Is.EqualTo("The request was refused by tenant access confinement."));
    }

    [Test]
    public void Reserved_default_tenant_maps_to_an_invalid_argument()
    {
        Assert.That(
            TenantAccessToolFaults.TryTranslate(new ReservedTenantOperationException("default", "ListGroupsAsync"), out var mapped),
            Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
            Assert.That(mapped.Message, Is.EqualTo(TenantAccessToolFaults.ReservedTenantMessage));
        });
    }

    [Test]
    public void Last_admin_maps_to_an_invalid_argument_without_echoing_the_subject()
    {
        Assert.That(
            TenantAccessToolFaults.TryTranslate(new TenantLastAdminSubjectException("acme", "t/acme/admins"), out var mapped),
            Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
            Assert.That(mapped.Message, Does.Not.Contain("t/acme/admins"));
        });
    }

    [Test]
    public void Quota_maps_to_a_message_naming_the_dimension_and_limit()
    {
        var source = new LatticeQuotaExceededException("raw", string.Empty, "MaxGroups", 500, 500, "acme");

        Assert.That(TenantAccessToolFaults.TryTranslate(source, out var mapped), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(mapped.Message, Does.Contain("MaxGroups cap of 500"));
            Assert.That(mapped.Message, Does.Contain("lattice_tenant_set_quotas"));
            Assert.That(mapped.InnerException, Is.SameAs(source));
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out _), Is.False);
        });
    }

    [Test]
    public void Quota_without_a_dimension_maps_to_a_generic_cap_message()
    {
        Assert.That(TenantAccessToolFaults.TryTranslate(new LatticeQuotaExceededException("raw"), out var mapped), Is.True);
        Assert.That(mapped.Message, Does.StartWith("The tenant is at one of its delegated access caps."));
    }

    [Test]
    public void Facade_argument_validation_maps_to_an_invalid_argument_with_its_message()
    {
        Assert.That(
            TenantAccessToolFaults.TryTranslate(new ArgumentException("Tenant 'acme' has no group 'x'.", "groupName"), out var mapped),
            Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(McpToolClientErrors.TryGetReason(mapped, out var reason), Is.True);
            Assert.That(reason, Is.EqualTo(McpToolClientErrorReason.InvalidArgument));
            Assert.That(mapped.Message, Does.Contain("has no group 'x'"));
        });
    }

    [Test]
    public void Authorization_denial_is_not_mapped()
    {
        Assert.That(
            TenantAccessToolFaults.TryTranslate(new LatticeAuthorizationDeniedException("denied"), out _),
            Is.False,
            "A denial keeps its existing path and is never downgraded to a client error.");
    }

    [Test]
    public void Unrelated_faults_are_not_mapped()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TenantAccessToolFaults.TryTranslate(new InvalidOperationException("boom"), out _), Is.False);
            Assert.That(TenantAccessToolFaults.TryTranslate(new McpException("already"), out _), Is.False);
            Assert.That(TenantAccessToolFaults.TryTranslate(new OperationCanceledException(), out _), Is.False);
        });
    }

    [Test]
    public void TryTranslate_rejects_null()
    {
        Assert.That(() => TenantAccessToolFaults.TryTranslate(null!, out _), Throws.ArgumentNullException);
    }
}
