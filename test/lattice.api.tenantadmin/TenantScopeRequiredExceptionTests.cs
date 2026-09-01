using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="TenantScopeRequiredException"/>, the fail-closed
/// signal raised when a tenant-scoped operation runs with no active tenant in
/// scope. They pin that each public constructor overload carries the message (and
/// inner exception) a transport binding relies on to surface the refusal, and that
/// the parameterless overload uses the fixed default fail-closed message.
/// </summary>
[TestFixture]
public sealed class TenantScopeRequiredExceptionTests
{
    [Test]
    public void Default_ctor_carries_the_fixed_fail_closed_message()
    {
        var exception = new TenantScopeRequiredException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Does.Contain("No active tenant is in scope"));
            Assert.That(exception.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_ctor_carries_the_custom_message()
    {
        var exception = new TenantScopeRequiredException("scope required here");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("scope required here"));
            Assert.That(exception.InnerException, Is.Null);
        });
    }

    [Test]
    public void Message_and_inner_ctor_carries_both()
    {
        var inner = new InvalidOperationException("cause");

        var exception = new TenantScopeRequiredException("scope required here", inner);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("scope required here"));
            Assert.That(exception.InnerException, Is.SameAs(inner));
        });
    }
}
