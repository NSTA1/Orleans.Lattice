using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for <see cref="NullTenantGateEnforcer"/>: the allow-everything null
/// seam a cluster without the tenancy add-on runs with. It reports inactive and
/// always allows, so the auth gate short-circuits on the <c>IsActive</c> read and
/// the no-tenancy path is unchanged.
/// </summary>
[TestFixture]
public sealed class NullTenantGateEnforcerTests
{
    [Test]
    public void IsActive_is_false()
    {
        var enforcer = new NullTenantGateEnforcer();

        Assert.That(enforcer.IsActive, Is.False);
    }

    [Test]
    public void Enforce_allows_every_request()
    {
        var enforcer = new NullTenantGateEnforcer();
        var request = new LatticeAccessRequest("app", LatticeOperation.Write, new LatticeSubject("alice"), "k");

        var decision = enforcer.Enforce(in request);

        Assert.That(decision.Allowed, Is.True);
    }
}
