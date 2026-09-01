namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>Unit tests for <see cref="OidcClaimNames"/>.</summary>
public class OidcClaimNamesTests
{
    [Test]
    public void Constants_carry_the_standard_openid_connect_claim_names()
    {
        Assert.That(OidcClaimNames.Subject, Is.EqualTo("sub"));
        Assert.That(OidcClaimNames.Groups, Is.EqualTo("groups"));
        Assert.That(OidcClaimNames.Roles, Is.EqualTo("roles"));
        Assert.That(OidcClaimNames.Role, Is.EqualTo("role"));
    }
}
