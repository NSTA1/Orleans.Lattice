using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Additional unit tests for <see cref="EntraCredentialAuthenticator"/> covering
/// the network-free public constructor overload and the fail-closed guard
/// branches (empty credential token, an issuer-less token, and an empty group /
/// role claim value) that the primary fixture does not exercise. All tokens are
/// minted in-test with a self-signed key; no live Entra endpoint is contacted.
/// </summary>
public class EntraCredentialAuthenticatorCoverageTests
{
    private static LatticeEntraAuthenticatorOptions CreateOptions(string? schemeHint = null)
    {
        var options = new LatticeEntraAuthenticatorOptions
        {
            Authority = "https://login.microsoftonline.com/common/v2.0",
            MetadataAddress = EntraTestAuthority.MetadataAddress,
            SchemeHint = schemeHint,
        };
        options.TenantIds.Add(EntraTestAuthority.TenantId);
        options.Audiences.Add(EntraTestAuthority.Audience);
        return options;
    }

    [Test]
    public void Constructor_public_overload_builds_default_configuration_source()
    {
        // The public (options-only and options+resolver) constructors derive a
        // real EntraOpenIdConfigurationSource from the options. Construction is
        // lazy - no metadata fetch happens until authentication - so this stays
        // network-free.
        var authenticator = new EntraCredentialAuthenticator(CreateOptions(schemeHint: "Entra"));

        Assert.That(authenticator, Is.Not.Null);
        Assert.That(authenticator.CanHandle(new LatticeCredential("opaque", scheme: "Entra")), Is.True);
    }

    [Test]
    public void Constructor_public_overload_with_resolver_builds_authenticator()
    {
        var resolver = new FakeEntraGroupResolver("unused");

        var authenticator = new EntraCredentialAuthenticator(CreateOptions(), resolver);

        Assert.That(authenticator, Is.Not.Null);
    }

    [Test]
    public void CanHandle_empty_token_without_scheme_hint_returns_false()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = new EntraCredentialAuthenticator(CreateOptions(), authority.ConfigurationSource);

        Assert.That(authenticator.CanHandle(new LatticeCredential(string.Empty)), Is.False);
    }

    [Test]
    public void CanHandle_allowed_tenant_token_with_empty_issuer_returns_false()
    {
        // A token whose tenant id is on the allow-list but which carries no issuer
        // must not be handled: the issuer-template match fails closed on an
        // empty issuer rather than treating the absent issuer as a match.
        using var authority = new EntraTestAuthority();
        var authenticator = new EntraCredentialAuthenticator(CreateOptions(), authority.ConfigurationSource);
        var token = authority.MintToken(issuer: string.Empty);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token)), Is.False);
    }

    [Test]
    public async Task AuthenticateAsync_empty_role_claim_value_is_ignored()
    {
        // An empty role claim value must be dropped, not surfaced as a group, and
        // the principal still resolves from its oid.
        using var authority = new EntraTestAuthority();
        var authenticator = new EntraCredentialAuthenticator(CreateOptions(), authority.ConfigurationSource);
        var token = authority.MintToken(
            objectId: "dave-oid",
            groups: new[] { "group-a" },
            roles: new[] { string.Empty });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo("dave-oid"));
        Assert.That(principal.AssertedGroups, Is.EquivalentTo(new[] { "group-a" }));
    }
}
