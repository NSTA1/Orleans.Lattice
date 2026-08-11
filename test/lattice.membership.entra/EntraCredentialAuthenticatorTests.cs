using Microsoft.IdentityModel.Tokens;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// Unit tests for <see cref="EntraCredentialAuthenticator"/>. Tokens are minted
/// in-test with a self-signed RSA key and validated against a network-free
/// configuration source, so no live Entra endpoint is contacted.
/// </summary>
public class EntraCredentialAuthenticatorTests
{
    private const string OtherTenant = "22222222-2222-2222-2222-222222222222";

    private static LatticeEntraAuthenticatorOptions CreateOptions(
        EntraGroupResolutionMode mode = EntraGroupResolutionMode.TokenOnly,
        string? schemeHint = null,
        params string[] tenantIds)
    {
        var options = new LatticeEntraAuthenticatorOptions
        {
            Authority = "https://login.microsoftonline.com/common/v2.0",
            MetadataAddress = EntraTestAuthority.MetadataAddress,
            GroupResolutionMode = mode,
            SchemeHint = schemeHint,
        };

        foreach (var tenant in tenantIds.Length > 0 ? tenantIds : new[] { EntraTestAuthority.TenantId })
        {
            options.TenantIds.Add(tenant);
        }

        options.Audiences.Add(EntraTestAuthority.Audience);
        return options;
    }

    private static EntraCredentialAuthenticator CreateAuthenticator(
        EntraTestAuthority authority,
        EntraGroupResolutionMode mode = EntraGroupResolutionMode.TokenOnly,
        IEntraGroupResolver? resolver = null,
        string? schemeHint = null,
        params string[] tenantIds)
    {
        var options = CreateOptions(mode, schemeHint, tenantIds);
        return new EntraCredentialAuthenticator(options, authority.ConfigurationSource, resolver);
    }

    [Test]
    public void Constructor_null_options_throws()
    {
        using var authority = new EntraTestAuthority();
        Assert.That(
            () => new EntraCredentialAuthenticator(null!, authority.ConfigurationSource),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_missing_metadata_and_authority_throws()
    {
        using var authority = new EntraTestAuthority();
        var options = new LatticeEntraAuthenticatorOptions();
        options.TenantIds.Add(EntraTestAuthority.TenantId);
        options.Audiences.Add(EntraTestAuthority.Audience);

        Assert.That(
            () => new EntraCredentialAuthenticator(options, authority.ConfigurationSource),
            Throws.ArgumentException);
    }

    [Test]
    public void CanHandle_matching_scheme_hint_returns_true()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority, schemeHint: "Entra");

        Assert.That(authenticator.CanHandle(new LatticeCredential("opaque", scheme: "Entra")), Is.True);
    }

    [Test]
    public void CanHandle_allowed_tenant_token_returns_true()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);

        Assert.That(authenticator.CanHandle(new LatticeCredential(authority.MintToken())), Is.True);
    }

    [Test]
    public void CanHandle_token_from_unlisted_tenant_returns_false()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(tenantId: OtherTenant);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token)), Is.False);
    }

    [Test]
    public void CanHandle_malformed_token_returns_false()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);

        Assert.That(authenticator.CanHandle(new LatticeCredential("not-a-jwt")), Is.False);
    }

    [Test]
    public void CanHandle_multi_tenant_allow_list_accepts_listed_and_rejects_unlisted()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority, tenantIds: new[] { EntraTestAuthority.TenantId, OtherTenant });

        Assert.That(authenticator.CanHandle(new LatticeCredential(authority.MintToken(tenantId: OtherTenant))), Is.True);
        Assert.That(authenticator.CanHandle(new LatticeCredential(authority.MintToken(tenantId: "33333333-3333-3333-3333-333333333333"))), Is.False);
    }

    [Test]
    public async Task AuthenticateAsync_valid_v2_token_resolves_oid_groups_and_roles()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var expires = DateTime.UtcNow.AddMinutes(30);
        var token = authority.MintToken(
            objectId: "alice-oid",
            groups: new[] { "group-a", "group-b" },
            roles: new[] { "Reader" },
            expires: expires);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo("alice-oid"));
        Assert.That(principal.Issuer, Is.EqualTo(EntraTestAuthority.IssuerFor(EntraTestAuthority.TenantId)));
        Assert.That(principal.AssertedGroups, Is.EquivalentTo(new[] { "group-a", "group-b", "Reader" }));
        Assert.That(principal.Claims, Is.Not.Null);
        Assert.That(principal.Claims![EntraClaimNames.TenantId], Is.EqualTo(EntraTestAuthority.TenantId));
        Assert.That(principal.ExpiresAt, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_with_reserved_sentinel_oid_returns_null()
    {
        // A token whose oid collides with a reserved sentinel (here the system
        // subject) must never resolve to an authorized principal, even though it is
        // validly signed by a trusted tenant and carries groups / roles.
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(
            objectId: LatticeSubject.SystemSubjectId,
            groups: new[] { "group-a" },
            roles: new[] { "Admin" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_with_no_usable_subject_but_groups_returns_null()
    {
        // A validly-signed Entra token that carries groups but no usable oid/sub
        // must resolve to the anonymous subject (null principal), not an
        // anonymous-labelled principal that still carries those groups.
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(objectId: string.Empty, groups: new[] { "group-a" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_from_unlisted_tenant_returns_null()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(tenantId: OtherTenant);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_audience_returns_null()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(audience: "api://someone-else");

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_signing_key_returns_null()
    {
        using var authority = new EntraTestAuthority();
        using var otherAuthority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);

        // Sign with a different authority's config source: the validator publishes
        // the first authority's key, so the signature does not verify.
        var token = otherAuthority.MintToken();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_default_pins_rs256_and_authenticates()
    {
        // The default Algorithms allow-list is RS256, which is exactly what the
        // authority mints, so a valid token still authenticates.
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(authority.MintToken()));

        Assert.That(principal, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_algorithm_outside_pinned_set_returns_null()
    {
        // Restricting the pinned set to an algorithm the authority does not use
        // must reject its RS256 token (CWE-347 algorithm confusion).
        using var authority = new EntraTestAuthority();
        var options = CreateOptions();
        options.Algorithms.Clear();
        options.Algorithms.Add(SecurityAlgorithms.RsaSsaPssSha256);
        var authenticator = new EntraCredentialAuthenticator(options, authority.ConfigurationSource);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(authority.MintToken()));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wildcard_common_issuer_token_is_rejected()
    {
        // A token whose tenant id is on the allow-list but whose issuer is the
        // multi-tenant "common" authority (not the concrete per-tenant issuer)
        // must be rejected: the validator pins the concrete tenant issuer, so a
        // wildcard-issuer token cannot impersonate a listed tenant.
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(
            tenantId: EntraTestAuthority.TenantId,
            issuer: "https://login.microsoftonline.com/common/v2.0");

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_templated_placeholder_issuer_token_is_rejected()
    {
        // A token whose issuer carries the literal "{tenantid}" template placeholder
        // rather than a resolved tenant guid must be rejected: issuer validation
        // resolves the template to the concrete allowed tenant and does not accept
        // the unresolved placeholder.
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);
        var token = authority.MintToken(
            tenantId: EntraTestAuthority.TenantId,
            issuer: "https://login.microsoftonline.com/{tenantid}/v2.0");

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_overage_with_resolver_expands_full_membership()
    {
        using var authority = new EntraTestAuthority();
        var resolver = new FakeEntraGroupResolver("full-1", "full-2", "full-3");
        var authenticator = CreateAuthenticator(authority, EntraGroupResolutionMode.ResolveOnOverage, resolver);
        var token = authority.MintToken(objectId: "bob-oid", roles: new[] { "Admin" }, groupsOverage: true);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(resolver.CallCount, Is.EqualTo(1));
        Assert.That(resolver.LastContext!.SubjectId, Is.EqualTo("bob-oid"));
        Assert.That(resolver.LastContext.TenantId, Is.EqualTo(EntraTestAuthority.TenantId));
        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "full-1", "full-2", "full-3", "Admin" }));
    }

    [Test]
    public async Task AuthenticateAsync_overage_without_resolver_falls_back_to_token_only()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority, EntraGroupResolutionMode.ResolveOnOverage);
        var token = authority.MintToken(objectId: "carol-oid", roles: new[] { "Writer" }, groupsOverage: true);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "Writer" }));
    }

    [Test]
    public async Task AuthenticateAsync_overage_in_token_only_mode_does_not_call_resolver()
    {
        using var authority = new EntraTestAuthority();
        var resolver = new FakeEntraGroupResolver("should-not-be-used");
        var authenticator = CreateAuthenticator(authority, EntraGroupResolutionMode.TokenOnly, resolver);
        var token = authority.MintToken(groups: new[] { "partial" }, groupsOverage: true);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(resolver.CallCount, Is.EqualTo(0));
    }

    [Test]
    public async Task AuthenticateAsync_non_overage_token_does_not_call_resolver()
    {
        using var authority = new EntraTestAuthority();
        var resolver = new FakeEntraGroupResolver("unused");
        var authenticator = CreateAuthenticator(authority, EntraGroupResolutionMode.ResolveOnOverage, resolver);
        var token = authority.MintToken(groups: new[] { "group-a" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(resolver.CallCount, Is.EqualTo(0));
        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "group-a" }));
    }

    [Test]
    public async Task AuthenticateAsync_empty_token_returns_null()
    {
        using var authority = new EntraTestAuthority();
        var authenticator = CreateAuthenticator(authority);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(string.Empty));

        Assert.That(principal, Is.Null);
    }
}
