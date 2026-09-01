using System.Security.Claims;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// Unit tests for <see cref="OidcCredentialAuthenticator"/>. Every test runs
/// entirely in memory: the OIDC discovery document, its signing keys, and its
/// advertised algorithms are supplied by <see cref="OidcTestAuthority"/>, so no
/// network call is ever made.
/// </summary>
public class OidcCredentialAuthenticatorTests
{
    private OidcTestAuthority _authority = null!;

    [SetUp]
    public void SetUp() => _authority = new OidcTestAuthority();

    [TearDown]
    public void TearDown() => _authority.Dispose();

    private static LatticeOidcAuthenticatorOptions CreateOptions(Action<LatticeOidcAuthenticatorOptions>? configure = null)
    {
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = OidcTestAuthority.Authority,
            Issuer = OidcTestAuthority.Issuer,
        };
        options.Audiences.Add(OidcTestAuthority.Audience);
        configure?.Invoke(options);
        return options;
    }

    private OidcCredentialAuthenticator CreateAuthenticator(
        Action<LatticeOidcAuthenticatorOptions>? configure = null,
        string[]? advertisedAlgorithms = null) =>
        new(CreateOptions(configure), _authority.CreateConfigurationSource(advertisedAlgorithms));

    // ---------------------------------------------------------------- happy path

    [Test]
    public async Task AuthenticateAsync_valid_token_returns_principal()
    {
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(_authority.MintToken());

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo(OidcTestAuthority.SubjectId));
        Assert.That(principal.Issuer, Is.EqualTo(OidcTestAuthority.Issuer));
        Assert.That(principal.ExpiresAt, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_records_the_token_issuer_verbatim()
    {
        var authenticator = CreateAuthenticator();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(principal!.Issuer, Is.EqualTo(OidcTestAuthority.Issuer));
    }

    [Test]
    public async Task AuthenticateAsync_copies_remaining_claims_into_the_claim_bag()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(extraClaims: new[] { new Claim("email", "alice@example.com") });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.Claims, Is.Not.Null);
        Assert.That(principal.Claims!["email"], Is.EqualTo("alice@example.com"));
    }

    [Test]
    public async Task AuthenticateAsync_empty_token_returns_null()
    {
        var authenticator = CreateAuthenticator();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(string.Empty));

        Assert.That(principal, Is.Null);
    }

    // ------------------------------------------------------------ issuer / audience

    [Test]
    public async Task AuthenticateAsync_wrong_issuer_returns_null()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_issuer_advertised_by_discovery_but_not_configured_returns_null()
    {
        // The token validator would otherwise also accept whatever issuer the
        // discovery document advertises. The pinned IssuerValidator keeps
        // acceptance to the exact configured value (epic decision D4).
        var options = CreateOptions();
        var source = new OidcTestAuthority.StaticOidcConfigurationSource(
            _authority.SigningKey,
            new[] { SecurityAlgorithms.RsaSha256 },
            OidcTestAuthority.ForeignIssuer);
        var authenticator = new OidcCredentialAuthenticator(options, source);
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_audience_returns_null()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(audience: "api://someone-else");

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_second_configured_audience_is_accepted()
    {
        var authenticator = CreateAuthenticator(o => o.Audiences.Add("api://second"));
        var token = _authority.MintToken(audience: "api://second");

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
    }

    // ------------------------------------------------------------------- lifetime

    [Test]
    public async Task AuthenticateAsync_expired_token_returns_null()
    {
        var authenticator = CreateAuthenticator(o => o.ClockSkew = TimeSpan.Zero);
        var token = _authority.MintToken(
            expires: DateTime.UtcNow.AddMinutes(-10),
            notBefore: DateTime.UtcNow.AddMinutes(-70));

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_expired_token_is_accepted_when_lifetime_validation_is_off()
    {
        var authenticator = CreateAuthenticator(o => o.ValidateLifetime = false);
        var token = _authority.MintToken(
            expires: DateTime.UtcNow.AddMinutes(-10),
            notBefore: DateTime.UtcNow.AddMinutes(-70));

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
    }

    // ------------------------------------------------------------------ algorithms

    [Test]
    public async Task AuthenticateAsync_algorithm_outside_the_discovered_allow_list_returns_null()
    {
        // The discovery document advertises RS256 only; the token advertises RS512
        // in its header. Without pinning this would validate, because the signature
        // itself is genuine.
        var authenticator = CreateAuthenticator(advertisedAlgorithms: new[] { SecurityAlgorithms.RsaSha256 });
        var token = _authority.MintToken(algorithm: SecurityAlgorithms.RsaSha512);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_algorithm_inside_the_discovered_allow_list_is_accepted()
    {
        var authenticator = CreateAuthenticator(
            advertisedAlgorithms: new[] { SecurityAlgorithms.RsaSha256, SecurityAlgorithms.RsaSha512 });
        var token = _authority.MintToken(algorithm: SecurityAlgorithms.RsaSha512);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_explicit_algorithms_override_the_discovery_document()
    {
        // Configuration is narrower than what the provider advertises: RS256 is
        // advertised but not configured, so it must be refused.
        var authenticator = CreateAuthenticator(
            o => o.Algorithms.Add(SecurityAlgorithms.RsaSha512),
            advertisedAlgorithms: new[] { SecurityAlgorithms.RsaSha256, SecurityAlgorithms.RsaSha512 });

        var refused = await authenticator.AuthenticateAsync(
            new LatticeCredential(_authority.MintToken(algorithm: SecurityAlgorithms.RsaSha256)));
        var accepted = await authenticator.AuthenticateAsync(
            new LatticeCredential(_authority.MintToken(algorithm: SecurityAlgorithms.RsaSha512)));

        Assert.That(refused, Is.Null);
        Assert.That(accepted, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_provider_advertising_no_algorithms_rejects_every_token()
    {
        // Fail closed: an empty allow-list is "accept nothing", never "accept
        // anything" (the permissive default the base JWT authenticator inherits).
        var authenticator = CreateAuthenticator(advertisedAlgorithms: Array.Empty<string>());
        var token = _authority.MintToken();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_symmetric_token_signed_with_the_public_key_returns_null()
    {
        // Algorithm-confusion (CWE-347): the forger reads the published JWKS,
        // treats the public key as an HMAC secret, and mints an HS256 token.
        var authenticator = CreateAuthenticator();
        var token = _authority.MintSymmetricConfusionToken();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_symmetric_token_returns_null_even_when_hs256_is_pinned()
    {
        // Even an operator who wrongly pins HS256 must not have a published
        // asymmetric key accepted as a shared secret.
        var authenticator = CreateAuthenticator(
            o => o.Algorithms.Add(SecurityAlgorithms.HmacSha256),
            advertisedAlgorithms: new[] { SecurityAlgorithms.HmacSha256 });
        var token = _authority.MintSymmetricConfusionToken();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_signed_with_an_unpublished_key_returns_null()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(signingKey: _authority.ForeignSigningKey);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    // ---------------------------------------------------------------- claim mapping

    [Test]
    public async Task AuthenticateAsync_missing_subject_returns_null()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(subject: null);

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [TestCase(LatticeSubject.AnonymousSubjectId)]
    [TestCase(LatticeSubject.SystemSubjectId)]
    public async Task AuthenticateAsync_reserved_sentinel_subject_returns_null(string sentinel)
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(subject: sentinel, groups: new[] { "admins" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_maps_groups_and_roles()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(groups: new[] { "engineering" }, roles: new[] { "reader" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "engineering", "reader" }));
    }

    [Test]
    public async Task AuthenticateAsync_maps_the_singular_role_claim()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(extraClaims: new[] { new Claim(OidcClaimNames.Role, "auditor") });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "auditor" }));
    }

    [Test]
    public async Task AuthenticateAsync_honours_a_custom_group_claim_type()
    {
        var authenticator = CreateAuthenticator(o =>
        {
            o.GroupClaimTypes.Clear();
            o.GroupClaimTypes.Add("https://example.com/claims/teams");
        });
        var token = _authority.MintToken(
            groups: new[] { "ignored" },
            extraClaims: new[] { new Claim("https://example.com/claims/teams", "platform") });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.AssertedGroups, Is.EquivalentTo(new[] { "platform" }));
    }

    [Test]
    public async Task AuthenticateAsync_cleared_group_claim_types_asserts_no_groups()
    {
        var authenticator = CreateAuthenticator(o => o.GroupClaimTypes.Clear());
        var token = _authority.MintToken(groups: new[] { "engineering" });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.AssertedGroups, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_honours_a_custom_subject_claim_type()
    {
        var authenticator = CreateAuthenticator(o =>
        {
            o.SubjectClaimTypes.Clear();
            o.SubjectClaimTypes.Add("uid");
            o.SubjectClaimTypes.Add(OidcClaimNames.Subject);
        });
        var token = _authority.MintToken(extraClaims: new[] { new Claim("uid", "employee-42") });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal!.SubjectId, Is.EqualTo("employee-42"));
    }

    [Test]
    public async Task AuthenticateAsync_falls_back_to_the_next_subject_claim_type()
    {
        var authenticator = CreateAuthenticator(o =>
        {
            o.SubjectClaimTypes.Clear();
            o.SubjectClaimTypes.Add("uid");
            o.SubjectClaimTypes.Add(OidcClaimNames.Subject);
        });

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(principal!.SubjectId, Is.EqualTo(OidcTestAuthority.SubjectId));
    }

    // -------------------------------------------------------------------- CanHandle

    [Test]
    public void CanHandle_matching_issuer_returns_true()
    {
        var authenticator = CreateAuthenticator();

        Assert.That(authenticator.CanHandle(new LatticeCredential(_authority.MintToken())), Is.True);
    }

    [Test]
    public void CanHandle_foreign_issuer_returns_false()
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token)), Is.False);
    }

    [TestCase(OidcTestAuthority.Issuer + "/extra")]
    [TestCase("https://idp.example.com")]
    [TestCase("https://idp.example.com/oauth2")]
    public void CanHandle_issuer_that_is_only_a_prefix_or_extension_returns_false(string issuer)
    {
        var authenticator = CreateAuthenticator();
        var token = _authority.MintToken(issuer: issuer);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token)), Is.False);
    }

    [Test]
    public void CanHandle_scheme_hint_returns_true()
    {
        var authenticator = CreateAuthenticator(o => o.SchemeHint = "okta");
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token, "okta")), Is.True);
    }

    [Test]
    public void CanHandle_scheme_hint_match_is_case_insensitive()
    {
        var authenticator = CreateAuthenticator(o => o.SchemeHint = "okta");
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token, "OKTA")), Is.True);
    }

    [Test]
    public void CanHandle_non_matching_scheme_still_selects_on_an_exact_issuer_match()
    {
        var authenticator = CreateAuthenticator(o => o.SchemeHint = "okta");

        Assert.That(authenticator.CanHandle(new LatticeCredential(_authority.MintToken(), "bearer")), Is.True);
    }

    [Test]
    public void CanHandle_non_matching_scheme_and_foreign_issuer_returns_false()
    {
        var authenticator = CreateAuthenticator(o => o.SchemeHint = "okta");
        var token = _authority.MintToken(issuer: OidcTestAuthority.ForeignIssuer);

        Assert.That(authenticator.CanHandle(new LatticeCredential(token, "auth0")), Is.False);
    }

    [Test]
    public void CanHandle_malformed_token_returns_false()
    {
        var authenticator = CreateAuthenticator();

        Assert.That(authenticator.CanHandle(new LatticeCredential("not-a-jwt")), Is.False);
    }

    [Test]
    public void CanHandle_empty_token_returns_false()
    {
        var authenticator = CreateAuthenticator();

        Assert.That(authenticator.CanHandle(new LatticeCredential(string.Empty)), Is.False);
    }

    // ----------------------------------------------------------------- construction

    [Test]
    public void Constructor_null_options_throws()
    {
        Assert.That(
            () => new OidcCredentialAuthenticator(null!, _authority.CreateConfigurationSource()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_configuration_source_throws()
    {
        Assert.That(
            () => new OidcCredentialAuthenticator(CreateOptions(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_missing_issuer_throws()
    {
        var options = CreateOptions(o => o.Issuer = string.Empty);

        Assert.That(
            () => new OidcCredentialAuthenticator(options, _authority.CreateConfigurationSource()),
            Throws.ArgumentException.With.Message.Contains("Issuer"));
    }

    [Test]
    public void Constructor_empty_audiences_throws()
    {
        // Epic decision D5: audience validation is always on, so an empty
        // audience list must fail at construction rather than silently accepting
        // every audience.
        var options = new LatticeOidcAuthenticatorOptions
        {
            Authority = OidcTestAuthority.Authority,
            Issuer = OidcTestAuthority.Issuer,
        };

        Assert.That(
            () => new OidcCredentialAuthenticator(options, _authority.CreateConfigurationSource()),
            Throws.ArgumentException.With.Message.Contains("Audiences"));
    }

    [Test]
    public void Constructor_empty_subject_claim_types_throws()
    {
        var options = CreateOptions(o => o.SubjectClaimTypes.Clear());

        Assert.That(
            () => new OidcCredentialAuthenticator(options, _authority.CreateConfigurationSource()),
            Throws.ArgumentException.With.Message.Contains("SubjectClaimTypes"));
    }

    [Test]
    public void Constructor_without_authority_or_metadata_address_throws()
    {
        var options = CreateOptions(o => o.Authority = string.Empty);

        Assert.That(
            () => new OidcCredentialAuthenticator(options, _authority.CreateConfigurationSource()),
            Throws.ArgumentException.With.Message.Contains("MetadataAddress"));
    }

    [Test]
    public void Constructor_public_overload_builds_a_live_discovery_backed_authenticator()
    {
        // Constructing never touches the network - the configuration manager only
        // fetches on first validation, which this test does not perform.
        var authenticator = new OidcCredentialAuthenticator(CreateOptions());

        Assert.That(authenticator.CanHandle(new LatticeCredential(_authority.MintToken())), Is.True);
    }

    [Test]
    public void Constructor_public_overload_null_options_throws()
    {
        Assert.That(() => new OidcCredentialAuthenticator(null!), Throws.ArgumentNullException);
    }

    // -------------------------------------------------- validation-parameter surface

    [Test]
    public async Task ResolveValidationParametersAsync_pins_issuer_audience_and_discovered_algorithms()
    {
        var source = _authority.CreateConfigurationSource();
        var probe = new ProbeAuthenticator(CreateOptions(), source);

        var parameters = await probe.ResolveAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(parameters.ValidateAudience, Is.True);
        Assert.That(parameters.ValidAudiences, Is.EquivalentTo(new[] { OidcTestAuthority.Audience }));
        Assert.That(parameters.ValidateIssuer, Is.True);
        Assert.That(parameters.ValidIssuer, Is.EqualTo(OidcTestAuthority.Issuer));
        Assert.That(parameters.IssuerValidator, Is.Not.Null);
        Assert.That(parameters.ValidateIssuerSigningKey, Is.True);
        Assert.That(parameters.ConfigurationManager, Is.SameAs(source.GetOrCreate(OidcTestAuthority.MetadataAddress)));
        Assert.That(parameters.ValidAlgorithms, Is.EquivalentTo(new[] { SecurityAlgorithms.RsaSha256 }));
        Assert.That(parameters.AlgorithmValidator, Is.Null);
    }

    [Test]
    public async Task ResolveValidationParametersAsync_carries_the_configured_lifetime_settings()
    {
        var probe = new ProbeAuthenticator(
            CreateOptions(o =>
            {
                o.ValidateLifetime = false;
                o.ClockSkew = TimeSpan.FromSeconds(37);
            }),
            _authority.CreateConfigurationSource());

        var parameters = await probe.ResolveAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(parameters.ValidateLifetime, Is.False);
        Assert.That(parameters.ClockSkew, Is.EqualTo(TimeSpan.FromSeconds(37)));
    }

    [Test]
    public async Task AuthenticateAsync_clock_skew_admits_a_token_that_expired_within_the_skew()
    {
        var authenticator = CreateAuthenticator(o => o.ClockSkew = TimeSpan.FromMinutes(10));
        var token = _authority.MintToken(
            expires: DateTime.UtcNow.AddMinutes(-2),
            notBefore: DateTime.UtcNow.AddMinutes(-62));

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(token));

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo(OidcTestAuthority.SubjectId));
    }

    [Test]
    public async Task ResolveValidationParametersAsync_installs_a_deny_all_validator_when_nothing_is_advertised()
    {
        var probe = new ProbeAuthenticator(
            CreateOptions(),
            _authority.CreateConfigurationSource(Array.Empty<string>()));

        var parameters = await probe.ResolveAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(parameters.ValidAlgorithms, Is.Null.Or.Empty);
        Assert.That(parameters.AlgorithmValidator, Is.Not.Null);
        Assert.That(
            parameters.AlgorithmValidator!(SecurityAlgorithms.RsaSha256, _authority.SigningKey, null!, parameters),
            Is.False);
    }

    [Test]
    public async Task ResolveValidationParametersAsync_reuses_the_discovered_algorithm_projection()
    {
        var probe = new ProbeAuthenticator(CreateOptions(), _authority.CreateConfigurationSource());
        var credential = new LatticeCredential(_authority.MintToken());

        var first = await probe.ResolveAsync(credential);
        var second = await probe.ResolveAsync(credential);

        Assert.That(second.ValidAlgorithms, Is.SameAs(first.ValidAlgorithms));
    }

    [Test]
    public async Task ResolveValidationParametersAsync_resolves_the_configured_metadata_address()
    {
        var source = _authority.CreateConfigurationSource();
        var probe = new ProbeAuthenticator(
            CreateOptions(o => o.MetadataAddress = "https://idp.example.com/custom-metadata"),
            source);

        await probe.ResolveAsync(new LatticeCredential(_authority.MintToken()));

        Assert.That(source.LastRequestedAddress, Is.EqualTo("https://idp.example.com/custom-metadata"));
    }

    [Test]
    public void MapPrincipal_null_token_throws()
    {
        var probe = new ProbeAuthenticator(CreateOptions(), _authority.CreateConfigurationSource());

        Assert.That(() => probe.Map(null!, new ClaimsIdentity()), Throws.ArgumentNullException);
    }

    [Test]
    public void MapPrincipal_null_identity_throws()
    {
        var probe = new ProbeAuthenticator(CreateOptions(), _authority.CreateConfigurationSource());
        var token = new JsonWebToken(_authority.MintToken());

        Assert.That(() => probe.Map(token, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void MapPrincipal_maps_the_token_expiry_onto_the_principal()
    {
        var probe = new ProbeAuthenticator(CreateOptions(), _authority.CreateConfigurationSource());
        var token = new JsonWebToken(_authority.MintToken());
        var identity = new ClaimsIdentity(new[] { new Claim(OidcClaimNames.Subject, "alice") });

        // A JsonWebToken built directly from the wire carries its own ValidTo, so
        // the principal must surface it; the assertion pins the mapping shape.
        var principal = probe.Map(token, identity);

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo("alice"));
        Assert.That(principal.ExpiresAt, Is.Not.Null);
    }

    /// <summary>Exposes the protected extension points so they can be asserted directly.</summary>
    private sealed class ProbeAuthenticator(
        LatticeOidcAuthenticatorOptions options,
        IOidcConfigurationSource configurationSource)
        : OidcCredentialAuthenticator(options, configurationSource)
    {
        public ValueTask<TokenValidationParameters> ResolveAsync(LatticeCredential credential) =>
            ResolveValidationParametersAsync(credential, CancellationToken.None);

        public LatticePrincipal? Map(JsonWebToken token, ClaimsIdentity identity) => MapPrincipal(token, identity);
    }
}
