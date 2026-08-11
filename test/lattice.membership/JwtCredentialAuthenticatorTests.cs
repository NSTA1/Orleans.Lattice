using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="JwtCredentialAuthenticator"/>. Tokens are minted
/// in-test with a self-signed symmetric key: no live identity provider and no
/// network are involved.
/// </summary>
public class JwtCredentialAuthenticatorTests
{
    private const string Issuer = "https://issuer.example/";
    private const string Audience = "lattice-audience";
    private static readonly SymmetricSecurityKey SigningKey =
        new(Encoding.UTF8.GetBytes("membership-unit-test-signing-key-0123456789"));

    private static JwtCredentialAuthenticator CreateAuthenticator(
        string issuer = Issuer,
        string? schemeHint = null,
        SecurityKey? key = null,
        TimeSpan? clockSkew = null)
    {
        var options = new JwtAuthenticatorOptions { Issuer = issuer, SchemeHint = schemeHint };
        options.Audiences.Add(Audience);
        options.SigningKeys.Add(key ?? SigningKey);
        if (clockSkew is { } skew)
        {
            options.ClockSkew = skew;
        }

        return new JwtCredentialAuthenticator(options);
    }

    private static string MintToken(
        string issuer = Issuer,
        string subject = "user-1",
        IEnumerable<string>? groups = null,
        SecurityKey? key = null,
        DateTime? expires = null)
    {
        var claims = new List<Claim> { new("sub", subject) };
        if (groups is not null)
        {
            foreach (var group in groups)
            {
                claims.Add(new Claim("groups", group));
            }
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = Audience,
            Subject = new ClaimsIdentity(claims),
            Expires = expires ?? DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(key ?? SigningKey, SecurityAlgorithms.HmacSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    // Mints an unsigned token (the "alg":"none" downgrade attack): a well-formed
    // JWT with a header and payload but no signature part.
    private static string MintUnsignedToken(string issuer = Issuer, string subject = "user-1")
    {
        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = Audience,
            Subject = new ClaimsIdentity(new[] { new Claim("sub", subject) }),
            Expires = DateTime.UtcNow.AddHours(1),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    // Mints a validly-signed token but for an untrusted audience.
    private static string MintTokenForAudience(string audience, string subject = "user-1")
    {
        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Audience = audience,
            Subject = new ClaimsIdentity(new[] { new Claim("sub", subject) }),
            Expires = DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    // Mints a validly-signed token that carries group claims but no subject claim
    // at all: the case that must resolve to the anonymous subject (no groups)
    // rather than an anonymous-labelled principal that still carries the groups.
    private static string MintTokenWithoutSubject(IEnumerable<string> groups)
    {
        var claims = new List<Claim>();
        foreach (var group in groups)
        {
            claims.Add(new Claim("groups", group));
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Audience = Audience,
            Subject = new ClaimsIdentity(claims),
            Expires = DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    [Test]
    public void Constructor_null_options_throws()
    {
        Assert.That(() => new JwtCredentialAuthenticator(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_missing_issuer_throws()
    {
        Assert.That(() => new JwtCredentialAuthenticator(new JwtAuthenticatorOptions()),
            Throws.ArgumentException);
    }

    [Test]
    public void CanHandle_matching_scheme_hint_returns_true()
    {
        var authenticator = CreateAuthenticator(schemeHint: "Bearer");
        var credential = new LatticeCredential("opaque", scheme: "Bearer");

        Assert.That(authenticator.CanHandle(credential), Is.True);
    }

    [Test]
    public void CanHandle_matching_issuer_parsed_from_token_returns_true()
    {
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintToken());

        Assert.That(authenticator.CanHandle(credential), Is.True);
    }

    [Test]
    public void CanHandle_token_for_different_issuer_returns_false()
    {
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintToken(issuer: "https://other.example/"));

        Assert.That(authenticator.CanHandle(credential), Is.False);
    }

    [Test]
    public void CanHandle_malformed_token_returns_false()
    {
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential("not-a-jwt");

        Assert.That(authenticator.CanHandle(credential), Is.False);
    }

    [Test]
    public async Task AuthenticateAsync_valid_token_resolves_subject_groups_and_expiry()
    {
        var authenticator = CreateAuthenticator();
        var expires = DateTime.UtcNow.AddMinutes(30);
        var credential = new LatticeCredential(
            MintToken(subject: "alice", groups: new[] { "admins", "readers" }, expires: expires));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Not.Null);
        Assert.That(principal!.SubjectId, Is.EqualTo("alice"));
        Assert.That(principal.Issuer, Is.EqualTo(Issuer));
        Assert.That(principal.AssertedGroups, Is.EquivalentTo(new[] { "admins", "readers" }));
        Assert.That(principal.ExpiresAt, Is.Not.Null);
        Assert.That(principal.ExpiresAt!.Value.UtcDateTime, Is.EqualTo(expires).Within(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public async Task AuthenticateAsync_expired_token_returns_null()
    {
        var authenticator = CreateAuthenticator(clockSkew: TimeSpan.Zero);
        var credential = new LatticeCredential(
            MintToken(expires: DateTime.UtcNow.AddMinutes(-5)));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_signing_key_returns_null()
    {
        var authenticator = CreateAuthenticator();
        var otherKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("a-totally-different-signing-key-9876543210"));
        var credential = new LatticeCredential(MintToken(key: otherKey));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_empty_token_returns_null()
    {
        var authenticator = CreateAuthenticator();

        var principal = await authenticator.AuthenticateAsync(new LatticeCredential(string.Empty));

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_unsigned_alg_none_token_returns_null()
    {
        // A silo that accepted an "alg":"none" token would let any client forge an
        // identity with no key at all. The authenticator requires a trusted
        // signature, so the unsigned token must resolve to no principal.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintUnsignedToken(subject: "attacker"));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_audience_token_returns_null()
    {
        // A token minted for a different relying party must not authenticate here,
        // even though it is validly signed by the trusted key.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintTokenForAudience("some-other-service"));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_signed_with_unpinned_algorithm_returns_null()
    {
        // Pinning the accepted algorithm set to RS256 must reject an otherwise
        // valid HS256-signed token: the header advertises an algorithm outside
        // the allow-list (CWE-347 algorithm confusion).
        var options = new JwtAuthenticatorOptions { Issuer = Issuer };
        options.Audiences.Add(Audience);
        options.SigningKeys.Add(SigningKey);
        options.Algorithms.Add(SecurityAlgorithms.RsaSha256);
        var authenticator = new JwtCredentialAuthenticator(options);
        var credential = new LatticeCredential(MintToken());

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_signed_with_pinned_algorithm_authenticates()
    {
        // A token whose algorithm is on the pinned allow-list still authenticates.
        var options = new JwtAuthenticatorOptions { Issuer = Issuer };
        options.Audiences.Add(Audience);
        options.SigningKeys.Add(SigningKey);
        options.Algorithms.Add(SecurityAlgorithms.HmacSha256);
        var authenticator = new JwtCredentialAuthenticator(options);
        var credential = new LatticeCredential(MintToken());

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Not.Null);
    }

    [Test]
    public async Task AuthenticateAsync_wrong_issuer_token_returns_null()
    {
        // A validly-signed token from an untrusted issuer must be rejected: the
        // configured issuer is the anchor of trust for the whole token.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintToken(issuer: "https://evil.example/"));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_with_no_subject_but_groups_returns_null()
    {
        // A validly-signed token that asserts group claims but no subject must NOT
        // resolve to an "anonymous"-labelled principal carrying those groups: that
        // would grant it access through any group Allow rule. It resolves to the
        // anonymous subject (a null principal, no groups) instead.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(MintTokenWithoutSubject(new[] { "admins" }));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_with_reserved_anonymous_subject_returns_null()
    {
        // A subject id that collides with the reserved anonymous sentinel must not
        // resolve to an authorized principal - it would let a token impersonate the
        // well-known anonymous identity while carrying real groups.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(
            MintToken(subject: LatticeSubject.AnonymousSubjectId, groups: new[] { "admins" }));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }

    [Test]
    public async Task AuthenticateAsync_token_with_reserved_system_subject_returns_null()
    {
        // The system sentinel is the most dangerous collision: a validly-signed
        // token must never resolve to the well-known system subject.
        var authenticator = CreateAuthenticator();
        var credential = new LatticeCredential(
            MintToken(subject: LatticeSubject.SystemSubjectId, groups: new[] { "admins" }));

        var principal = await authenticator.AuthenticateAsync(credential);

        Assert.That(principal, Is.Null);
    }
}
