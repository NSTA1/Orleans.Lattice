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
}
