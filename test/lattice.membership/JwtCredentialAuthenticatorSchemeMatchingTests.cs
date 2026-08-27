using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Covers the scheme-matching branches of
/// <see cref="JwtCredentialAuthenticator.CanHandle"/> that the main fixture's
/// hint-and-token cases leave untaken: an explicit scheme that equals the
/// configured issuer (the "scheme carries the issuer" convention used by the
/// facade credential bridges), a non-matching explicit scheme (which must be
/// refused outright rather than falling back to parsing the token), and an empty
/// token on the no-scheme path.
/// </summary>
[TestFixture]
public sealed class JwtCredentialAuthenticatorSchemeMatchingTests
{
    private const string Issuer = "https://issuer.example/";
    private const string Audience = "lattice-audience";

    private static readonly SymmetricSecurityKey SigningKey =
        new(Encoding.UTF8.GetBytes("membership-unit-test-signing-key-0123456789"));

    private static JwtCredentialAuthenticator Authenticator(string? schemeHint = null)
    {
        var options = new JwtAuthenticatorOptions { Issuer = Issuer, SchemeHint = schemeHint };
        options.Audiences.Add(Audience);
        options.SigningKeys.Add(SigningKey);
        return new JwtCredentialAuthenticator(options);
    }

    [Test]
    public void CanHandle_accepts_a_scheme_that_names_the_configured_issuer()
    {
        // No scheme hint configured, but the credential's scheme carries the
        // issuer id directly.
        var credential = new LatticeCredential("any-token", Issuer);

        Assert.That(Authenticator().CanHandle(credential), Is.True);
    }

    [Test]
    public void CanHandle_accepts_the_scheme_hint_case_insensitively()
    {
        var credential = new LatticeCredential("any-token", "bearer");

        Assert.That(Authenticator(schemeHint: "Bearer").CanHandle(credential), Is.True);
    }

    [Test]
    public void CanHandle_refuses_a_non_matching_explicit_scheme_without_parsing_the_token()
    {
        // The scheme is present but matches neither the hint nor the issuer, so
        // the authenticator must decline rather than fall through to token
        // parsing - otherwise a token for this issuer presented under another
        // scheme would be silently adopted.
        var credential = new LatticeCredential("any-token", "some-other-scheme");

        Assert.That(Authenticator(schemeHint: "Bearer").CanHandle(credential), Is.False);
    }

    [Test]
    public void CanHandle_issuer_match_is_ordinal_so_a_case_variant_scheme_is_refused()
    {
        var credential = new LatticeCredential("any-token", Issuer.ToUpperInvariant());

        Assert.That(Authenticator().CanHandle(credential), Is.False,
            "issuer comparison is ordinal; a case variant is a different issuer");
    }

    [Test]
    public void CanHandle_refuses_an_empty_token_on_the_no_scheme_path()
    {
        Assert.That(Authenticator().CanHandle(new LatticeCredential(string.Empty, null)), Is.False);
    }

    [Test]
    public void A_credential_cannot_be_built_without_a_token()
    {
        Assert.That(() => new LatticeCredential(null!, null), Throws.ArgumentNullException);
    }
}
