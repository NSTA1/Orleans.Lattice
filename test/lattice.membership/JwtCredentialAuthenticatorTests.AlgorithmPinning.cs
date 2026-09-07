using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Regression tests for the signature-algorithm allow-list
/// <see cref="JwtCredentialAuthenticator"/> derives when
/// <see cref="JwtAuthenticatorOptions.Algorithms"/> is left empty.
/// </summary>
/// <remarks>
/// <para>
/// An empty <see cref="TokenValidationParameters.ValidAlgorithms"/> is treated by
/// the token validator as "no restriction", which leaves the algorithm-confusion
/// gap (CWE-347) open: a token whose header advertises a symmetric <c>alg</c> is
/// accepted if any symmetric key sits in the trusted key set, even when the
/// deployment only ever intended asymmetric verification. Deny-all is not an
/// option because empty is the documented default, so the authenticator instead
/// narrows to the key families the host actually pinned.
/// </para>
/// <para>
/// The tests below pin all three arms of that derivation: an asymmetric-only key
/// set refuses HMAC, a symmetric-only key set still works (the shape every other
/// fixture in this project uses), and a mixed key set deliberately keeps the
/// historical permissive behaviour rather than silently breaking a host.
/// </para>
/// </remarks>
public partial class JwtCredentialAuthenticatorTests
{
    private static SymmetricSecurityKey NewSymmetricKey()
        => new(Encoding.UTF8.GetBytes("membership-algorithm-pinning-key-0123456789"));

    private static RsaSecurityKey NewRsaKey() => new(RSA.Create(2048));

    private static JwtCredentialAuthenticator CreatePinningAuthenticator(
        IEnumerable<SecurityKey> keys,
        IEnumerable<string>? algorithms = null)
    {
        var options = new JwtAuthenticatorOptions { Issuer = Issuer };
        options.Audiences.Add(Audience);
        foreach (var key in keys)
        {
            options.SigningKeys.Add(key);
        }

        if (algorithms is not null)
        {
            foreach (var algorithm in algorithms)
            {
                options.Algorithms.Add(algorithm);
            }
        }

        return new JwtCredentialAuthenticator(options);
    }

    private static string MintTokenWithAlgorithm(SecurityKey key, string algorithm, string subject = "user-1")
    {
        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = Issuer,
            Audience = Audience,
            Subject = new ClaimsIdentity([new Claim("sub", subject)]),
            Expires = DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(key, algorithm),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    /// <summary>
    /// The core of the finding: an RSA-only deployment that never pinned
    /// <see cref="JwtAuthenticatorOptions.Algorithms"/> must not verify an HMAC
    /// token. The symmetric key that signed the token is deliberately NOT in the
    /// trusted set, so acceptance could only come from an unrestricted algorithm
    /// set combined with a symmetric key reaching the validator.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_rsa_only_key_set_rejects_an_hmac_token()
    {
        var authenticator = CreatePinningAuthenticator([NewRsaKey()]);
        var token = MintTokenWithAlgorithm(NewSymmetricKey(), SecurityAlgorithms.HmacSha256);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.That(result, Is.Null);
    }

    /// <summary>
    /// The narrowing must not lock out the family the host actually configured -
    /// otherwise it would be a denial of service rather than a tightening.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_rsa_only_key_set_accepts_an_rsa_token()
    {
        var rsa = NewRsaKey();
        var authenticator = CreatePinningAuthenticator([rsa]);
        var token = MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha256);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.SubjectId, Is.EqualTo("user-1"));
        });
    }

    /// <summary>
    /// A symmetric-only key set is the shape every other membership fixture uses,
    /// so the derivation must leave it working untouched.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_symmetric_only_key_set_accepts_an_hmac_token()
    {
        var key = NewSymmetricKey();
        var authenticator = CreatePinningAuthenticator([key]);
        var token = MintTokenWithAlgorithm(key, SecurityAlgorithms.HmacSha256);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.That(result, Is.Not.Null);
    }

    /// <summary>
    /// A mixed key set legitimately uses both families, so the derivation cannot
    /// narrow it safely and deliberately keeps the historical permissive
    /// behaviour. This pins that decision explicitly: such a host must pin
    /// algorithms itself, and the library must not break it on upgrade.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_mixed_key_set_keeps_the_permissive_behaviour()
    {
        var symmetric = NewSymmetricKey();
        var authenticator = CreatePinningAuthenticator([NewRsaKey(), symmetric]);
        var token = MintTokenWithAlgorithm(symmetric, SecurityAlgorithms.HmacSha256);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.That(result, Is.Not.Null);
    }

    /// <summary>
    /// An explicit pin is authoritative: the derivation must never widen it back
    /// out to the whole family.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_explicit_pin_is_not_widened_by_the_derivation()
    {
        var rsa = NewRsaKey();
        var authenticator = CreatePinningAuthenticator([rsa], [SecurityAlgorithms.RsaSha512]);
        var token = MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha256);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.That(result, Is.Null);
    }

    /// <summary>
    /// An explicit pin that matches still authenticates, proving the previous test
    /// failed on the algorithm rather than on the key or the token shape.
    /// </summary>
    [Test]
    public async Task AuthenticateAsync_explicit_pin_accepts_the_pinned_algorithm()
    {
        var rsa = NewRsaKey();
        var authenticator = CreatePinningAuthenticator([rsa], [SecurityAlgorithms.RsaSha512]);
        var token = MintTokenWithAlgorithm(rsa, SecurityAlgorithms.RsaSha512);

        var result = await authenticator.AuthenticateAsync(new LatticeCredential(token), CancellationToken.None);

        Assert.That(result, Is.Not.Null);
    }
}
