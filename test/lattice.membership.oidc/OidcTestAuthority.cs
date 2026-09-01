using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Oidc.Tests;

/// <summary>
/// An in-test OpenID Connect authority: it owns a self-signed RSA signing key,
/// mints standard OIDC-shaped JWTs, and exposes a network-free
/// <see cref="IOidcConfigurationSource"/> that publishes the matching public
/// signing key and advertised signing algorithms. No live identity provider or
/// network is involved.
/// </summary>
internal sealed class OidcTestAuthority : IDisposable
{
    private readonly RSA _rsa = RSA.Create(2048);
    private readonly RSA _foreignRsa = RSA.Create(2048);

    /// <summary>The issuer minted tokens carry and the authenticator is configured with.</summary>
    public const string Issuer = "https://idp.example.com/oauth2/default";

    /// <summary>An unrelated issuer, used to prove selection and validation never widen.</summary>
    public const string ForeignIssuer = "https://other-idp.example.com/oauth2/default";

    /// <summary>The default audience minted tokens carry.</summary>
    public const string Audience = "api://lattice-oidc-test";

    /// <summary>The authority the metadata address is derived from.</summary>
    public const string Authority = Issuer;

    /// <summary>The discovery document address the authenticator resolves from <see cref="Authority"/>.</summary>
    public const string MetadataAddress = Issuer + "/.well-known/openid-configuration";

    /// <summary>The default subject minted tokens carry.</summary>
    public const string SubjectId = "00u1a2b3c4d5e6f7g8h9";

    /// <summary>Initializes a new authority with a fresh RSA key pair.</summary>
    public OidcTestAuthority()
    {
        SigningKey = new RsaSecurityKey(_rsa) { KeyId = "oidc-test-key-1" };
        ForeignSigningKey = new RsaSecurityKey(_foreignRsa) { KeyId = "oidc-test-key-2" };
    }

    /// <summary>The key this authority signs with, and publishes through its discovery document.</summary>
    public RsaSecurityKey SigningKey { get; }

    /// <summary>A key this authority never publishes, used to prove an unknown signer is rejected.</summary>
    public RsaSecurityKey ForeignSigningKey { get; }

    /// <summary>
    /// The raw public-key material of <see cref="SigningKey"/>. An attacker who
    /// reads the published JWKS has exactly this, which is what makes the
    /// symmetric-key-confusion attack possible against a validator that does not
    /// pin its algorithms.
    /// </summary>
    public byte[] PublicKeyMaterial => _rsa.ExportRSAPublicKey();

    /// <summary>
    /// Mints a standard OIDC-shaped token signed with this authority's key.
    /// </summary>
    /// <param name="subject">The <c>sub</c> claim value; <c>null</c> to omit the claim entirely.</param>
    /// <param name="groups">Values emitted as <c>groups</c> claims.</param>
    /// <param name="roles">Values emitted as <c>roles</c> claims.</param>
    /// <param name="extraClaims">Additional claims emitted verbatim.</param>
    /// <param name="audience">The <c>aud</c> claim value.</param>
    /// <param name="issuer">The <c>iss</c> claim value.</param>
    /// <param name="expires">The token expiry.</param>
    /// <param name="algorithm">The signature algorithm advertised in the header.</param>
    /// <param name="signingKey">The key to sign with; defaults to this authority's key.</param>
    public string MintToken(
        string? subject = SubjectId,
        IEnumerable<string>? groups = null,
        IEnumerable<string>? roles = null,
        IEnumerable<Claim>? extraClaims = null,
        string audience = Audience,
        string issuer = Issuer,
        DateTime? expires = null,
        DateTime? notBefore = null,
        string algorithm = SecurityAlgorithms.RsaSha256,
        SecurityKey? signingKey = null)
    {
        var claims = new List<Claim>();
        if (subject is not null)
        {
            claims.Add(new Claim(OidcClaimNames.Subject, subject));
        }

        if (groups is not null)
        {
            foreach (var group in groups)
            {
                claims.Add(new Claim(OidcClaimNames.Groups, group));
            }
        }

        if (roles is not null)
        {
            foreach (var role in roles)
            {
                claims.Add(new Claim(OidcClaimNames.Roles, role));
            }
        }

        if (extraClaims is not null)
        {
            claims.AddRange(extraClaims);
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = audience,
            Subject = new ClaimsIdentity(claims),
            Expires = expires ?? DateTime.UtcNow.AddHours(1),
            // Left unset the handler stamps nbf/iat at "now", which makes any
            // already-expired token fail the nbf-must-precede-exp sanity check
            // before the clock skew is ever consulted. A test that wants to
            // exercise expiry (or the skew that forgives it) has to backdate this.
            NotBefore = notBefore,
            IssuedAt = notBefore,
            SigningCredentials = new SigningCredentials(signingKey ?? SigningKey, algorithm),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    /// <summary>
    /// Mints an <c>HS256</c> token whose HMAC key is this authority's <b>public</b>
    /// key material - the classic algorithm-confusion attack. A validator that
    /// does not pin its algorithms would treat the published public key as a
    /// shared secret and accept the forgery.
    /// </summary>
    public string MintSymmetricConfusionToken(string subject = SubjectId, string issuer = Issuer, string audience = Audience)
    {
        var symmetric = new SymmetricSecurityKey(PublicKeyMaterial) { KeyId = SigningKey.KeyId };
        return MintToken(
            subject: subject,
            issuer: issuer,
            audience: audience,
            algorithm: SecurityAlgorithms.HmacSha256,
            signingKey: symmetric);
    }

    /// <summary>
    /// Builds a network-free configuration source publishing this authority's
    /// public signing key and the supplied advertised signing algorithms.
    /// </summary>
    /// <param name="advertisedAlgorithms">
    /// The values published as <c>id_token_signing_alg_values_supported</c>;
    /// <c>null</c> (the default) advertises <c>RS256</c>, and an empty array
    /// advertises none.
    /// </param>
    public StaticOidcConfigurationSource CreateConfigurationSource(string[]? advertisedAlgorithms = null) =>
        new(SigningKey, advertisedAlgorithms ?? new[] { SecurityAlgorithms.RsaSha256 }, Issuer);

    /// <summary>Releases the RSA key material this authority owns.</summary>
    public void Dispose()
    {
        _rsa.Dispose();
        _foreignRsa.Dispose();
    }

    /// <summary>
    /// A network-free <see cref="IOidcConfigurationSource"/> serving one fixed
    /// in-memory discovery document. It caches per metadata address the way the
    /// production source does, so a test can assert the configuration instance is
    /// stable across authentications.
    /// </summary>
    internal sealed class StaticOidcConfigurationSource : IOidcConfigurationSource
    {
        private readonly Dictionary<string, BaseConfigurationManager> _managers = new(StringComparer.Ordinal);
        private readonly RsaSecurityKey _signingKey;
        private readonly string[] _advertisedAlgorithms;
        private readonly string _discoveryIssuer;

        public StaticOidcConfigurationSource(
            RsaSecurityKey signingKey,
            string[] advertisedAlgorithms,
            string discoveryIssuer)
        {
            _signingKey = signingKey;
            _advertisedAlgorithms = advertisedAlgorithms;
            _discoveryIssuer = discoveryIssuer;
        }

        /// <summary>The number of times <see cref="GetOrCreate"/> was called.</summary>
        public int GetOrCreateCallCount { get; private set; }

        /// <summary>The metadata address most recently requested.</summary>
        public string? LastRequestedAddress { get; private set; }

        /// <inheritdoc />
        public BaseConfigurationManager GetOrCreate(string metadataAddress)
        {
            GetOrCreateCallCount++;
            LastRequestedAddress = metadataAddress;

            if (_managers.TryGetValue(metadataAddress, out var existing))
            {
                return existing;
            }

            var configuration = new OpenIdConnectConfiguration { Issuer = _discoveryIssuer };
            configuration.SigningKeys.Add(_signingKey);
            foreach (var algorithm in _advertisedAlgorithms)
            {
                configuration.IdTokenSigningAlgValuesSupported.Add(algorithm);
            }

            var manager = new StaticConfigurationManager<OpenIdConnectConfiguration>(configuration);
            _managers[metadataAddress] = manager;
            return manager;
        }
    }
}
