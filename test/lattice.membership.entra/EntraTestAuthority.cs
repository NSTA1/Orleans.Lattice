using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Entra.Tests;

/// <summary>
/// An in-test Entra authority: it owns a self-signed RSA signing key, mints
/// Entra-shaped v2.0 JWTs, and exposes a network-free
/// <see cref="IEntraOpenIdConfigurationSource"/> that publishes the matching
/// public signing key. No live identity provider or network is involved.
/// </summary>
internal sealed class EntraTestAuthority : IDisposable
{
    private readonly RSA _rsa = RSA.Create(2048);
    private readonly RsaSecurityKey _signingKey;

    public EntraTestAuthority()
    {
        _signingKey = new RsaSecurityKey(_rsa) { KeyId = "test-key-1" };
    }

    /// <summary>The default tenant id minted tokens are issued for.</summary>
    public const string TenantId = "11111111-1111-1111-1111-111111111111";

    /// <summary>The default audience minted tokens carry.</summary>
    public const string Audience = "api://lattice-entra-test";

    /// <summary>The default metadata address the authenticator resolves.</summary>
    public const string MetadataAddress = "https://login.microsoftonline.com/common/v2.0/.well-known/openid-configuration";

    /// <summary>Builds the Entra v2.0 issuer for a tenant id.</summary>
    public static string IssuerFor(string tenantId) => $"https://login.microsoftonline.com/{tenantId}/v2.0";

    /// <summary>
    /// Mints an Entra-shaped v2.0 token signed with this authority's key.
    /// </summary>
    public string MintToken(
        string tenantId = TenantId,
        string objectId = "00000000-0000-0000-0000-000000000abc",
        IEnumerable<string>? groups = null,
        IEnumerable<string>? roles = null,
        string audience = Audience,
        string? issuer = null,
        DateTime? expires = null,
        bool groupsOverage = false,
        RsaSecurityKey? signingKey = null)
    {
        var claims = new List<Claim>
        {
            new(EntraClaimNames.ObjectId, objectId),
            new(EntraClaimNames.TenantId, tenantId),
        };

        if (groups is not null)
        {
            foreach (var group in groups)
            {
                claims.Add(new Claim(EntraClaimNames.Groups, group));
            }
        }

        if (roles is not null)
        {
            foreach (var role in roles)
            {
                claims.Add(new Claim(EntraClaimNames.Roles, role));
            }
        }

        if (groupsOverage)
        {
            claims.Add(new Claim(EntraClaimNames.ClaimNames, "{\"groups\":\"src1\"}", JsonClaimValueTypes.Json));
            claims.Add(new Claim(EntraClaimNames.ClaimSources, "{\"src1\":{\"endpoint\":\"https://graph.microsoft.com\"}}", JsonClaimValueTypes.Json));
        }

        var descriptor = new SecurityTokenDescriptor
        {
            Issuer = issuer ?? IssuerFor(tenantId),
            Audience = audience,
            Subject = new ClaimsIdentity(claims),
            Expires = expires ?? DateTime.UtcNow.AddHours(1),
            SigningCredentials = new SigningCredentials(signingKey ?? _signingKey, SecurityAlgorithms.RsaSha256),
        };

        return new JsonWebTokenHandler().CreateToken(descriptor);
    }

    /// <summary>A network-free configuration source publishing this authority's public signing key.</summary>
    public IEntraOpenIdConfigurationSource ConfigurationSource => new StaticSource(_signingKey);

    public void Dispose() => _rsa.Dispose();

    private sealed class StaticSource(RsaSecurityKey signingKey) : IEntraOpenIdConfigurationSource
    {
        public BaseConfigurationManager GetOrCreate(string metadataAddress)
        {
            var configuration = new OpenIdConnectConfiguration { Issuer = metadataAddress };
            configuration.SigningKeys.Add(signingKey);
            return new StaticConfigurationManager<OpenIdConnectConfiguration>(configuration);
        }
    }
}
