using System.Security.Claims;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// A Microsoft Entra ID (Azure AD) credential authenticator that specializes the
/// built-in <see cref="JwtCredentialAuthenticator"/>. It reuses the base issuer,
/// audience, signing-key, and lifetime validation and overrides only the
/// Entra-specific extension points:
/// <list type="bullet">
/// <item><description><see cref="ResolveValidationParametersAsync"/> - OIDC/JWKS discovery and signing-key rotation from the tenant authority, plus templated single- and multi-tenant issuer validation.</description></item>
/// <item><description><see cref="MapPrincipal"/> - Entra v2.0 claim conventions (<c>oid</c> subject, <c>tid</c> tenant, <c>groups</c>, app <c>roles</c>).</description></item>
/// <item><description><see cref="CanHandle"/> - selection restricted to the configured tenant allow-list.</description></item>
/// </list>
/// It also resolves the Entra groups-overage case out of band through an
/// optional <see cref="IEntraGroupResolver"/>.
/// </summary>
public class EntraCredentialAuthenticator : JwtCredentialAuthenticator
{
    private readonly LatticeEntraAuthenticatorOptions _entraOptions;
    private readonly IEntraOpenIdConfigurationSource _configurationSource;
    private readonly IEntraGroupResolver? _groupResolver;
    private readonly HashSet<string> _allowedTenants;
    private readonly string _metadataAddress;

    /// <summary>
    /// Initializes a new <see cref="EntraCredentialAuthenticator"/> that discovers
    /// OIDC metadata from the live Entra authority.
    /// </summary>
    /// <param name="options">The Entra authenticator configuration. Must not be <c>null</c>.</param>
    /// <param name="groupResolver">Optional resolver used for the groups-overage case; <c>null</c> to use the token-only fallback.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="options"/> has no authority, tenant, or audience configured.</exception>
    public EntraCredentialAuthenticator(
        LatticeEntraAuthenticatorOptions options,
        IEntraGroupResolver? groupResolver = null)
        : this(options, CreateDefaultConfigurationSource(options), groupResolver)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="EntraCredentialAuthenticator"/> with an
    /// explicit OIDC configuration source. Used by the registration path and by
    /// tests to supply an in-memory (network-free) configuration.
    /// </summary>
    /// <param name="options">The Entra authenticator configuration. Must not be <c>null</c>.</param>
    /// <param name="configurationSource">The OIDC configuration source that supplies the JWKS-backed configuration manager. Must not be <c>null</c>.</param>
    /// <param name="groupResolver">Optional resolver used for the groups-overage case; <c>null</c> to use the token-only fallback.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> or <paramref name="configurationSource"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="options"/> has no authority, tenant, or audience configured.</exception>
    internal EntraCredentialAuthenticator(
        LatticeEntraAuthenticatorOptions options,
        IEntraOpenIdConfigurationSource configurationSource,
        IEntraGroupResolver? groupResolver = null)
        : base(BuildBaseOptions(options))
    {
        ArgumentNullException.ThrowIfNull(configurationSource);
        _entraOptions = options;
        _configurationSource = configurationSource;
        _groupResolver = groupResolver;
        _allowedTenants = new HashSet<string>(options.TenantIds, StringComparer.OrdinalIgnoreCase);
        _metadataAddress = options.ResolveMetadataAddress();
        if (string.IsNullOrEmpty(_metadataAddress))
        {
            throw new ArgumentException(
                $"{nameof(LatticeEntraAuthenticatorOptions)} must set Authority or MetadataAddress.",
                nameof(options));
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Selects this authenticator when the credential's scheme matches the
    /// configured hint, or when the token's tenant id is in the configured
    /// allow-list and its issuer matches the tenant-substituted issuer template. A
    /// token from an unlisted tenant returns <c>false</c> so resolution falls
    /// through to the next authenticator.
    /// </remarks>
    public override bool CanHandle(in LatticeCredential credential)
    {
        var scheme = credential.Scheme;
        if (!string.IsNullOrEmpty(scheme)
            && _entraOptions.SchemeHint is { } hint
            && string.Equals(scheme, hint, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (!TryReadEntraToken(credential.Token, out var issuer, out var tenantId))
        {
            return false;
        }

        return IsTenantAllowed(tenantId) && IssuerMatchesTemplate(issuer, tenantId);
    }

    /// <inheritdoc />
    public override async ValueTask<LatticePrincipal?> AuthenticateAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken = default)
    {
        var principal = await base.AuthenticateAsync(credential, cancellationToken).ConfigureAwait(false);
        if (principal is null)
        {
            return principal;
        }

        // Overage is only resolved out of band when configured to do so and when
        // the token actually overflowed its groups claim.
        if (_entraOptions.GroupResolutionMode != EntraGroupResolutionMode.ResolveOnOverage
            || !IsGroupsOverage(principal))
        {
            return principal;
        }

        // Documented fallback: with no resolver registered we never throw; the
        // token-asserted groups stand and the local directory merge upstream fills
        // in the rest.
        if (_groupResolver is null)
        {
            return principal;
        }

        var tenantId = principal.Claims is { } claims && claims.TryGetValue(EntraClaimNames.TenantId, out var tid)
            ? tid
            : null;
        var context = new EntraGroupResolutionContext(principal.SubjectId, tenantId, principal.AssertedGroups);
        var resolved = await _groupResolver.ResolveGroupsAsync(context, cancellationToken).ConfigureAwait(false);

        return principal with { AssertedGroups = Union(resolved, principal.AssertedGroups) };
    }

    /// <inheritdoc />
    protected override ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken)
    {
        var parameters = new TokenValidationParameters
        {
            ValidateAudience = true,
            ValidAudiences = _entraOptions.Audiences.ToArray(),
            ValidateLifetime = _entraOptions.ValidateLifetime,
            ClockSkew = _entraOptions.ClockSkew,
            ValidateIssuerSigningKey = true,
            ValidateIssuer = true,
            IssuerValidator = ValidateIssuer,
            ConfigurationManager = _configurationSource.GetOrCreate(_metadataAddress),
        };

        return new ValueTask<TokenValidationParameters>(parameters);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Maps Entra v2.0 claims: the subject id from <c>oid</c> (falling back to
    /// <c>sub</c>), the asserted groups from the union of the <c>groups</c> and
    /// <c>roles</c> claims, and the token issuer verbatim so the principal records
    /// the concrete tenant issuer. A token with neither <c>oid</c> nor <c>sub</c>
    /// (or a subject that collides with a reserved well-known sentinel) maps to
    /// <c>null</c> so the caller resolves to the anonymous subject.
    /// </remarks>
    protected override LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(token);
        ArgumentNullException.ThrowIfNull(identity);

        var subjectId = identity.FindFirst(EntraClaimNames.ObjectId)?.Value
            ?? identity.FindFirst(EntraClaimNames.Subject)?.Value;
        if (!IsAuthorizableSubjectId(subjectId))
        {
            // No oid/sub claim, or a reserved sentinel subject: resolve to the
            // anonymous subject (no groups) rather than an anonymous-labelled
            // principal that still carries the token's groups / roles. See
            // JwtCredentialAuthenticator.IsAuthorizableSubjectId.
            return null;
        }

        var groups = CollectGroupsAndRoles(identity);
        var claims = ResolveClaims(identity);
        DateTimeOffset? expiresAt = token.ValidTo == default
            ? null
            : new DateTimeOffset(token.ValidTo, TimeSpan.Zero);
        var issuer = string.IsNullOrEmpty(token.Issuer) ? _entraOptions.Authority : token.Issuer;

        return new LatticePrincipal(subjectId, issuer, claims, groups, expiresAt);
    }

    private string ValidateIssuer(string issuer, SecurityToken securityToken, TokenValidationParameters validationParameters)
    {
        var tenantId = securityToken is JsonWebToken jwt ? ReadClaim(jwt, EntraClaimNames.TenantId) : null;
        if (!string.IsNullOrEmpty(tenantId)
            && IsTenantAllowed(tenantId)
            && IssuerMatchesTemplate(issuer, tenantId))
        {
            return issuer;
        }

        throw new SecurityTokenInvalidIssuerException(
            $"Issuer '{issuer}' is not a trusted Entra issuer for an allowed tenant.")
        {
            InvalidIssuer = issuer,
        };
    }

    private bool IsTenantAllowed(string? tenantId) =>
        !string.IsNullOrEmpty(tenantId) && _allowedTenants.Contains(tenantId);

    private bool IssuerMatchesTemplate(string? issuer, string tenantId)
    {
        if (string.IsNullOrEmpty(issuer))
        {
            return false;
        }

        var expected = _entraOptions.IssuerTemplate.Replace(
            LatticeEntraAuthenticatorOptionsValidator.TenantPlaceholder,
            tenantId,
            StringComparison.Ordinal);
        return string.Equals(issuer, expected, StringComparison.Ordinal);
    }

    private static bool IsGroupsOverage(LatticePrincipal principal) =>
        principal.Claims is { } claims
        && claims.ContainsKey(EntraClaimNames.ClaimNames)
        && !claims.ContainsKey(EntraClaimNames.Groups);

    private static IReadOnlyCollection<string>? CollectGroupsAndRoles(ClaimsIdentity identity)
    {
        HashSet<string>? groups = null;
        foreach (var claim in identity.FindAll(EntraClaimNames.Groups))
        {
            AddIfPresent(ref groups, claim.Value);
        }

        foreach (var claim in identity.FindAll(EntraClaimNames.Roles))
        {
            AddIfPresent(ref groups, claim.Value);
        }

        return groups;
    }

    private static void AddIfPresent(ref HashSet<string>? set, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return;
        }

        set ??= new HashSet<string>(StringComparer.Ordinal);
        set.Add(value);
    }

    private static IReadOnlyCollection<string> Union(
        IReadOnlyCollection<string> resolved,
        IReadOnlyCollection<string>? existing)
    {
        var union = new HashSet<string>(resolved, StringComparer.Ordinal);
        if (existing is not null)
        {
            foreach (var group in existing)
            {
                union.Add(group);
            }
        }

        return union;
    }

    private static bool TryReadEntraToken(string? token, out string issuer, out string tenantId)
    {
        issuer = string.Empty;
        tenantId = string.Empty;
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        try
        {
            var jwt = new JsonWebToken(token);
            issuer = jwt.Issuer ?? string.Empty;
            tenantId = ReadClaim(jwt, EntraClaimNames.TenantId) ?? string.Empty;
            return true;
        }
        catch (ArgumentException)
        {
            // Not a well-formed JWT: this authenticator does not own it.
            return false;
        }
    }

    private static string? ReadClaim(JsonWebToken token, string claimType) =>
        token.TryGetPayloadValue<string>(claimType, out var value) ? value : null;

    private static IEntraOpenIdConfigurationSource CreateDefaultConfigurationSource(LatticeEntraAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new EntraOpenIdConfigurationSource(options.AutomaticRefreshInterval, options.RefreshInterval);
    }

    private static JwtAuthenticatorOptions BuildBaseOptions(LatticeEntraAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var baseOptions = new JwtAuthenticatorOptions
        {
            // The base requires a non-empty issuer; the Entra subclass validates
            // issuers per token via IssuerValidator, so this is only a placeholder
            // for the base's unused static parameters.
            Issuer = string.IsNullOrWhiteSpace(options.Authority) ? "entra" : options.Authority,
            SchemeHint = options.SchemeHint,
            ValidateLifetime = options.ValidateLifetime,
            ClockSkew = options.ClockSkew,
        };

        foreach (var audience in options.Audiences)
        {
            baseOptions.Audiences.Add(audience);
        }

        baseOptions.SubjectClaimTypes.Clear();
        baseOptions.SubjectClaimTypes.Add(EntraClaimNames.ObjectId);
        baseOptions.SubjectClaimTypes.Add(EntraClaimNames.Subject);

        baseOptions.GroupClaimTypes.Clear();
        baseOptions.GroupClaimTypes.Add(EntraClaimNames.Groups);
        baseOptions.GroupClaimTypes.Add(EntraClaimNames.Roles);

        return baseOptions;
    }
}
