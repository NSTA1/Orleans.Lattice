using System.Diagnostics.CodeAnalysis;
using System.Security.Claims;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The built-in JWT <see cref="ILatticeCredentialAuthenticator"/>, designed as
/// an <b>extensible base</b>: it validates issuer / audience / signing-key /
/// lifetime and maps token claims into a <see cref="LatticePrincipal"/>, and
/// exposes every provider-specific concern as an overridable extension point so
/// a concrete provider authenticator (for example Microsoft Entra ID, shipped
/// separately) is a thin subclass rather than a second token-validation
/// implementation.
/// <para>
/// Extension points: <see cref="CanHandle"/> (selection), <see cref="ResolveValidationParametersAsync"/>
/// (OIDC / JWKS metadata discovery and signing-key rotation), and
/// <see cref="MapPrincipal"/> (subject / groups / claim mapping).
/// </para>
/// </summary>
public class JwtCredentialAuthenticator : ILatticeCredentialAuthenticator
{
    private readonly JsonWebTokenHandler _handler = new();
    private readonly TokenValidationParameters _staticParameters;

    /// <summary>
    /// Initializes a new <see cref="JwtCredentialAuthenticator"/> from the
    /// supplied <paramref name="options"/>.
    /// </summary>
    /// <param name="options">The per-issuer configuration. Must not be <c>null</c> and must set an issuer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="options"/> does not set an issuer, or requests audience validation without listing any audience.</exception>
    public JwtCredentialAuthenticator(JwtAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.Issuer))
        {
            throw new ArgumentException("JwtAuthenticatorOptions.Issuer must be set.", nameof(options));
        }

        // Fail closed on the audience-validation footgun: with ValidateAudience
        // defaulting to true, an operator reasonably assumes the aud claim is
        // checked, but an empty Audiences list would silently disable it and
        // accept any validly-signed token from the trusted issuer - including one
        // minted for a different relying party (audience/token confusion,
        // CWE-287 / CWE-1032). Require an explicit opt-out (ValidateAudience =
        // false) rather than inferring "disable" from a missing audience. The
        // check is skipped when an explicit ValidationParameters override is
        // supplied, because that override governs validation verbatim.
        if (options.ValidationParameters is null
            && options.ValidateAudience
            && options.Audiences.Count == 0)
        {
            throw new ArgumentException(
                "JwtAuthenticatorOptions.ValidateAudience is true but no Audiences are configured, "
                + "which would silently disable audience validation. Add at least one audience, "
                + "or set ValidateAudience = false to accept any audience explicitly.",
                nameof(options));
        }

        Options = options;
        _staticParameters = options.ValidationParameters ?? BuildValidationParameters(options);
    }

    /// <summary>The configuration this authenticator was built from.</summary>
    protected JwtAuthenticatorOptions Options { get; }

    /// <inheritdoc />
    /// <remarks>
    /// Selects this authenticator when the credential's
    /// <see cref="LatticeCredential.Scheme"/> matches the configured scheme hint
    /// or issuer; when neither hint is present it parses the token's <c>iss</c>
    /// claim and matches on the configured issuer. A malformed token never
    /// matches.
    /// </remarks>
    public virtual bool CanHandle(in LatticeCredential credential)
    {
        var scheme = credential.Scheme;
        if (!string.IsNullOrEmpty(scheme))
        {
            if (Options.SchemeHint is { } hint && string.Equals(scheme, hint, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (string.Equals(scheme, Options.Issuer, StringComparison.Ordinal))
            {
                return true;
            }

            // A hint was supplied but did not match this authenticator.
            return false;
        }

        // No hint: fall back to parsing the token issuer.
        return TryReadIssuer(credential.Token, out var issuer)
            && string.Equals(issuer, Options.Issuer, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public virtual async ValueTask<LatticePrincipal?> AuthenticateAsync(LatticeCredential credential, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(credential.Token))
        {
            return null;
        }

        var parameters = await ResolveValidationParametersAsync(credential, cancellationToken).ConfigureAwait(false);
        var result = await _handler.ValidateTokenAsync(credential.Token, parameters).ConfigureAwait(false);
        if (!result.IsValid || result.SecurityToken is not JsonWebToken token || result.ClaimsIdentity is null)
        {
            return null;
        }

        return MapPrincipal(token, result.ClaimsIdentity);
    }

    /// <summary>
    /// Resolves the <see cref="TokenValidationParameters"/> to validate
    /// <paramref name="credential"/> against. The base returns the static
    /// parameters built from <see cref="Options"/>; override to plug in OIDC /
    /// JWKS metadata discovery and signing-key rotation (for example returning a
    /// parameters instance whose <see cref="TokenValidationParameters.IssuerSigningKeys"/>
    /// come from a refreshed JWKS document).
    /// </summary>
    /// <param name="credential">The credential being validated.</param>
    /// <param name="cancellationToken">Cancels any metadata fetch.</param>
    protected virtual ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken) =>
        new(_staticParameters);

    /// <summary>
    /// Maps a validated token and its identity into a
    /// <see cref="LatticePrincipal"/>. The base resolves the subject id from the
    /// first present <see cref="JwtAuthenticatorOptions.SubjectClaimTypes"/>,
    /// collects group ids from <see cref="JwtAuthenticatorOptions.GroupClaimTypes"/>,
    /// copies the remaining claims into a flat bag, and surfaces the token
    /// expiry. Returns <c>null</c> when the token has no authorizable subject (a
    /// missing subject claim, or a subject that collides with a reserved
    /// well-known sentinel id), so the caller resolves to the anonymous subject
    /// rather than to an anonymous-labelled principal that still carries the
    /// token's groups. Override for provider-specific claim shapes.
    /// </summary>
    /// <param name="token">The validated token.</param>
    /// <param name="identity">The claims identity produced by validation.</param>
    protected virtual LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)
    {
        var subjectId = ResolveSubjectId(identity);
        if (!IsAuthorizableSubjectId(subjectId))
        {
            // A validated token that asserts no subject, or whose subject collides
            // with a reserved well-known sentinel (anonymous / system), must not
            // resolve to an authorized principal: it would otherwise be granted
            // access through a group / role rule while wearing the anonymous label,
            // or impersonate the system subject. Return null so the caller resolves
            // it to the anonymous subject (no groups).
            return null;
        }

        var groups = ResolveGroups(identity);
        var claims = ResolveClaims(identity);
        DateTimeOffset? expiresAt = token.ValidTo == default ? null : new DateTimeOffset(token.ValidTo, TimeSpan.Zero);

        return new LatticePrincipal(subjectId, Options.Issuer, claims, groups, expiresAt);
    }

    /// <summary>
    /// Resolves the subject id from the first present configured subject claim,
    /// falling back to the standard name-identifier claim. Returns <c>null</c>
    /// when the token asserts no subject claim: such a token has no authorizable
    /// identity, so <see cref="MapPrincipal"/> resolves it to the anonymous
    /// subject rather than to an anonymous-labelled principal that still carries
    /// the token's group / role claims.
    /// </summary>
    /// <param name="identity">The validated claims identity.</param>
    protected string? ResolveSubjectId(ClaimsIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        foreach (var claimType in Options.SubjectClaimTypes)
        {
            var value = identity.FindFirst(claimType)?.Value;
            if (!string.IsNullOrEmpty(value))
            {
                return value;
            }
        }

        return identity.FindFirst(ClaimTypes.NameIdentifier)?.Value;
    }

    /// <summary>
    /// Determines whether <paramref name="subjectId"/> is a usable, authorizable
    /// identity: non-empty and not a reserved well-known sentinel
    /// (<see cref="LatticeSubject.AnonymousSubjectId"/> or
    /// <see cref="LatticeSubject.SystemSubjectId"/>). A validated token whose
    /// subject is missing or reserved must not resolve to an authorized principal
    /// - it would let a token carrying only group / role claims be granted through
    /// a group rule while labelled anonymous, or impersonate the system subject.
    /// Shared by the built-in authenticators (and available to subclasses) so the
    /// convention lives in exactly one place.
    /// </summary>
    /// <param name="subjectId">The candidate subject id, or <c>null</c>.</param>
    protected static bool IsAuthorizableSubjectId([NotNullWhen(true)] string? subjectId) =>
        !string.IsNullOrEmpty(subjectId)
        && !string.Equals(subjectId, LatticeSubject.AnonymousSubjectId, StringComparison.Ordinal)
        && !string.Equals(subjectId, LatticeSubject.SystemSubjectId, StringComparison.Ordinal);

    /// <summary>Collects token-asserted group ids from the configured group claim types.</summary>
    /// <param name="identity">The validated claims identity.</param>
    protected IReadOnlyCollection<string>? ResolveGroups(ClaimsIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        HashSet<string>? groups = null;
        foreach (var claimType in Options.GroupClaimTypes)
        {
            foreach (var claim in identity.FindAll(claimType))
            {
                if (string.IsNullOrEmpty(claim.Value))
                {
                    continue;
                }

                groups ??= new HashSet<string>(StringComparer.Ordinal);
                groups.Add(claim.Value);
            }
        }

        return groups;
    }

    /// <summary>Copies non-group, non-subject claims into a flat, last-wins bag.</summary>
    /// <param name="identity">The validated claims identity.</param>
    protected IReadOnlyDictionary<string, string>? ResolveClaims(ClaimsIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(identity);
        Dictionary<string, string>? claims = null;
        foreach (var claim in identity.Claims)
        {
            claims ??= new Dictionary<string, string>(StringComparer.Ordinal);
            claims[claim.Type] = claim.Value;
        }

        return claims;
    }

    private static bool TryReadIssuer(string? token, out string issuer)
    {
        issuer = string.Empty;
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        try
        {
            issuer = new JsonWebToken(token).Issuer ?? string.Empty;
            return !string.IsNullOrEmpty(issuer);
        }
        catch (ArgumentException)
        {
            // Not a well-formed JWT: this authenticator does not own it.
            return false;
        }
    }

    private static TokenValidationParameters BuildValidationParameters(JwtAuthenticatorOptions options)
    {
        var parameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidIssuer = options.Issuer,
            ValidateAudience = options.ValidateAudience && options.Audiences.Count > 0,
            ValidateLifetime = options.ValidateLifetime,
            ClockSkew = options.ClockSkew,
            ValidateIssuerSigningKey = true,
            IssuerSigningKeys = options.SigningKeys.ToArray(),
        };

        if (parameters.ValidateAudience)
        {
            parameters.ValidAudiences = options.Audiences.ToArray();
        }

        if (options.Algorithms.Count > 0)
        {
            // Pin the accepted signature algorithms so the validator refuses a
            // token whose header advertises an algorithm outside the configured
            // allow-list, closing the algorithm-confusion gap (CWE-347) that an
            // unbounded ValidAlgorithms leaves open. Empty leaves the set
            // unrestricted (the permissive default).
            parameters.ValidAlgorithms = options.Algorithms.ToArray();
        }

        return parameters;
    }
}
