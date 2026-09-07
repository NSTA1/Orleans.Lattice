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

    /// <summary>Copies every claim the identity carries into a flat, last-wins bag keyed by claim type.</summary>
    /// <remarks>
    /// No claim is filtered out, so the subject and group claims appear here as
    /// well as in <see cref="LatticePrincipal.SubjectId"/> and
    /// <see cref="LatticePrincipal.AssertedGroups"/>. Because the bag is keyed by
    /// claim type, a claim that appears more than once - which is how a JSON array
    /// claim such as <c>groups</c> is surfaced - keeps only its last value; read
    /// repeated claims from the identity itself, not from here.
    /// </remarks>
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
            // unbounded ValidAlgorithms leaves open.
            parameters.ValidAlgorithms = options.Algorithms.ToArray();
        }
        else
        {
            // No explicit pin. An empty ValidAlgorithms is treated as "no
            // restriction" by the token validator, which leaves the
            // algorithm-confusion gap open: a token whose header advertises a
            // symmetric alg can be checked against a symmetric key the host
            // configured for some other purpose. Rather than deny-all - which
            // would break every host relying on the documented permissive
            // default - constrain acceptance to the key FAMILIES the host
            // actually pinned, so an RSA-only or EC-only deployment can never be
            // talked into HMAC. A host that pinned a mixed key set, or whose keys
            // yield no recognisable family, keeps the historical unrestricted
            // behaviour and can pin explicitly through
            // JwtAuthenticatorOptions.Algorithms or supply a verbatim
            // JwtAuthenticatorOptions.ValidationParameters.
            var derived = DeriveAlgorithmsFromKeys(options.SigningKeys);
            if (derived is not null)
            {
                parameters.ValidAlgorithms = derived;
            }
        }

        return parameters;
    }

    /// <summary>
    /// Derives the signature-algorithm allow-list implied by the configured
    /// signing keys: the asymmetric algorithms for an RSA-only or EC-only key set,
    /// the HMAC algorithms for a symmetric-only one. Returns <see langword="null"/>
    /// when the key set is empty, mixes families, or contains a key whose family
    /// cannot be established - in which case no restriction is applied and the
    /// caller keeps the historical permissive behaviour rather than locking out a
    /// working host.
    /// </summary>
    /// <param name="keys">The host's configured issuer signing keys.</param>
    /// <returns>The derived algorithm allow-list, or <see langword="null"/> to leave it unrestricted.</returns>
    private static string[]? DeriveAlgorithmsFromKeys(IList<SecurityKey> keys)
    {
        if (keys.Count == 0)
        {
            return null;
        }

        var rsa = false;
        var ecdsa = false;
        var symmetric = false;
        for (var i = 0; i < keys.Count; i++)
        {
            switch (keys[i])
            {
                case RsaSecurityKey:
                case X509SecurityKey:
                    rsa = true;
                    break;
                case ECDsaSecurityKey:
                    ecdsa = true;
                    break;
                case SymmetricSecurityKey:
                    symmetric = true;
                    break;
                default:
                    // An unrecognised key type (a JsonWebKey, a custom key, a
                    // subclass): the family cannot be established, so fail open to
                    // the historical behaviour rather than lock a working host out.
                    return null;
            }
        }

        if (symmetric && (rsa || ecdsa))
        {
            // A mixed key set is exactly the shape the derivation cannot narrow
            // safely, because both families are legitimately in use.
            return null;
        }

        if (symmetric)
        {
            return
            [
                SecurityAlgorithms.HmacSha256,
                SecurityAlgorithms.HmacSha384,
                SecurityAlgorithms.HmacSha512,
            ];
        }

        var derived = new List<string>(9);
        if (rsa)
        {
            derived.AddRange(
            [
                SecurityAlgorithms.RsaSha256,
                SecurityAlgorithms.RsaSha384,
                SecurityAlgorithms.RsaSha512,
                SecurityAlgorithms.RsaSsaPssSha256,
                SecurityAlgorithms.RsaSsaPssSha384,
                SecurityAlgorithms.RsaSsaPssSha512,
            ]);
        }

        if (ecdsa)
        {
            derived.AddRange(
            [
                SecurityAlgorithms.EcdsaSha256,
                SecurityAlgorithms.EcdsaSha384,
                SecurityAlgorithms.EcdsaSha512,
            ]);
        }

        return derived.ToArray();
    }
}
