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
    /// The one deny-all algorithm validator, shared by every authenticator in this
    /// hierarchy. Declared once here because an empty allow-list is read by the
    /// token validator as "accept any algorithm", so refusing every algorithm has
    /// to be expressed as an explicit validator delegate; each provider keeping its
    /// own copy of that delegate is how the guard drifted between providers before.
    /// </summary>
    internal static readonly AlgorithmValidator DenyAllAlgorithms = static (_, _, _, _) => false;

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

        var parameters = await ResolvePinnedValidationParametersAsync(credential, cancellationToken).ConfigureAwait(false);
        var result = await _handler.ValidateTokenAsync(credential.Token, parameters).ConfigureAwait(false);
        if (!result.IsValid || result.SecurityToken is not JsonWebToken token || result.ClaimsIdentity is null)
        {
            return null;
        }

        var principal = MapPrincipal(token, result.ClaimsIdentity);
        return principal is null
            ? null
            : await EnrichPrincipalAsync(principal, credential, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Post-validation extension point, invoked with the mapped principal once the
    /// token has been fully validated. Override to enrich the principal from a
    /// source outside the token - resolving group membership out of band, for
    /// example. The base returns <paramref name="principal"/> unchanged.
    /// </summary>
    /// <remarks>
    /// This exists so a provider never has to override
    /// <see cref="AuthenticateAsync"/> to post-process a result. An override there
    /// would sit outside the algorithm-pin seam and could route around it by not
    /// calling the base, which is the drift this hierarchy is built to prevent; an
    /// override here runs after validation has already been enforced and cannot.
    /// It is only ever called with a non-null principal, so an implementation does
    /// not have to re-check that the authentication succeeded.
    /// </remarks>
    /// <param name="principal">The validated, mapped principal. Never <c>null</c>.</param>
    /// <param name="credential">The credential that produced <paramref name="principal"/>.</param>
    /// <param name="cancellationToken">Cancels any out-of-band lookup.</param>
    /// <returns>The principal to return, enriched or unchanged.</returns>
    protected virtual ValueTask<LatticePrincipal?> EnrichPrincipalAsync(
        LatticePrincipal principal,
        LatticeCredential credential,
        CancellationToken cancellationToken) => new(principal);

    /// <summary>
    /// The single seam every authentication funnels through, whichever subclass
    /// supplied the parameters. It resolves the parameters through the overridable
    /// <see cref="ResolveValidationParametersAsync"/> extension point and then
    /// applies the signature-algorithm allow-list to whatever comes back.
    /// </summary>
    /// <remarks>
    /// This method is deliberately <b>not</b> virtual, and
    /// <see cref="AuthenticateAsync"/> calls it rather than the extension point
    /// directly. The algorithm allow-list is a property of every validation, not of
    /// any one provider, so enforcing it here means a subclass cannot construct a
    /// <see cref="TokenValidationParameters"/> that silently skips it - including a
    /// subclass written after this type. Enforcing it inside each override instead
    /// is what previously let the guard drift between providers.
    /// <para>
    /// It is <c>protected</c> so a subclass can resolve the fully-enforced
    /// parameters when it needs them, and non-virtual so no subclass can replace
    /// the enforcement.
    /// </para>
    /// </remarks>
    /// <param name="credential">The credential being validated.</param>
    /// <param name="cancellationToken">Cancels any metadata fetch.</param>
    /// <returns>The resolved parameters, with the signature-algorithm allow-list established.</returns>
    protected async ValueTask<TokenValidationParameters> ResolvePinnedValidationParametersAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken)
    {
        var parameters = await ResolveValidationParametersAsync(credential, cancellationToken).ConfigureAwait(false);
        ApplyAlgorithmPin(parameters, Options.RequireAlgorithmPin);
        return parameters;
    }

    /// <summary>
    /// Establishes the signature-algorithm allow-list on <paramref name="parameters"/>
    /// when it carries none, closing the algorithm-confusion gap (CWE-347) that an
    /// unrestricted allow-list leaves open.
    /// </summary>
    /// <remarks>
    /// An empty or absent <see cref="TokenValidationParameters.ValidAlgorithms"/> is
    /// read by the token validator as "accept any algorithm" rather than "accept
    /// none", so an allow-list that must admit nothing has to be expressed as an
    /// explicit <see cref="TokenValidationParameters.AlgorithmValidator"/>. That
    /// inversion is the footgun behind this whole guard and is stated once, here,
    /// rather than at each provider.
    /// <para>
    /// Precedence: an allow-list the caller already established is authoritative and
    /// is never widened or replaced. Otherwise the allow-list is derived from the
    /// families of the resolved signing keys, which can only refuse a token that
    /// would have been verified against a key of a different family - the attack -
    /// and never one that legitimately verifies. Only when no family can be
    /// established does <paramref name="requirePin"/> decide between failing closed
    /// and preserving the historical unrestricted behaviour.
    /// </para>
    /// </remarks>
    /// <param name="parameters">The validation parameters to establish the allow-list on.</param>
    /// <param name="requirePin">Whether to fail closed when no allow-list can be established.</param>
    internal static void ApplyAlgorithmPin(TokenValidationParameters parameters, bool requirePin)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        if (HasAlgorithmRestriction(parameters))
        {
            return;
        }

        var derived = DeriveAlgorithmsFromKeys(CollectResolvedKeys(parameters));
        if (derived is not null)
        {
            parameters.ValidAlgorithms = derived;
            return;
        }

        if (requirePin)
        {
            parameters.AlgorithmValidator = DenyAllAlgorithms;
        }
    }

    /// <summary>
    /// Whether <paramref name="parameters"/> already constrains which signature
    /// algorithms are accepted, by either an explicit allow-list or a validator
    /// delegate. An empty <see cref="TokenValidationParameters.ValidAlgorithms"/> is
    /// not a restriction - the validator reads it as "accept any".
    /// </summary>
    /// <param name="parameters">The validation parameters to inspect.</param>
    internal static bool HasAlgorithmRestriction(TokenValidationParameters parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        if (parameters.AlgorithmValidator is not null)
        {
            return true;
        }

        var algorithms = parameters.ValidAlgorithms;
        if (algorithms is null)
        {
            return false;
        }

        foreach (var algorithm in algorithms)
        {
            if (!string.IsNullOrWhiteSpace(algorithm))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gathers every signing key the parameters make available for derivation - the
    /// collection and the singular property - so a provider that resolves one key
    /// rather than a set is covered identically. Keys a
    /// <see cref="TokenValidationParameters.ConfigurationManager"/> supplies during
    /// validation are not visible here, so such a provider derives nothing and falls
    /// through to <c>requirePin</c>, which is why the shipped discovery-driven
    /// providers turn that flag on.
    /// </summary>
    /// <param name="parameters">The validation parameters to read keys from.</param>
    private static List<SecurityKey>? CollectResolvedKeys(TokenValidationParameters parameters)
    {
        if (parameters.IssuerSigningKeyResolver is not null
            || parameters.IssuerSigningKeyResolverUsingConfiguration is not null
            || parameters.ConfigurationManager is not null)
        {
            // Another source can supply keys of a family the statically-visible keys
            // do not show, so deriving from what is visible here could refuse a
            // token that legitimately verifies. Derive nothing and let requirePin
            // decide.
            return null;
        }

        List<SecurityKey>? keys = null;
        if (parameters.IssuerSigningKeys is { } configured)
        {
            foreach (var key in configured)
            {
                if (key is not null)
                {
                    (keys ??= []).Add(key);
                }
            }
        }

        if (parameters.IssuerSigningKey is { } single)
        {
            (keys ??= []).Add(single);
        }

        return keys;
    }

    /// <summary>
    /// Resolves the <see cref="TokenValidationParameters"/> to validate
    /// <paramref name="credential"/> against. The base returns the static
    /// parameters built from <see cref="Options"/>; override to plug in OIDC /
    /// JWKS metadata discovery and signing-key rotation (for example returning a
    /// parameters instance whose <see cref="TokenValidationParameters.IssuerSigningKeys"/>
    /// come from a refreshed JWKS document).
    /// </summary>
    /// <remarks>
    /// An override does not need to establish the signature-algorithm allow-list:
    /// whatever it returns is passed through the non-overridable pin seam before any
    /// token is validated, so the allow-list cannot be forgotten here. An override
    /// that <em>does</em> establish one is authoritative and is never widened.
    /// </remarks>
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
    /// <param name="keys">The signing keys the authenticator resolved.</param>
    /// <returns>The derived algorithm allow-list, or <see langword="null"/> to leave it unrestricted.</returns>
    private static string[]? DeriveAlgorithmsFromKeys(IList<SecurityKey>? keys)
    {
        if (keys is null || keys.Count == 0)
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
                case JsonWebKey jwk:
                    // A JWKS document is the ordinary way a host supplies keys, so
                    // this is the common case rather than an exotic one. A
                    // JsonWebKey derives from SecurityKey directly rather than from
                    // the family-specific types above, so it has to be classified
                    // by its declared key type or every discovery-driven deployment
                    // silently keeps an unrestricted allow-list.
                    switch (jwk.Kty)
                    {
                        case JsonWebAlgorithmsKeyTypes.RSA:
                            rsa = true;
                            break;
                        case JsonWebAlgorithmsKeyTypes.EllipticCurve:
                            ecdsa = true;
                            break;
                        case JsonWebAlgorithmsKeyTypes.Octet:
                            symmetric = true;
                            break;
                        default:
                            return null;
                    }

                    break;
                default:
                    // An unrecognised key type (a custom key, a subclass): the
                    // family cannot be established, so fail open to the historical
                    // behaviour rather than lock a working host out.
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
