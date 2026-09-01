using System.Security.Claims;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// A generic OpenID Connect credential authenticator that specializes the
/// built-in <see cref="JwtCredentialAuthenticator"/> for any conformant OIDC
/// provider (Okta, Auth0, Keycloak, Ping, Google, and the like). It reuses the
/// base audience, signing-key, lifetime, and claim-mapping machinery and
/// overrides only the three provider-shaped extension points:
/// <list type="bullet">
/// <item><description><see cref="CanHandle"/> - selection by exact ordinal issuer match, or by an explicitly configured scheme hint. There is no prefix, wildcard, or catch-all form, so a generic OIDC authenticator never claims another issuer's token.</description></item>
/// <item><description><see cref="ResolveValidationParametersAsync"/> - discovery-document-driven JWKS rotation from the provider authority, exact issuer validation, and fail-closed signature-algorithm pinning.</description></item>
/// <item><description><see cref="MapPrincipal"/> - standard OIDC claim conventions (<c>sub</c> subject, configurable group / role claims), with the issuer recorded verbatim.</description></item>
/// </list>
/// <para>
/// It is an additive sibling to the Entra authenticator, not a replacement:
/// both can be registered on the same silo, and selection stays unambiguous
/// because each one only claims its own issuer.
/// </para>
/// </summary>
public class OidcCredentialAuthenticator : JwtCredentialAuthenticator
{
    private readonly LatticeOidcAuthenticatorOptions _oidcOptions;
    private readonly IOidcConfigurationSource _configurationSource;
    private readonly string _metadataAddress;
    private readonly string[] _audiences;
    private readonly string[]? _configuredAlgorithms;
    private readonly IssuerValidator _issuerValidator;

    /// <summary>
    /// A cached deny-all algorithm validator. It is installed when neither the
    /// configuration nor the discovery document names a signature algorithm, so
    /// the authenticator fails closed instead of inheriting the base's permissive
    /// "empty allow-list means accept anything" behaviour (CWE-347). Cached in a
    /// static field so installing it costs no allocation per authentication.
    /// </summary>
    private static readonly AlgorithmValidator DenyAllAlgorithms =
        static (algorithm, key, token, parameters) => false;

    /// <summary>The empty pin, reused so the discovery-derived cache never allocates for a provider that advertises no algorithms.</summary>
    private static readonly string[] NoAlgorithms = Array.Empty<string>();

    /// <summary>
    /// The largest credential <see cref="CanHandle"/> will parse, in characters.
    /// It is deliberately the same bound the validating handler applies
    /// (<see cref="TokenValidationParameters.DefaultMaximumTokenSizeInBytes"/>,
    /// the default of <c>JsonWebTokenHandler.MaximumTokenSizeInBytes</c>), so a
    /// credential this selection pass declines to parse is one
    /// <c>ValidateTokenAsync</c> would have rejected anyway - the guard can
    /// therefore never reject a credential that would otherwise have
    /// authenticated. A well-formed JWT is base64url plus dots, so it is pure
    /// ASCII and its character count equals its byte count; a credential
    /// carrying multi-byte characters is not a JWT and only ever measures
    /// shorter here than the byte bound, so the comparison stays conservative.
    /// </summary>
    private const int MaxParsableTokenLength = TokenValidationParameters.DefaultMaximumTokenSizeInBytes;

    private DiscoveredAlgorithms? _discoveredAlgorithms;

    /// <summary>
    /// Initializes a new <see cref="OidcCredentialAuthenticator"/> that discovers
    /// OIDC metadata from the live provider authority.
    /// </summary>
    /// <param name="options">The OIDC authenticator configuration. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="options"/> has no issuer, audience, subject claim type, or metadata address configured.</exception>
    public OidcCredentialAuthenticator(LatticeOidcAuthenticatorOptions options)
        : this(options, CreateDefaultConfigurationSource(options))
    {
    }

    /// <summary>
    /// Initializes a new <see cref="OidcCredentialAuthenticator"/> with an
    /// explicit OIDC configuration source. Used by the registration path and by
    /// tests to supply an in-memory (network-free) configuration.
    /// </summary>
    /// <param name="options">The OIDC authenticator configuration. Must not be <c>null</c>.</param>
    /// <param name="configurationSource">The configuration source that supplies the JWKS-backed configuration manager. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> or <paramref name="configurationSource"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="options"/> has no issuer, audience, subject claim type, or metadata address configured.</exception>
    internal OidcCredentialAuthenticator(
        LatticeOidcAuthenticatorOptions options,
        IOidcConfigurationSource configurationSource)
        : base(BuildBaseOptions(options))
    {
        ArgumentNullException.ThrowIfNull(configurationSource);
        _oidcOptions = options;
        _configurationSource = configurationSource;
        _audiences = options.Audiences.ToArray();
        _configuredAlgorithms = options.Algorithms.Count > 0 ? options.Algorithms.ToArray() : null;
        _issuerValidator = ValidateIssuer;
        _metadataAddress = options.ResolveMetadataAddress();
        if (string.IsNullOrEmpty(_metadataAddress))
        {
            throw new ArgumentException(
                $"{nameof(LatticeOidcAuthenticatorOptions)} must set Authority or MetadataAddress.",
                nameof(options));
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// Selects this authenticator when the credential's scheme matches the
    /// explicitly configured <see cref="LatticeOidcAuthenticatorOptions.SchemeHint"/>,
    /// or when the token's <c>iss</c> claim is an exact ordinal match for the
    /// configured <see cref="LatticeOidcAuthenticatorOptions.Issuer"/>. Matching is
    /// never widened to a prefix, a wildcard, or a catch-all: a token from any
    /// other issuer returns <c>false</c> so resolution falls through to the next
    /// authenticator. A malformed token never matches.
    /// <para>
    /// This deliberately diverges from
    /// <see cref="JwtCredentialAuthenticator.CanHandle"/>, which treats a
    /// non-empty scheme that matches neither the hint nor the issuer as a
    /// decisive "not mine" and returns without reading the token. That
    /// short-circuit is unusable here: the credential bridges stamp the scheme
    /// from operator configuration rather than from the caller, so it is
    /// non-empty on essentially every request, and inheriting the base rule
    /// would make an authenticator that leaves
    /// <see cref="LatticeOidcAuthenticatorOptions.SchemeHint"/> unset - the
    /// documented default - permanently unselectable. Falling through to the
    /// issuer match is what makes the exact-issuer promise the real selection
    /// key. The cost is that a non-matching scheme still parses the token, so
    /// the parse is bounded by <see cref="MaxParsableTokenLength"/> to keep this
    /// pre-authentication path from being an amplification lever.
    /// </para>
    /// </remarks>
    public override bool CanHandle(in LatticeCredential credential)
    {
        var scheme = credential.Scheme;
        if (!string.IsNullOrEmpty(scheme)
            && _oidcOptions.SchemeHint is { } hint
            && string.Equals(scheme, hint, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return TryReadIssuer(credential.Token, out var issuer)
            && string.Equals(issuer, _oidcOptions.Issuer, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Binds the provider's cached, auto-refreshing discovery configuration so
    /// signing keys rotate without a restart, validates the issuer against the
    /// exact configured value, and pins the accepted signature algorithms. The pin
    /// comes from <see cref="LatticeOidcAuthenticatorOptions.Algorithms"/> when it
    /// is populated and from the discovery document's
    /// <c>id_token_signing_alg_values_supported</c> otherwise; when neither names
    /// an algorithm the validator rejects every token rather than accepting any.
    /// </remarks>
    protected override async ValueTask<TokenValidationParameters> ResolveValidationParametersAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken)
    {
        var configurationManager = _configurationSource.GetOrCreate(_metadataAddress);

        // Documented intentional allocation. TokenValidationParameters is
        // mutable and is handed to the validator, which may stamp per-call state
        // onto it, so it cannot be cached and shared across concurrent
        // authentications. This mirrors EntraCredentialAuthenticator. Removing it
        // would require the base JwtCredentialAuthenticator to expose an
        // immutable, reusable parameter shape.
        var parameters = new TokenValidationParameters
        {
            ValidateAudience = true,
            ValidAudiences = _audiences,
            ValidateLifetime = _oidcOptions.ValidateLifetime,
            ClockSkew = _oidcOptions.ClockSkew,
            ValidateIssuerSigningKey = true,
            ValidateIssuer = true,
            ValidIssuer = _oidcOptions.Issuer,
            // Pin issuer acceptance to the configured value even though a
            // configuration manager is attached: the base validator would
            // otherwise also accept whatever issuer the discovery document
            // advertises, which is a wider set than the exact ordinal match this
            // authenticator promises.
            IssuerValidator = _issuerValidator,
            ConfigurationManager = configurationManager,
        };

        var pinned = _configuredAlgorithms
            ?? await ResolveDiscoveredAlgorithmsAsync(configurationManager, cancellationToken).ConfigureAwait(false);

        if (pinned.Length > 0)
        {
            // Pin the accepted signature algorithms so the validator refuses a
            // token whose header advertises an algorithm outside the allow-list,
            // closing the algorithm-confusion gap (CWE-347) that an unbounded
            // ValidAlgorithms leaves open.
            parameters.ValidAlgorithms = pinned;
        }
        else
        {
            // Fail closed. An empty ValidAlgorithms is treated as "no restriction"
            // by the token validator, so an empty pin must be expressed as an
            // explicit deny-all validator instead.
            parameters.AlgorithmValidator = DenyAllAlgorithms;
        }

        return parameters;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Maps standard OpenID Connect claims: the subject id from the first present
    /// <see cref="LatticeOidcAuthenticatorOptions.SubjectClaimTypes"/> entry
    /// (<c>sub</c> by default), the asserted groups from every value found across
    /// <see cref="LatticeOidcAuthenticatorOptions.GroupClaimTypes"/> (<c>groups</c>,
    /// <c>roles</c>, and <c>role</c> by default), and the token issuer verbatim. A
    /// token with no subject claim - or one whose subject collides with a reserved
    /// well-known sentinel - maps to <c>null</c> so the caller resolves to the
    /// anonymous subject rather than to an anonymous-labelled principal that still
    /// carries the token's groups.
    /// </remarks>
    protected override LatticePrincipal? MapPrincipal(JsonWebToken token, ClaimsIdentity identity)
    {
        ArgumentNullException.ThrowIfNull(token);
        ArgumentNullException.ThrowIfNull(identity);

        var subjectId = ResolveSubjectId(identity);
        if (!IsAuthorizableSubjectId(subjectId))
        {
            // No subject claim, or a reserved sentinel subject: resolve to the
            // anonymous subject (no groups) rather than an anonymous-labelled
            // principal that still carries the token's groups / roles. See
            // JwtCredentialAuthenticator.IsAuthorizableSubjectId.
            return null;
        }

        var groups = ResolveGroups(identity);
        var claims = ResolveClaims(identity);
        DateTimeOffset? expiresAt = token.ValidTo == default
            ? null
            : new DateTimeOffset(token.ValidTo, TimeSpan.Zero);
        var issuer = string.IsNullOrEmpty(token.Issuer) ? _oidcOptions.Issuer : token.Issuer;

        return new LatticePrincipal(subjectId, issuer, claims, groups, expiresAt);
    }

    private string ValidateIssuer(string issuer, SecurityToken securityToken, TokenValidationParameters validationParameters)
    {
        if (string.Equals(issuer, _oidcOptions.Issuer, StringComparison.Ordinal))
        {
            return issuer;
        }

        throw new SecurityTokenInvalidIssuerException(
            $"Issuer '{issuer}' is not the trusted OIDC issuer for this authenticator.")
        {
            InvalidIssuer = issuer,
        };
    }

    /// <summary>
    /// Reads the provider-advertised signing algorithms out of the current
    /// discovery configuration, memoized against the configuration instance so a
    /// steady-state authentication reuses the cached array rather than
    /// re-projecting it. The memo is keyed on the configuration instance by
    /// reference, so a document refresh that rotates the advertised set is
    /// picked up on the next call rather than being pinned to the stale set.
    /// This trims the projection, not the whole call: reaching the configuration
    /// still costs the configuration manager's own await, so this path stays
    /// measurably dearer than an explicitly configured
    /// <see cref="LatticeOidcAuthenticatorOptions.Algorithms"/> pin, which skips
    /// it entirely.
    /// </summary>
    private async ValueTask<string[]> ResolveDiscoveredAlgorithmsAsync(
        BaseConfigurationManager configurationManager,
        CancellationToken cancellationToken)
    {
        var configuration = await configurationManager
            .GetBaseConfigurationAsync(cancellationToken)
            .ConfigureAwait(false);

        if (configuration is null)
        {
            return NoAlgorithms;
        }

        // Benign race: two threads may both project the same configuration. Both
        // produce an equivalent array, and the last write wins.
        var cached = Volatile.Read(ref _discoveredAlgorithms);
        if (cached is not null && ReferenceEquals(cached.Configuration, configuration))
        {
            return cached.Algorithms;
        }

        var algorithms = configuration is OpenIdConnectConfiguration oidc
            && oidc.IdTokenSigningAlgValuesSupported.Count > 0
            ? oidc.IdTokenSigningAlgValuesSupported.ToArray()
            : NoAlgorithms;

        Volatile.Write(ref _discoveredAlgorithms, new DiscoveredAlgorithms(configuration, algorithms));
        return algorithms;
    }

    private static bool TryReadIssuer(string? token, out string issuer)
    {
        issuer = string.Empty;
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        if (token.Length > MaxParsableTokenLength)
        {
            // Bound the pre-authentication parse. CanHandle runs for every
            // registered authenticator on every resolution-cache miss, and
            // parsing allocates on the order of the token's own length, so an
            // unbounded parse lets an unauthenticated caller amplify a large
            // request body into a multiple of itself once per registered
            // authenticator. The bound is the validating handler's own maximum
            // token size, so a credential declined here is one validation would
            // have rejected regardless: declining is never a false negative.
            return false;
        }

        try
        {
            // Documented intentional allocation. Reading `iss` requires parsing
            // the token, and the authenticator seam (CanHandle(in
            // LatticeCredential) followed by a separate AuthenticateAsync call)
            // carries no state between the two, so the parse cannot be shared
            // with the validation pass. Hand-rolling a span-based header/payload
            // scan to avoid it would introduce a second, divergent JWT parser -
            // a parser-differential risk that is not worth one allocation on a
            // path that runs once per credential resolution. Eliminating it
            // would require widening ILatticeCredentialAuthenticator to hand a
            // pre-parsed token to AuthenticateAsync.
            issuer = new JsonWebToken(token).Issuer ?? string.Empty;
            return !string.IsNullOrEmpty(issuer);
        }
        catch (ArgumentException)
        {
            // Not a well-formed JWT: this authenticator does not own it.
            return false;
        }
    }

    private static IOidcConfigurationSource CreateDefaultConfigurationSource(LatticeOidcAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new OidcConfigurationSource(options.AutomaticRefreshInterval, options.RefreshInterval);
    }

    private static JwtAuthenticatorOptions BuildBaseOptions(LatticeOidcAuthenticatorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.Issuer))
        {
            throw new ArgumentException(
                $"{nameof(LatticeOidcAuthenticatorOptions)}.Issuer must be set to the exact issuer this authenticator accepts.",
                nameof(options));
        }

        // Fail closed on the audience-validation footgun even though this
        // authenticator always overrides the validation parameters: audience
        // validation is unconditionally enabled on the override path, so an empty
        // audience list would leave ValidAudiences empty and silently accept any
        // validly-signed token from the trusted issuer, including one minted for a
        // different relying party (CWE-287 / CWE-1032). The OIDC options expose no
        // ValidationParameters escape hatch, so this guard cannot be bypassed.
        if (options.Audiences.Count == 0)
        {
            throw new ArgumentException(
                $"{nameof(LatticeOidcAuthenticatorOptions)}.Audiences is empty, which would silently disable "
                + "audience validation. Add at least one audience.",
                nameof(options));
        }

        if (options.SubjectClaimTypes.Count == 0)
        {
            throw new ArgumentException(
                $"{nameof(LatticeOidcAuthenticatorOptions)}.SubjectClaimTypes must contain at least one claim type.",
                nameof(options));
        }

        var baseOptions = new JwtAuthenticatorOptions
        {
            Issuer = options.Issuer,
            SchemeHint = options.SchemeHint,
            ValidateLifetime = options.ValidateLifetime,
            ClockSkew = options.ClockSkew,
        };

        foreach (var audience in options.Audiences)
        {
            baseOptions.Audiences.Add(audience);
        }

        baseOptions.SubjectClaimTypes.Clear();
        foreach (var claimType in options.SubjectClaimTypes)
        {
            baseOptions.SubjectClaimTypes.Add(claimType);
        }

        baseOptions.GroupClaimTypes.Clear();
        foreach (var claimType in options.GroupClaimTypes)
        {
            baseOptions.GroupClaimTypes.Add(claimType);
        }

        return baseOptions;
    }

    /// <summary>
    /// The memoized projection of one discovery configuration instance onto the
    /// signing algorithms it advertises. Held as a single immutable object so the
    /// pair is read and written atomically.
    /// </summary>
    private sealed class DiscoveredAlgorithms(BaseConfiguration configuration, string[] algorithms)
    {
        /// <summary>The configuration instance this projection was taken from.</summary>
        public BaseConfiguration Configuration { get; } = configuration;

        /// <summary>The advertised signing algorithms, empty when the document names none.</summary>
        public string[] Algorithms { get; } = algorithms;
    }
}
