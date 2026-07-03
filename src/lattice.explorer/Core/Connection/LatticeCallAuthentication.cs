using System.Text;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The authentication seam for state-API calls. Left unset for anonymous
/// development connections; populated when the user signs in so every call
/// carries the configured credentials. Static headers (for example an
/// <c>authorization: Basic ...</c> credential or a bearer token) are attached to
/// every unary and streaming RPC.
/// </summary>
public sealed record LatticeCallAuthentication
{
    /// <summary>The lower-case gRPC metadata key for the standard authorization header.</summary>
    public const string AuthorizationHeaderName = "authorization";

    /// <summary>
    /// Metadata headers added to every call. Header names must be valid gRPC
    /// metadata keys (lower-case ASCII). <see langword="null"/> or empty means
    /// anonymous.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary><see langword="true"/> when at least one header will be attached.</summary>
    public bool HasHeaders => Headers is { Count: > 0 };

    /// <summary>
    /// A live credential provider consulted on every call for a current
    /// <c>authorization</c> header. Set (in place of static
    /// <see cref="Headers"/>) when the credential is a short-lived bearer token
    /// the provider refreshes transparently. <see langword="null"/> for a static
    /// credential (for example Basic) or anonymous connections.
    /// </summary>
    public ILatticeCallCredentialProvider? CredentialProvider { get; init; }

    /// <summary><see langword="true"/> when a live token provider is attached.</summary>
    public bool HasCredentialProvider => CredentialProvider is not null;

    /// <summary>
    /// Creates an authentication seam that attaches an
    /// <c>authorization: Basic base64(username:password)</c> header to every
    /// call, matching the server-side <c>EnvVarCredentialAuthorizer</c> contract.
    /// </summary>
    /// <param name="username">The credential username. Must be non-empty.</param>
    /// <param name="password">The credential password. Must not be <see langword="null"/>.</param>
    /// <returns>A populated <see cref="LatticeCallAuthentication"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="username"/> is null, empty, or whitespace.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="password"/> is <see langword="null"/>.</exception>
    public static LatticeCallAuthentication Basic(string username, string password)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(username);
        ArgumentNullException.ThrowIfNull(password);

        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
        return new LatticeCallAuthentication
        {
            Headers = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [AuthorizationHeaderName] = $"Basic {encoded}",
            },
        };
    }

    /// <summary>
    /// Creates an authentication seam backed by a live token
    /// <paramref name="provider"/>. The connection queries the provider on every
    /// call for a currently-valid <c>authorization</c> header, so a token that
    /// the provider refreshes in the background is always attached fresh. Used by
    /// interactive/federated login providers (for example Entra) whose bearer
    /// tokens expire and are renewed silently.
    /// </summary>
    /// <param name="provider">The live credential provider. Must not be <see langword="null"/>.</param>
    /// <returns>A <see cref="LatticeCallAuthentication"/> carrying the provider.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="provider"/> is <see langword="null"/>.</exception>
    public static LatticeCallAuthentication Bearer(ILatticeCallCredentialProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        return new LatticeCallAuthentication { CredentialProvider = provider };
    }
}
