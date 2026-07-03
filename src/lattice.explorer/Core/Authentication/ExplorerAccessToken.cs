namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// A single acquired access token and the moment it stops being valid. Local,
/// in-memory application state (never a wire type and never persisted to the
/// plaintext config store); produced by an auth method's challenge and renewed
/// by <see cref="ExplorerAccessTokenSource"/>.
/// </summary>
public readonly record struct ExplorerAccessToken
{
    /// <summary>Creates a token, defaulting the scheme to <c>Bearer</c>.</summary>
    public ExplorerAccessToken()
    {
    }

    /// <summary>The raw access-token string attached after the scheme.</summary>
    public required string Token { get; init; }

    /// <summary>The instant the token expires (absolute, UTC-based).</summary>
    public required DateTimeOffset ExpiresOn { get; init; }

    /// <summary>
    /// The authorization scheme prefix written before the token, defaulting to
    /// <c>Bearer</c> so the composed header is <c>"Bearer &lt;token&gt;"</c>.
    /// </summary>
    public string Scheme { get; init; } = "Bearer";

    /// <summary>The composed <c>authorization</c> header value (<c>"&lt;scheme&gt; &lt;token&gt;"</c>).</summary>
    public string ToAuthorizationHeader() => $"{Scheme} {Token}";
}
