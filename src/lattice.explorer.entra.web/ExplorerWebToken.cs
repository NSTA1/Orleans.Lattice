namespace Orleans.Lattice.Explorer.Entra.Web;

/// <summary>
/// A single access token acquired for the signed-in browser user, targeting the
/// downstream State API. Local, in-memory application state - never persisted and
/// never a wire type.
/// </summary>
public readonly record struct ExplorerWebToken
{
    /// <summary>The raw access-token string.</summary>
    public required string AccessToken { get; init; }

    /// <summary>The instant the token expires (absolute, UTC-based).</summary>
    public required DateTimeOffset ExpiresOn { get; init; }

    /// <summary>The signed-in account name, or <see langword="null"/> when unknown.</summary>
    public string? Username { get; init; }
}
