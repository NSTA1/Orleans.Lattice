namespace Orleans.Lattice.Explorer.Entra;

/// <summary>
/// The outcome of a successful Entra token acquisition: the access token, its
/// absolute expiry, and the signed-in account's display name. In-memory only;
/// never persisted to the plaintext config store.
/// </summary>
public readonly record struct EntraTokenResult
{
    /// <summary>The acquired access token attached as a bearer credential.</summary>
    public required string AccessToken { get; init; }

    /// <summary>The instant the token expires (absolute, UTC-based).</summary>
    public required DateTimeOffset ExpiresOn { get; init; }

    /// <summary>A friendly name for the signed-in account (a username or UPN).</summary>
    public string Username { get; init; }
}
