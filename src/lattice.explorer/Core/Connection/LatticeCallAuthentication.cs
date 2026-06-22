namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// The authentication seam for state-API calls. Left unset for anonymous
/// development connections; populated by a future security feature so every
/// call carries the configured credentials. Static headers (for example an
/// <c>Authorization: Bearer ...</c> token or an API key) are attached to every
/// unary and streaming RPC.
/// </summary>
public sealed record LatticeCallAuthentication
{
    /// <summary>
    /// Metadata headers added to every call. Header names must be valid gRPC
    /// metadata keys (lower-case ASCII). <see langword="null"/> or empty means
    /// anonymous.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary><see langword="true"/> when at least one header will be attached.</summary>
    public bool HasHeaders => Headers is { Count: > 0 };
}
