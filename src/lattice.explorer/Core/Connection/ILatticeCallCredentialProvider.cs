namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// A live source of the <c>authorization</c> header value for state-API calls,
/// used when the credential is a short-lived bearer token rather than a static
/// header. Unlike <see cref="LatticeCallAuthentication.Headers"/> (baked once at
/// channel-build time), the connection queries a provider on every call so the
/// token attached is always current. The provider owns its own refresh: it
/// renews the token proactively, before it expires, and coalesces concurrent
/// renewals so a burst of calls triggers at most one refresh.
/// </summary>
/// <remarks>
/// Implementations must be thread-safe: the gRPC stack can invoke
/// <see cref="GetAuthorizationHeaderAsync"/> concurrently from many in-flight
/// calls. Neither the returned header value nor the underlying token is ever
/// written to the plaintext config store; token material lives only in memory
/// (or in a provider-owned, opt-in store).
/// </remarks>
public interface ILatticeCallCredentialProvider
{
    /// <summary>
    /// Returns the current, valid <c>authorization</c> header value (for
    /// example <c>"Bearer eyJ..."</c>), refreshing the token first if it is at
    /// or near expiry. Returns <see langword="null"/> only when no credential is
    /// available and none can be acquired silently, in which case the caller is
    /// left anonymous and the server decides whether to reject the call.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<string?> GetAuthorizationHeaderAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Forces a single silent refresh of the token, regardless of its remaining
    /// lifetime. The connection calls this once, after a mid-session
    /// authentication failure, before retrying the call. Returns
    /// <see langword="true"/> when a fresh token was acquired (retry is worth
    /// it), or <see langword="false"/> when refresh is no longer possible (the
    /// refresh material is expired, revoked, or consent was withdrawn) and the
    /// user must be re-challenged interactively.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<bool> RefreshAsync(CancellationToken cancellationToken = default);
}
