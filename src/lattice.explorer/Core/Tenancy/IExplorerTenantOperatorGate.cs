namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The fail-closed gate that decides whether the current Explorer caller is a
/// platform operator entitled to the cross-tenant, all-tenant view. It reuses the
/// cluster's platform-operator root of trust - authorizing <c>Admin</c> on the
/// reserved auth policy tree - which the Explorer already probes once per session
/// and caches as the coarse Access capability, rather than introducing a new
/// operator signal.
/// </summary>
/// <remarks>
/// This mirrors the server-side observability seam's operator check on the client
/// side: the operator subject is validated against the same control-plane gate,
/// never trusted from a wire-supplied classification. The capability it reads is a
/// real, server-probed signal, so the view scoping it drives is defence-in-depth
/// over the cluster's own fail-closed enforcement, not dead configuration.
/// </remarks>
public interface IExplorerTenantOperatorGate
{
    /// <summary>
    /// Returns <see langword="true"/> when the current caller validates as a
    /// platform operator (authorized as <c>Admin</c> on the reserved auth policy
    /// tree), and <see langword="false"/> otherwise, including for an anonymous or
    /// unauthenticated caller.
    /// </summary>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns><see langword="true"/> when the caller is a validated platform operator.</returns>
    ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default);
}
