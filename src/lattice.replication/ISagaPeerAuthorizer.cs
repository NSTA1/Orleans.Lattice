namespace Orleans.Lattice.Replication;

/// <summary>
/// Peer-authorization seam for the imperative saga control RPCs. Unlike
/// the additive, self-authenticating replication data plane, the saga
/// <c>Prepare</c>/<c>Commit</c>/<c>Abort</c>/<c>GetStatus</c> calls
/// mutate participant state and must be gated by explicit peer
/// authorization before any state change. The
/// <c>orleans.lattice.replication.LatticeSaga</c> gRPC service consults
/// this authorizer with the caller's origin cluster id and rejects an
/// unauthorized peer before the request reaches
/// <see cref="ILatticeSagaControlHandler"/>.
/// <para>
/// The gRPC binding ships a default that authorizes exactly the
/// configured peer set (the clusters this silo is set up to replicate
/// with). Hosts can replace it to apply a stricter policy.
/// </para>
/// </summary>
public interface ISagaPeerAuthorizer
{
    /// <summary>
    /// Determines whether the caller identified by
    /// <paramref name="originClusterId"/> is an authorized saga peer.
    /// Returns <see langword="false"/> for an unknown, empty, or
    /// unauthorized origin so the caller is rejected before any
    /// participant state changes.
    /// </summary>
    /// <param name="originClusterId">
    /// The caller's origin cluster id, taken from the transport origin
    /// header (falling back to the request's coordinator cluster id).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="true"/> when the origin is an authorized peer;
    /// otherwise <see langword="false"/>.
    /// </returns>
    Task<bool> IsAuthorizedAsync(string? originClusterId, CancellationToken cancellationToken = default);
}
