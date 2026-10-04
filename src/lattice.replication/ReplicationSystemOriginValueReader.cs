using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads a key's current value and version for the replication receiver's
/// content-manifest exchange (#4585) under a system-origin access-gate scope.
/// </summary>
/// <remarks>
/// The exchange handler runs as trusted in-silo infrastructure, reachable only
/// by a peer that cleared the replication shared-secret interceptor, after the
/// wire-supplied tree id has been re-resolved against local enrollment. Like
/// <see cref="ReplicationSystemOriginDigestReader"/>, it opens a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope so the access
/// gate's documented infrastructure bypass applies instead of the anonymous
/// subject a deny-by-default tree refuses. The flag is established inside the
/// trust boundary and flows only on the in-silo grain call. Nothing read here is
/// returned to the peer: the handler uses the version only to decide whether an
/// entry the peer already holds may be elided.
/// </remarks>
internal static class ReplicationSystemOriginValueReader
{
    /// <summary>
    /// Reads <paramref name="key"/>'s value and version under a system-origin
    /// scope. An absent or tombstoned key reads as a <see langword="null"/>
    /// value at <see cref="HybridLogicalClock.Zero"/>.
    /// </summary>
    public static async Task<VersionedValue> ReadAsync(
        ILattice lattice,
        string key,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(key);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        return await lattice.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
    }
}
