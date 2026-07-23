using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads a tree's projection digest for the cross-cluster anti-entropy chain
/// under a system-origin access-gate scope.
/// </summary>
/// <remarks>
/// <para>
/// A projection digest is read through the fail-closed data-plane access gate
/// (<see cref="ILattice.GetLeafProjectionDigestAsync"/> and
/// <see cref="ILattice.GetLeafProjectionDigestForRangeAsync"/> both enforce a
/// uniform range-read). The anti-entropy probe and the replication gRPC service
/// handlers read a digest as trusted in-silo infrastructure, not under a user
/// identity, so absent an ambient principal the read resolves to the anonymous
/// subject that a deny-by-default tree refuses - which would silently disable
/// digest detection and remediation on exactly the secured estates that need it.
/// </para>
/// <para>
/// This helper centralises the fix: it opens a
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope around the
/// read so the access gate's documented infrastructure bypass applies. The flag
/// is established inside the trust boundary and flows only on the in-silo grain
/// call (the silo-wide capability-stripping call filter strips it from any
/// genuine external client, so it can never be forged from the wire). The tree
/// id is resolved by the caller against local state; the wire never supplies a
/// bypass. The read remains the single enforcement seam - this only supplies the
/// correct origin classification for it.
/// </para>
/// </remarks>
internal static class ReplicationSystemOriginDigestReader
{
    /// <summary>
    /// Reads the whole-shard projection digest under a system-origin scope.
    /// </summary>
    public static async Task<LeafProjectionDigest> ReadShardDigestAsync(
        ILattice lattice,
        int shardIndex,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lattice);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        return await lattice
            .GetLeafProjectionDigestAsync(shardIndex, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Reads the projection digest for a half-open key range under a
    /// system-origin scope.
    /// </summary>
    public static async Task<LeafProjectionDigest> ReadRangeDigestAsync(
        ILattice lattice,
        int shardIndex,
        string? startKeyInclusive,
        string? endKeyExclusive,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lattice);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        return await lattice
            .GetLeafProjectionDigestForRangeAsync(
                shardIndex, startKeyInclusive, endKeyExclusive, cancellationToken)
            .ConfigureAwait(false);
    }
}
