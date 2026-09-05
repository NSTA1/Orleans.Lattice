namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Defense-in-depth internal-origin assertion for the two <em>internal-only</em>
/// grain interfaces <see cref="LatticeGrain"/> implements beside the public
/// <see cref="ILattice"/> facade: <see cref="ISystemLattice"/> (the reserved
/// system-tree bypass) and <see cref="IReplicationApplyGrain"/> (the replication
/// apply seam).
/// </summary>
/// <remarks>
/// <para>
/// Both interfaces deliberately skip the access gate - <see cref="ISystemLattice"/>
/// enters <see cref="LatticeSystemTreeBoundary"/> so it can address the reserved
/// <c>sys-</c> namespace the public facade refuses outright, and the apply seam
/// installs a remote mutation with the authoring cluster's clock preserved. That
/// is correct for their real callers, which are all silo-sourced, but it means a
/// call arriving from an <em>external</em> Orleans client would bypass every
/// policy check in the library. Being declared <see langword="internal"/> is not
/// by itself a boundary: Orleans resolves a grain interface by its stable
/// <see cref="AliasAttribute"/>, so a client that declares a structurally
/// matching interface carrying the same alias binds to the same grain.
/// </para>
/// <para>
/// The physical shard and leaf grains already carry this assertion (issue #1103),
/// as do the internal coordinator and saga grains. These two surfaces were the
/// remaining members of that family without it. The guard is gated on the
/// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel that the
/// authorization layer registers, so a cluster that never called
/// <c>AddLatticeAuth</c> is unaffected and pays only a single cached-service
/// lookup on these low-frequency internal paths.
/// </para>
/// </remarks>
internal sealed partial class LatticeGrain
{
    /// <summary>
    /// Asserts that the current turn on an internal-only interface
    /// (<see cref="ISystemLattice"/> / <see cref="IReplicationApplyGrain"/>)
    /// originated inside the cluster trust boundary, and refuses it otherwise.
    /// </summary>
    /// <param name="operation">The operation being attempted, for the thrown exception.</param>
    private void EnsureInternalOrigin(LatticeOperation operation) =>
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            context.ActivationServices, TreeId, operation);
}
